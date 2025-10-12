// Storage Uploader plugin (RDIO-style config, async worker)
// Uploads finished files to S3 and/or SFTP per-system, selected by shortName.
// - Per-system "audio" control: "auto" (default), "m4a", "wav", "all"
// - "auto": prefer .m4a if present, else .wav
//
// Build inside trunk-recorder tree under user_plugins/.
//
// Requires these project headers in this repo:
//   include/config.hpp
//   include/util.hpp
//   include/s3_client.hpp (+ src/s3_client.cc)
//   include/sftp_client.hpp (+ src/sftp_client.cc)
//
// Relies on TR headers from the main tree:
//   trunk-recorder/plugin_manager/plugin_api.h

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <memory>
#include <vector>
#include <iostream>
#include <algorithm> // std::transform (lowercasing)

#include <boost/property_tree/ptree.hpp>

#include "config.hpp"
#include "util.hpp"
#include "s3_client.hpp"
#include "sftp_client.hpp"

// TR plugin API (function-pointer style)
#include <trunk-recorder/plugin_manager/plugin_api.h>

using boost::property_tree::ptree;
namespace fs = std::filesystem;

// ---------------- Small helpers ----------------

static inline bool file_exists(const std::string& p) {
    std::error_code ec;
    return !p.empty() && fs::exists(p, ec);
}

static inline std::string to_lower_copy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

// ---------------- Data passed to worker ----------------

/**
 * UploadJob:
 *  - wav_path:    original WAV (call_info.filename)
 *  - m4a_path:    optional M4A (call_info.converted when compress_wav == true)
 *  - json_path:   call JSON (call_info.status_filename) if present/enabled
 *  - shortName:   system key for looking up per-system config
 *  - startTime:   used in path templating (yyyy, MM, dd, etc.)
 */
struct UploadJob {
    std::string wav_path;
    std::string m4a_path;
    std::string json_path;
    std::string shortName;
    std::time_t startTime{0};
};

// ---------------- Per-system client context ----------------

struct SystemCtx {
    PluginConfig cfg;                      // per-system config
    std::unique_ptr<S3Client>  s3;        // constructed from cfg.s3
    std::unique_ptr<SftpClient> sftp;     // constructed from cfg.sftp
    explicit SystemCtx(const PluginConfig& c)
        : cfg(c),
          s3(std::make_unique<S3Client>(cfg.s3)),
          sftp(std::make_unique<SftpClient>(cfg.sftp)) {}
};

// ---------------- Worker with async queue ----------------

class StorageUploader {
public:
    explicit StorageUploader(const std::unordered_map<std::string, PluginConfig>& by_system) {
        for (const auto& kv : by_system) {
            systems_.emplace(kv.first, std::make_unique<SystemCtx>(kv.second));
        }
    }

    void start() {
        stop_flag_ = false;
        worker_ = std::thread([this]{ this->run(); });
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_flag_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    void enqueue(const UploadJob& job) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push(job);
        }
        cv_.notify_one();
    }

private:
    void run() {
        while (true) {
            UploadJob job;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [&]{ return stop_flag_ || !queue_.empty(); });
                if (stop_flag_ && queue_.empty()) break;
                job = queue_.front();
                queue_.pop();
            }

            // Lookup system by shortName
            auto it = systems_.find(job.shortName);
            if (it == systems_.end()) {
                std::cerr << "[storage-uploader] no config for system '"
                          << job.shortName << "' — skipping upload\n";
                continue;
            }
            SystemCtx* sys = it->second.get();
            const auto& cfg = sys->cfg;

            // Decide which audio files to upload based on cfg.audio
            // - "all": upload m4a then wav (if present)
            // - "m4a" : m4a else fall back to wav
            // - "wav" : wav else fall back to m4a
            // - "auto": prefer m4a else wav
            std::string mode = to_lower_copy(cfg.audio);
            const bool has_wav = file_exists(job.wav_path);
            const bool has_m4a = file_exists(job.m4a_path);

            std::vector<std::string> audio_files; // absolute paths to upload
            if (mode == "all") {
                if (has_m4a) audio_files.push_back(job.m4a_path);
                if (has_wav) audio_files.push_back(job.wav_path);
            } else if (mode == "m4a") {
                if      (has_m4a) audio_files.push_back(job.m4a_path);
                else if (has_wav) {
                    audio_files.push_back(job.wav_path);
                    std::cerr << "[storage-uploader] m4a requested but not found; falling back to wav\n";
                }
            } else if (mode == "wav") {
                if      (has_wav) audio_files.push_back(job.wav_path);
                else if (has_m4a) {
                    audio_files.push_back(job.m4a_path);
                    std::cerr << "[storage-uploader] wav requested but not found; falling back to m4a\n";
                }
            } else { // "auto" (default)
                if      (has_m4a) audio_files.push_back(job.m4a_path);
                else if (has_wav) audio_files.push_back(job.wav_path);
            }

            if (audio_files.empty()) {
                std::cerr << "[storage-uploader] no audio file present for system '"
                          << job.shortName << "' — skipping\n";
                continue;
            }

            // Prepare JSON keys/paths up-front
            std::string s3_json_key, sftp_json_rel;
            if (cfg.upload_json && file_exists(job.json_path)) {
                const std::string json_base =
                    basename_of(job.json_path);
                s3_json_key   = substitute_template(cfg.s3.prefix_template,
                                                    job.shortName, job.startTime, json_base);
                sftp_json_rel = substitute_template(cfg.sftp.prefix_template,
                                                    job.shortName, job.startTime, json_base);
            }

            // Retry wrapper: label is std::string to avoid dangling c_str()
            auto try_with_retries = [&](auto fn, int max_retries, const std::string& label) {
                int attempt = 0;
                for (;;) {
                    std::string err;
                    if (fn(&err)) return true;
                    if (attempt >= max_retries) {
                        std::cerr << "[storage-uploader] " << label
                                  << " failed after " << (attempt+1)
                                  << " attempts: " << err << "\n";
                        return false;
                    }
                    int backoff = (1 << attempt);
                    std::cerr << "[storage-uploader] " << label << " failed: " << err
                              << " — retrying in " << backoff << "s\n";
                    std::this_thread::sleep_for(std::chrono::seconds(backoff));
                    attempt++;
                }
            };

            bool ok = true;

            // Upload each chosen audio file
            for (const auto& audio_path : audio_files) {
                const std::string base =
                    basename_of(audio_path);

                const std::string s3_key   =
                    substitute_template(cfg.s3.prefix_template,   job.shortName, job.startTime, base);
                const std::string sftp_rel =
                    substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, base);

                if (cfg.s3.enabled) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->s3->upload_file(audio_path, s3_key, {}, e); },
                        cfg.s3.max_retries,
                        "s3 audio: " + base);
                }
                if (!ok) break;

                if (cfg.sftp.enabled) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->sftp->upload_file(audio_path, sftp_rel, e); },
                        cfg.sftp.max_retries,
                        "sftp audio: " + base);
                }
                if (!ok) break;
            }

            // Upload JSON once (if requested)
            if (ok && cfg.upload_json && file_exists(job.json_path)) {
                if (cfg.s3.enabled) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->s3->upload_file(job.json_path, s3_json_key, {}, e); },
                        cfg.s3.max_retries,
                        "s3 json");
                }
                if (ok && cfg.sftp.enabled) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->sftp->upload_file(job.json_path, sftp_json_rel, e); },
                        cfg.sftp.max_retries,
                        "sftp json");
                }
            }

            // Optional cleanup
            if (ok && cfg.delete_after_upload) {
                std::error_code ec;
                for (const auto& p : audio_files) fs::remove(p, ec);
                if (cfg.upload_json) fs::remove(job.json_path, ec);
            }
        }
    }

    // per-system contexts
    std::unordered_map<std::string, std::unique_ptr<SystemCtx>> systems_;

    // queue infra
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<UploadJob> queue_;
    std::thread worker_;
    bool stop_flag_{false};
};

// ---------------- Plugin globals ----------------

static std::unordered_map<std::string, PluginConfig> g_cfg_by_system;
static StorageUploader* g_uploader = nullptr;

// ---------------- Required plugin entry points ----------------

extern "C" {

plugin_t* storage_uploader_plugin_new() {
    plugin_t* plugin = new plugin_t();
    plugin->name = (char*)"Storage Uploader";

    // Parse RDIO-style per-system config array at plugins[].systems[]
    plugin->parse_config = [](plugin_t* /*p*/, ptree::value_type &cfg)->int {
        try {
            g_cfg_by_system.clear();

            auto systems_opt = cfg.second.get_child_optional("systems");
            if (!systems_opt) {
                std::cerr << "[storage-uploader] plugin config missing \"systems\" array\n";
                return -1;
            }

            for (auto& node : *systems_opt) {
                const ptree& el = node.second;

                std::string shortName = el.get<std::string>("shortName", "");
                if (shortName.empty()) {
                    std::cerr << "[storage-uploader] system entry missing shortName — skipping\n";
                    continue;
                }

                PluginConfig pc; // defaults from config.hpp

                // General flags
                pc.upload_json         = el.get<bool>("uploadJson", true);
                pc.delete_after_upload = el.get<bool>("deleteAfterUpload", false);
                pc.log_debug           = el.get<bool>("debug", false);

                // Audio mode (auto/m4a/wav/all); store lowercase
                pc.audio               = to_lower_copy(el.get<std::string>("audio", "auto"));

                // S3 block
                if (auto s3 = el.get_child_optional("s3")) {
                    pc.s3.enabled              = s3->get<bool>("enabled", false);
                    pc.s3.bucket               = s3->get<std::string>("bucket", "");
                    pc.s3.region               = s3->get<std::string>("region", "us-east-1");
                    pc.s3.endpoint             = s3->get<std::string>("endpoint", "");
                    pc.s3.prefix_template      = s3->get<std::string>("prefix_template", pc.s3.prefix_template);
                    pc.s3.access_key           = s3->get<std::string>("access_key", "");
                    pc.s3.secret_key           = s3->get<std::string>("secret_key", "");
                    if (auto v = s3->get_optional<std::string>("session_token")) pc.s3.session_token = *v;
                    if (auto v = s3->get_optional<std::string>("storage_class")) pc.s3.storage_class = *v;
                    if (auto v = s3->get_optional<std::string>("acl"))           pc.s3.acl = *v;
                    if (auto v = s3->get_optional<std::string>("sse"))           pc.s3.sse = *v;
                    if (auto v = s3->get_optional<std::string>("kms_key"))       pc.s3.kms_key = *v;
                    pc.s3.connect_timeout_ms    = s3->get<long>("connect_timeout_ms", 10000);
                    pc.s3.transfer_timeout_ms   = s3->get<long>("transfer_timeout_ms", 0);
                    pc.s3.max_retries           = s3->get<int>("max_retries", 5);
                }

                // SFTP block
                if (auto sf = el.get_child_optional("sftp")) {
                    pc.sftp.enabled             = sf->get<bool>("enabled", false);
                    pc.sftp.host                = sf->get<std::string>("host", "");
                    pc.sftp.port                = sf->get<int>("port", 22);
                    pc.sftp.username            = sf->get<std::string>("username", "");
                    pc.sftp.password            = sf->get<std::string>("password", "");
                    pc.sftp.key_path            = sf->get<std::string>("key_path", "");
                    pc.sftp.known_hosts         = sf->get<std::string>("known_hosts", "");
                    pc.sftp.remote_root         = sf->get<std::string>("remote_root", "/uploads");
                    pc.sftp.prefix_template     = sf->get<std::string>("prefix_template", pc.sftp.prefix_template);
                    pc.sftp.connect_timeout_ms  = sf->get<long>("connect_timeout_ms", 10000);
                    pc.sftp.transfer_timeout_ms = sf->get<long>("transfer_timeout_ms", 0);
                    pc.sftp.max_retries         = sf->get<int>("max_retries", 5);
                }

                g_cfg_by_system.emplace(shortName, std::move(pc));
            }

            if (g_cfg_by_system.empty()) {
                std::cerr << "[storage-uploader] no valid systems configured\n";
                return -1;
            }
        } catch (const std::exception& e) {
            std::cerr << "[storage-uploader] parse_config error: " << e.what() << "\n";
            return -1;
        }
        return 0;
    };

    plugin->init = [](plugin_t* /*p*/)->int {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        g_uploader = new StorageUploader(g_cfg_by_system);
        g_uploader->start();
        return 0;
    };

    plugin->start = [](plugin_t* /*p*/)->int { return 0; };

    plugin->stop = [](plugin_t* /*p*/)->int {
        if (g_uploader) {
            g_uploader->stop();
            delete g_uploader;
            g_uploader = nullptr;
        }
        curl_global_cleanup();
        return 0;
    };

    // When a call ends, hand off all possible audio paths + JSON to the worker
    plugin->call_end = [](plugin_t* /*p*/, Call_Data_t call_info)->int {
        try {
            UploadJob job;
            job.wav_path   = call_info.filename;         // WAV (original)
            job.m4a_path   = call_info.converted;        // M4A (if present)
            job.json_path  = call_info.status_filename;  // JSON metadata
            job.shortName  = call_info.short_name;       // match config.systems[].shortName
            job.startTime  = static_cast<std::time_t>(call_info.start_time);
            if (g_uploader) g_uploader->enqueue(job);
        } catch (...) {
            // Never let exceptions bubble through TR
            return -1;
        }
        return 0;
    };

    // We don't implement other callbacks
    return plugin;
}

} // extern "C"
