
// Storage Uploader plugin (RDIO-style config, async worker)
// Uploads finished files to S3 and/or SFTP per-system, selected by shortName.
//
// Build inside trunk-recorder tree under user_plugins/.
// Requires: s3_client.hpp/.cc, sftp_client.hpp/.cc, config.hpp, util.hpp.

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

#include <boost/property_tree/ptree.hpp>

#include "config.hpp"
#include "util.hpp"
#include "s3_client.hpp"
#include "sftp_client.hpp"

// Trunk-Recorder headers (adjust include path if needed)
#include "trunk-recorder/plugin_manager/plugin_api.h"   // plugin_t, Call_Data_t, etc.

using boost::property_tree::ptree;
namespace fs = std::filesystem;

// ---------------- data passed to worker ----------------

struct UploadJob {
    std::string audio_path;
    std::string json_path;
    std::string shortName;
    std::time_t startTime{0};
};

// ---------------- per-system context ------------------

struct SystemCtx {
    PluginConfig cfg;                      // per-system config
    std::unique_ptr<S3Client>  s3;        // constructed from cfg.s3
    std::unique_ptr<SftpClient> sftp;     // constructed from cfg.sftp
    explicit SystemCtx(const PluginConfig& c)
        : cfg(c),
          s3(std::make_unique<S3Client>(cfg.s3)),
          sftp(std::make_unique<SftpClient>(cfg.sftp)) {}
};

// ---------------- worker with async queue --------------

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
                std::cerr << "[storage-uploader] no config for system '" << job.shortName
                          << "' — skipping upload" << std::endl;
                continue;
            }
            SystemCtx* sys = it->second.get();
            const auto& cfg = sys->cfg;

            // Build key/path templates
            std::string base      = basename_of(job.audio_path);
            std::string json_base = basename_of(job.json_path);
            std::string s3_key    = substitute_template(cfg.s3.prefix_template,   job.shortName, job.startTime, base);
            std::string sftp_rel  = substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, base);
            std::string s3_json_key, sftp_json_rel;
            if (cfg.upload_json && fs::exists(job.json_path)) {
                s3_json_key  = substitute_template(cfg.s3.prefix_template,   job.shortName, job.startTime, json_base);
                sftp_json_rel= substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, json_base);
            }

            // Uploads with retries (per-dest)
            auto try_with_retries = [&](auto fn, int max_retries, const char* label) {
                int attempt = 0;
                for (;;) {
                    std::string err;
                    if (fn(&err)) return true;
                    if (attempt >= max_retries) {
                        std::cerr << "[storage-uploader] " << label << " failed after "
                                  << (attempt+1) << " attempts: " << err << std::endl;
                        return false;
                    }
                    int backoff = (1 << attempt);
                    std::cerr << "[storage-uploader] " << label << " failed: " << err
                              << " — retrying in " << backoff << "s" << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(backoff));
                    attempt++;
                }
            };

            bool ok = true;

            if (cfg.s3.enabled) {
                ok &= try_with_retries(
                    [&](std::string* e){ return sys->s3->upload_file(job.audio_path, s3_key, {}, e); },
                    cfg.s3.max_retries,
                    "s3 audio");

                if (ok && cfg.upload_json && fs::exists(job.json_path)) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->s3->upload_file(job.json_path, s3_json_key, {}, e); },
                        cfg.s3.max_retries,
                        "s3 json");
                }
            }

            if (ok && cfg.sftp.enabled) {
                ok &= try_with_retries(
                    [&](std::string* e){ return sys->sftp->upload_file(job.audio_path, sftp_rel, e); },
                    cfg.sftp.max_retries,
                    "sftp audio");

                if (ok && cfg.upload_json && fs::exists(job.json_path)) {
                    ok &= try_with_retries(
                        [&](std::string* e){ return sys->sftp->upload_file(job.json_path, sftp_json_rel, e); },
                        cfg.sftp.max_retries,
                        "sftp json");
                }
            }

            if (ok && cfg.delete_after_upload) {
                std::error_code ec;
                fs::remove(job.audio_path, ec);
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

// ---------------- plugin globals / helpers --------------

static std::unordered_map<std::string, PluginConfig> g_cfg_by_system;
static StorageUploader* g_uploader = nullptr;

static bool file_exists(const std::string& p) {
    std::error_code ec;
    return !p.empty() && fs::exists(p, ec);
}

static std::string choose_audio_path_from_call(const Call_Data_t& call) {
    // Prefer converted when present; else use original filename
    if (file_exists(call.converted)) return call.converted;
    return call.filename;
}

// ---------------- required plugin entry points -----------

extern "C" {

plugin_t* storage_uploader_plugin_new() {
    plugin_t* plugin = new plugin_t();
    plugin->name = (char*)"Storage Uploader";

    // Parse RDIO-style per-system config
    plugin->parse_config = [](plugin_t* /*p*/, ptree::value_type &cfg)->int {
        try {
            g_cfg_by_system.clear();

            // Expect an array at "systems"
            auto systems_opt = cfg.second.get_child_optional("systems");
            if (!systems_opt) {
                std::cerr << "[storage-uploader] plugin config missing \"systems\" array" << std::endl;
                return -1;
            }

            for (auto& node : *systems_opt) {
                const ptree& el = node.second;

                std::string shortName = el.get<std::string>("shortName", "");
                if (shortName.empty()) {
                    std::cerr << "[storage-uploader] system entry missing shortName — skipping" << std::endl;
                    continue;
                }

                PluginConfig pc; // defaults from config.hpp

                pc.upload_json         = el.get<bool>("uploadJson", true);
                pc.delete_after_upload = el.get<bool>("deleteAfterUpload", false);
                pc.log_debug           = el.get<bool>("debug", false);

                // S3 block
                if (auto s3 = el.get_child_optional("s3")) {
                    pc.s3.enabled            = s3->get<bool>("enabled", false);
                    pc.s3.bucket             = s3->get<std::string>("bucket", "");
                    pc.s3.region             = s3->get<std::string>("region", "us-east-1");
                    pc.s3.endpoint           = s3->get<std::string>("endpoint", "");
                    pc.s3.prefix_template    = s3->get<std::string>("prefix_template", pc.s3.prefix_template);
                    pc.s3.access_key         = s3->get<std::string>("access_key", "");
                    pc.s3.secret_key         = s3->get<std::string>("secret_key", "");
                    if (auto v = s3->get_optional<std::string>("session_token")) pc.s3.session_token = *v;
                    if (auto v = s3->get_optional<std::string>("storage_class")) pc.s3.storage_class = *v;
                    if (auto v = s3->get_optional<std::string>("acl"))           pc.s3.acl = *v;
                    if (auto v = s3->get_optional<std::string>("sse"))           pc.s3.sse = *v;
                    if (auto v = s3->get_optional<std::string>("kms_key"))       pc.s3.kms_key = *v;
                    pc.s3.connect_timeout_ms  = s3->get<long>("connect_timeout_ms", 10000);
                    pc.s3.transfer_timeout_ms = s3->get<long>("transfer_timeout_ms", 0);
                    pc.s3.max_retries         = s3->get<int>("max_retries", 5);
                }

                // SFTP block
                if (auto sf = el.get_child_optional("sftp")) {
                    pc.sftp.enabled           = sf->get<bool>("enabled", false);
                    pc.sftp.host              = sf->get<std::string>("host", "");
                    pc.sftp.port              = sf->get<int>("port", 22);
                    pc.sftp.username          = sf->get<std::string>("username", "");
                    pc.sftp.password          = sf->get<std::string>("password", "");
                    pc.sftp.key_path          = sf->get<std::string>("key_path", "");
                    pc.sftp.known_hosts       = sf->get<std::string>("known_hosts", "");
                    pc.sftp.remote_root       = sf->get<std::string>("remote_root", "/uploads");
                    pc.sftp.prefix_template   = sf->get<std::string>("prefix_template", pc.sftp.prefix_template);
                    pc.sftp.connect_timeout_ms  = sf->get<long>("connect_timeout_ms", 10000);
                    pc.sftp.transfer_timeout_ms = sf->get<long>("transfer_timeout_ms", 0);
                    pc.sftp.max_retries         = sf->get<int>("max_retries", 5);
                }

                g_cfg_by_system.emplace(shortName, std::move(pc));
            }

            if (g_cfg_by_system.empty()) {
                std::cerr << "[storage-uploader] no valid systems configured" << std::endl;
                return -1;
            }
        } catch (const std::exception& e) {
            std::cerr << "[storage-uploader] parse_config error: " << e.what() << std::endl;
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

    plugin->call_end = [](plugin_t* /*p*/, Call_Data_t call)->int {
        try {
            UploadJob job;
            job.audio_path = choose_audio_path_from_call(call); // prefer converted if present
            job.json_path  = call.statusfilename;
            job.shortName  = call.shortName;
            job.startTime  = (std::time_t)call.startTime;
            if (g_uploader) g_uploader->enqueue(job);
        } catch (...) {
            return -1;
        }
        return 0;
    };

    return plugin;
}

} // extern "C"
