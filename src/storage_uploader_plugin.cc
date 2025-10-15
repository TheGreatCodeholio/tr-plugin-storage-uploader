// Storage Uploader plugin
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
#include <algorithm>
#include <fstream>
#include <boost/log/trivial.hpp>

#include <boost/dll/alias.hpp>
#include <json.hpp>

#include "config.hpp"
#include "util.hpp"
#include "s3_client.hpp"
#include "sftp_client.hpp"

// TR plugin API
#include <trunk-recorder/plugin_manager/plugin_api.h>

#include <curl/curl.h>

using json = nlohmann::json;
namespace fs = std::filesystem;

static constexpr const char* SU_TAG = "\t[Storage Uploader]\t";
#define SU_LOG(sev) BOOST_LOG_TRIVIAL(sev) << SU_TAG

// ---------------- Small helpers ----------------

static bool file_exists(const std::string& p) {
    std::error_code ec;
    return !p.empty() && fs::exists(p, ec);
}

static std::string to_lower_copy(std::string s) {
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

// ---------------- Remote links helper ---------------------
static void add_remote_links_to_json(
    const std::string& json_path,
    const std::vector<nlohmann::json>& s3_links,
    const std::vector<nlohmann::json>& sftp_links)
{
    if (json_path.empty()) return;
    std::error_code fec;
    if (!std::filesystem::exists(json_path, fec)) return;

    try {
        std::ifstream in(json_path);
        nlohmann::json j = nlohmann::json::parse(in, /*cb*/nullptr, /*allow_exceptions*/true, /*ignore_comments*/true);
        in.close();

        // namespaced block to avoid collisions with TR fields
        nlohmann::json& storage = j["storage_uploader"];
        if (!storage.is_object()) storage = nlohmann::json::object();

        if (!s3_links.empty())   storage["s3"]   = s3_links;
        if (!sftp_links.empty()) storage["sftp"] = sftp_links;

        std::ofstream out(json_path, std::ios::trunc);
        out << j.dump(2);
    } catch (const std::exception& e) {
        SU_LOG(warning) << "[JSON] failed to add remote links: " << e.what();
    }
}

// ---------------- Worker with async queue ----------------

class StorageUploaderWorker {
public:
    explicit StorageUploaderWorker(const std::unordered_map<std::string, PluginConfig>& by_system) {
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
                SU_LOG(warning) << "[" << job.shortName << "] no per-system config — skipping upload";
                continue;
            }
            SystemCtx* sys = it->second.get();
            const auto& cfg = sys->cfg;

            if (!cfg.s3.enabled && !cfg.sftp.enabled) {
                SU_LOG(warning) << "[" << job.shortName << "] both S3 and SFTP are disabled — nothing to upload";
                continue;
            }

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
                    SU_LOG(warning) << "[" << job.shortName << "] m4a requested but not found; falling back to wav";
                }
            } else if (mode == "wav") {
                if      (has_wav) audio_files.push_back(job.wav_path);
                else if (has_m4a) {
                    audio_files.push_back(job.m4a_path);
                    SU_LOG(warning) << "[" << job.shortName << "] wav requested but not found; falling back to m4a";
                }
            } else { // "auto" (default)
                if      (has_m4a) audio_files.push_back(job.m4a_path);
                else if (has_wav) audio_files.push_back(job.wav_path);
            }

            if (audio_files.empty()) {
                SU_LOG(warning) << "[" << job.shortName << "] no audio file present — skipping";
                continue;
            }

            // Prepare JSON keys/paths up-front
            std::string s3_json_key, sftp_json_rel;
            const bool has_json = cfg.upload_json && file_exists(job.json_path);

            if (has_json) {
                const std::string json_base = basename_of(job.json_path);
                s3_json_key   = substitute_template(cfg.s3.prefix_template,
                                                    job.shortName, job.startTime, json_base);
                sftp_json_rel = substitute_template(cfg.sftp.prefix_template,
                                                    job.shortName, job.startTime, json_base);
            }

            // Retry wrapper
            auto try_with_retries = [&](auto fn, int max_retries, const std::string& label) {
                int attempt = 0;
                for (;;) {
                    std::string err;
                    if (fn(&err)) return true;
                    if (attempt >= max_retries) {
                        SU_LOG(error) << "[" << job.shortName << "] " << label
                                      << " failed after " << (attempt + 1) << " attempts: " << err;
                        return false;
                    }
                    int backoff = (1 << attempt);
                    SU_LOG(warning) << "[" << job.shortName << "] " << label
                                    << " failed: " << err << " — retrying in " << backoff << "s";
                    std::this_thread::sleep_for(std::chrono::seconds(backoff));
                    attempt++;
                }
            };

            bool all_ok = true; // tracks whether *both* destinations (if enabled) succeeded for all files

            std::vector<nlohmann::json> s3_links;
            std::vector<nlohmann::json> sftp_links;
            nlohmann::json json_links_s3 = nlohmann::json::object();
            nlohmann::json json_links_sftp = nlohmann::json::object();

            auto sftp_web_url = [&](const std::string& rel) -> std::string {
                if (cfg.sftp.public_base_url && !cfg.sftp.public_base_url->empty()) {
                    std::string base = *cfg.sftp.public_base_url;
                    if (!base.empty() && base.back() == '/') base.pop_back();
                    std::string path = rel;
                    while (!path.empty() && (path.front() == '/' || path.front() == '\\')) path.erase(path.begin());
                    return base + "/" + path;
                }
                return {};
            };

            for (const auto& audio_path : audio_files) {
                const std::string base = basename_of(audio_path);

                const std::string s3_key   =
                    substitute_template(cfg.s3.prefix_template,   job.shortName, job.startTime, base);
                const std::string sftp_rel =
                    substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, base);

                bool s3_ok   = true;
                bool sftp_ok = true;

                if (cfg.s3.enabled) {
                    s3_ok = try_with_retries(
                        [&](std::string* e){ return sys->s3->upload_file(audio_path, s3_key, {}, e); },
                        cfg.s3.max_retries,
                        "s3 audio: " + base);

                    if (s3_ok) {
                        nlohmann::json rec = {
                            {"bucket", cfg.s3.bucket},
                            {"key",    s3_key},
                            {"region", cfg.s3.region},
                            {"s3_url", sys->s3->url_for_key(s3_key)}   // ← NEW
                        };
                        s3_links.push_back(std::move(rec));
                        if (cfg.log_debug) SU_LOG(info) << "[" << job.shortName << "] uploaded (S3): " << base;
                    }
                }

                if (cfg.sftp.enabled) {
                    sftp_ok = try_with_retries(
                        [&](std::string* e){ return sys->sftp->upload_file(audio_path, sftp_rel, e); },
                        cfg.sftp.max_retries,
                        "sftp audio: " + base);

                    if (sftp_ok) {
                        nlohmann::json rec = {
                            {"host", cfg.sftp.host},
                            {"path", sftp_rel}
                        };
                        // optional public URL if configured
                        if (auto wu = sftp_web_url(sftp_rel); !wu.empty()) {
                            rec["web_url"] = wu;          // ← NEW
                        }
                        sftp_links.push_back(std::move(rec));
                        if (cfg.log_debug) SU_LOG(info) << "[" << job.shortName << "] uploaded (SFTP): " << base;
                    }
                }

                // This file is considered successful only if every enabled destination succeeded.
                const bool file_ok =
                    (!cfg.s3.enabled   || s3_ok) &&
                    (!cfg.sftp.enabled || sftp_ok);

                all_ok &= file_ok;
            }

            // Upload JSON once
            if (has_json) {
                const std::string json_base = basename_of(job.json_path);
                const std::string s3_json_key_planned =
                    substitute_template(cfg.s3.prefix_template, job.shortName, job.startTime, json_base);
                const std::string sftp_json_rel_planned =
                    substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, json_base);

                // Build the public URLs for the JSON itself (to stamp later)
                std::string json_s3_url, json_web_url;
                if (cfg.s3.enabled)   json_s3_url  = sys->s3->url_for_key(s3_json_key_planned);
                if (cfg.sftp.enabled) json_web_url = sftp_web_url(sftp_json_rel_planned);

                bool s3_json_ok   = true;
                bool sftp_json_ok = true;

                // Actually upload the JSON now
                if (cfg.s3.enabled) {
                    SU_LOG(info) << "[" << job.shortName << "] S3 PUT json: key=\"" << s3_json_key_planned
                                 << "\" url=" << json_s3_url;
                    s3_json_ok = try_with_retries(
                        [&](std::string* e){ return sys->s3->upload_file(job.json_path, s3_json_key_planned, {}, e); },
                        cfg.s3.max_retries, "s3 json");
                }

                if (cfg.sftp.enabled) {
                    SU_LOG(info) << "[" << job.shortName << "] SFTP PUT json: path=\"" << sftp_json_rel_planned
                                 << "\" host=" << cfg.sftp.host;
                    sftp_json_ok = try_with_retries(
                        [&](std::string* e){ return sys->sftp->upload_file(job.json_path, sftp_json_rel_planned, e); },
                        cfg.sftp.max_retries, "sftp json");
                }

                // Final JSON stamp AFTER uploads
                try {
                    if (file_exists(job.json_path)) {
                        std::ifstream in(job.json_path);
                        nlohmann::json j = nlohmann::json::parse(in);
                        in.close();

                        nlohmann::json& storage = j["storage_uploader"];
                        if (!storage.is_object()) storage = nlohmann::json::object();

                        // Make a "files" array with s3_url / web_url for convenience
                        std::vector<nlohmann::json> files_urls;
                        for (const auto& ap : audio_files) {
                            const std::string basef = basename_of(ap);
                            nlohmann::json f = { {"basename", basef} };

                            if (cfg.s3.enabled) {
                                const std::string k = substitute_template(cfg.s3.prefix_template, job.shortName, job.startTime, basef);
                                f["s3_url"] = sys->s3->url_for_key(k);
                            }
                            if (cfg.sftp.enabled) {
                                const std::string r = substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, basef);
                                if (auto wu = sftp_web_url(r); !wu.empty()) f["web_url"] = wu;
                            }
                            files_urls.push_back(std::move(f));
                        }

                        // Write new-style keys
                        if (!files_urls.empty()) storage["files"] = files_urls;
                        if (!json_s3_url.empty())  storage["json_s3_url"]  = json_s3_url;
                        if (!json_web_url.empty()) storage["json_web_url"] = json_web_url;

                        // status reflects audio + JSON destinations
                        const bool json_ok = (!cfg.s3.enabled || s3_json_ok) && (!cfg.sftp.enabled || sftp_json_ok);
                        const bool upload_ok = all_ok && json_ok;
                        storage["status"] = upload_ok ? "uploaded" : "partial";

                        std::ofstream out(job.json_path, std::ios::trunc);
                        out << j.dump(2);
                    } else {
                        SU_LOG(warning) << "[JSON] status file vanished before final stamp: " << job.json_path;
                    }
                } catch (const std::exception& e) {
                    SU_LOG(warning) << "[JSON] failed to finalize: " << e.what();
                }

                if (json_links_s3.is_object() || json_links_sftp.is_object()) {
                    // (Optional) keep legacy blocks if you still want them elsewhere
                }

                all_ok &= (!cfg.s3.enabled || s3_json_ok) && (!cfg.sftp.enabled || sftp_json_ok);
            }



            // Optional cleanup
            if (all_ok && cfg.delete_after_upload) {
                std::error_code ec;
                for (const auto& p : audio_files) fs::remove(p, ec);
                if (has_json) fs::remove(job.json_path, ec);
                if (cfg.log_debug) {
                    SU_LOG(info) << "[" << job.shortName << "] deleted local files after upload";
                }
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

// =======================
//  Plugin class
// =======================

class Storage_Uploader_Plugin : public Plugin_Api {
public:
    Storage_Uploader_Plugin() = default;

    // parse_config(json)
    int parse_config(json config_data) override {
        try {
            SU_LOG(info) << "Parsing plugin config...";
            cfg_by_system_.clear();

            if (!config_data.contains("systems") || !config_data["systems"].is_array()) {
                SU_LOG(error) << "plugin config missing \"systems\" array";
                return -1;
            }

            for (const auto& el : config_data["systems"]) {
                if (!el.contains("shortName")) {
                    SU_LOG(warning) << "system entry missing shortName — skipping";
                    continue;
                }
                std::string shortName = el.value("shortName", "");
                if (shortName.empty()) continue;

                PluginConfig pc; // defaults from config.hpp

                // General
                pc.upload_json         = el.value("uploadJson", true);
                pc.delete_after_upload = el.value("deleteAfterUpload", false);
                pc.log_debug           = el.value("debug", false);
                pc.audio               = to_lower_copy(el.value("audio", "auto"));

                // S3 block
                if (el.contains("s3")) {
                    const auto& s3 = el["s3"];
                    pc.s3.enabled            = s3.value("enabled", false);
                    pc.s3.bucket             = s3.value("bucket", "");
                    pc.s3.region             = s3.value("region", "us-east-1");
                    pc.s3.endpoint           = s3.value("endpoint", "");
                    pc.s3.prefix_template    = s3.value("prefix_template", pc.s3.prefix_template);
                    pc.s3.access_key         = s3.value("access_key", "");
                    pc.s3.secret_key         = s3.value("secret_key", "");
                    if (s3.contains("session_token")) pc.s3.session_token = s3.value("session_token", "");
                    if (s3.contains("storage_class")) pc.s3.storage_class = s3.value("storage_class", "");
                    if (s3.contains("acl"))           pc.s3.acl           = s3.value("acl", "");
                    if (s3.contains("sse"))           pc.s3.sse           = s3.value("sse", "");
                    if (s3.contains("kms_key"))       pc.s3.kms_key       = s3.value("kms_key", "");
                    pc.s3.connect_timeout_ms  = s3.value("connect_timeout_ms", 10000L);
                    pc.s3.transfer_timeout_ms = s3.value("transfer_timeout_ms", 0L);
                    pc.s3.max_retries         = s3.value("max_retries", 5);
                }

                // SFTP block
                if (el.contains("sftp")) {
                    const auto& sf = el["sftp"];
                    pc.sftp.enabled           = sf.value("enabled", false);
                    pc.sftp.host              = sf.value("host", "");
                    pc.sftp.port              = sf.value("port", 22);
                    pc.sftp.username          = sf.value("username", "");
                    pc.sftp.password          = sf.value("password", "");
                    pc.sftp.key_path          = sf.value("key_path", "");
                    pc.sftp.known_hosts       = sf.value("known_hosts", "");
                    pc.sftp.remote_root       = sf.value("remote_root", "/uploads");
                    pc.sftp.prefix_template   = sf.value("prefix_template", pc.sftp.prefix_template);
                    pc.sftp.connect_timeout_ms  = sf.value("connect_timeout_ms", 10000L);
                    pc.sftp.transfer_timeout_ms = sf.value("transfer_timeout_ms", 0L);
                    pc.sftp.max_retries         = sf.value("max_retries", 5);

                    pc.sftp.accept_unknown_host =
                        sf.value("accept_unknown_host",
                        sf.value("auto_accept_unknown_host",
                        sf.value("insecure_accept_unknown_host", false)));

                    if (sf.contains("public_base_url")) {
                        const std::string p = sf.value("public_base_url", "");
                        if (!p.empty()) pc.sftp.public_base_url = p;
                    }
                }

                const bool s3_on   = pc.s3.enabled;
                const bool sftp_on = pc.sftp.enabled;

                SU_LOG(info)
                    << "[" << shortName << "] "
                    << "audio=" << pc.audio
                    << " json=" << (pc.upload_json ? "on" : "off")
                    << " deleteAfterUpload=" << (pc.delete_after_upload ? "on" : "off");

                if (s3_on) {
                    SU_LOG(info)
                        << "[" << shortName << "] S3: "
                        << "bucket=" << pc.s3.bucket
                        << " region=" << pc.s3.region
                        << " endpoint=" << (pc.s3.endpoint.empty() ? "(default)" : pc.s3.endpoint)
                        << " prefix=\"" << pc.s3.prefix_template << "\"";
                }
                if (sftp_on) {
                    SU_LOG(info)
                        << "[" << shortName << "] SFTP: "
                        << pc.sftp.host << ":" << pc.sftp.port
                        << " root=" << pc.sftp.remote_root
                        << " prefix=\"" << pc.sftp.prefix_template << "\""
                        << " accept_unknown_host=" << (pc.sftp.accept_unknown_host ? "on" : "off");
                }

                cfg_by_system_.emplace(shortName, std::move(pc));
            }

            if (cfg_by_system_.empty()) {
                SU_LOG(error) << "no valid systems configured";
                return -1;
            }
        } catch (const std::exception& e) {
            SU_LOG(error) << "parse_config error: " << e.what();
            return -1;
        }
        SU_LOG(info) << "Loaded " << cfg_by_system_.size() << " system config"
             << (cfg_by_system_.size() == 1 ? "" : "s") << ".";
        return 0;
    }

    // init(): start the worker
    int init(Config*,
             std::vector<Source*>,
             std::vector<System*>) override {
        SU_LOG(info) << "Initializing (curl + worker)...";
        curl_global_init(CURL_GLOBAL_DEFAULT);
        worker_ = std::make_unique<StorageUploaderWorker>(cfg_by_system_);
        worker_->start();
        return 0;
    }

    int start() override {
        SU_LOG(info) << "Worker started.";
        return 0;
    }

    int stop() override {
        SU_LOG(info) << "Stopping worker...";
        if (worker_) {
            worker_->stop();
            worker_.reset();
        }
        curl_global_cleanup();
        SU_LOG(info) << "Worker Stopped.";
        return 0;
    }

    // call_end(): enqueue job with both potential audio paths + JSON
    int call_end(Call_Data_t call_info) override {
        try {
            UploadJob job;
            job.wav_path   = call_info.filename;        // WAV (original)
            job.m4a_path   = call_info.converted;       // M4A (if present)
            job.json_path  = call_info.status_filename; // JSON already written by Call_Concluder
            job.shortName  = call_info.short_name;
            job.startTime  = static_cast<std::time_t>(call_info.start_time);

            // --- 1) find per-system cfg
            auto it = cfg_by_system_.find(job.shortName);
            if (it == cfg_by_system_.end()) {
                SU_LOG(warning) << "[" << job.shortName << "] no per-system config — enqueueing without JSON stamp";
                if (worker_) worker_->enqueue(job);
                return 0;
            }
            const PluginConfig& cfg = it->second;

            // --- 2) choose audio files (same rules as worker)
            std::vector<std::string> audio_files;
            const bool has_wav = file_exists(job.wav_path);
            const bool has_m4a = file_exists(job.m4a_path);
            const std::string mode = to_lower_copy(cfg.audio);

            if (mode == "all") {
                if (has_m4a) audio_files.push_back(job.m4a_path);
                if (has_wav) audio_files.push_back(job.wav_path);
            } else if (mode == "m4a") {
                if      (has_m4a) audio_files.push_back(job.m4a_path);
                else if (has_wav) audio_files.push_back(job.wav_path);
            } else if (mode == "wav") {
                if      (has_wav) audio_files.push_back(job.wav_path);
                else if (has_m4a) audio_files.push_back(job.m4a_path);
            } else { // auto
                if      (has_m4a) audio_files.push_back(job.m4a_path);
                else if (has_wav) audio_files.push_back(job.wav_path);
            }

            // --- 3) compute planned keys/paths (and urls)
            std::vector<nlohmann::json> files;
            std::optional<S3Client> s3c;
            if (cfg.s3.enabled) s3c.emplace(cfg.s3);

            // helper to build web URL from SFTP config
            auto make_web_url = [&](const std::string& rel) -> std::string {
                if (cfg.sftp.public_base_url && !cfg.sftp.public_base_url->empty()) {
                    std::string base = *cfg.sftp.public_base_url;
                    if (!base.empty() && base.back() == '/') base.pop_back();
                    std::string path = rel;
                    while (!path.empty() && (path.front() == '/' || path.front() == '\\')) path.erase(path.begin());
                    return base + "/" + path;
                }
                return {};
            };

            for (const auto& p : audio_files) {
                const std::string base = basename_of(p);
                nlohmann::json f = { {"basename", base} };

                if (cfg.s3.enabled) {
                    const std::string key = substitute_template(cfg.s3.prefix_template, job.shortName, job.startTime, base);
                    if (s3c) f["s3_url"] = s3c->url_for_key(key);
                }
                if (cfg.sftp.enabled) {
                    const std::string rel = substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, base);
                    const std::string wu  = make_web_url(rel);
                    if (!wu.empty()) f["web_url"] = wu;
                }
                files.push_back(std::move(f));
            }

            // planned location of the JSON itself
            std::string json_s3_url, json_web_url;
            if (cfg.upload_json && file_exists(job.json_path)) {
                const std::string jbase = basename_of(job.json_path);
                if (cfg.s3.enabled) {
                    const std::string jkey = substitute_template(cfg.s3.prefix_template, job.shortName, job.startTime, jbase);
                    json_s3_url = s3c ? s3c->url_for_key(jkey) : "";
                }
                if (cfg.sftp.enabled) {
                    const std::string jrel = substitute_template(cfg.sftp.prefix_template, job.shortName, job.startTime, jbase);
                    json_web_url = make_web_url(jrel);
                }
            }

            // --- 4) stamp JSON on disk (status="planned")
            if (cfg.upload_json && file_exists(job.json_path)) {
                try {
                    std::ifstream in(job.json_path);
                    nlohmann::json j = nlohmann::json::parse(in);
                    in.close();

                    nlohmann::json& su = j["storage_uploader"];
                    if (!su.is_object()) su = nlohmann::json::object();

                    su["status"] = "planned";
                    if (!files.empty())         su["files"]        = files;
                    if (!json_s3_url.empty())   su["json_s3_url"]  = json_s3_url;
                    if (!json_web_url.empty())  su["json_web_url"] = json_web_url;

                    std::ofstream out(job.json_path, std::ios::trunc);
                    out << j.dump(2);

                    if (cfg.log_debug) {
                        SU_LOG(info) << "[" << job.shortName << "] stamped storage URLs (s3_url/web_url) into JSON";
                    }
                } catch (const std::exception& e) {
                    SU_LOG(warning) << "[" << job.shortName << "] failed stamping JSON: " << e.what();
                }
            }


            // --- 5) enqueue async upload
            SU_LOG(info) << "[" << job.shortName << "] queue upload: "
                         << "wav=" << basename_of(job.wav_path)
                         << " m4a=" << basename_of(job.m4a_path)
                         << " json=" << basename_of(job.json_path);
            if (worker_) worker_->enqueue(job);

        } catch (...) {
            return -1;
        }

        return 0;
    }

    // Factory method for BOOST_DLL_ALIAS
    static boost::shared_ptr<Storage_Uploader_Plugin> create() {
        return boost::shared_ptr<Storage_Uploader_Plugin>(
            new Storage_Uploader_Plugin());
    }

private:
    std::unordered_map<std::string, PluginConfig> cfg_by_system_;
    std::unique_ptr<StorageUploaderWorker> worker_;
};

// Export symbol "create_plugin" so TR can load us (matches MQTT style).
BOOST_DLL_ALIAS(
    Storage_Uploader_Plugin::create,
    create_plugin
)
