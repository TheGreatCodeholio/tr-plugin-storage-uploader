// s3_client.cc
#include "s3_client.hpp"

#include <curl/curl.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <mutex>
#include <cstdlib>

#include <boost/log/trivial.hpp>
#define S3_TAG  "\t[Storage Uploader][S3]\t"
#define S3_LOG(sev) BOOST_LOG_TRIVIAL(sev) << S3_TAG

S3Client::S3Client(const S3Config& cfg) : cfg_(cfg) {}

static std::once_flag g_curl_ver_once;

static std::string mask_id(const std::string& s) {
    if (s.empty()) return "";
    if (s.size() <= 8) return "****";
    return s.substr(0, 4) + "…" + s.substr(s.size() - 4);
}

static size_t append_body(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const size_t total = size * nmemb;
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, total);
    return total;
}

static size_t append_header(char* buffer, size_t size, size_t nitems, void* userdata) {
    const size_t total = size * nitems;
    auto* out = static_cast<std::string*>(userdata);
    out->append(buffer, total);
    return total;
}

static std::string extract_header_ci(const std::string& headers, const std::string& key) {
    // case-insensitive search for "key: value"
    std::string hlow(headers);
    std::transform(hlow.begin(), hlow.end(), hlow.begin(), [](unsigned char c){ return std::tolower(c); });
    std::string klow = key;
    std::transform(klow.begin(), klow.end(), klow.begin(), [](unsigned char c){ return std::tolower(c); });
    klow += ":";

    size_t pos = hlow.find(klow);
    if (pos == std::string::npos) return "";
    // move past "key:"
    pos += klow.size();
    // skip whitespace
    while (pos < hlow.size() && (hlow[pos] == ' ' || hlow[pos] == '\t')) ++pos;

    // read until end-of-line
    size_t end = headers.find_first_of("\r\n", pos);
    if (end == std::string::npos) end = headers.size();
    return std::string(headers.substr(pos, end - pos));
}

std::string S3Client::endpoint_for_bucket() const {
    if (!cfg_.endpoint.empty()) {
        return cfg_.endpoint;
    }
    // Default AWS S3 virtual-hosted style endpoint for region
    return "https://" + cfg_.bucket + ".s3." + cfg_.region + ".amazonaws.com";
}

std::string S3Client::url_for_key(const std::string& key) const {
    if (!cfg_.endpoint.empty()) {
        // custom endpoint; https://endpoint/bucket/key
        std::string base = cfg_.endpoint;
        if (!base.empty() && base.back() == '/') base.pop_back();
        return base + "/" + cfg_.bucket + "/" + key;
    }
    return endpoint_for_bucket() + "/" + key;
}

std::string S3Client::guess_content_type(const std::string& path) const {
    auto ext = std::filesystem::path(path).extension().string();
    if (ext == ".wav") return "audio/wav";
    if (ext == ".m4a") return "audio/mp4";
    if (ext == ".json") return "application/json";
    if (ext == ".mp3") return "audio/mpeg";
    return "application/octet-stream";
}

bool S3Client::upload_file(const std::string& local_path,
                           const std::string& object_key,
                           const std::vector<std::string>& extra_headers,
                           std::string* err_out) {
    if (!cfg_.enabled) return true;

    std::call_once(g_curl_ver_once, []{
        const auto* vi = curl_version_info(CURLVERSION_NOW);
        if (vi && vi->version) {
            S3_LOG(info) << "libcurl version: " << vi->version;
        }
    });

    FILE* fh = fopen(local_path.c_str(), "rb");
    if (!fh) {
        if (err_out) *err_out = "Failed to open " + local_path;
        S3_LOG(error) << "open(" << local_path << ") failed";
        return false;
    }

    std::error_code ec;
    const auto file_size = std::filesystem::file_size(local_path, ec);
    if (ec) {
        if (err_out) *err_out = "Failed to stat " + local_path + ": " + ec.message();
        S3_LOG(error) << "stat(" << local_path << ") failed: " << ec.message();
        fclose(fh);
        return false;
    }

    const std::string url = url_for_key(object_key);
    const std::string ct  = guess_content_type(local_path);

    // Attempt pre-log
    {
        std::ostringstream oss;
        oss << "PUT s3://" << cfg_.bucket << "/" << object_key
            << " | url=" << url
            << " | size=" << file_size
            << " | content-type=" << ct
            << " | region=" << (cfg_.region.empty() ? "(default)" : cfg_.region)
            << " | endpoint=" << (cfg_.endpoint.empty() ? "(aws)" : cfg_.endpoint);

        if (!cfg_.access_key.empty()) {
            oss << " | auth=sigv4(" << mask_id(cfg_.access_key) << ")";
        } else {
            oss << " | auth=(no explicit key)";
        }
        if (cfg_.storage_class) oss << " | storage-class=" << *cfg_.storage_class;
        if (cfg_.acl)           oss << " | acl=" << *cfg_.acl;
        if (cfg_.sse)           oss << " | sse=" << *cfg_.sse
                                    << (cfg_.kms_key ? " (kms-key set)" : "");
        oss << " | timeouts connect=" << cfg_.connect_timeout_ms << "ms"
            << " transfer=" << cfg_.transfer_timeout_ms << "ms";
        S3_LOG(info) << oss.str();
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        if (err_out) *err_out = "curl_easy_init failed";
        S3_LOG(error) << "curl_easy_init failed";
        fclose(fh);
        return false;
    }

    // gather response body & headers for diagnostics
    std::string resp_body;
    std::string resp_headers;

    // Build headers
    struct curl_slist* headers = nullptr;
    const std::string h_content_type = "Content-Type: " + ct;
    headers = curl_slist_append(headers, h_content_type.c_str());
    if (cfg_.storage_class) {
        std::string h = "x-amz-storage-class: " + *cfg_.storage_class;
        headers = curl_slist_append(headers, h.c_str());
    }
    if (cfg_.acl) {
        std::string h = "x-amz-acl: " + *cfg_.acl;
        headers = curl_slist_append(headers, h.c_str());
    }
    if (cfg_.sse) {
        std::string h = "x-amz-server-side-encryption: " + *cfg_.sse;
        headers = curl_slist_append(headers, h.c_str());
        if (*cfg_.sse == "aws:kms" && cfg_.kms_key) {
            std::string k = "x-amz-server-side-encryption-aws-kms-key-id: " + *cfg_.kms_key;
            headers = curl_slist_append(headers, k.c_str());
        }
    }
    for (const auto& h : extra_headers) headers = curl_slist_append(headers, h.c_str());

    // Core cURL options
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READDATA, fh);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, cfg_.connect_timeout_ms);
    if (cfg_.transfer_timeout_ms > 0) curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, cfg_.transfer_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    // Capture response body + headers for errors/success diagnostics
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, append_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp_body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, append_header);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &resp_headers);

    // Optional wire-level verbosity via env var (very chatty)
    if (const char* v = std::getenv("TR_STORAGE_S3_VERBOSE")) {
        if (std::string(v) == "1" || std::string(v) == "true") {
            curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        }
    }

    // AWS SigV4 when keys are provided
    if (!cfg_.access_key.empty() && !cfg_.secret_key.empty()) {
        // provider "aws:amz", region, service s3
        std::string sig = "aws:amz:" + cfg_.region + ":s3";
        curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, sig.c_str());
        // credentials
        std::string userpwd = cfg_.access_key + ":" + cfg_.secret_key;
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
        if (cfg_.session_token) {
            std::string tok = "x-amz-security-token: " + *cfg_.session_token;
            headers = curl_slist_append(headers, tok.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        }
    }

    long http_code = 0;
    CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    // In case of redirect, get the effective URL (handy when custom endpoints bounce)
    char* eff_url_c = nullptr;
    if (curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &eff_url_c) == CURLE_OK && eff_url_c) {
        S3_LOG(info) << "effective_url=" << eff_url_c;
    }

    bool ok = (res == CURLE_OK) && ((http_code / 100) == 2);

    if (!ok) {
        const std::string req_id  = extract_header_ci(resp_headers, "x-amz-request-id");
        const std::string host_id = extract_header_ci(resp_headers, "x-amz-id-2");

        // trim body for log
        std::string body_log = resp_body;
        const size_t maxb = 1024; // keep logs readable
        if (body_log.size() > maxb) {
            body_log.resize(maxb);
            body_log += "...(truncated)";
        }

        std::ostringstream oss;
        oss << "S3 upload failed: CURL " << curl_easy_strerror(res)
            << ", HTTP " << http_code;
        if (!req_id.empty())  oss << " | x-amz-request-id=" << req_id;
        if (!host_id.empty()) oss << " | x-amz-id-2=" << host_id;

        if (err_out) *err_out = oss.str();

        S3_LOG(error) << oss.str();
        if (!body_log.empty()) {
            S3_LOG(warning) << "Response body: " << body_log;
        }
        if (!resp_headers.empty()) {
            // print just the first line and the amz ids to avoid spam
            std::istringstream hs(resp_headers);
            std::string first;
            std::getline(hs, first);
            S3_LOG(info) << "Response status line: " << first;
        }

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        fclose(fh);
        return false;
    }

    S3_LOG(info) << "PUT OK s3://" << cfg_.bucket << "/" << object_key
                 << " (" << file_size << " bytes)";
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    fclose(fh);
    return true;
}
