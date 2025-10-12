
#include "s3_client.hpp"
#include <curl/curl.h>
#include <filesystem>
#include <fstream>
#include <sstream>

S3Client::S3Client(const S3Config& cfg) : cfg_(cfg) {}

std::string S3Client::endpoint_for_bucket() const {
    if (!cfg_.endpoint.empty()) {
        return cfg_.endpoint;
    }
    // Default AWS S3 virtual-hosted style endpoint for region
    return "https://" + cfg_.bucket + ".s3." + cfg_.region + ".amazonaws.com";
}

std::string S3Client::url_for_key(const std::string& key) const {
    if (!cfg_.endpoint.empty()) {
        // custom endpoint; assume path-style is acceptable: https://endpoint/bucket/key
        std::string base = cfg_.endpoint;
        if (base.back() == '/') base.pop_back();
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

    FILE* fh = fopen(local_path.c_str(), "rb");
    if (!fh) {
        if (err_out) *err_out = "Failed to open " + local_path;
        return false;
    }
    std::error_code ec;
    auto file_size = std::filesystem::file_size(local_path, ec);
    if (ec) { if (err_out) *err_out = "Failed to stat " + local_path + ": " + ec.message(); fclose(fh); return false; }

    CURL* curl = curl_easy_init();
    if (!curl) { if (err_out) *err_out = "curl_easy_init failed"; fclose(fh); return false; }

    struct curl_slist* headers = nullptr;
    std::string content_type = "Content-Type: " + guess_content_type(local_path);
    headers = curl_slist_append(headers, content_type.c_str());
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
    for (auto& h : extra_headers) headers = curl_slist_append(headers, h.c_str());

    std::string url = url_for_key(object_key);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READDATA, fh);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, cfg_.connect_timeout_ms);
    if (cfg_.transfer_timeout_ms > 0) curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, cfg_.transfer_timeout_ms);

    // Use AWS SigV4 auth if keys present
    if (!cfg_.access_key.empty() && !cfg_.secret_key.empty()) {
        // Set AWS4 signature: provider "aws:amz", region, service s3
        std::string sig = "aws:amz:" + cfg_.region + ":s3";
        curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, sig.c_str());
        // Use access/secret via USERPWD
        std::string userpwd = cfg_.access_key + ":" + cfg_.secret_key;
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
        if (cfg_.session_token) {
            std::string tok = "x-amz-security-token: " + *cfg_.session_token;
            headers = curl_slist_append(headers, tok.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        }
    }
    // Follow redirects in case of path-style endpoint issuing 301
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    long http_code = 0;
    CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (res != CURLE_OK || (http_code / 100) != 2) {
        if (err_out) {
            std::ostringstream oss;
            oss << "S3 upload failed: CURL " << curl_easy_strerror(res) << ", HTTP " << http_code;
            *err_out = oss.str();
        }
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        fclose(fh);
        return false;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    fclose(fh);
    return true;
}
