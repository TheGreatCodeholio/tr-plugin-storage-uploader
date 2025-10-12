
#include "sftp_client.hpp"
#include <curl/curl.h>
#include <filesystem>
#include <sstream>

SftpClient::SftpClient(const SftpConfig& cfg) : cfg_(cfg) {}

std::string SftpClient::remote_url_for(const std::string& rel) const {
    std::string root = cfg_.remote_root;
    if (!root.empty() && root.front() != '/') root = "/" + root;
    if (!root.empty() && root.back() == '/') root.pop_back();
    std::string r = rel;
    if (!r.empty() && r.front() == '/') r.erase(0,1);
    std::ostringstream os;
    os << "sftp://" << cfg_.host << ":" << cfg_.port << root << "/" << r;
    return os.str();
}

bool SftpClient::upload_file(const std::string& local_path,
                             const std::string& remote_rel_path,
                             std::string* err_out) {
    if (!cfg_.enabled) return true;
    FILE* fh = fopen(local_path.c_str(), "rb");
    if (!fh) { if (err_out) *err_out = "Failed to open " + local_path; return false; }

    CURL* curl = curl_easy_init();
    if (!curl) { if (err_out) *err_out = "curl_easy_init failed"; fclose(fh); return false; }

    // Request may create intermediate directories if not present
    curl_easy_setopt(curl, CURLOPT_FTP_CREATE_MISSING_DIRS, 1L);
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READDATA, fh);

    auto url = remote_url_for(remote_rel_path);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, cfg_.connect_timeout_ms);
    if (cfg_.transfer_timeout_ms > 0) curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, cfg_.transfer_timeout_ms);

    // Auth
    if (!cfg_.username.empty()) {
        curl_easy_setopt(curl, CURLOPT_USERNAME, cfg_.username.c_str());
    }
    if (!cfg_.password.empty()) {
        curl_easy_setopt(curl, CURLOPT_PASSWORD, cfg_.password.c_str());
    }
    if (!cfg_.key_path.empty()) {
        curl_easy_setopt(curl, CURLOPT_SSH_PRIVATE_KEYFILE, cfg_.key_path.c_str());
        // Disable password if key provided and no password
        if (cfg_.password.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PUBLICKEY);
        }
    }
    if (!cfg_.known_hosts.empty()) {
        curl_easy_setopt(curl, CURLOPT_SSH_KNOWNHOSTS, cfg_.known_hosts.c_str());
    }

    // Perform
    long http_code = 0;
    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        if (err_out) *err_out = std::string("SFTP upload failed: ") + curl_easy_strerror(res);
        curl_easy_cleanup(curl);
        fclose(fh);
        return false;
    }

    curl_easy_cleanup(curl);
    fclose(fh);
    return true;
}
