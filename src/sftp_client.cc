
#include "sftp_client.hpp"
#include <curl/curl.h>
#include <filesystem>
#include <sstream>

#include <boost/log/trivial.hpp>
static constexpr const char* SU_TAG_SFTP = "\t[Storage Uploader][SFTP]\t";
#define SU_SFTP_LOG(sev) BOOST_LOG_TRIVIAL(sev) << SU_TAG_SFTP

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

#if defined(CURLOPT_SSH_HOSTKEYFUNCTION)
static int ssh_hostkey_cb(
    CURL* /*easy*/,
    const struct curl_khkey* /*key*/,
    const struct curl_khkey* /*prevkey*/,
    enum curl_khmatch match,
    void* clientp)
{
    const SftpConfig* cfg = static_cast<const SftpConfig*>(clientp);

    // OK: host key matches what we have
    if (match == CURLKHMATCH_OK) {
        return CURLKHSTAT_FINE;
    }

    // MISMATCH is dangerous — never auto-accept
    if (match == CURLKHMATCH_MISMATCH) {
        SU_SFTP_LOG(error) << "Host key MISMATCH detected; rejecting.";
        return CURLKHSTAT_REJECT;
    }

    // MISSING: we don't have this host in known_hosts
    if (match == CURLKHMATCH_MISSING) {
        if (cfg && cfg->accept_unknown_host) {
            // If a known_hosts path is provided, ask curl to add it automatically.
            if (!cfg->known_hosts.empty()) {
                SU_SFTP_LOG(warning) << "Unknown host; accepting and adding to known_hosts.";
                return CURLKHSTAT_FINE_ADD_TO_FILE; // curl will append to known_hosts
            }
            // Otherwise, accept for this session only.
            SU_SFTP_LOG(warning) << "Unknown host; accepting for this session (not saved).";
            return CURLKHSTAT_FINE;
        }
        // Not allowed to accept — reject
        SU_SFTP_LOG(error) << "Unknown host; set sftp.accept_unknown_host=true to allow.";
        return CURLKHSTAT_REJECT;
    }

    // Default: defer (behaves like reject)
    return CURLKHSTAT_DEFER;
}
#endif

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
        if (cfg_.password.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PUBLICKEY);
        }
    }

    // Known-hosts (verification db)
    if (!cfg_.known_hosts.empty()) {
        curl_easy_setopt(curl, CURLOPT_SSH_KNOWNHOSTS, cfg_.known_hosts.c_str());
    }

    // host-key callback to accept unknown host when configured
#if defined(CURLOPT_SSH_HOSTKEYFUNCTION)
    curl_easy_setopt(curl, CURLOPT_SSH_HOSTKEYFUNCTION, ssh_hostkey_cb);
    curl_easy_setopt(curl, CURLOPT_SSH_HOSTKEYDATA, (void*)&cfg_);
#else
    // Fallback note: your libcurl is too old for SSH hostkey callback.
    // If you need auto-accept, upgrade libcurl (7.80.0+). Without the callback,
    // libcurl will fail on unknown hosts unless the host is present in known_hosts.
    if (cfg_.accept_unknown_host) {
        SU_SFTP_LOG(warning) << "accept_unknown_host requested, but libcurl lacks "
                                "CURLOPT_SSH_HOSTKEYFUNCTION support; cannot auto-accept.";
    }
#endif

    // Perform
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

