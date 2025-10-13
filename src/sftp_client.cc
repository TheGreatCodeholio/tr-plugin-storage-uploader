#include "sftp_client.hpp"
#include <curl/curl.h>
#include <filesystem>
#include <fstream>      // <<< ADD
#include <sstream>
#include <boost/log/trivial.hpp>  // <<< ADD

static constexpr const char* SFTP_TAG = "\t[Storage Uploader][SFTP]\t"; // <<< ADD
#define SFTP_LOG(sev) BOOST_LOG_TRIVIAL(sev) << SFTP_TAG                // <<< ADD

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

// ---------- NEW helpers for known_hosts handling ----------

// Ensure we have a writable known_hosts path. If user didn't provide one,
// use a plugin-local default under /tmp and create it if needed.
static std::string ensure_known_hosts_path(const std::string& user_path) { // <<< ADD
    namespace fs = std::filesystem;
    std::string path = user_path.empty()
        ? std::string("/tmp/tr_storage_uploader_known_hosts")  // default
        : user_path;

    fs::path p(path);
    if (p.has_parent_path()) {
        std::error_code ec;
        fs::create_directories(p.parent_path(), ec); // best effort
    }
    if (!fs::exists(p)) {
        std::ofstream ofs(path); // create empty file
        ofs.close();
    }
    return path;
}

// libcurl SSH key callback: decide how to handle host key verification
// We only auto-accept when the host is *missing* from known_hosts.
// A *mismatch* is rejected to avoid MITM.                              // <<< ADD
struct HostKeyCtx { bool accept_unknown; std::string kh_path; };       // <<< ADD

static int ssh_key_cb(                                                  // <<< ADD
    CURL* /*easy*/,
    const struct curl_khkey* /*knownkey*/,
    const struct curl_khkey* /*foundkey*/,
    enum curl_khmatch match,
    void* clientp)
{
    auto* ctx = static_cast<HostKeyCtx*>(clientp);

    switch (match) {
        case CURLKHMATCH_OK:
            // Known and matches — proceed
            return CURLKHSTAT_FINE;

        case CURLKHMATCH_MISSING:
            if (ctx && ctx->accept_unknown) {
                SFTP_LOG(info) << "Host key missing; auto-accept enabled → adding to known_hosts: "
                               << (ctx->kh_path.empty() ? "(unset)" : ctx->kh_path);
                // Tell libcurl to append the key to the file set by CURLOPT_SSH_KNOWNHOSTS
                return CURLKHSTAT_FINE_ADD_TO_FILE;
            }
            SFTP_LOG(warning) << "Host key missing and auto-accept disabled — rejecting.";
            return CURLKHSTAT_REJECT;

        case CURLKHMATCH_MISMATCH:
        default:
            // Mismatch is dangerous; always reject
            SFTP_LOG(error) << "Host key MISMATCH — rejecting.";
            return CURLKHSTAT_REJECT;
    }
}
// ---------- end new helpers ----------

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

    // ---------- SSH known_hosts + auto-accept handling ----------
    // Always provide a known_hosts path (user or default).                 // <<< CHANGED
    const std::string kh_path = ensure_known_hosts_path(cfg_.known_hosts);
    curl_easy_setopt(curl, CURLOPT_SSH_KNOWNHOSTS, kh_path.c_str());

    // If user asked to accept unknown hosts, install the KEYFUNCTION.
    if (cfg_.accept_unknown_host) {                                       // <<< CHANGED
        HostKeyCtx hkctx{true, kh_path};
        curl_easy_setopt(curl, CURLOPT_SSH_KEYFUNCTION, ssh_key_cb);
        curl_easy_setopt(curl, CURLOPT_SSH_KEYDATA, &hkctx);
        SFTP_LOG(info) << "accept_unknown_host=ON, known_hosts=\"" << kh_path << "\"";
        // Note: hkctx lives on this stack frame until perform() returns.
    } else {
        SFTP_LOG(info) << "accept_unknown_host=OFF, known_hosts=\"" << kh_path << "\"";
    }
    // ------------------------------------------------------------

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
