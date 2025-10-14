// include/s3_client.hpp
#pragma once
#include <optional>
#include <string>
#include <vector>

struct S3Config {
    bool enabled{false};
    std::string bucket, region, endpoint, access_key, secret_key;
    std::optional<std::string> session_token, storage_class, acl, sse, kms_key;
    long connect_timeout_ms{10000};
    long transfer_timeout_ms{0};
    int  max_retries{5};

    std::optional<std::string> public_base_url;
};

class S3Client {
public:
    explicit S3Client(const S3Config& cfg);
    bool upload_file(const std::string& local_path,
                     const std::string& object_key,
                     const std::vector<std::string>& extra_headers,
                     std::string* err_out);

    // <-- make sure this is declared:
    std::string url_for_key(const std::string& key) const;

    // (optional to expose)
    std::string guess_content_type(const std::string& path) const;

private:
    std::string endpoint_for_bucket() const;

    S3Config cfg_;
};
