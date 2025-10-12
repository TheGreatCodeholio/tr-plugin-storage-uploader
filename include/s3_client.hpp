
#pragma once
#include <string>
#include <vector>
#include <optional>
#include "config.hpp"

class S3Client {
public:
    explicit S3Client(const S3Config& cfg);
    bool upload_file(const std::string& local_path,
                     const std::string& object_key,
                     const std::vector<std::string>& extra_headers = {},
                     std::string* err_out = nullptr);
private:
    S3Config cfg_;
    std::string endpoint_for_bucket() const;
    std::string url_for_key(const std::string& key) const;
    std::string guess_content_type(const std::string& path) const;
};
