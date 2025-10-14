
#pragma once
#include <string>
#include <vector>
#include "config.hpp"

class SftpClient {
public:
    explicit SftpClient(const SftpConfig& cfg);
    bool upload_file(const std::string& local_path,
                     const std::string& remote_rel_path,
                     std::string* err_out = nullptr);

    std::string public_url_for(const std::string& rel) const;

private:
    SftpConfig cfg_;
    std::string remote_url_for(const std::string& rel) const;
};
