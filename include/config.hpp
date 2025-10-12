
#pragma once
#include <string>
#include <optional>

struct S3Config {
    bool enabled{false};
    std::string bucket;
    std::string region{"us-east-1"};
    std::string endpoint; // optional custom endpoint (https://s3.amazonaws.com by default)
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"};
    std::string access_key;   // if empty, libcurl will use env/instance creds when possible
    std::string secret_key;
    std::optional<std::string> session_token;
    std::optional<std::string> storage_class;
    std::optional<std::string> acl;
    std::optional<std::string> sse;      // e.g., "AES256" or "aws:kms"
    std::optional<std::string> kms_key;  // if sse == "aws:kms"
    long connect_timeout_ms{10000};
    long transfer_timeout_ms{0}; // 0 = unlimited
    int max_retries{5};
};

struct SftpConfig {
    bool enabled{false};
    std::string host;
    int port{22};
    std::string username;
    std::string password; // or use key_path
    std::string key_path; // optional
    std::string known_hosts; // optional path to known_hosts
    std::string remote_root{"/uploads"};
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"};
    long connect_timeout_ms{10000};
    long transfer_timeout_ms{0};
    int max_retries{5};
};

struct PluginConfig {
    // General
    std::string name{"Storage Uploader"};
    bool upload_json{true};
    bool delete_after_upload{false};
    bool log_debug{false};

    // Destinations
    S3Config s3;
    SftpConfig sftp;
};
