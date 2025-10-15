#pragma once
#include <string>
#include <optional>  // ← add this

struct S3Config {
    bool        enabled{false};
    std::string bucket;
    std::string region{"us-east-1"};
    std::string endpoint;
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"};
    std::string access_key;
    std::string secret_key;
    std::optional<std::string> session_token;
    std::optional<std::string> storage_class;
    std::optional<std::string> acl;
    std::optional<std::string> sse;
    std::optional<std::string> kms_key;
    long connect_timeout_ms{10000};
    long transfer_timeout_ms{0};
    int  max_retries{5};
};

struct SftpConfig {
    bool        enabled{false};
    std::string host;
    int         port{22};
    std::string username;
    std::string password;
    std::string key_path;
    std::string known_hosts;
    std::string remote_root{"/uploads"};
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"};
    long connect_timeout_ms{10000};
    long transfer_timeout_ms{0};
    int  max_retries{5};
    bool accept_unknown_host{false};

    // base URL to build public web links for SFTP/SCP (e.g., https://media.example.com)
    std::optional<std::string> public_base_url;   // ← add this
};

struct PluginConfig {
    bool upload_json{true};
    bool delete_after_upload{false};
    bool log_debug{false};
    std::string audio{"auto"}; // auto|m4a|wav|all
    S3Config   s3;
    SftpConfig sftp;
};
