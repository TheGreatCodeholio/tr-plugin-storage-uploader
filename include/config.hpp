#pragma once
#include <string>
#include <optional>

/**
 * S3 destination configuration.
 * All fields map 1:1 to the plugin JSON (per-system) under "s3".
 */
struct S3Config {
    bool enabled{false};                 // Enable S3 uploads for this system
    std::string bucket;                  // Bucket name
    std::string region{"us-east-1"};     // AWS region (used by libcurl AWS SigV4)
    std::string endpoint;                // Optional custom endpoint (e.g. MinIO)
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"}; // Key template
    std::string access_key;              // If empty, libcurl may use env/instance creds
    std::string secret_key;
    std::optional<std::string> session_token; // AWS STS session token (optional)
    std::optional<std::string> storage_class; // e.g., "STANDARD_IA"
    std::optional<std::string> acl;           // e.g., "private", "public-read"
    std::optional<std::string> sse;           // "AES256" or "aws:kms"
    std::optional<std::string> kms_key;       // KMS key id/arn if sse == "aws:kms"
    long connect_timeout_ms{10000};           // Connect timeout
    long transfer_timeout_ms{0};              // 0 = unlimited transfer time
    int max_retries{5};                       // Exponential backoff retries
};

/**
 * SFTP destination configuration.
 * All fields map 1:1 to the plugin JSON (per-system) under "sftp".
 */
struct SftpConfig {
    bool enabled{false};                 // Enable SFTP uploads for this system
    std::string host;                    // SFTP host
    int port{22};                        // SFTP port
    std::string username;                // Username (optional if key auth only)
    std::string password;                // Password (optional when using key)
    std::string key_path;                // Path to private key (optional)
    std::string known_hosts;             // Path to known_hosts (optional)
    std::string remote_root{"/uploads"}; // Remote root; we'll append the prefix
    std::string prefix_template{"{shortName}/{yyyy}/{MM}/{dd}/{basename}"}; // Remote path template
    long connect_timeout_ms{10000};      // Connect timeout
    long transfer_timeout_ms{0};         // 0 = unlimited transfer time
    int max_retries{5};                  // Exponential backoff retries
};

/**
 * Per-system plugin configuration.
 * This is what we store internally for each "systems[...]" entry in the plugin block.
 */
struct PluginConfig {
    // General behavior
    std::string name{"Storage Uploader"}; // Human readable
    /**
     * Audio selection per system:
     *  - "auto" (default): prefer m4a if present, else wav
     *  - "m4a"          : upload m4a; if missing, fall back to wav
     *  - "wav"          : upload wav; if missing, fall back to m4a
     *  - "all"         : upload all m4a and wav if present
     */
    std::string audio{"auto"};

    bool upload_json{true};                // Also upload the call JSON
    bool delete_after_upload{false};       // Remove local files after successful uploads
    bool log_debug{false};                 // Reserved (not used yet)

    // Destinations
    S3Config s3;
    SftpConfig sftp;
};
