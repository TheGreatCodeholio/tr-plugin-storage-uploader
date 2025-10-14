# Trunk Recorder – Storage Uploader Plugin

Uploads call audio (and optional JSON metadata) **per-system** to **S3** and/or **SFTP/SCP**.  
Designed to be non-blocking: an internal worker queue with retry/backoff handles transfers in the background.

## Features

- **Async uploads** so decoder threads aren’t blocked.
- **Per-system config** (by `shortName`) for S3 and/or SFTP with independent paths, buckets, hosts, etc.
- **JSON enrichment**: writes remote links into each call’s status JSON under `storage_uploader` with:
    - `s3_url` – public/constructed URL for the S3 object (or the exact PUT URL if not public).
    - `web_url` – web URL derived from your SFTP/SCP `web_base_url` + uploaded path.
    - Full arrays of objects for `s3` and `sftp` with bucket/keys/paths for every uploaded file.
    - JSON location entries `json_s3` / `json_sftp` for the status file itself.
    - `status` field transitions: `"planned"` → `"uploaded"`.
- **Robust S3** (AWS SigV4 via libcurl; optional custom endpoints, SSE/KMS, ACL, storage class).
- **SFTP/SCP** via libcurl; optional `known_hosts` and `accept_unknown_host`.
- **Flexible audio selection**: `audio = "auto" | "m4a" | "wav" | "all"`.

---

## Build & Install

Place this repo under `trunk-recorder/user_plugins/storage-uploader/`, then from your TR build dir:

```bash
cmake ..
sudo make install
```

This installs `libstorage_uploader.so` to TR’s plugin path.

> **Note:** The plugin uses libcurl for S3 and SFTP. Ensure your build links libcurl with the features you need (OpenSSL, SSH, etc.).

---

## Configuration

Add a plugin block with one or more **systems** (matched by `shortName`).  
Template tokens supported in `prefix_template`:

- `{shortName}`, `{yyyy}`, `{MM}`, `{dd}`, `{basename}`

> **Execution order:** Trunk Recorder runs the **upload script** (if configured on the system) first, then calls each plugin’s `call_end`. If you want this plugin to run **after** your other plugins, list the Storage Uploader **last** in your `plugins` array.

### Minimal Example

```json
{
  "plugins": [
    {
      "name": "Storage Uploader",
      "library": "libstorage_uploader.so",
      "systems": [
        {
          "shortName": "chemung",
          "audio": "auto",
          "uploadJson": true,
          "deleteAfterUpload": true,
          "s3": {
            "enabled": true,
            "bucket": "chemung-calls",
            "region": "us-east-1",
            "prefix_template": "chemung/{yyyy}/{MM}/{dd}/{basename}"
          }
        },
        {
          "shortName": "steuben",
          "audio": "wav",
          "uploadJson": true,
          "deleteAfterUpload": false,
          "sftp": {
            "enabled": true,
            "host": "sftp.steuben.net",
            "port": 22,
            "username": "upload",
            "remote_root": "/srv/steuben/calls",
            "prefix_template": "steuben/{yyyy}/{MM}/{dd}/{basename}",

            // New: base URL used to build a public/HTTP URL for the uploaded file
            "web_base_url": "https://media.steuben.net/calls"
          }
        }
      ]
    }
  ]
}
```

### Full Featured Example (with credentials, timeouts, SSE, and both S3+SFTP)

```jsonc
{
  "plugins": [
    {
      "name": "Storage Uploader",
      "library": "libstorage_uploader.so",
      "systems": [
        {
          "shortName": "bradford-pa",
          "audio": "all",                  // m4a + wav if both exist
          "uploadJson": true,              // enrich JSON and upload it too
          "deleteAfterUpload": true,       // delete local files only if ALL uploads succeed
          "debug": true,                   // extra info logs

          "s3": {
            "enabled": true,
            "bucket": "trunk-player",
            "region": "us-east-1",
            "prefix_template": "bradford-pa/{yyyy}/{MM}/{dd}/{basename}",

            // Credentials (or rely on instance metadata/role if empty)
            "access_key": "AKIA...",
            "secret_key": "abcd...",
            "session_token": "",

            // Optional:
            "endpoint": "",                // e.g. https://s3.us-east-1.amazonaws.com or custom compatible storage
            "storage_class": "STANDARD",   // or STANDARD_IA, GLACIER, etc.
            "acl": "bucket-owner-full-control",
            "sse": "AES256",               // or "aws:kms"
            "kms_key": "",                 // required only if sse=aws:kms
            "connect_timeout_ms": 10000,
            "transfer_timeout_ms": 0,      // 0 = no overall timeout
            "max_retries": 5
          },

          "sftp": {
            "enabled": true,
            "host": "mercury.icarey.net",
            "port": 22,
            "username": "upload",
            "password": "********",
            "key_path": "",                // alternative to password auth
            "known_hosts": "",             // path to known_hosts file (optional)
            "accept_unknown_host": true,   // set false for strict host checking

            "remote_root": "/var/www/media/calls",
            "prefix_template": "bradford-pa/{yyyy}/{MM}/{dd}/{basename}",

            // New: base URL used to build a public/HTTP URL for the uploaded file
            "web_base_url": "https://media.example.com/calls",

            "connect_timeout_ms": 10000,
            "transfer_timeout_ms": 0,
            "max_retries": 5
          }
        }
      ]
    }
  ]
}
```

---

## Options Reference

### General (per system)

- `audio`: `"auto"` (default), `"m4a"`, `"wav"`, or `"all"`
    - `"auto"` prefers m4a (if present) else wav
    - `"all"` uploads both (m4a then wav)
- `uploadJson` (bool): write remote links into the call JSON and upload it too.
- `deleteAfterUpload` (bool): remove local files only after **all** configured destinations succeed.
- `debug` (bool): log extra info lines.

### S3

- `enabled` (bool), `bucket` (string), `region` (string; e.g., `us-east-1`)
- `prefix_template` (string): path template under the bucket (see tokens above).
- `access_key`, `secret_key`, `session_token` (optional; SigV4). If omitted, libcurl/ENV/IMDS may be used.
- `endpoint` (optional): custom S3-compatible endpoint. If empty, standard AWS virtual-hosted style is used.
- `storage_class`, `acl`, `sse` (`AES256` or `aws:kms`), `kms_key` (when `sse=aws:kms`).
- `connect_timeout_ms`, `transfer_timeout_ms`, `max_retries`.

### SFTP/SCP

- `enabled` (bool), `host`, `port`, `username`, `password` or `key_path`.
- `known_hosts` (optional path), `accept_unknown_host` (bool).
- `remote_root` (string): base directory on the server.
- `prefix_template` (string): subpath under `remote_root`.
- `web_base_url` (string): **public HTTP base URL** that mirrors `remote_root`. The plugin concatenates `web_base_url + "/" + prefix_template` (with tokens resolved) to build `web_url`.
- `connect_timeout_ms`, `transfer_timeout_ms`, `max_retries`.

> The plugin *always* writes to the SFTP filesystem at `remote_root + "/" + prefix_template`, and if `web_base_url` is provided, also computes a public `web_url` using the same relative path.

---

## JSON Enrichment

When `uploadJson` is true, the plugin updates each call’s status JSON **before** uploading it:

```jsonc
{
  "freq": 155415000,
  "talkgroup": 300,
  "...": "other TR fields ...",
  "storage_uploader": {
    "status": "uploaded",                 // "planned" before upload, "uploaded" after success

    // Convenience URLs (if available)
    "s3_url": "https://trunk-player.s3.us-east-1.amazonaws.com/bradford-pa/2025/10/13/300-...-call_6.m4a",
    "web_url": "https://media.example.com/calls/bradford-pa/2025/10/13/300-...-call_6.m4a",

    // Full per-file details
    "s3": [
      { "bucket": "trunk-player", "key": "bradford-pa/2025/10/13/300-...-call_6.m4a", "region": "us-east-1",
        "url": "https://trunk-player.s3.us-east-1.amazonaws.com/bradford-pa/2025/10/13/300-...-call_6.m4a" }
    ],
    "sftp": [
      { "host": "mercury.icarey.net", "path": "bradford-pa/2025/10/13/300-...-call_6.m4a",
        "web_url": "https://media.example.com/calls/bradford-pa/2025/10/13/300-...-call_6.m4a" }
    ],

    // Where the JSON itself is uploaded
    "json_s3":   { "bucket": "trunk-player", "key": "bradford-pa/...call_6.json", "region": "us-east-1",
                   "url": "https://trunk-player.s3.us-east-1.amazonaws.com/bradford-pa/...call_6.json" },
    "json_sftp": { "host": "mercury.icarey.net", "path": "bradford-pa/...call_6.json",
                   "web_url": "https://media.example.com/calls/bradford-pa/...call_6.json" }
  }
}
```

- If only one destination is enabled (just S3 or just SFTP), only the corresponding blocks/URLs are present.
- The plugin sets `"status": "planned"` just before starting transfers and updates it to `"uploaded"` once **all** enabled uploads for that file type (audio + JSON) succeed.

---

## Troubleshooting

- **HTTP 404 / `NoSuchBucket`** from S3:
    - Verify `bucket` spelling and that it exists.
    - Ensure `region` matches the actual bucket region.
    - If using a custom `endpoint`, confirm the format and that DNS/SSL are correct.
- **Auth errors**: Confirm IAM permissions (`s3:PutObject`, `s3:PutObjectAcl` if using ACLs, KMS permissions if using `aws:kms`).
- **Path issues**: Inspect the final object key/path in logs (the plugin logs the exact S3 URL and SFTP path).
- **Verbose S3 tooling**: Set env var `TR_STORAGE_S3_VERBOSE=1` to enable libcurl’s verbose output for S3 calls.
- **Delete-after-upload didn’t fire**: Both S3 *and* SFTP (if enabled) must succeed for the given files.

---

## Security Notes

- Don’t commit plaintext secrets. Prefer environment-provisioned credentials (instances/roles), or keep configs out of source control.
- Limit IAM policies to required actions on the target bucket/prefix.

---

## License

MIT