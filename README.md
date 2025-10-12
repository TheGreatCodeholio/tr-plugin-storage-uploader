
# Trunk Recorder – Storage Uploader Plugin

Uploads call audio (and optional JSON metadata) **per-system** to S3 and/or SFTP/SCP.
Configuration mirrors the RDIO plugin pattern: a `systems[]` array keyed by `shortName`.

## Highlights
- Keeps an **async worker** and retry/backoff so uploads don't block decoding.
- Per-system overrides: each `shortName` can enable S3 and/or SFTP with its own paths, bucket, host, etc.
- Uses libcurl for **S3 (SigV4)** and **SFTP/SCP** (no heavy SDK).

## Build & Install

Place this repo under `trunk-recorder/user_plugins/storage-uploader/`, then from your TR build dir:

```bash
cmake ..
sudo make install
```

This installs `libstorage_uploader.so` to TR's plugin library path.

## Configure

```jsonc
{
  "plugins": [
    {
      "name": "Storage Uploader",
      "library": "libstorage_uploader.so",
      "systems": [
        {
          "shortName": "chemung",
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
          "uploadJson": true,
          "deleteAfterUpload": false,
          "sftp": {
            "enabled": true,
            "host": "sftp.steuben.net",
            "port": 22,
            "username": "upload",
            "remote_root": "/srv/steuben/calls",
            "prefix_template": "steuben/{yyyy}/{MM}/{dd}/{basename}"
          }
        }
      ]
    }
  ]
}
```

Only systems listed in `systems[]` are uploaded. Others are skipped.

## Notes
- The plugin selects the **converted** path if present (e.g., m4a) or falls back to the original `filename`.
- Template tokens available: `{shortName}`, `{yyyy}`, `{MM}`, `{dd}`, `{basename}`.
- See `include/config.hpp` for all S3/SFTP options (timeouts, SSE/KMS, ACL, storage class, etc.).

## License
MIT
