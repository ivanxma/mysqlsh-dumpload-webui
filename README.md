# MySQL Shell Dump/Load Web UI

`mysqlsh-dumpload-webui` is a Flask application for running MySQL Shell dump and load workflows from a browser session tied to a live MySQL login.

## What It Does

- Authenticates to MySQL with saved profiles and optional SSH tunneling
- Manages OCI Object Storage settings for namespace, bucket, and prefix selection
- Creates and tracks Pre-Authenticated Requests (PARs) used by MySQL Shell operations
- Lets you browse, create, rename, and delete managed Object Storage prefixes
- Runs MySQL Shell `dumpInstance`, `dumpSchemas`, and `loadDump` jobs from the UI
- Stores progress files and generated MySQL Shell config under a local runtime directory

## Repository Layout

- `app.py`: Flask entrypoint and route handlers
- `modules/`: MySQL connectivity, Object Storage helpers, session handling, and MySQL Shell script generation
- `templates/` and `static/`: UI templates and styling
- `setup.sh`: bootstrap script for Python environment, runtime config, and optional service setup
- `start_http.sh` / `start_https.sh`: local launch scripts
- `profiles.json`: starter MySQL connection profiles
- `object_storage.json`: starter OCI Object Storage defaults

## Requirements

- Python 3
- MySQL Shell installed on the host
- Network access from the app host to the target MySQL server
- OCI credentials available when using Object Storage features

## Quick Start

1. Run `./setup.sh`.
2. Start the app with `./start_http.sh` or `./start_https.sh`.
3. Open the login page and sign in with a configured MySQL profile.
4. Save Object Storage settings.
5. Create a PAR for the dump/load target prefix.
6. Run `dumpInstance`, `dumpSchemas`, or `loadDump` from the Shell Operations screen.

## Runtime and Local Files

- `.runtime.env`, `.venv/`, `runtime/`, `tls/`, and `par_registry.json` are local runtime artifacts and are git-ignored.
- `profiles.json` and `object_storage.json` are intended as editable local defaults checked into the repo.
- `loadDump` operations using PAR URLs require a progress file; the app pre-fills a path under `runtime/progress/`.

## Notes

- Prefix PAR URLs are stored locally because OCI returns the generated URL only at creation time.
- Empty Object Storage folders are virtual; planned empty prefixes are tracked until objects exist beneath them.
