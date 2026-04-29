# MySQL Shell Dump/Load Web UI

`mysqlsh-dumpload-webui` is a Flask application for running MySQL Shell dump and load workflows from a browser session tied to a live MySQL login.

## What It Does

- Authenticates to MySQL with saved profiles and optional SSH tunneling
- Shows SSH tunnel controls on the Login and Profile pages and only enables SSH connection fields when `Use SSH Tunnel` is checked
- Runs a MySQL connection health check on the main application pages and redirects back to Login if the active connection is no longer valid
- Shows MySQL overview details including `server_uuid`, GTID state, event scheduler status, visible events, and replication/applier errors when present
- Manages OCI Object Storage settings for namespace, bucket, and prefix selection
- Creates and tracks Pre-Authenticated Requests (PARs) used by MySQL Shell operations
- Lets you browse, create, rename, and delete managed Object Storage prefixes
- Runs MySQL Shell `dumpInstance`, `dumpSchemas`, and `loadDump` jobs from the UI
- Saves reusable dump and load option profiles so common Shell option sets can be applied without re-entering fields
- Shows the generated MySQL Shell Python call using valid Python literals (`True` / `False`) and repo-relative `progressFile` paths when possible
- Tracks background MySQL Shell jobs with top-level operation tabs plus a consolidated History tab, retry details, connection profile names, and cleanup actions for completed jobs
- Uses an app-managed SSH tunnel for SSH-enabled MySQL Shell jobs and keeps that tunnel open for the full `mysqlsh` process
- Stores progress files, job metadata, and generated MySQL Shell config under a local runtime directory

## Repository Layout

- `app.py`: Flask entrypoint and route handlers
- `modules/`: MySQL connectivity, Object Storage helpers, session handling, and MySQL Shell script generation
- `templates/` and `static/`: UI templates and styling
- `setup.sh`: bootstrap script for Python environment, runtime config, and optional service setup
- `start_http.sh` / `start_https.sh`: local launch scripts
- `profiles.json`: starter MySQL connection profiles
- `object_storage.json`: local OCI Object Storage settings file created in the working copy and intentionally git-ignored
- `mysqlsh_option_profiles.json`: dump/load option profile store created on first use
- `runtime/`: embedded `mysqlsh`, progress files, and background job state

## Requirements

- Python 3
- Internet access during `./setup.sh` so the embedded MySQL Shell tarball can be downloaded
- Network access from the app host to the target MySQL server
- SSH private key access on the app host when using SSH-enabled profiles
- OCI credentials available when using Object Storage features

## Quick Start

1. Run `./setup.sh`.
   This downloads an embedded MySQL Shell Innovation tarball for the current macOS/Linux architecture into `runtime/mysqlsh/` and saves its path in `.runtime.env`.
2. Start the app with `./start_http.sh` or `./start_https.sh`.
3. Open the login page and sign in with a configured MySQL profile.
   Enable `Use SSH Tunnel` only when the MySQL server is reached through a jump host; the SSH fields stay disabled otherwise.
4. Protected pages re-test the current MySQL connection and return to Login if the session can no longer reach MySQL.
5. Save Object Storage settings.
6. Create a PAR for the dump/load target prefix.
7. Open the `dumpInstance`, `dumpSchemas`, or `loadDump` tab on the Shell Operations screen, then optionally apply a saved dump or load option profile before running the job.
8. Use the top-level History tab to reopen completed jobs, inspect retries/stdout/stderr, and clean up finished job files.

## setup.sh Port Setup

- `setup.sh` accepts listener ports either as positional arguments, flags, or environment variables.
- Positional form:
  - `./setup.sh <os_family> <deploy_mode> [http_port] [https_port]`
- Flag form:
  - `./setup.sh ubuntu both --http-port 8080 --https-port 8443`
- Environment form:
  - `HTTP_PORT=8080 HTTPS_PORT=8443 ./setup.sh ol9 both`
- In an interactive run, `setup.sh` prompts for the port or ports required by the selected deploy mode:
  - `http`: prompts for the HTTP port
  - `https`: prompts for the HTTPS port
  - `both`: prompts for both ports
  - `none`: does not prompt for listener ports
- If a required port was omitted, the prompt shows the current or default value as guidance, but you must enter an explicit numeric port. Pressing Enter does not silently accept the displayed value.
- Listener ports must be numeric values between `1` and `65535`.
- The selected ports are saved in `.runtime.env` as `DEFAULT_HTTP_PORT` and `DEFAULT_HTTPS_PORT`.
- `./start_http.sh` uses the saved HTTP port and `./start_https.sh` uses the saved HTTPS port.
- You can still override the saved port temporarily at launch time with `PORT=<port> ./start_http.sh` or `PORT=<port> ./start_https.sh`.

## Overview and Operations

- The Overview page has Environment, Events, Workflow, and Active PARs tabs.
- Environment shows server host/version, `server_id`, `server_uuid`, `gtid_mode`, `gtid_executed`, `gtid_purged`, and schema counts for the current connection.
- The Events tab shows event scheduler status plus visible events with enabled state, schedule, and last execution time.
- Replication and applier panels filter out normal channels/workers and only render rows that currently have errors.
- Shell Operations uses top-level `dumpInstance`, `dumpSchemas`, `loadDump`, `Option Profiles`, and `History` tabs.
- The History tab is consolidated for the current MySQL username and shows which saved connection profile and database were used for each recorded job.
- Dump and load option profiles are stored separately, so you can reuse common option sets without reselecting the PAR source/target.
- Finished jobs can be reopened later and cleaned up from the History view once they are no longer active.

## SSH Tunnel Behavior

- Saved profiles can define a jump host, SSH user, SSH port, and private key path.
- Profiles can also define an optional MySQL Shell SSH config file for long-running dump/load keepalive settings such as `ServerAliveInterval`.
- Profile-based MySQL connectivity is available from the login flow and schema discovery helpers.
- SSH-enabled MySQL Shell jobs keep an app-managed SSH forward open for the full `mysqlsh` process and rewrite the runtime request so `mysqlsh` connects through that local forwarded port.
- SSH-backed `loadDump` jobs automatically retry connection-loss failures by reusing the progress file. Parallel runs retry with `threads=1`; single-threaded runs retry once more with the same options.
- The private key path in the selected profile must exist on the host where this web app runs.

## Runtime and Local Files

- `.runtime.env`, `.venv/`, `runtime/`, `tls/`, and `par_registry.json` are local runtime artifacts and are git-ignored.
- `.runtime.env` stores the resolved embedded `MYSQLSH_BINARY` path used by the app and systemd services.
- `profiles.json` is an editable local default file checked into the repo.
- `object_storage.json` is intentionally local-only and git-ignored because it can contain sensitive tenancy, namespace, bucket, and folder metadata.
- `mysqlsh_option_profiles.json` is created on first use and stores saved dump/load option profiles.
- `runtime/progress/` stores generated progress files and transient request payloads.
- `runtime/jobs/` stores background job metadata plus `stdout`/`stderr` logs for each Shell operation.
- `loadDump` operations using PAR URLs require a progress file; the app pre-fills a path under `runtime/progress/` and renders it as a relative path when it lives under the repo root.

## Notes

- Prefix PAR URLs are stored locally because OCI returns the generated URL only at creation time.
- Empty Object Storage folders are virtual; planned empty prefixes are tracked until objects exist beneath them.
- Option profiles store Shell option fields only; PAR selection, schema selection, and other runtime context stay outside the saved profile.
