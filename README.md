# MySQL Shell Dump/Load Web UI

`mysqlsh-dumpload-webui` is a Flask application for running MySQL Shell dump and load workflows from a browser session tied to a live MySQL login.

## What It Does

- Authenticates to MySQL with saved profiles and optional SSH tunneling
- Shows SSH tunnel controls on the Login and Profile pages and only enables SSH connection fields when `Use SSH Tunnel` is checked
- Runs a MySQL connection health check on the main application pages and redirects back to Login if the active connection is no longer valid
- Shows MySQL overview details including `server_uuid`, GTID state, and replication/applier errors when present
- Adds `Admin > DB Admin` with event controls and primary key auditing/fix actions for the current MySQL connection
- Manages OCI Object Storage settings for namespace, bucket, and prefix selection
- Creates and tracks Pre-Authenticated Requests (PARs) used by MySQL Shell operations
- Lets you browse, create, rename, and delete managed Object Storage prefixes
- Runs MySQL Shell `dumpInstance`, `dumpSchemas`, and `loadDump` jobs from the UI
- Saves reusable dump and load option profiles so common Shell option sets can be applied without re-entering fields
- Lets dump option profiles build include/exclude filters from selector-driven tabs for schemas, tables, users, events, routines, triggers, and libraries
- Shows the generated MySQL Shell Python call using valid Python literals (`True` / `False`) and repo-relative `progressFile` paths when possible
- Tracks background MySQL Shell jobs with top-level operation tabs plus a consolidated History tab, retry details, connection profile names, and cleanup actions for completed jobs
- Uses an app-managed SSH tunnel for SSH-enabled MySQL Shell jobs and keeps that tunnel open for the full `mysqlsh` process
- Stores progress files, job metadata, and generated MySQL Shell config under a local runtime directory

## Repository Layout

- `app.py`: Flask entrypoint and route handlers
- `modules/`: MySQL connectivity, Object Storage helpers, session handling, and MySQL Shell script generation
- `templates/` and `static/`: UI templates and styling
- `setup.sh`: bootstrap script for Python environment, runtime config, optional service setup, and fresh-host repo bootstrap through `curl | sh`
- `start_http.sh` / `start_https.sh`: local launch scripts
- `profiles.json`: starter MySQL connection profiles
- `object_storage.json`: local OCI Object Storage settings file created in the working copy and intentionally git-ignored
- `mysqlsh_option_profiles.json`: dump/load option profile store created on first use
- `runtime/`: embedded `mysqlsh`, progress files, and background job state

## Requirements

- Python 3
- Internet access during bootstrap so the Git repo and embedded MySQL Shell tarball can be downloaded
- `sudo` or root access when the host needs package installation, firewall changes, or systemd service setup
- Network access from the app host to the target MySQL server
- SSH private key access on the app host when using SSH-enabled profiles
- OCI credentials available when using Object Storage features

## Setup

### Existing Clone

1. Clone the repository or work from an existing checkout.
2. Run `./setup.sh`.
   This creates `.venv/`, installs Python dependencies, downloads an embedded MySQL Shell Innovation tarball into `runtime/mysqlsh/`, and saves the resolved runtime settings in `.runtime.env`.
3. Start the app with `./start_http.sh` or `./start_https.sh`.
4. Open the login page and sign in with a configured MySQL profile.
   Enable `Use SSH Tunnel` only when the MySQL server is reached through a jump host; the SSH fields stay disabled otherwise.

### Fresh Host Bootstrap With `curl | sh`

Use this when the host does not already have a local repo checkout:

```sh
curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/setup.sh | sh -s -- ol9 http --http-port 80
```

The streamed bootstrap path does the following before handing off to the repo-local `setup.sh`:

- Installs `git` automatically when it is missing on Oracle Linux, Ubuntu, and macOS with Homebrew available
- Clones `https://github.com/ivanxma/mysqlsh-dumpload-webui.git`
- If the target clone directory already exists, renames it to `<folder>.<YYYYmmddHHMMSS>`
- Re-runs the cloned `setup.sh` with the same arguments so the normal install flow continues

Optional bootstrap overrides:

- `BOOTSTRAP_REPO_URL`: clone from a different Git URL
- `BOOTSTRAP_CLONE_DIR`: clone into a different folder name
- `BOOTSTRAP_PARENT_DIR`: clone into a different parent directory

Example:

```sh
BOOTSTRAP_PARENT_DIR="$HOME/apps" \
BOOTSTRAP_CLONE_DIR=mysqlsh-dumpload-webui \
curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/setup.sh | sh -s -- ol9 both --http-port 80 --https-port 443
```

### OCI Compute Instance

For an OCI Compute deployment, create a Linux VM and let the instance bootstrap itself from the Git repo during first boot.

1. Create an OCI Compute instance with a supported image such as Oracle Linux 9 or Ubuntu.
2. Attach a public IP or provide private access through a bastion host.
3. Add ingress rules for `TCP/22` and the app listener ports you plan to use such as `80` and `443`.
4. In the instance creation flow, open the initialization or cloud-init script field and paste a script like the following.
5. After the instance finishes provisioning, connect over SSH and check the generated `.runtime.env` plus the systemd services created by `setup.sh`. For the example below, the main unit is `mysql-shell-web-http.service`.

Example init script for Oracle Linux 9:

```bash
#!/bin/bash
set -euxo pipefail

APP_REPO="https://github.com/ivanxma/mysqlsh-dumpload-webui.git"
APP_DIR="/home/opc/mysqlsh-dumpload-webui"
APP_USER="opc"
APP_GROUP="opc"
OS_FAMILY="ol9"

if command -v git >/dev/null 2>&1; then
  :
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y git
elif command -v yum >/dev/null 2>&1; then
  yum install -y git
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y git
else
  echo "git could not be installed automatically" >&2
  exit 1
fi

if [ -d "$APP_DIR" ]; then
  mv "$APP_DIR" "${APP_DIR}.$(date +%Y%m%d%H%M%S)"
fi

sudo -u "$APP_USER" git clone "$APP_REPO" "$APP_DIR"
cd "$APP_DIR"
sudo -u "$APP_USER" env \
  HOST=0.0.0.0 \
  SERVICE_USER="$APP_USER" \
  SERVICE_GROUP="$APP_GROUP" \
  bash ./setup.sh "$OS_FAMILY" http --http-port 80
```

Adjust `APP_USER` and `OS_FAMILY` when you use Ubuntu instead of Oracle Linux:

- Oracle Linux 9: `APP_USER=opc`, `APP_GROUP=opc`, `OS_FAMILY=ol9`
- Ubuntu: `APP_USER=ubuntu`, `APP_GROUP=ubuntu`, `OS_FAMILY=ubuntu`

Once setup finishes, open the app in a browser and continue with the normal workflow:

1. Save Object Storage settings.
2. Create a PAR for the dump/load target prefix.
3. Open the `dumpInstance`, `dumpSchemas`, or `loadDump` tab on the Shell Operations screen, then optionally apply a saved dump or load option profile before running the job.
4. Define reusable dump filters from `Option Profiles` when you want selector-driven include/exclude lists for schemas, tables, users, events, routines, triggers, or libraries.
5. Use the top-level History tab to reopen completed jobs, inspect retries, and clean up finished job files.

## setup.sh Port Setup

- `setup.sh` accepts listener ports either as positional arguments, flags, or environment variables.
- Positional form:
  - `./setup.sh <os_family> <deploy_mode> [http_port] [https_port]`
- Flag form:
  - `./setup.sh ubuntu both --http-port 8080 --https-port 8443`
- Environment form:
  - `HTTP_PORT=8080 HTTPS_PORT=8443 ./setup.sh ol9 both`
- Streamed bootstrap form:
  - `curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/setup.sh | sh -s -- ubuntu both --http-port 8080 --https-port 8443`
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

- The Overview page has Environment, Workflow, and Active PARs tabs.
- Environment shows server host/version, `server_id`, `server_uuid`, `gtid_mode`, `gtid_executed`, `gtid_purged`, and schema counts for the current connection.
- Replication and applier panels filter out normal channels/workers and only render rows that currently have errors.
- `Admin > DB Admin` has `Event Tab` and `Primary Key Check` tabs.
- `Event Tab` shows event scheduler status, visible events, schedules, and enable/disable actions.
- `Primary Key Check` shows database/table counters plus detail panels for tables with and without primary keys.
- The primary key fix flow supports bulk row selection and applies one fix request across the selected tables.
- When a selected table is partitioned, the automatic primary key fix includes the partition columns together with the `AUTO_INCREMENT` column or the generated invisible `my_row_id` column.
- Shell Operations uses top-level `dumpInstance`, `dumpSchemas`, `loadDump`, `Option Profiles`, and `History` tabs.
- `dumpInstance` always shows validation counters for tables without primary keys, `ENGINE=InnoDB`, `ENGINE=Lakehouse`, `secondary_engine=rapid`, and enabled events.
- `dumpSchemas` shows the same validation counters only after schemas are selected and the `Validation` action is run.
- Dump and load option profiles are stored separately, so you can reuse common option sets without reselecting the PAR source/target.
- Dump option profiles now include selector-driven filter tabs for schemas, tables, users, events, routines, triggers, and libraries, with mutually exclusive include/exclude placement per selected object.
- The History tab is consolidated for the current MySQL username and shows which saved connection profile and database were used for each recorded job.
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
- `profiles.json` is an editable local default file checked into the repo, but environment-specific SSH hosts, usernames, and private key paths should stay local and should not be committed back.
- `object_storage.json` is intentionally local-only and git-ignored because it can contain sensitive tenancy, namespace, bucket, and folder metadata.
- `mysqlsh_option_profiles.json` is created on first use and stores saved dump/load option profiles.
- `runtime/progress/` stores generated progress files and transient request payloads.
- `runtime/jobs/` stores background job metadata plus `stdout`/`stderr` logs for each Shell operation.
- `loadDump` operations using PAR URLs require a progress file; the app pre-fills a path under `runtime/progress/` and renders it as a relative path when it lives under the repo root.

## Notes

- Prefix PAR URLs are stored locally because OCI returns the generated URL only at creation time.
- Empty Object Storage folders are virtual; planned empty prefixes are tracked until objects exist beneath them.
- Option profiles store Shell option fields only; PAR selection, schema selection, and other runtime context stay outside the saved profile.
