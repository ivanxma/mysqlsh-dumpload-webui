# MySQL Shell Dump/Load Web UI

`mysqlsh-dumpload-webui` is a Flask application for running MySQL Shell dump and load workflows from a browser session tied to a live MySQL login.

## What It Does

- Authenticates to MySQL with saved profiles and optional SSH tunneling
- Shows SSH tunnel controls on the Login and Profile pages and only enables SSH connection fields when `Use SSH Tunnel` is checked
- Runs a MySQL connection health check on the main application pages and redirects back to Login if the active connection is no longer valid
- Shows MySQL overview details including `server_uuid`, GTID state, and replication/applier errors when present
- Adds `Admin > DB Admin` with table-style event controls, downloadable primary-key reports, and selected-row primary-key fix actions for the current MySQL connection
- Adds `Admin > OCI Configuration` for OCI config setup, Object Storage bucket scope, and an effective settings review
- Creates and tracks Pre-Authenticated Requests (PARs) used by MySQL Shell operations
- Lets you browse, create, rename, delete, and download managed Object Storage prefix and PAR tables with selected-row actions
- Runs MySQL Shell `dumpInstance`, `dumpSchemas`, and `loadDump` jobs from the UI
- Saves reusable dump and load option profiles so common Shell option sets can be applied without re-entering fields
- Lets dump option profiles build include/exclude filters from selector-driven tabs for schemas, tables, users, events, routines, triggers, and libraries
- Provides separate HTML validation references for dump and load workflows from the MySQL Shell menu
- Shows the generated MySQL Shell Python call using valid Python literals (`True` / `False`) and repo-relative `progressFile` paths when possible
- Tracks background MySQL Shell jobs with top-level operation tabs plus a consolidated History tab, full Job ID links, retry details, connection profile names, selected-row cleanup, and downloadable history
- Uses an app-managed SSH tunnel for SSH-enabled MySQL Shell jobs and keeps that tunnel open for the full `mysqlsh` process
- Stores progress files, job metadata, and generated MySQL Shell config under a local runtime directory
- Shows the active user, profile, app version, connection, bucket, and logout action from the top-right user icon menu after login
- Adds `Admin > Update MySQL Shell Web` to refresh the Git checkout, rerun setup, and restart the configured service

## Repository Layout

- `app.py`: Flask entrypoint and route handlers
- `mysql_shell_web_update_worker.py`: background updater used by the Admin update page
- `modules/`: MySQL connectivity, OCI configuration, Object Storage helpers, session handling, and MySQL Shell script generation
- `templates/` and `static/`: UI templates and styling
- `setup.sh`: bootstrap script for Python environment, runtime config, optional service setup, and fresh-host repo bootstrap through `curl | sh`
- `start_http.sh` / `start_https.sh`: local launch scripts
- `profiles.json`: starter MySQL connection profiles
- `object_storage.json`: local OCI configuration mode and Object Storage scope file created in the working copy and intentionally git-ignored
- `mysqlsh_option_profiles.json`: dump/load option profile store created on first use
- `runtime/`: embedded `mysqlsh`, progress files, and background job state

## Requirements

- Python 3
- Internet access during bootstrap so the Git repo and embedded MySQL Shell tarball can be downloaded
- `sudo` or root access when the host needs package installation, firewall changes, or systemd service setup
- MySQL Server `9.x` for the app-managed socket-only `local-admin-profile` bootstrap. `setup.sh` downloads an embedded MySQL Community Server tarball into `.embedded/mysql-server/` and does not rely on package-managed `mysqld`; it rejects an existing OS `8.0` `mysqld` for this local admin store.
  Existing app-managed datadirs initialized by MySQL `8.0` are bridged through an embedded MySQL `8.4` LTS tarball before final MySQL `9.x` startup because direct `8.0` to `9.x` server upgrades are not valid.
- Network access from the app host to the target MySQL server
- SSH private key access on the app host when using SSH-enabled profiles
- OCI credentials available when using Object Storage features

## Setup

### Existing Clone

1. Clone the repository or work from an existing checkout.
2. Run `./setup.sh`.
   This creates `.venv/`, installs Python dependencies, downloads an embedded MySQL Shell Innovation tarball into `runtime/mysqlsh/`, and saves the resolved runtime settings in `.runtime.env`.
   For secured local-admin profile bootstrap, setup also downloads embedded MySQL Server, default `MYSQL_SERVER_EMBEDDED_VERSION=9.7.0`, and requires the configured server series, default `MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES=9`.
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

For an OCI Compute deployment, create a Linux VM and let the instance bootstrap itself from the Git repo during first boot. Keep tenancy-specific values such as compartment OCID, subnet OCID, image OCID, and SSH public key as your own deployment inputs.

Instance values to choose before creation:

- Compartment: your target compartment name or OCID
- Platform: Oracle Linux 8, Oracle Linux 9, or Ubuntu
- Shape and image: select a supported shape and matching platform image
- Network: VCN, subnet, public IP setting, and security list or NSG
- SSH public key: the key used for the expected login user (`opc` on Oracle Linux, `ubuntu` on Ubuntu)
- Deploy mode: `http`, `https`, or `both`
- Ingress: TCP `22` for SSH plus TCP `80`, `443`, or your chosen listener ports
- Local admin bootstrap password: an explicit temporary password passed as `LOCAL_MYSQL_ADMIN_PASSWORD`

In the OCI Console, create the instance, open `Advanced options` > `Management`, and paste the matching initialization script into `Initialization script`. The script installs `git` if needed, clones or refreshes this repository, runs `setup.sh`, records install state, and installs a login banner for setup progress.

The reusable init script lives at `oci_compute_init.sh`. It records:

- Init log: `/var/log/mysql-shell-web-init.log`
- State directory: `/var/lib/mysql-shell-web-init`
- Login banner: `/etc/profile.d/mysql-shell-web-login-banner.sh`
- Default HTTPS service: `mysql-shell-web-https.service`

If you do not set `SSL_CERT_FILE` and `SSL_KEY_FILE`, `setup.sh` generates a self-signed certificate automatically for the HTTPS service. Generated systemd units grant `CAP_NET_BIND_SERVICE` only when the configured listener port is below `1024`, so the non-root service account can listen on privileged ports such as `80` and `443`.

The OCI init script requires `LOCAL_MYSQL_ADMIN_PASSWORD` when `LOCAL_MYSQL_BOOTSTRAP=1`. The password is passed only through the setup process environment. Do not put it in Git, `.runtime.env`, shell history, or long-lived scripts. After setup, log in with `local-admin-profile`, user `localadmin`, and the temporary password; the app forces an immediate password change and logs out.

#### Oracle Linux 8

Expected login user: `opc`

Paste this OL8 init wrapper into the OCI initialization script field:

```bash
#!/bin/bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y curl
  else
    yum install -y curl
  fi
fi

curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/oci_compute_init.sh \
  -o /tmp/mysql-shell-web-oci-compute-init.sh

APP_REPO="https://github.com/ivanxma/mysqlsh-dumpload-webui.git" \
APP_DIR="/home/opc/mysqlsh-dumpload-webui" \
APP_USER="opc" \
APP_GROUP="opc" \
OS_FAMILY="ol8" \
DEPLOY_MODE="https" \
HTTP_PORT="80" \
HTTPS_PORT="443" \
SERVICE_NAME="mysql-shell-web-https.service" \
LOCAL_MYSQL_ADMIN_PASSWORD="<temporary-localadmin-password>" \
bash /tmp/mysql-shell-web-oci-compute-init.sh
```

Verify OL8 deployment:

```bash
ssh -i <ssh-private-key> opc@<instance-public-ip>
sudo systemctl status mysql-shell-web-https.service
sudo tail -n 100 /var/log/mysql-shell-web-init.log
curl -k -I https://<instance-public-ip>/
```

#### Oracle Linux 9

Expected login user: `opc`

Paste this OL9 init wrapper into the OCI initialization script field:

```bash
#!/bin/bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y curl
  else
    yum install -y curl
  fi
fi

curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/oci_compute_init.sh \
  -o /tmp/mysql-shell-web-oci-compute-init.sh

APP_REPO="https://github.com/ivanxma/mysqlsh-dumpload-webui.git" \
APP_DIR="/home/opc/mysqlsh-dumpload-webui" \
APP_USER="opc" \
APP_GROUP="opc" \
OS_FAMILY="ol9" \
DEPLOY_MODE="https" \
HTTP_PORT="80" \
HTTPS_PORT="443" \
SERVICE_NAME="mysql-shell-web-https.service" \
LOCAL_MYSQL_ADMIN_PASSWORD="<temporary-localadmin-password>" \
bash /tmp/mysql-shell-web-oci-compute-init.sh
```

Verify OL9 deployment:

```bash
ssh -i <ssh-private-key> opc@<instance-public-ip>
sudo systemctl status mysql-shell-web-https.service
sudo tail -n 100 /var/log/mysql-shell-web-init.log
curl -k -I https://<instance-public-ip>/
```

#### Ubuntu

Expected login user: `ubuntu`

Paste this Ubuntu init wrapper into the OCI initialization script field:

```bash
#!/bin/bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y curl
fi

curl -fsSL https://raw.githubusercontent.com/ivanxma/mysqlsh-dumpload-webui/main/oci_compute_init.sh \
  -o /tmp/mysql-shell-web-oci-compute-init.sh

APP_REPO="https://github.com/ivanxma/mysqlsh-dumpload-webui.git" \
APP_DIR="/home/ubuntu/mysqlsh-dumpload-webui" \
APP_USER="ubuntu" \
APP_GROUP="ubuntu" \
OS_FAMILY="ubuntu" \
DEPLOY_MODE="https" \
HTTP_PORT="80" \
HTTPS_PORT="443" \
SERVICE_NAME="mysql-shell-web-https.service" \
LOCAL_MYSQL_ADMIN_PASSWORD="<temporary-localadmin-password>" \
bash /tmp/mysql-shell-web-oci-compute-init.sh
```

Verify Ubuntu deployment:

```bash
ssh -i <ssh-private-key> ubuntu@<instance-public-ip>
sudo systemctl status mysql-shell-web-https.service
sudo tail -n 100 /var/log/mysql-shell-web-init.log
curl -k -I https://<instance-public-ip>/
```

The login banner is silent for non-interactive shells. For the platform login user, it shows `Please wait until installation to be completed.` while setup is running, `MySQL Shell Web setup has been completed` after success, or a failure message pointing to `/var/log/mysql-shell-web-init.log`.

Once setup finishes, open the app in a browser and continue with the normal workflow:

1. Save OCI Configuration settings.
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
- On Linux, the systemd units generated by `setup.sh` include `CAP_NET_BIND_SERVICE`, which allows the configured non-root service user to bind to ports below `1024` such as `80` and `443`.

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
- Dump validation is enabled after a dump option profile is selected. It checks tables without primary keys, table engines, Lakehouse tables, enabled events, authentication plugins, table character sets, and column character sets.
- `dumpInstance` validates the selected option profile against all accessible user schemas, excluding system schemas and `mysql_%` schemas.
- `dumpSchemas` validates the selected option profile only after schemas are selected and the `Validation` action is run.
- Dump and load option profiles are stored separately, so you can reuse common option sets without reselecting the PAR source/target.
- Dump option profiles now include selector-driven filter tabs for schemas, tables, users, events, routines, triggers, and libraries, with mutually exclusive include/exclude placement per selected object.
- The History tab is consolidated for the current MySQL username and shows which saved connection profile and database were used for each recorded job.
- Finished jobs can be reopened later and cleaned up from the History view once they are no longer active.

## Validation References

The application includes separate HTML reference pages for validation rules and mitigation guidance:

- `MySQL Shell > Dump Validation` at `/mysql-shell/validation/dump`
- `MySQL Shell > Load Validation` at `/mysql-shell/validation/load`

Dump validation rules cover primary key checks, non-InnoDB engines, Lakehouse table filtering, enabled events, `mysql_native_password` accounts, and table or column character set drift from `utf8mb4` / `utf8mb4_0900_ai_ci`.

Lakehouse filtering is profile-aware. The generated `excludeTables` list includes only Lakehouse tables that remain in scope after the selected dump profile's `includeSchemas`, `excludeSchemas`, `includeTables`, and `excludeTables` filters are applied. If `includeTables` overlaps with the Lakehouse tables that would be excluded, the UI reports an option conflict and blocks the dump until the overlap is resolved.

Load validation rules cover target DB System identity, `server_uuid`, GTID mode, `gtid_executed`, `gtid_purged`, mixed server UUID warnings, and risky load option profile settings such as `loadUsers` off, `updateGTIDSet=off`, or `resetProgress` off.

## OCI Configuration

Use `Admin > OCI Configuration` to manage the OCI settings used by PAR Manager, Folders, and MySQL Shell dump/load workflows. The module is served from `/admin/oci-configuration`; the previous `/admin/object-storage` route redirects there for compatibility.

The page uses three tabs:

- `OCI Config`: choose the OCI configuration source with the left-side radio panel.
- `Object Storage`: set the region override, namespace, bucket name, and root prefix.
- `Review`: confirm the effective config file, active profile, readability status, and bucket target.

The `OCI Config` tab supports two modes:

- `Define config file`: reference an existing OCI config file, usually `~/.oci/config`, and select or enter the profile name. The default profile name is `HeatWave_Demo`.
- `Store in local folder`: enter the profile, region, tenancy OCID, user OCID, and fingerprint, then upload the OCI API private key file. The app writes `runtime/oci/config` and stores the uploaded key beside it with restricted file permissions.

The Object Storage bucket settings remain local to the checkout in `object_storage.json`. The app expands the selected config source at runtime so downstream Object Storage and MySQL Shell flows use the same effective OCI profile.

## SSH Tunnel Behavior

- Profile management is available only after signing in through `local-admin-profile`.
- Saved profiles can define a jump host, SSH user, SSH port, and an uploaded private key.
- Profiles can also define an optional MySQL Shell SSH config file for long-running dump/load keepalive settings such as `ServerAliveInterval`.
- Profile-based MySQL connectivity is available from the login flow and schema discovery helpers.
- SSH-enabled MySQL Shell jobs keep an app-managed SSH forward open for the full `mysqlsh` process and rewrite the runtime request so `mysqlsh` connects through that local forwarded port.
- SSH-backed `loadDump` jobs automatically retry connection-loss failures by reusing the progress file. Parallel runs retry with `threads=1`; single-threaded runs retry once more with the same options.
- Uploaded SSH private keys are stored under `profile_ssh_keys/` with owner-only permissions and the key path is not rendered back to the browser.

## Login Session Security

- The login page displays only saved profile names and an optional default username. It does not render hostnames, ports, sockets, SSH jump hosts, or key paths before authentication.
- `local-admin-profile` is socket-only. First login after bootstrap forces a password change and logs the user out after the password is changed.
- The browser-visible Flask session cookie contains only app scope/version markers and an opaque server-side session id.
- MySQL usernames, MySQL passwords, and active connection profiles are held in server-side memory for the active app session and are cleared on logout, connection loss, or login replacement.
- If the server restarts and the in-memory login entry is gone, authenticated requests are redirected to the login page instead of reusing stale cookie state.
- Login and authenticated pages send `Cache-Control: no-store` headers to reduce browser and proxy retention of credential-bearing pages.

## Runtime and Local Files

- `.runtime.env`, `.venv/`, `runtime/`, `.data/`, `.embedded/`, `tls/`, `profile_ssh_keys/`, generated `etc/my.cnf`, and `par_registry.json` are local runtime artifacts and are git-ignored.
- `appver.json` stores the running application version shown in the authenticated header and Admin update page.
- `.runtime.env` stores the resolved embedded `MYSQLSH_BINARY` path used by the app and systemd services.
- `secured_connection_profile_setup.sh` creates or repairs the non-secret `local-admin-profile` metadata and prepares the app-owned SSH key directory.
- `setup.sh` persists deploy mode, listener ports, Python policy, secure-cookie mode, version-check URL/CA-bundle overrides, and update trust-boundary settings in `.runtime.env`.
- `profiles.example.json` shows the safe starter shape. `profiles.json` should remain local to each deployment; do not commit environment-specific SSH hosts, usernames, key paths, tokens, or passwords.
- `profiles.json` is git-ignored and should stay deployment-local so saved profiles do not block Admin auto-update.
- `object_storage.json` is intentionally local-only and git-ignored because it can contain sensitive tenancy, namespace, bucket, and folder metadata.
- `mysqlsh_option_profiles.json` is created on first use and stores saved dump/load option profiles.
- `runtime/progress/` stores generated progress files and transient request payloads.
- `runtime/jobs/` stores background job metadata plus `stdout`/`stderr` logs for each Shell operation.
- `runtime/oci/config` stores the application-local OCI config when `Admin > OCI Configuration` is set to `Store in local folder`.
- `loadDump` operations using PAR URLs require a progress file; the app pre-fills a path under `runtime/progress/` and renders it as a relative path when it lives under the repo root.

## Admin Auto-Update

Use `Admin > Update MySQL Shell Web` after logging in to update the running application from its current Git branch.

On successful login, the app compares local `appver.json` with the repository version file. Set `MYSQL_SHELL_WEB_VERSION_URL` when the repository version URL cannot be inferred from `remote.origin.url`; GitHub raw URLs are normalized to the GitHub contents API to avoid stale raw CDN reads. Set `MYSQL_SHELL_WEB_VERSION_CA_BUNDLE` when the host uses a private trust store. If the repository version differs, the user is redirected to `Admin > Update MySQL Shell Web`. The update page also provides `Retrieve Repo Version` to rerun that version check without starting an update.

The updater:

- requires a clean Git worktree before it pulls changes, ignoring only explicit local deployment state such as `.runtime.env`, `.data/`, `runtime/`, `tls/`, `profile_ssh_keys/`, and generated security/audit output
- verifies `MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL` and `MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH` when those trust-boundary values are set
- runs `git fetch --all --prune` and `git pull --ff-only`
- uses `MYSQL_SHELL_WEB_OS_FAMILY`, `.runtime.env OS_FAMILY`, or host detection to choose the `setup.sh` OS family
- reruns `setup.sh` with saved host, port, TLS, Python, embedded MySQL Server tarball, dependency-audit, local-admin bootstrap, and update trust-boundary defaults
- restarts the active `mysql-shell-web-http.service`, `mysql-shell-web-https.service`, or both when systemd is in use
- stores progress state and logs under `runtime/updates/` so the update page can recover after a restart
- polls update status with a job-scoped token header so status reads can survive the brief server-side session reset during restart

After `local-admin-profile` exists, starting updates requires a session authenticated through that profile. Older update pages that cannot prompt for the local admin password can perform a code refresh first; after restart, rerun Auto-Update from the refreshed page and provide the one-time `localadmin` temporary password so setup can repair the socket-only local admin profile.

For a full update from the web UI, the running service account needs passwordless `sudo` for setup steps that refresh systemd units, firewall rules, and TLS file ownership. If passwordless `sudo` is unavailable, the updater falls back to `SKIP_PRIVILEGED_SETUP=1`; it still refreshes the repository and Python environment, then restarts by terminating the current service process and letting systemd recover it. Run `./setup.sh` manually later if the pulled changes require privileged service, firewall, or TLS ownership changes.

## Notes

- Prefix PAR URLs are stored locally because OCI returns the generated URL only at creation time.
- Empty Object Storage folders are virtual; planned empty prefixes are tracked until objects exist beneath them.
- Option profiles store Shell option fields only; PAR selection, schema selection, and other runtime context stay outside the saved profile.
