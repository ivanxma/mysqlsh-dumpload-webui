# Version History

Current version: `1.0.10`

## 1.0.10 - 2026-05-14

Status: Current

- Added an `iptables` firewall fallback for Ubuntu OCI images that do not use `ufw` but reject inbound traffic after SSH by default.
- Persisted the generated `iptables` allowance to `/etc/iptables/rules.v4` when `iptables-save` and `/etc/iptables` are available.

## 1.0.9 - 2026-05-14

Status: Completed

- Made Oracle Linux firewall setup bounded and warn-only so a stuck `firewall-cmd` DBus call cannot leave OCI init scripts permanently in the installing state.
- Kept setup completion independent from firewall automation; operators still receive a manual port-opening message when firewall tooling fails or times out.

## 1.0.8 - 2026-05-14

Status: Completed

- Added Ubuntu 24.04 `libaio1t64` compatibility for embedded MySQL Server tarballs that still link to `libaio.so.1`.
- Created an app-local embedded MySQL compatibility library directory instead of modifying system library paths.
- Passed the compatibility library path to MySQL version checks, initialization, setup-time starts, and the local embedded MySQL systemd unit.

## 1.0.7 - 2026-05-14

Status: Completed

- Added a dedicated `mysql-shell-web-local-mysql.service` for the embedded socket-only local-admin MySQL Server on Linux deployments.
- Updated setup reruns and Auto-Update reruns to stop any ad-hoc embedded MySQL process and restart it under systemd before restarting the web service.
- Fixed OL9 Auto-Update/login recovery where the web service returned after update but `local-admin-profile` could not connect because `.data/run/mysql.sock` no longer existed.
- Updated deployment output and README service references to include the local embedded MySQL systemd unit.

## 1.0.6 - 2026-05-14

Status: Completed

- Reworked local-admin MySQL Server bootstrap to use embedded MySQL Community Server tarballs under `.embedded/mysql-server` instead of package-managed `mysqld`.
- Defaulted the embedded MySQL Server target to `9.7.0` with configurable tarball URLs, runtime directory, downloads directory, and required major series.
- Added embedded MySQL `8.4` LTS bridge-tarball support for old app-managed `8.0` datadirs before final `9.x` startup.
- Updated OCI Compute init-script handling to install only embedded-server prerequisites and pass embedded MySQL settings through to `setup.sh`.
- Updated Auto-Update pass-through so embedded MySQL Server version, URL, bridge, runtime, and download settings survive setup reruns.
- Kept embedded MySQL prerequisite installer output on stderr so only the selected basedir path is captured during setup.
- Split required and optional Oracle Linux embedded-MySQL prerequisite packages so missing optional ncurses compatibility packages do not block `xz` or `libaio` installation.

## 1.0.5 - 2026-05-14

Status: Completed

- Reworked local-admin MySQL Server bootstrap to require MySQL Server `9.x` by default instead of accepting an existing `8.0` server binary.
- Added MySQL Innovation repository setup for Oracle Linux and Ubuntu before installing `mysql-community-server` and `mysql-community-client`.
- Persisted and passed through `MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES` so setup reruns and Auto-Update keep the required server major series.
- Added a clear setup failure when the installed `mysqld` version does not match the required series.
- Restarted the app-managed local MySQL process during setup reruns so upgraded server binaries are actually used.
- Enforced MySQL Server series checks for existing socket-only local-admin profiles, even when no bootstrap password is supplied.
- Repaired ownership for generated runtime/profile/local-MySQL files when setup is rerun with root privileges for system deployment.
- Allowed app-user setup reruns to stop stale root-owned app-managed MySQL processes through `sudo` during recovery.
- Added an Oracle Linux MySQL 8.4 LTS bridge upgrade path so existing `8.0` app-managed datadirs can be upgraded before final MySQL `9.x` startup.

## 1.0.4 - 2026-05-14

Status: Completed

- Finalized OCI Compute init-script documentation for OL8, OL9, and Ubuntu with platform login users and explicit local admin bootstrap password placeholders.
- Prevented setup reruns without a bootstrap password from re-forcing local-admin password rotation.
- Documented old Auto-Update code-refresh compatibility for deployments without embedded MySQL or secure connection profile support.
- Added standalone verification and security vulnerability HTML reports.
- Verified shell syntax, Python syntax, JSON metadata, login secrecy, profile route protection, update status protection, and dependency audit.

## 1.0.3 - 2026-05-14

Status: Completed

- Added app-managed socket-only MySQL bootstrap flow in `setup.sh` for `local-admin-profile` provisioning.
- Resolved uploaded SSH keys server-side by key id without rendering filesystem paths.
- Generated and persisted a per-deployment Flask secret key through setup and start scripts.
- Changed update status restart-window token reads to require the custom polling header.
- Added old Auto-Update code-refresh-only propagation into `setup.sh`.
- Updated OCI Compute init script for OL and Ubuntu login-user defaults, explicit local admin password bootstrap, and non-traced logging.
- Pinned Paramiko to the upstream patched commit for `CVE-2026-44405` until a fixed PyPI release is available.

## 1.0.2 - 2026-05-14

Status: Historical

- Hardened login profile secrecy so unauthenticated login renders profile names only.
- Restricted profile management to `local-admin-profile` authenticated sessions.
- Added socket-mode local-admin profile metadata, first-login password-change flow, and restart-safe update polling token support.
- Removed `profiles.json` from source tracking and added `profiles.example.json` for safe starter metadata.
- Added Python 3.12 metadata, setup dependency audit hooks, update trust-boundary settings, and runtime-state git-ignore rules.

## 1.0.1

Status: Historical incremental

- Reserved for prior incremental deployment and auto-update changes discovered from repository history.

## 1.0.0

Status: Historical baseline

- Initial MySQL Shell Web application baseline with Flask, MySQL login profiles, Object Storage workflows, MySQL Shell operations, and deployment scripts.
