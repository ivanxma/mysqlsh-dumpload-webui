# Version History

Current version: `1.0.5`

## 1.0.5 - 2026-05-14

Status: Current

- Reworked local-admin MySQL Server bootstrap to require MySQL Server `9.x` by default instead of accepting an existing `8.0` server binary.
- Added MySQL Innovation repository setup for Oracle Linux and Ubuntu before installing `mysql-community-server` and `mysql-community-client`.
- Persisted and passed through `MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES` so setup reruns and Auto-Update keep the required server major series.
- Added a clear setup failure when the installed `mysqld` version does not match the required series.
- Restarted the app-managed local MySQL process during setup reruns so upgraded server binaries are actually used.
- Enforced MySQL Server series checks for existing socket-only local-admin profiles, even when no bootstrap password is supplied.
- Repaired ownership for generated runtime/profile/local-MySQL files when setup is rerun with root privileges for system deployment.

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
