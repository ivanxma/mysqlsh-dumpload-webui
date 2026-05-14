#!/usr/bin/env python3
"""Background updater for the MySQL Shell Web Admin update page."""

import argparse
import grp
import json
import os
import platform
import pwd
import shlex
import shutil
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path


APP_SLUG = "mysql-shell-web"
HTTP_SERVICE = f"{APP_SLUG}-http.service"
HTTPS_SERVICE = f"{APP_SLUG}-https.service"
SUPPORTED_OS_FAMILIES = {"ol8", "ol9", "ubuntu", "macos"}
LOCAL_STATE_PREFIXES = (
    ".flask_secret_key",
    ".runtime.env",
    ".data/",
    ".embedded/",
    "runtime/",
    "tls/",
    "profile_ssh_keys/",
    ".cache/",
    "profiles.json",
    "object_storage.json",
    "par_registry.json",
    "mysqlsh_option_profiles.json",
    "etc/my.cnf",
)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class UpdateWorker:
    def __init__(self, repo_dir, status_file, log_file, service_pid=None):
        self.repo_dir = Path(repo_dir).resolve()
        self.status_file = Path(status_file).resolve()
        self.log_file = Path(log_file).resolve()
        self.service_pid = self.normalize_pid(service_pid)
        self.status = self.load_status()

    @staticmethod
    def normalize_pid(value):
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return None
        return normalized if normalized > 0 else None

    def load_status(self):
        if not self.status_file.exists():
            return {}
        try:
            payload = json.loads(self.status_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def write_status(self, **updates):
        self.status.update(updates)
        self.status["updated_at"] = utc_now_iso()
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.status_file.with_suffix(".tmp")
        temp_file.write_text(json.dumps(self.status, indent=2, ensure_ascii=False), encoding="utf-8")
        temp_file.replace(self.status_file)
        return self.status

    def append_log(self, message):
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(str(message or ""))
            if not str(message or "").endswith("\n"):
                handle.write("\n")

    def repair_permissions(self):
        for path in (
            self.repo_dir / ".flask_secret_key",
            self.repo_dir / ".runtime.env",
            self.repo_dir / "profiles.json",
            self.repo_dir / "object_storage.json",
            self.status_file,
            self.log_file,
        ):
            try:
                if path.exists():
                    path.chmod(0o600)
            except OSError:
                pass
        for path in (self.repo_dir / "profile_ssh_keys", self.repo_dir / "tls", self.repo_dir / ".data"):
            try:
                if path.exists():
                    path.chmod(0o700)
            except OSError:
                pass

    def log_step(self, step, message):
        self.write_status(state="running", step=step, message=message)
        self.append_log(f"[{utc_now_iso()}] {message}")

    def run_command(self, command, *, cwd=None, env=None):
        display_command = shlex.join(command)
        self.append_log(f"$ {display_command}")
        process = subprocess.Popen(
            command,
            cwd=str(cwd or self.repo_dir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            self.append_log(line.rstrip("\n"))
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"Command failed with exit code {return_code}: {display_command}")

    def run_capture(self, command, *, cwd=None):
        result = subprocess.run(
            command,
            cwd=str(cwd or self.repo_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            error_output = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(error_output or f"Command failed: {shlex.join(command)}")
        return result.stdout

    def detect_os_family(self):
        if platform.system() == "Darwin":
            return "macos"

        os_release = Path("/etc/os-release")
        if not os_release.exists():
            raise RuntimeError("Unable to detect the operating system for setup.sh.")

        fields = {}
        for line in os_release.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            fields[key.strip()] = value.strip().strip('"')

        distro_id = fields.get("ID", "").lower()
        version_major = fields.get("VERSION_ID", "").split(".", 1)[0]
        if distro_id in {"ol", "oraclelinux"} and version_major == "8":
            return "ol8"
        if distro_id in {"ol", "oraclelinux"} and version_major == "9":
            return "ol9"
        if distro_id == "ubuntu":
            return "ubuntu"
        raise RuntimeError(
            f"Unsupported operating system for setup.sh: {distro_id or 'unknown'} {version_major or ''}".strip()
        )

    def load_runtime_env(self):
        runtime_env = {}
        runtime_env_file = self.repo_dir / ".runtime.env"
        if not runtime_env_file.exists():
            return runtime_env

        for raw_line in runtime_env_file.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            runtime_env[key.strip()] = value.strip()
        return runtime_env

    def resolve_os_family(self, runtime_env):
        for source_name, value in (
            ("MYSQL_SHELL_WEB_OS_FAMILY", os.environ.get("MYSQL_SHELL_WEB_OS_FAMILY", "")),
            (".runtime.env OS_FAMILY", runtime_env.get("OS_FAMILY", "")),
        ):
            normalized = str(value or "").strip().lower()
            if not normalized:
                continue
            if normalized not in SUPPORTED_OS_FAMILIES:
                raise RuntimeError(
                    f"Unsupported OS family `{normalized}` from {source_name}. "
                    "Expected ol8, ol9, ubuntu, or macos."
                )
            return normalized, source_name

        return self.detect_os_family(), "host detection"

    def systemctl_state(self, service_name, command):
        if not shutil.which("systemctl"):
            return False
        result = subprocess.run(
            ["systemctl", command, service_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.returncode == 0

    def detect_deploy_mode_and_services(self, runtime_env):
        active_services = []
        http_enabled = self.systemctl_state(HTTP_SERVICE, "is-enabled") or self.systemctl_state(
            HTTP_SERVICE, "is-active"
        )
        https_enabled = self.systemctl_state(HTTPS_SERVICE, "is-enabled") or self.systemctl_state(
            HTTPS_SERVICE, "is-active"
        )
        if http_enabled:
            active_services.append(HTTP_SERVICE)
        if https_enabled:
            active_services.append(HTTPS_SERVICE)

        if http_enabled and https_enabled:
            return "both", active_services
        if https_enabled:
            return "https", active_services
        if http_enabled:
            return "http", active_services

        env_mode = str(runtime_env.get("DEPLOY_MODE", "")).strip().lower()
        if env_mode in {"http", "https", "both", "none"}:
            if not shutil.which("systemctl"):
                return env_mode, []
            if env_mode == "http":
                return env_mode, [HTTP_SERVICE]
            if env_mode == "https":
                return env_mode, [HTTPS_SERVICE]
            if env_mode == "both":
                return env_mode, [HTTP_SERVICE, HTTPS_SERVICE]
            return env_mode, []

        if runtime_env.get("SSL_CERT_FILE") and runtime_env.get("SSL_KEY_FILE"):
            return "https", [HTTPS_SERVICE] if shutil.which("systemctl") else []
        return "http", [HTTP_SERVICE] if shutil.which("systemctl") else []

    def ensure_clean_worktree(self):
        status_output = self.run_capture(["git", "status", "--porcelain"], cwd=self.repo_dir).strip()
        blocking_lines = []
        for line in status_output.splitlines():
            path = line[3:] if len(line) > 3 else line
            if any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in LOCAL_STATE_PREFIXES):
                continue
            blocking_lines.append(line)
        if blocking_lines:
            self.append_log("\n".join(blocking_lines))
            raise RuntimeError("Repository has local changes outside allowed deployment state. Commit or stash them before running the updater.")

    def verify_update_trust_boundary(self, runtime_env):
        expected_remote = os.environ.get("MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL") or runtime_env.get(
            "MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL", ""
        )
        expected_branch = os.environ.get("MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH") or runtime_env.get(
            "MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH", ""
        )
        actual_remote = self.run_capture(["git", "config", "--get", "remote.origin.url"], cwd=self.repo_dir).strip()
        actual_branch = self.run_capture(["git", "branch", "--show-current"], cwd=self.repo_dir).strip()
        if expected_remote and actual_remote != expected_remote:
            raise RuntimeError("Update remote does not match MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL.")
        if expected_branch and actual_branch != expected_branch:
            raise RuntimeError("Update branch does not match MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH.")
        return actual_remote, actual_branch

    def current_user_group(self):
        try:
            user_name = pwd.getpwuid(os.getuid()).pw_name
        except KeyError:
            user_name = ""
        try:
            group_name = grp.getgrgid(os.getgid()).gr_name
        except KeyError:
            group_name = ""
        return user_name, group_name

    def run_setup(self, os_family, deploy_mode, runtime_env, *, skip_privileged_setup=False):
        setup_env = os.environ.copy()
        setup_env["RUNTIME_ENV_FILE"] = str(self.repo_dir / ".runtime.env")
        user_name, group_name = self.current_user_group()
        if user_name:
            setup_env["SERVICE_USER"] = user_name
        if group_name:
            setup_env["SERVICE_GROUP"] = group_name
        if skip_privileged_setup:
            setup_env["SKIP_PRIVILEGED_SETUP"] = "1"

        runtime_values = {
            "HOST": runtime_env.get("HOST", ""),
            "HTTP_PORT": runtime_env.get("DEFAULT_HTTP_PORT", ""),
            "HTTPS_PORT": runtime_env.get("DEFAULT_HTTPS_PORT", ""),
            "SSL_CERT_FILE": runtime_env.get("SSL_CERT_FILE", ""),
            "SSL_KEY_FILE": runtime_env.get("SSL_KEY_FILE", ""),
        }
        for key, value in runtime_values.items():
            if value:
                setup_env[key] = value
        for key in (
            "MYSQL_SHELL_WEB_PYTHON_BIN",
            "MYSQL_SHELL_WEB_PYTHON_MIN_VERSION",
            "MYSQL_SHELL_WEB_DEPENDENCY_AUDIT",
            "MYSQL_SHELL_WEB_DEPENDENCY_AUDIT_STRICT",
            "MYSQL_SHELL_WEB_MYSQL_SERVER_SERIES",
            "MYSQL_SERVER_EMBEDDED_VERSION",
            "MYSQL_SERVER_BRIDGE_VERSION",
            "MYSQL_SERVER_RUNTIME_DIR",
            "MYSQL_SERVER_DOWNLOADS_DIR",
            "MYSQL_SERVER_URL_LINUX_X86",
            "MYSQL_SERVER_URL_LINUX_ARM",
            "MYSQL_SERVER_URL_MACOS_X86",
            "MYSQL_SERVER_URL_MACOS_ARM",
            "MYSQL_SERVER_BRIDGE_URL_LINUX_X86",
            "MYSQL_SERVER_BRIDGE_URL_LINUX_ARM",
            "MYSQL_SERVER_BRIDGE_URL_MACOS_X86",
            "MYSQL_SERVER_BRIDGE_URL_MACOS_ARM",
            "MYSQL_SHELL_WEB_UPDATE_ALLOWED_REMOTE_URL",
            "MYSQL_SHELL_WEB_UPDATE_ALLOWED_BRANCH",
            "LOCAL_MYSQL_PROFILE_NAME",
            "LOCAL_MYSQL_ADMIN_USER",
            "LOCAL_MYSQL_ADMIN_PASSWORD",
            "LOCAL_MYSQL_SOCKET",
            "LOCAL_MYSQL_DATABASE",
            "MYSQL_SHELL_WEB_UPDATE_CODE_REFRESH_ONLY",
        ):
            value = os.environ.get(key) or runtime_env.get(key, "")
            if value:
                setup_env[key] = value

        command = ["/bin/bash", str(self.repo_dir / "setup.sh"), os_family, deploy_mode]
        if runtime_values["HTTP_PORT"]:
            command.extend(["--http-port", runtime_values["HTTP_PORT"]])
        if runtime_values["HTTPS_PORT"]:
            command.extend(["--https-port", runtime_values["HTTPS_PORT"]])
        self.run_command(command, cwd=self.repo_dir, env=setup_env)

    def passwordless_sudo_available(self):
        if os.geteuid() == 0:
            return True, ""
        if not shutil.which("sudo"):
            return False, "sudo is not installed."
        true_command = "/bin/true" if Path("/bin/true").exists() else (shutil.which("true") or "true")
        result = subprocess.run(
            ["sudo", "-n", true_command],
            cwd=str(self.repo_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return True, ""
        output = (result.stderr or result.stdout or "").strip()
        return False, output or "sudo -n true failed."

    def begin_restart_wait(self, service_names, completion_message):
        restart_requested_at = utc_now_iso()
        self.write_status(
            state="restarting",
            step="Restarting",
            message=f"Waiting for {' and '.join(service_names)} to restart.",
            restart_requested_at=restart_requested_at,
            service_names=service_names,
            completion_message=completion_message,
        )
        return restart_requested_at

    def schedule_service_restart(self, service_names, completion_message):
        if not service_names:
            return
        if not shutil.which("systemctl"):
            raise RuntimeError("systemctl is required to restart the service.")

        privilege_prefix = [] if os.geteuid() == 0 else ["sudo", "-n"]
        restart_requested_at = self.begin_restart_wait(service_names, completion_message)
        self.append_log(f"[{restart_requested_at}] Scheduling service restart for {', '.join(service_names)}.")

        if shutil.which("systemd-run"):
            transient_unit_name = f"{APP_SLUG}-self-update-{os.getpid()}"
            restart_command = "sleep 2 && /bin/systemctl restart " + " ".join(
                shlex.quote(service_name) for service_name in service_names
            )
            self.run_command(
                privilege_prefix
                + [
                    "systemd-run",
                    "--unit",
                    transient_unit_name,
                    "--collect",
                    "/bin/sh",
                    "-lc",
                    restart_command,
                ],
                cwd=self.repo_dir,
            )
            self.append_log(f"Restart scheduled in transient unit {transient_unit_name}.")
            return

        self.run_command(privilege_prefix + ["systemctl", "restart", *service_names], cwd=self.repo_dir)
        completed_at = utc_now_iso()
        self.write_status(
            state="completed",
            step="Completed",
            message=completion_message,
            completion_message=completion_message,
            finished_at=completed_at,
        )
        self.append_log(f"[{completed_at}] Service restart completed.")

    def schedule_self_restart(self, service_names, completion_message):
        if not service_names:
            raise RuntimeError("No active systemd service was detected for restart.")
        if not self.service_pid:
            raise RuntimeError("The running service PID is unknown.")

        restart_requested_at = self.begin_restart_wait(service_names, completion_message)
        self.append_log(
            f"[{restart_requested_at}] Scheduling service self-restart by terminating PID {self.service_pid}."
        )
        subprocess.Popen(
            ["/bin/sh", "-lc", f"sleep 2 && kill -{signal.SIGKILL.value} {self.service_pid}"],
            cwd=str(self.repo_dir),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        self.append_log("Restart will be triggered by terminating the current service process and letting systemd recover it.")

    def run(self):
        self.write_status(
            state="running",
            step="Starting",
            message="Update worker is running.",
            started_at=self.status.get("started_at") or utc_now_iso(),
            finished_at="",
            worker_pid=os.getpid(),
        )
        self.append_log(f"[{utc_now_iso()}] Update worker started.")

        runtime_env = self.load_runtime_env()
        self.repair_permissions()
        os_family, os_family_source = self.resolve_os_family(runtime_env)
        deploy_mode, service_names = self.detect_deploy_mode_and_services(runtime_env)
        self.write_status(service_names=service_names)
        self.log_step(
            "Inspecting",
            f"Using OS family `{os_family}` from {os_family_source} with deploy mode `{deploy_mode}`.",
        )

        self.log_step("Checking repository", "Validating the git worktree.")
        self.ensure_clean_worktree()

        _remote_url, branch_name = self.verify_update_trust_boundary(runtime_env)
        self.append_log(f"Updating branch {branch_name or 'detached'}.")

        self.log_step("Pulling repository", "Fetching the latest repository changes.")
        self.run_command(["git", "fetch", "--all", "--prune"], cwd=self.repo_dir)
        self.run_command(["git", "pull", "--ff-only"], cwd=self.repo_dir)
        self.repair_permissions()

        full_completion_message = "Repository refresh, setup, and service restart completed."
        limited_completion_message = (
            "Repository refresh, Python dependencies, and service restart completed. "
            "Privileged setup changes were skipped because passwordless sudo was unavailable from the running service."
        )
        sudo_ready, sudo_error = self.passwordless_sudo_available()

        if sudo_ready:
            self.log_step("Running setup", "Rerunning setup.sh to refresh dependencies and service wiring.")
            self.run_setup(os_family, deploy_mode, runtime_env)
        else:
            self.log_step(
                "Running setup",
                "Passwordless sudo is unavailable from the running service. Refreshing in unprivileged mode.",
            )
            if sudo_error:
                self.append_log(f"Passwordless sudo check failed: {sudo_error}")
            self.append_log(
                "setup.sh will skip privileged steps such as firewall changes and systemd unit rewrites."
            )
            self.run_setup(os_family, deploy_mode, runtime_env, skip_privileged_setup=True)
        self.repair_permissions()

        if service_names:
            if sudo_ready:
                self.schedule_service_restart(service_names, full_completion_message)
            else:
                self.schedule_self_restart(service_names, limited_completion_message)
            return

        completion_time = utc_now_iso()
        self.write_status(
            state="completed",
            step="Completed",
            message="Repository refresh and setup completed. No systemd service restart was required.",
            finished_at=completion_time,
            restart_requested_at="",
            service_names=[],
            completion_message="",
        )
        self.append_log(f"[{completion_time}] Update completed without a service restart.")


def main():
    parser = argparse.ArgumentParser(description="Refresh the repository, rerun setup, and restart the service.")
    parser.add_argument("--repo-dir", required=True)
    parser.add_argument("--status-file", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--service-pid")
    args = parser.parse_args()

    worker = UpdateWorker(args.repo_dir, args.status_file, args.log_file, service_pid=args.service_pid)
    try:
        worker.run()
    except Exception as error:
        failed_at = utc_now_iso()
        worker.append_log(f"[{failed_at}] ERROR: {error}")
        worker.write_status(
            state="error",
            step="Failed",
            message=str(error),
            finished_at=failed_at,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
