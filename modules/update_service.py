import json
import os
import re
import secrets
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from functools import wraps
from uuid import uuid4

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from modules.config import (
    APP_VERSION_FILE,
    APP_TITLE,
    MYSQL_SHELL_WEB_VERSION_URL,
    LOCAL_ADMIN_PROFILE_NAME,
    NAV_GROUPS,
    PAR_ACCESS_OPTIONS,
    PAR_TARGET_OPTIONS,
    ROOT_DIR,
    RUNTIME_DIR,
    SHELL_OPERATION_OPTIONS,
    MYSQL_SHELL_WEB_SESSION_COOKIE_NAME,
    MYSQL_SHELL_WEB_SESSION_COOKIE_PATH,
    MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE,
    MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE,
)
from modules.mysql_connection import (
    MYSQL_CONNECTION_ERRORS,
    apply_primary_key_fix,
    change_current_user_password,
    fetch_accessible_schemas,
    fetch_db_admin_overview,
    fetch_dump_filter_catalog,
    fetch_dump_validation_summary,
    fetch_enabled_event_count,
    fetch_lakehouse_table_names,
    fetch_mysql_overview,
    is_user_schema_name,
    set_event_status,
    test_mysql_connection,
)
from modules.mysqlsh_runner import (
    build_dump_instance_request,
    build_dump_schemas_request,
    build_load_dump_request,
    default_progress_file,
    ensure_runtime_dirs,
    get_mysqlsh_status,
    normalize_progress_file_value,
)
from modules.mysqlsh_jobs import (
    build_mysqlsh_job_snapshot,
    cancel_mysqlsh_job,
    cleanup_mysqlsh_job,
    ensure_job_store,
    list_mysqlsh_job_history,
    submit_mysqlsh_job,
)
from modules.option_profiles import (
    delete_option_profile,
    ensure_option_profile_store,
    get_option_profile,
    load_option_profiles,
    save_option_profile,
)
from modules.object_storage import (
    create_managed_folder,
    create_par_record,
    delete_folder,
    delete_par_record,
    effective_oci_config_file,
    ensure_object_storage_store,
    ensure_par_store,
    format_datetime_local,
    get_folder_browser_state,
    get_par_entries_for_bucket,
    get_par_entry_by_id,
    join_relative_prefixes,
    list_active_pars_for_purpose,
    load_object_storage_config,
    normalize_object_storage,
    normalize_relative_prefix,
    parent_relative_prefix,
    rename_folder,
    save_object_storage_config,
)
from modules.oci_configuration import (
    build_oci_config_status,
    save_local_oci_config_text,
    store_local_oci_config_from_upload,
)
from modules.profiles import (
    ensure_profile_store,
    get_profile_by_name,
    harden_profile_store_permissions,
    is_local_admin_profile,
    local_admin_profile_ready,
    load_profiles,
    normalize_profile,
    profile_allows_management,
    public_login_profiles,
    save_profiles,
    set_profile_force_password_change,
    store_uploaded_ssh_key,
    validate_profile,
)
from modules.shell_options import (
    COMPRESSION_OPTIONS,
    DUMP_COMPATIBILITY_OPTIONS,
    DUMP_DIALECT_OPTIONS,
    LOAD_ANALYZE_TABLES_OPTIONS,
    LOAD_DEFER_TABLE_INDEXES_OPTIONS,
    LOAD_HANDLE_GRANT_ERRORS_OPTIONS,
    LOAD_UPDATE_GTID_SET_OPTIONS,
    normalize_multiselect,
    normalize_select,
    parse_json_options,
    parse_sql_statement_list,
    parse_string_list,
)
from modules.session_utils import (
    clear_login_state,
    ensure_session_scope,
    get_current_profile_name,
    get_current_username,
    get_session_credentials,
    get_session_profile,
    get_session_value,
    get_version_check,
    has_server_login_state,
    is_logged_in,
    set_session_value,
    set_login_state,
    set_session_profile,
    set_version_check,
)

from modules.app_state import *
from modules.form_utils import *
from modules.shell_form_service import *
from modules.update_service import *
from modules.web_helpers import *

def _utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_update_timestamp(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_json_file(path):
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _app_version_payload():
    payload = _read_json_file(APP_VERSION_FILE)
    version = str(payload.get("version", "")).strip()
    return {
        "version": version or "1.0",
        "source": str(APP_VERSION_FILE),
    }


def _current_git_branch():
    result = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=str(ROOT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    branch_name = str(result.stdout or "").strip()
    return branch_name or "main"


def _git_remote_origin_url():
    result = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        cwd=str(ROOT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return str(result.stdout or "").strip()


def _github_raw_version_url_from_remote(remote_url, branch_name):
    remote = str(remote_url or "").strip()
    branch = str(branch_name or "main").strip() or "main"
    owner_repo = ""
    if remote.startswith("git@github.com:"):
        owner_repo = remote.split(":", 1)[1]
    elif "github.com/" in remote:
        owner_repo = remote.split("github.com/", 1)[1]
    if not owner_repo:
        return ""
    owner_repo = owner_repo.removesuffix(".git").strip("/")
    if owner_repo.count("/") < 1:
        return ""
    return (
        "https://api.github.com/repos/"
        f"{owner_repo}/contents/appver.json?ref={urllib.parse.quote(branch, safe='')}"
    )


def _normalize_github_version_url(version_url):
    url = str(version_url or "").strip()
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc == "raw.githubusercontent.com":
        parts = [part for part in parsed.path.strip("/").split("/") if part]
        if len(parts) >= 4 and parts[-1] == "appver.json":
            owner, repo, branch = parts[0], parts[1], "/".join(parts[2:-1])
            return (
                "https://api.github.com/repos/"
                f"{owner}/{repo}/contents/appver.json?ref={urllib.parse.quote(branch, safe='')}"
            )
    if parsed.netloc == "github.com" and "/raw/" in parsed.path and parsed.path.endswith("/appver.json"):
        before_raw, after_raw = parsed.path.strip("/").split("/raw/", 1)
        owner_repo = before_raw.strip("/")
        branch = after_raw.removesuffix("/appver.json").strip("/")
        if owner_repo.count("/") == 1 and branch:
            return (
                "https://api.github.com/repos/"
                f"{owner_repo}/contents/appver.json?ref={urllib.parse.quote(branch, safe='')}"
            )
    return url


def _resolve_version_url():
    configured_url = str(MYSQL_SHELL_WEB_VERSION_URL or "").strip()
    if configured_url:
        return _normalize_github_version_url(configured_url)
    return _github_raw_version_url_from_remote(_git_remote_origin_url(), _current_git_branch())


def _fetch_repo_version(version_url):
    if not version_url:
        return "", "Set MYSQL_SHELL_WEB_VERSION_URL when the repository version URL cannot be inferred."
    request_object = urllib.request.Request(
        version_url,
        headers={
            "Accept": "application/vnd.github.raw+json, application/json",
            "User-Agent": "mysql-shell-web-version-check",
        },
    )
    ca_bundle_override = os.environ.get("MYSQL_SHELL_WEB_VERSION_CA_BUNDLE", "").strip()
    try:
        if ca_bundle_override:
            context = ssl.create_default_context(cafile=ca_bundle_override)
        else:
            import certifi

            context = ssl.create_default_context(cafile=certifi.where())
        with urllib.request.urlopen(request_object, timeout=2, context=context) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except ssl.SSLError as error:
        return "", (
            "TLS verification failed while retrieving the repository version. "
            "Set MYSQL_SHELL_WEB_VERSION_CA_BUNDLE to a trusted CA bundle if this host uses a private trust store. "
            f"Details: {error}"
        )
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        return "", str(error or "Unable to retrieve repository version.")
    if not isinstance(payload, dict):
        return "", "Repository version payload is not a JSON object."
    version = str(payload.get("version", "")).strip()
    if not version:
        return "", "Repository version payload does not contain a version."
    return version, ""


def _check_repository_version():
    app_version = _app_version_payload()["version"]
    version_url = _resolve_version_url()
    repo_version, error = _fetch_repo_version(version_url)
    result = {
        "checked_at": _utc_now_iso(),
        "app_version": app_version,
        "repo_version": repo_version,
        "version_url": version_url,
        "error": error,
        "update_available": bool(repo_version and repo_version != app_version),
    }
    set_version_check(result)
    return result


def _current_version_check():
    app_version = _app_version_payload()["version"]
    version_check = get_version_check()
    version_check.setdefault("app_version", app_version)
    version_check.setdefault("repo_version", "")
    version_check.setdefault("version_url", _resolve_version_url())
    version_check.setdefault("error", "")
    version_check.setdefault("checked_at", "")
    version_check["update_available"] = bool(
        version_check.get("repo_version") and version_check.get("repo_version") != app_version
    )
    return version_check


def _write_update_status(payload):
    UPDATE_DIR.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = _utc_now_iso()
    temp_file = UPDATE_STATUS_FILE.with_suffix(".tmp")
    with temp_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    temp_file.replace(UPDATE_STATUS_FILE)
    return payload


def _read_update_log():
    try:
        return UPDATE_LOG_FILE.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _is_process_running(pid):
    try:
        normalized_pid = int(pid)
    except (TypeError, ValueError):
        return False
    if normalized_pid <= 0:
        return False
    try:
        os.kill(normalized_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _normalize_update_status(payload=None):
    status = dict(payload or _read_json_file(UPDATE_STATUS_FILE))
    state = str(status.get("state") or "idle").strip().lower()
    if state not in {"idle", "running", "restarting", "completed", "error"}:
        state = "idle"
    status["state"] = state
    status.setdefault("step", "")
    status.setdefault("started_at", "")
    status.setdefault("updated_at", "")
    status.setdefault("finished_at", "")
    status.setdefault("restart_requested_at", "")
    status.setdefault("service_names", [])
    status.setdefault("worker_pid", "")

    if state == "idle":
        status["message"] = status.get("message") or "No update has been started."
    elif state == "running":
        worker_pid = status.get("worker_pid")
        if worker_pid and not _is_process_running(worker_pid):
            status["state"] = "error"
            status["step"] = "Failed"
            status["message"] = "Update worker stopped before reporting completion."
            status["finished_at"] = status.get("finished_at") or _utc_now_iso()
            _write_update_status(status)
        else:
            status["message"] = status.get("message") or "Update worker is running."
    elif state == "restarting":
        restart_requested_at = _parse_update_timestamp(status.get("restart_requested_at"))
        if restart_requested_at and APP_STARTED_AT > restart_requested_at:
            status["state"] = "completed"
            status["step"] = "Completed"
            status["message"] = status.get("completion_message") or "Update completed and the service restarted."
            status["finished_at"] = status.get("finished_at") or _utc_now_iso()
            _write_update_status(status)
        else:
            status["message"] = status.get("message") or "Waiting for the service to restart."
    elif state == "completed":
        status["message"] = status.get("message") or "Update completed."
    elif state == "error":
        status["message"] = status.get("message") or "Update failed."

    status["log_text"] = _read_update_log()
    status["can_start"] = status["state"] not in {"running", "restarting"}
    return status


def _update_status_request_authorized(status=None):
    if has_server_login_state():
        return True
    status_payload = status or _read_json_file(UPDATE_STATUS_FILE)
    expected_token = str(status_payload.get("poll_token", "")).strip()
    supplied_token = str(request.headers.get("X-MySQL-Shell-Web-Update-Poll-Token", "")).strip()
    return bool(expected_token and supplied_token and expected_token == supplied_token)


def _public_update_status(status):
    payload = dict(status or {})
    payload.pop("poll_token", None)
    return payload


def _start_update_worker(*, bootstrap_payload=None, compatibility_code_refresh=False):
    current_status = _normalize_update_status()
    if not current_status.get("can_start"):
        raise RuntimeError("An update is already running.")

    UPDATE_DIR.mkdir(parents=True, exist_ok=True)
    if not UPDATE_WORKER_FILE.exists():
        raise RuntimeError(f"Update worker was not found at {UPDATE_WORKER_FILE}.")

    try:
        UPDATE_LOG_FILE.unlink()
    except FileNotFoundError:
        pass

    poll_token = uuid4().hex
    session[UPDATE_POLL_TOKEN_SESSION_KEY] = poll_token
    status = {
        "state": "running",
        "step": "Queued",
        "message": "Update worker has been queued.",
        "started_at": _utc_now_iso(),
        "finished_at": "",
        "restart_requested_at": "",
        "service_names": [],
        "poll_token": poll_token,
    }
    _write_update_status(status)

    env = os.environ.copy()
    bootstrap_payload = bootstrap_payload or {}
    for key in ("LOCAL_MYSQL_PROFILE_NAME", "LOCAL_MYSQL_ADMIN_USER", "LOCAL_MYSQL_SOCKET", "LOCAL_MYSQL_DATABASE"):
        value = str(bootstrap_payload.get(key, "")).strip()
        if value:
            env[key] = value
    if bootstrap_payload.get("LOCAL_MYSQL_ADMIN_PASSWORD"):
        env["LOCAL_MYSQL_ADMIN_PASSWORD"] = bootstrap_payload["LOCAL_MYSQL_ADMIN_PASSWORD"]
    if compatibility_code_refresh:
        env["MYSQL_SHELL_WEB_UPDATE_CODE_REFRESH_ONLY"] = "1"
    pythonpath_entries = [str(ROOT_DIR)]
    existing_pythonpath = str(env.get("PYTHONPATH", "")).strip()
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)

    command = [
        sys.executable,
        str(UPDATE_WORKER_FILE),
        "--repo-dir",
        str(ROOT_DIR),
        "--status-file",
        str(UPDATE_STATUS_FILE),
        "--log-file",
        str(UPDATE_LOG_FILE),
        "--service-pid",
        str(os.getpid()),
    ]
    try:
        with UPDATE_LOG_FILE.open("a", encoding="utf-8") as log_handle:
            process = subprocess.Popen(
                command,
                cwd=str(ROOT_DIR),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                close_fds=True,
                start_new_session=True,
            )
    except Exception as error:
        status["state"] = "error"
        status["step"] = "Failed"
        status["message"] = str(error)
        status["finished_at"] = _utc_now_iso()
        _write_update_status(status)
        raise
    status["worker_pid"] = process.pid
    status["message"] = "Update worker is running."
    _write_update_status(status)
    return status


__all__ = [name for name in globals() if not name.startswith("__")]
