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

def _normalize_checkbox(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_operation(value):
    normalized = str(value or "").strip().lower()
    allowed = {name for name, _label in SHELL_OPERATION_OPTIONS}
    if normalized == "option-profiles":
        return normalized
    return normalized if normalized in allowed else "dump-instance"


def _normalize_shell_operations_page(page_value, legacy_operation_value="", legacy_view_value=""):
    normalized = str(page_value or "").strip().lower()
    legacy_operation = str(legacy_operation_value or "").strip().lower()

    if normalized == "run":
        if legacy_operation in {"dump-instance", "dump-schemas", "load-dump"}:
            return legacy_operation
        return "dump-instance"

    if normalized in {"dump-instance", "dump-schemas", "load-dump", "history", "option-profiles"}:
        return normalized

    if legacy_operation in {"dump-instance", "dump-schemas", "load-dump", "history", "option-profiles"}:
        return legacy_operation

    legacy_view = str(legacy_view_value or "").strip().lower()
    if legacy_view == "history":
        return "history"

    return "dump-instance"


def _normalize_option_profile_kind(value, default="dump"):
    normalized = str(value or "").strip().lower()
    if normalized in {"dump", "load"}:
        return normalized
    return default


def _normalize_overview_tab(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"environment", "workflow", "pars"}:
        return normalized
    return "environment"


def _normalize_db_admin_tab(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"events", "primary-key"}:
        return normalized
    return "events"


def _normalize_db_admin_detail(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"databases", "with-primary-key", "without-primary-key"}:
        return normalized
    return "databases"


def _parse_selected_primary_key_targets(values):
    targets = []
    seen = set()
    for raw_value in values or []:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            continue
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as error:
            raise ValueError("Selected table list is invalid.") from error
        if not isinstance(payload, dict):
            raise ValueError("Selected table list is invalid.")

        table_schema = str(payload.get("schema", "")).strip()
        table_name = str(payload.get("table", "")).strip()
        if not table_schema or not table_name:
            raise ValueError("Selected table list is invalid.")

        target_key = (table_schema, table_name)
        if target_key in seen:
            continue
        seen.add(target_key)
        targets.append(target_key)
    return targets


def _normalize_threads(value, default=4):
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    return normalized if normalized > 0 else default


def _normalize_optional_positive_int(value, label):
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        normalized = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{label} must be an integer.") from error
    if normalized <= 0:
        raise ValueError(f"{label} must be greater than zero.")
    return normalized


def _normalize_optional_float(value, label):
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        return float(raw_value)
    except ValueError as error:
        raise ValueError(f"{label} must be numeric.") from error


def _request_text(name, default=""):
    return str(request.values.get(name, default)).strip()


def _request_checkbox(name, default=False):
    if request.method == "POST":
        return _normalize_checkbox(request.form.get(name))
    return _normalize_checkbox(request.args.get(name, "1" if default else ""))


def _request_multiselect(name, allowed_values):
    values = request.form.getlist(name) if request.method == "POST" else request.args.getlist(name)
    return normalize_multiselect(values, allowed_values)




def safe_current_prefix(value):
    try:
        return normalize_relative_prefix(value)
    except ValueError:
        return ""
