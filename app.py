import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from functools import wraps

import pymysql
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from modules.config import (
    APP_TITLE,
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
    apply_primary_key_fix,
    fetch_accessible_schemas,
    fetch_db_admin_overview,
    fetch_dump_filter_catalog,
    fetch_dump_validation_summary,
    fetch_enabled_event_count,
    fetch_mysql_overview,
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
    build_oci_config_status,
    create_managed_folder,
    create_par_record,
    delete_folder,
    delete_par_record,
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
    save_local_oci_config_text,
    save_object_storage_config,
)
from modules.profiles import (
    ensure_profile_store,
    get_profile_by_name,
    load_profiles,
    normalize_profile,
    save_profiles,
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
    get_session_credentials,
    get_session_profile,
    is_logged_in,
    set_login_state,
    set_session_profile,
)


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "mysql-shell-web-change-me")
app.config["SESSION_COOKIE_NAME"] = MYSQL_SHELL_WEB_SESSION_COOKIE_NAME
app.config["SESSION_COOKIE_PATH"] = MYSQL_SHELL_WEB_SESSION_COOKIE_PATH
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE
app.config["SESSION_COOKIE_SECURE"] = MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE
APP_STARTED_AT = datetime.now(timezone.utc).replace(microsecond=0)
UPDATE_DIR = RUNTIME_DIR / "updates"
UPDATE_STATUS_FILE = UPDATE_DIR / "mysql_shell_web_update_status.json"
UPDATE_LOG_FILE = UPDATE_DIR / "mysql_shell_web_update.log"
UPDATE_WORKER_FILE = ROOT_DIR / "mysql_shell_web_update_worker.py"

MYSQL_PAGE_HEALTHCHECK_ENDPOINTS = {
    "overview_page",
    "db_admin_page",
    "db_admin_event_toggle",
    "db_admin_apply_primary_key_fix",
    "profile_page",
    "par_manager_page",
    "folder_manager_page",
    "shell_operations_page",
}


@app.before_request
def ensure_mysql_shell_web_session_scope():
    ensure_session_scope()


@app.before_request
def ensure_mysql_connection_healthcheck():
    if not is_logged_in():
        return None

    current_endpoint = str(request.endpoint or "").strip()
    if current_endpoint not in MYSQL_PAGE_HEALTHCHECK_ENDPOINTS:
        return None

    try:
        test_mysql_connection(get_session_profile(), get_session_credentials())
    except Exception as error:  # pragma: no cover - depends on runtime services
        return _redirect_to_login_for_mysql_unavailable(error)
    return None


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


DUMP_OPTION_PROFILE_SUFFIXES = [
    "threads",
    "max_rate",
    "default_character_set",
    "compression",
    "dialect",
    "bytes_per_chunk",
    "target_version",
    "show_progress",
    "dry_run",
    "consistent",
    "skip_consistency_checks",
    "skip_upgrade_checks",
    "checksum",
    "chunking",
    "tz_utc",
    "ddl_only",
    "data_only",
    "events",
    "routines",
    "triggers",
    "libraries",
    "ocimds",
    "compatibility",
    "include_tables",
    "exclude_tables",
    "include_events",
    "exclude_events",
    "include_routines",
    "exclude_routines",
    "include_triggers",
    "exclude_triggers",
    "include_libraries",
    "exclude_libraries",
    "advanced_json",
]

DUMP_INSTANCE_ONLY_OPTION_PROFILE_SUFFIXES = [
    "users",
    "include_schemas",
    "exclude_schemas",
    "include_users",
    "exclude_users",
]

LOAD_OPTION_PROFILE_SUFFIXES = [
    "threads",
    "background_threads",
    "wait_dump_timeout",
    "progress_file",
    "schema",
    "character_set",
    "max_bytes_per_transaction",
    "show_progress",
    "dry_run",
    "reset_progress",
    "skip_binlog",
    "ignore_version",
    "drop_existing_objects",
    "ignore_existing_objects",
    "checksum",
    "show_metadata",
    "create_invisible_pks",
    "load_ddl",
    "load_data",
    "load_users",
    "load_indexes",
    "analyze_tables",
    "defer_table_indexes",
    "handle_grant_errors",
    "update_gtid_set",
    "session_init_sql",
    "include_schemas",
    "exclude_schemas",
    "include_tables",
    "exclude_tables",
    "include_users",
    "exclude_users",
    "include_events",
    "exclude_events",
    "include_routines",
    "exclude_routines",
    "include_triggers",
    "exclude_triggers",
    "include_libraries",
    "exclude_libraries",
    "advanced_json",
]

DUMP_OPTION_PROFILE_BOOLEAN_SUFFIXES = {
    "show_progress",
    "dry_run",
    "consistent",
    "skip_consistency_checks",
    "skip_upgrade_checks",
    "checksum",
    "chunking",
    "tz_utc",
    "ddl_only",
    "data_only",
    "events",
    "routines",
    "triggers",
    "libraries",
    "ocimds",
    "users",
}

DUMP_OPTION_PROFILE_LIST_SUFFIXES = {
    "compatibility",
}

DUMP_OPTION_PROFILE_MULTILINE_SUFFIXES = {
    "include_schemas",
    "exclude_schemas",
    "include_tables",
    "exclude_tables",
    "include_users",
    "exclude_users",
    "include_events",
    "exclude_events",
    "include_routines",
    "exclude_routines",
    "include_triggers",
    "exclude_triggers",
    "include_libraries",
    "exclude_libraries",
}

LOAD_OPTION_PROFILE_BOOLEAN_SUFFIXES = {
    "show_progress",
    "dry_run",
    "reset_progress",
    "skip_binlog",
    "ignore_version",
    "drop_existing_objects",
    "ignore_existing_objects",
    "checksum",
    "show_metadata",
    "create_invisible_pks",
    "load_ddl",
    "load_data",
    "load_users",
    "load_indexes",
}

LOAD_OPTION_PROFILE_MULTILINE_SUFFIXES = {
    "session_init_sql",
    "include_schemas",
    "exclude_schemas",
    "include_tables",
    "exclude_tables",
    "include_users",
    "exclude_users",
    "include_events",
    "exclude_events",
    "include_routines",
    "exclude_routines",
    "include_triggers",
    "exclude_triggers",
    "include_libraries",
    "exclude_libraries",
}

OPTION_PROFILE_JSON_TEXT_SUFFIXES = {
    "advanced_json",
}


def _copy_state_value(value):
    if isinstance(value, list):
        return list(value)
    return value


def _option_profile_allowed_suffixes(kind):
    normalized_kind = _normalize_option_profile_kind(kind)
    if normalized_kind == "load":
        return list(LOAD_OPTION_PROFILE_SUFFIXES)
    return _dump_option_profile_suffixes(include_users=True)


def _format_option_profile_editor_json(profile_values):
    return json.dumps(profile_values or {}, indent=2, sort_keys=True)


def _coerce_multiline_editor_value(value):
    if value in ("", None):
        return ""
    if isinstance(value, list):
        return "\n".join(str(item or "").strip() for item in value if str(item or "").strip())
    return str(value)


def _coerce_option_profile_editor_value(kind, suffix, value):
    normalized_kind = _normalize_option_profile_kind(kind)
    if normalized_kind == "load":
        boolean_suffixes = LOAD_OPTION_PROFILE_BOOLEAN_SUFFIXES
        list_suffixes = set()
        multiline_suffixes = LOAD_OPTION_PROFILE_MULTILINE_SUFFIXES
    else:
        boolean_suffixes = DUMP_OPTION_PROFILE_BOOLEAN_SUFFIXES
        list_suffixes = DUMP_OPTION_PROFILE_LIST_SUFFIXES
        multiline_suffixes = DUMP_OPTION_PROFILE_MULTILINE_SUFFIXES

    if suffix in boolean_suffixes:
        return _normalize_checkbox(value)
    if suffix in list_suffixes:
        if isinstance(value, list):
            return list(value)
        return parse_string_list(value)
    if suffix in OPTION_PROFILE_JSON_TEXT_SUFFIXES:
        if value in ("", None):
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, indent=2, sort_keys=True)
        return str(value)
    if suffix in multiline_suffixes:
        return _coerce_multiline_editor_value(value)
    if value is None:
        return ""
    if isinstance(value, (str, int, float)):
        return value
    raise ValueError(f"Unsupported value type for option profile key `{suffix}`.")


def _parse_option_profile_editor_json(raw_text, kind):
    normalized_text = str(raw_text or "").strip()
    if not normalized_text:
        return {}
    try:
        payload = json.loads(normalized_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"Option profile JSON is invalid: {error.msg}.") from error
    if not isinstance(payload, dict):
        raise ValueError("Option profile JSON must be a JSON object.")

    allowed_suffixes = set(_option_profile_allowed_suffixes(kind))
    normalized_values = {}
    for raw_key, raw_value in payload.items():
        suffix = str(raw_key or "").strip()
        if not suffix:
            continue
        if suffix not in allowed_suffixes:
            raise ValueError(f"Unsupported option profile key `{suffix}` for {kind} profiles.")
        normalized_values[suffix] = _coerce_option_profile_editor_value(kind, suffix, raw_value)
    return normalized_values


def _dump_option_profile_suffixes(include_users=False):
    if include_users:
        return DUMP_OPTION_PROFILE_SUFFIXES + DUMP_INSTANCE_ONLY_OPTION_PROFILE_SUFFIXES
    return list(DUMP_OPTION_PROFILE_SUFFIXES)


def _extract_dump_option_profile_values(form_state, prefix, *, include_users=False):
    profile_values = {}
    for suffix in _dump_option_profile_suffixes(include_users):
        key = f"{prefix}_{suffix}"
        if key in form_state:
            profile_values[suffix] = _copy_state_value(form_state[key])
    return profile_values


def _apply_dump_option_profile_values(form_state, profile_values, prefix, *, include_users=False):
    for suffix in _dump_option_profile_suffixes(include_users):
        key = f"{prefix}_{suffix}"
        if key not in form_state or suffix not in profile_values:
            continue
        form_state[key] = _copy_state_value(profile_values[suffix])


def _extract_load_option_profile_values(form_state):
    profile_values = {}
    for suffix in LOAD_OPTION_PROFILE_SUFFIXES:
        key = f"load_dump_{suffix}"
        if key in form_state:
            profile_values[suffix] = _copy_state_value(form_state[key])
    return profile_values


def _apply_load_option_profile_values(form_state, profile_values):
    for suffix in LOAD_OPTION_PROFILE_SUFFIXES:
        key = f"load_dump_{suffix}"
        if key not in form_state or suffix not in profile_values:
            continue
        form_state[key] = _copy_state_value(profile_values[suffix])


def _build_dump_form_state(prefix, *, include_users=False):
    compatibility_values = [value for value, _label in DUMP_COMPATIBILITY_OPTIONS]
    compression_values = [value for value, _label in COMPRESSION_OPTIONS]
    dialect_values = [value for value, _label in DUMP_DIALECT_OPTIONS]

    state = {
        f"{prefix}_par_id": _request_text(f"{prefix}_par_id"),
        f"{prefix}_threads": _request_text(f"{prefix}_threads", "4") or "4",
        f"{prefix}_max_rate": _request_text(f"{prefix}_max_rate", "0") or "0",
        f"{prefix}_default_character_set": _request_text(f"{prefix}_default_character_set", "utf8mb4") or "utf8mb4",
        f"{prefix}_compression": normalize_select(
            _request_text(f"{prefix}_compression", "zstd;level=1"),
            compression_values,
            "zstd;level=1",
        ),
        f"{prefix}_dialect": normalize_select(
            _request_text(f"{prefix}_dialect", "default"),
            dialect_values,
            "default",
        ),
        f"{prefix}_bytes_per_chunk": _request_text(f"{prefix}_bytes_per_chunk", "64M") or "64M",
        f"{prefix}_target_version": _request_text(f"{prefix}_target_version"),
        f"{prefix}_show_progress": _request_checkbox(f"{prefix}_show_progress", default=True),
        f"{prefix}_dry_run": _request_checkbox(f"{prefix}_dry_run"),
        f"{prefix}_consistent": _request_checkbox(f"{prefix}_consistent", default=True),
        f"{prefix}_skip_consistency_checks": _request_checkbox(f"{prefix}_skip_consistency_checks"),
        f"{prefix}_skip_upgrade_checks": _request_checkbox(f"{prefix}_skip_upgrade_checks"),
        f"{prefix}_checksum": _request_checkbox(f"{prefix}_checksum"),
        f"{prefix}_chunking": _request_checkbox(f"{prefix}_chunking", default=True),
        f"{prefix}_tz_utc": _request_checkbox(f"{prefix}_tz_utc", default=True),
        f"{prefix}_ddl_only": _request_checkbox(f"{prefix}_ddl_only"),
        f"{prefix}_data_only": _request_checkbox(f"{prefix}_data_only"),
        f"{prefix}_events": _request_checkbox(f"{prefix}_events", default=True),
        f"{prefix}_routines": _request_checkbox(f"{prefix}_routines", default=True),
        f"{prefix}_triggers": _request_checkbox(f"{prefix}_triggers", default=True),
        f"{prefix}_libraries": _request_checkbox(f"{prefix}_libraries", default=True),
        f"{prefix}_ocimds": _request_checkbox(f"{prefix}_ocimds"),
        f"{prefix}_compatibility": _request_multiselect(f"{prefix}_compatibility", compatibility_values),
        f"{prefix}_include_tables": _request_text(f"{prefix}_include_tables"),
        f"{prefix}_exclude_tables": _request_text(f"{prefix}_exclude_tables"),
        f"{prefix}_include_events": _request_text(f"{prefix}_include_events"),
        f"{prefix}_exclude_events": _request_text(f"{prefix}_exclude_events"),
        f"{prefix}_include_routines": _request_text(f"{prefix}_include_routines"),
        f"{prefix}_exclude_routines": _request_text(f"{prefix}_exclude_routines"),
        f"{prefix}_include_triggers": _request_text(f"{prefix}_include_triggers"),
        f"{prefix}_exclude_triggers": _request_text(f"{prefix}_exclude_triggers"),
        f"{prefix}_include_libraries": _request_text(f"{prefix}_include_libraries"),
        f"{prefix}_exclude_libraries": _request_text(f"{prefix}_exclude_libraries"),
        f"{prefix}_advanced_json": _request_text(f"{prefix}_advanced_json"),
    }

    if include_users:
        state.update(
            {
                f"{prefix}_users": _request_checkbox(f"{prefix}_users", default=True),
                f"{prefix}_include_schemas": _request_text(f"{prefix}_include_schemas"),
                f"{prefix}_exclude_schemas": _request_text(f"{prefix}_exclude_schemas"),
                f"{prefix}_include_users": _request_text(f"{prefix}_include_users"),
                f"{prefix}_exclude_users": _request_text(f"{prefix}_exclude_users"),
            }
        )

    return state


def _build_load_dump_form_state(default_progress_file):
    analyze_values = [value for value, _label in LOAD_ANALYZE_TABLES_OPTIONS]
    defer_values = [value for value, _label in LOAD_DEFER_TABLE_INDEXES_OPTIONS]
    grant_values = [value for value, _label in LOAD_HANDLE_GRANT_ERRORS_OPTIONS]
    gtid_values = [value for value, _label in LOAD_UPDATE_GTID_SET_OPTIONS]

    return {
        "load_dump_par_id": _request_text("load_dump_par_id"),
        "load_dump_threads": _request_text("load_dump_threads", "4") or "4",
        "load_dump_background_threads": _request_text("load_dump_background_threads"),
        "load_dump_wait_dump_timeout": _request_text("load_dump_wait_dump_timeout", "0") or "0",
        "load_dump_progress_file": _request_text("load_dump_progress_file", default_progress_file),
        "load_dump_schema": _request_text("load_dump_schema"),
        "load_dump_character_set": _request_text("load_dump_character_set"),
        "load_dump_max_bytes_per_transaction": _request_text("load_dump_max_bytes_per_transaction"),
        "load_dump_show_progress": _request_checkbox("load_dump_show_progress", default=True),
        "load_dump_dry_run": _request_checkbox("load_dump_dry_run"),
        "load_dump_reset_progress": _request_checkbox("load_dump_reset_progress"),
        "load_dump_skip_binlog": _request_checkbox("load_dump_skip_binlog"),
        "load_dump_ignore_version": _request_checkbox("load_dump_ignore_version"),
        "load_dump_drop_existing_objects": _request_checkbox("load_dump_drop_existing_objects"),
        "load_dump_ignore_existing_objects": _request_checkbox("load_dump_ignore_existing_objects"),
        "load_dump_checksum": _request_checkbox("load_dump_checksum"),
        "load_dump_show_metadata": _request_checkbox("load_dump_show_metadata"),
        "load_dump_create_invisible_pks": _request_checkbox("load_dump_create_invisible_pks"),
        "load_dump_load_ddl": _request_checkbox("load_dump_load_ddl", default=True),
        "load_dump_load_data": _request_checkbox("load_dump_load_data", default=True),
        "load_dump_load_users": _request_checkbox("load_dump_load_users"),
        "load_dump_load_indexes": _request_checkbox("load_dump_load_indexes", default=True),
        "load_dump_analyze_tables": normalize_select(
            _request_text("load_dump_analyze_tables", "off"),
            analyze_values,
            "off",
        ),
        "load_dump_defer_table_indexes": normalize_select(
            _request_text("load_dump_defer_table_indexes", "fulltext"),
            defer_values,
            "fulltext",
        ),
        "load_dump_handle_grant_errors": normalize_select(
            _request_text("load_dump_handle_grant_errors", "abort"),
            grant_values,
            "abort",
        ),
        "load_dump_update_gtid_set": normalize_select(
            _request_text("load_dump_update_gtid_set", "off"),
            gtid_values,
            "off",
        ),
        "load_dump_session_init_sql": _request_text("load_dump_session_init_sql"),
        "load_dump_include_schemas": _request_text("load_dump_include_schemas"),
        "load_dump_exclude_schemas": _request_text("load_dump_exclude_schemas"),
        "load_dump_include_tables": _request_text("load_dump_include_tables"),
        "load_dump_exclude_tables": _request_text("load_dump_exclude_tables"),
        "load_dump_include_users": _request_text("load_dump_include_users"),
        "load_dump_exclude_users": _request_text("load_dump_exclude_users"),
        "load_dump_include_events": _request_text("load_dump_include_events"),
        "load_dump_exclude_events": _request_text("load_dump_exclude_events"),
        "load_dump_include_routines": _request_text("load_dump_include_routines"),
        "load_dump_exclude_routines": _request_text("load_dump_exclude_routines"),
        "load_dump_include_triggers": _request_text("load_dump_include_triggers"),
        "load_dump_exclude_triggers": _request_text("load_dump_exclude_triggers"),
        "load_dump_include_libraries": _request_text("load_dump_include_libraries"),
        "load_dump_exclude_libraries": _request_text("load_dump_exclude_libraries"),
        "load_dump_advanced_json": _request_text("load_dump_advanced_json"),
    }


def _set_option(options, key, value):
    if value in ("", None, []):
        return
    options[key] = value


def _build_dump_options(form_state, prefix, *, include_users=False):
    if form_state[f"{prefix}_ddl_only"] and form_state[f"{prefix}_data_only"]:
        raise ValueError("`ddlOnly` and `dataOnly` cannot both be enabled.")

    options = {
        "threads": _normalize_threads(form_state[f"{prefix}_threads"]),
        "maxRate": form_state[f"{prefix}_max_rate"],
        "defaultCharacterSet": form_state[f"{prefix}_default_character_set"],
        "compression": form_state[f"{prefix}_compression"],
        "dialect": form_state[f"{prefix}_dialect"],
        "showProgress": form_state[f"{prefix}_show_progress"],
        "dryRun": form_state[f"{prefix}_dry_run"],
        "consistent": form_state[f"{prefix}_consistent"],
        "skipConsistencyChecks": form_state[f"{prefix}_skip_consistency_checks"],
        "skipUpgradeChecks": form_state[f"{prefix}_skip_upgrade_checks"],
        "checksum": form_state[f"{prefix}_checksum"],
        "chunking": form_state[f"{prefix}_chunking"],
        "tzUtc": form_state[f"{prefix}_tz_utc"],
        "ddlOnly": form_state[f"{prefix}_ddl_only"],
        "dataOnly": form_state[f"{prefix}_data_only"],
        "events": form_state[f"{prefix}_events"],
        "routines": form_state[f"{prefix}_routines"],
        "triggers": form_state[f"{prefix}_triggers"],
        "libraries": form_state[f"{prefix}_libraries"],
        "ocimds": form_state[f"{prefix}_ocimds"],
    }

    if include_users:
        options["users"] = form_state[f"{prefix}_users"]

    if form_state[f"{prefix}_chunking"]:
        _set_option(options, "bytesPerChunk", form_state[f"{prefix}_bytes_per_chunk"])
    _set_option(options, "targetVersion", form_state[f"{prefix}_target_version"])

    if form_state[f"{prefix}_compatibility"]:
        options["compatibility"] = list(form_state[f"{prefix}_compatibility"])

    list_option_map = {
        "includeTables": form_state[f"{prefix}_include_tables"],
        "excludeTables": form_state[f"{prefix}_exclude_tables"],
        "includeEvents": form_state[f"{prefix}_include_events"],
        "excludeEvents": form_state[f"{prefix}_exclude_events"],
        "includeRoutines": form_state[f"{prefix}_include_routines"],
        "excludeRoutines": form_state[f"{prefix}_exclude_routines"],
        "includeTriggers": form_state[f"{prefix}_include_triggers"],
        "excludeTriggers": form_state[f"{prefix}_exclude_triggers"],
        "includeLibraries": form_state[f"{prefix}_include_libraries"],
        "excludeLibraries": form_state[f"{prefix}_exclude_libraries"],
    }
    if include_users:
        list_option_map.update(
            {
                "includeSchemas": form_state[f"{prefix}_include_schemas"],
                "excludeSchemas": form_state[f"{prefix}_exclude_schemas"],
                "includeUsers": form_state[f"{prefix}_include_users"],
                "excludeUsers": form_state[f"{prefix}_exclude_users"],
            }
        )

    for option_name, raw_value in list_option_map.items():
        parsed_values = parse_string_list(raw_value)
        if parsed_values:
            options[option_name] = parsed_values

    advanced_options = parse_json_options(form_state[f"{prefix}_advanced_json"])
    options.update(advanced_options)
    return options


def _build_load_dump_options(form_state):
    if form_state["load_dump_drop_existing_objects"] and form_state["load_dump_ignore_existing_objects"]:
        raise ValueError("`dropExistingObjects` and `ignoreExistingObjects` cannot both be enabled.")

    progress_file = normalize_progress_file_value(form_state["load_dump_progress_file"])
    if not progress_file:
        raise ValueError("A progress file is required for loadDump.")

    options = {
        "threads": _normalize_threads(form_state["load_dump_threads"]),
        "progressFile": progress_file,
        "waitDumpTimeout": _normalize_optional_float(form_state["load_dump_wait_dump_timeout"], "Wait dump timeout")
        or 0.0,
        "showProgress": form_state["load_dump_show_progress"],
        "dryRun": form_state["load_dump_dry_run"],
        "resetProgress": form_state["load_dump_reset_progress"],
        "skipBinlog": form_state["load_dump_skip_binlog"],
        "ignoreVersion": form_state["load_dump_ignore_version"],
        "dropExistingObjects": form_state["load_dump_drop_existing_objects"],
        "ignoreExistingObjects": form_state["load_dump_ignore_existing_objects"],
        "checksum": form_state["load_dump_checksum"],
        "showMetadata": form_state["load_dump_show_metadata"],
        "createInvisiblePKs": form_state["load_dump_create_invisible_pks"],
        "loadDdl": form_state["load_dump_load_ddl"],
        "loadData": form_state["load_dump_load_data"],
        "loadUsers": form_state["load_dump_load_users"],
        "loadIndexes": form_state["load_dump_load_indexes"],
        "analyzeTables": form_state["load_dump_analyze_tables"],
        "deferTableIndexes": form_state["load_dump_defer_table_indexes"],
        "handleGrantErrors": form_state["load_dump_handle_grant_errors"],
        "updateGtidSet": form_state["load_dump_update_gtid_set"],
    }

    _set_option(
        options,
        "backgroundThreads",
        _normalize_optional_positive_int(form_state["load_dump_background_threads"], "Background threads"),
    )
    _set_option(options, "schema", form_state["load_dump_schema"])
    _set_option(options, "characterSet", form_state["load_dump_character_set"])
    _set_option(options, "maxBytesPerTransaction", form_state["load_dump_max_bytes_per_transaction"])

    session_init_sql = parse_sql_statement_list(form_state["load_dump_session_init_sql"])
    if session_init_sql:
        options["sessionInitSql"] = session_init_sql

    list_option_map = {
        "includeSchemas": form_state["load_dump_include_schemas"],
        "excludeSchemas": form_state["load_dump_exclude_schemas"],
        "includeTables": form_state["load_dump_include_tables"],
        "excludeTables": form_state["load_dump_exclude_tables"],
        "includeUsers": form_state["load_dump_include_users"],
        "excludeUsers": form_state["load_dump_exclude_users"],
        "includeEvents": form_state["load_dump_include_events"],
        "excludeEvents": form_state["load_dump_exclude_events"],
        "includeRoutines": form_state["load_dump_include_routines"],
        "excludeRoutines": form_state["load_dump_exclude_routines"],
        "includeTriggers": form_state["load_dump_include_triggers"],
        "excludeTriggers": form_state["load_dump_exclude_triggers"],
        "includeLibraries": form_state["load_dump_include_libraries"],
        "excludeLibraries": form_state["load_dump_exclude_libraries"],
    }
    for option_name, raw_value in list_option_map.items():
        parsed_values = parse_string_list(raw_value)
        if parsed_values:
            options[option_name] = parsed_values

    advanced_options = parse_json_options(form_state["load_dump_advanced_json"])
    options.update(advanced_options)
    return options


def _safe_current_prefix(value):
    try:
        return normalize_relative_prefix(value)
    except ValueError:
        return ""


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


def _start_update_worker():
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

    status = {
        "state": "running",
        "step": "Queued",
        "message": "Update worker has been queued.",
        "started_at": _utc_now_iso(),
        "finished_at": "",
        "restart_requested_at": "",
        "service_names": [],
    }
    _write_update_status(status)

    env = os.environ.copy()
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


def _redirect_to_login_for_mysql_unavailable(error):
    profile_name = str(session.get("profile_name", "")).strip()
    clear_login_state(keep_profile=True)
    flash(f"MySQL connection is unavailable: {error}", "error")
    redirect_values = {"profile": profile_name} if profile_name else {}
    return redirect(url_for("login", **redirect_values))


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not is_logged_in():
            flash("Log in to continue.", "error")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


def render_dashboard(template_name, **context):
    profile = get_session_profile()
    object_storage_config = context.pop("object_storage_config", None) or load_object_storage_config()
    par_entries = get_par_entries_for_bucket(object_storage_config)
    return render_template(
        template_name,
        app_title=APP_TITLE,
        logged_in=is_logged_in(),
        current_user=session.get("mysql_username", ""),
        current_profile_name=session.get("profile_name", ""),
        connection_summary=f"{profile['host'] or '-'}:{profile['port']}" if profile else "-",
        nav_groups=NAV_GROUPS,
        current_endpoint=request.endpoint or "",
        object_storage_config=object_storage_config,
        stored_par_count=len(par_entries),
        active_par_count=len([entry for entry in par_entries if entry["is_active"]]),
        **context,
    )


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        profile = normalize_profile(
            {
                "name": request.form.get("profile_name", ""),
                "host": request.form.get("host", ""),
                "port": request.form.get("port", ""),
                "database": request.form.get("database", ""),
                "ssh_enabled": request.form.get("ssh_enabled", ""),
                "ssh_host": request.form.get("ssh_host", ""),
                "ssh_port": request.form.get("ssh_port", ""),
                "ssh_user": request.form.get("ssh_user", ""),
                "ssh_key_path": request.form.get("ssh_key_path", ""),
                "ssh_config_file": request.form.get("ssh_config_file", ""),
            }
        )
        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        errors = validate_profile(profile, require_name=False)
        if not username:
            errors.append("MySQL username is required.")

        if errors:
            for message in errors:
                flash(message, "error")
        else:
            try:
                clear_login_state(keep_profile=False)
                set_login_state(profile, username, password)
                test_mysql_connection(profile, {"username": username, "password": password})
                flash("Connected to MySQL.", "success")
                return redirect(url_for("overview_page"))
            except Exception as error:  # pragma: no cover - depends on runtime services
                clear_login_state(keep_profile=True)
                flash(f"Unable to connect: {error}", "error")

    selected_name = str(request.args.get("profile", "")).strip()
    selected_profile = get_profile_by_name(selected_name) or get_session_profile()
    return render_template(
        "login.html",
        app_title=APP_TITLE,
        page_title="Login",
        logged_in=False,
        profiles=load_profiles(),
        selected_profile=selected_profile,
        selected_profile_name=selected_name or selected_profile.get("name", ""),
    )


@app.route("/logout", methods=["POST"])
def logout():
    clear_login_state(keep_profile=False)
    flash("Logged out.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def overview_page():
    profile = get_session_profile()
    credentials = get_session_credentials()
    overview_tab = _normalize_overview_tab(request.args.get("tab"))
    object_storage_config = load_object_storage_config()
    mysql_overview = fetch_mysql_overview(profile, credentials)
    mysqlsh_status = get_mysqlsh_status()
    par_entries = get_par_entries_for_bucket(object_storage_config)
    return render_dashboard(
        "overview.html",
        page_title="Overview",
        object_storage_config=object_storage_config,
        mysql_overview=mysql_overview,
        mysqlsh_status=mysqlsh_status,
        par_entries=par_entries[:8],
        overview_default_tab=f"overview-{overview_tab}",
    )


@app.route("/admin/db-admin")
@login_required
def db_admin_page():
    profile = get_session_profile()
    credentials = get_session_credentials()
    db_admin_tab = _normalize_db_admin_tab(request.args.get("tab"))
    db_admin_detail = _normalize_db_admin_detail(request.args.get("detail"))
    db_admin_overview = fetch_db_admin_overview(profile, credentials)
    return render_dashboard(
        "db_admin.html",
        page_title="DB Admin",
        db_admin_overview=db_admin_overview,
        db_admin_default_tab=f"db-admin-{db_admin_tab}",
        db_admin_default_detail=f"pk-{db_admin_detail}",
    )


@app.route("/dashboard/events/toggle", methods=["POST"])
@app.route("/admin/db-admin/events/toggle", methods=["POST"])
@login_required
def db_admin_event_toggle():
    profile = get_session_profile()
    credentials = get_session_credentials()
    event_schema = str(request.form.get("event_schema", "")).strip()
    event_name = str(request.form.get("event_name", "")).strip()
    event_action = str(request.form.get("event_action", "")).strip().lower()

    if event_action not in {"enable", "disable"}:
        flash("Choose a valid event action.", "error")
        return redirect(url_for("db_admin_page", tab="events"))

    try:
        set_event_status(
            profile,
            credentials,
            event_schema,
            event_name,
            enabled=event_action == "enable",
        )
        flash(
            f"Event `{event_schema}`.`{event_name}` {'enabled' if event_action == 'enable' else 'disabled'}.",
            "success",
        )
    except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as error:
        return _redirect_to_login_for_mysql_unavailable(error)
    except Exception as error:  # pragma: no cover - depends on runtime services
        flash(str(error), "error")

    return redirect(url_for("db_admin_page", tab="events"))


@app.route("/admin/db-admin/primary-key/apply", methods=["POST"])
@login_required
def db_admin_apply_primary_key_fix():
    profile = get_session_profile()
    credentials = get_session_credentials()
    detail = _normalize_db_admin_detail(request.form.get("detail"))
    redirect_target = url_for("db_admin_page", tab="primary-key", detail=detail)

    try:
        selected_targets = _parse_selected_primary_key_targets(request.form.getlist("selected_tables"))
    except ValueError as error:
        flash(str(error), "error")
        return redirect(redirect_target)

    if not selected_targets:
        table_schema = str(request.form.get("table_schema", "")).strip()
        table_name = str(request.form.get("table_name", "")).strip()
        if table_schema and table_name:
            selected_targets = [(table_schema, table_name)]

    if not selected_targets:
        flash("Select at least one table to apply the primary key fix.", "error")
        return redirect(redirect_target)

    successes = []
    failures = []
    for table_schema, table_name in selected_targets:
        try:
            result = apply_primary_key_fix(profile, credentials, table_schema, table_name)
            successes.append(result)
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as error:
            return _redirect_to_login_for_mysql_unavailable(error)
        except Exception as error:  # pragma: no cover - depends on runtime services
            failures.append((table_schema, table_name, str(error)))

    if len(successes) == 1 and not failures:
        result = successes[0]
        flash(
            f"Primary key fix applied to `{result['table_schema']}`.`{result['table_name']}`. "
            f"{result['message']}",
            "success",
        )
    elif successes:
        flash(
            f"Primary key fix applied to {len(successes)} "
            f"table{'s' if len(successes) != 1 else ''}.",
            "success",
        )

    for table_schema, table_name, message in failures:
        flash(f"Primary key fix failed for `{table_schema}`.`{table_name}`: {message}", "error")

    return redirect(redirect_target)


@app.route("/admin/profile", methods=["GET", "POST"])
def profile_page():
    profiles = load_profiles()
    selected_name = str(request.values.get("selected_profile", "")).strip()
    editing_profile = get_profile_by_name(selected_name) or get_session_profile()

    if request.method == "POST":
        action = str(request.form.get("profile_action", "")).strip()
        profile_payload = normalize_profile(request.form)
        errors = validate_profile(profile_payload)

        if action == "save":
            if errors:
                for message in errors:
                    flash(message, "error")
            else:
                remaining = [row for row in profiles if row["name"].lower() != profile_payload["name"].lower()]
                remaining.append(profile_payload)
                save_profiles(remaining)
                if get_session_profile()["name"].lower() == profile_payload["name"].lower():
                    set_session_profile(profile_payload)
                flash(f"Profile `{profile_payload['name']}` saved.", "success")
                return redirect(url_for("profile_page", selected_profile=profile_payload["name"]))
        elif action == "delete":
            if not profile_payload["name"]:
                flash("Choose a profile to delete.", "error")
            else:
                remaining = [row for row in profiles if row["name"].lower() != profile_payload["name"].lower()]
                if len(remaining) == len(profiles):
                    flash("Profile not found.", "error")
                else:
                    save_profiles(remaining)
                    if get_session_profile()["name"].lower() == profile_payload["name"].lower():
                        session["connection_profile"] = normalize_profile({})
                        session["profile_name"] = ""
                    flash(f"Profile `{profile_payload['name']}` deleted.", "success")
                    return redirect(url_for("profile_page"))

        editing_profile = profile_payload
        profiles = load_profiles()

    return render_dashboard(
        "profile.html",
        page_title="Profile",
        profiles=profiles,
        selected_profile_name=selected_name,
        editing_profile=editing_profile,
    )


@app.route("/admin/object-storage", methods=["GET", "POST"])
@login_required
def object_storage_settings_page():
    config = load_object_storage_config()
    if request.method == "POST":
        merged_payload = dict(config)
        merged_payload.update(request.form.to_dict())
        merged_payload["managed_folders"] = config.get("managed_folders", [])
        if str(merged_payload.get("config_source", "")).strip().lower() == "local":
            save_local_oci_config_text(request.form.get("local_config_text", ""))
        config = normalize_object_storage(merged_payload)
        save_object_storage_config(config)
        flash("OCI configuration saved.", "success")
        return redirect(url_for("object_storage_settings_page"))

    return render_dashboard(
        "object_storage_settings.html",
        page_title="OCI Configuration",
        object_storage_config=config,
        oci_config_status=build_oci_config_status(config),
    )


@app.route("/admin/update")
@login_required
def update_mysql_shell_web_page():
    return render_dashboard(
        "update_mysql_shell_web.html",
        page_title="Update MySQL Shell Web",
        update_status=_normalize_update_status(),
        status_url=url_for("update_mysql_shell_web_status"),
    )


@app.route("/admin/update/start", methods=["POST"])
@login_required
def update_mysql_shell_web_start():
    try:
        _start_update_worker()
        flash("Update started.", "success")
    except Exception as error:
        flash(str(error), "error")
    return redirect(url_for("update_mysql_shell_web_page"))


@app.route("/admin/update/status")
@login_required
def update_mysql_shell_web_status():
    return jsonify(_normalize_update_status())


@app.route("/object-storage/par", methods=["GET", "POST"])
@login_required
def par_manager_page():
    config = load_object_storage_config()
    form_values = {
        "name": "",
        "target_type": "prefix",
        "relative_prefix": "",
        "access_type": "AnyObjectReadWrite",
        "allow_listing": True,
        "expires_at": format_datetime_local(datetime.now().astimezone() + timedelta(days=7)),
    }

    if request.method == "POST":
        action = str(request.form.get("par_action", "")).strip()
        if action == "create":
            form_values.update(
                {
                    "name": str(request.form.get("name", "")).strip(),
                    "target_type": str(request.form.get("target_type", "prefix")).strip() or "prefix",
                    "relative_prefix": str(request.form.get("relative_prefix", "")).strip(),
                    "access_type": str(request.form.get("access_type", "AnyObjectReadWrite")).strip()
                    or "AnyObjectReadWrite",
                    "allow_listing": _normalize_checkbox(request.form.get("allow_listing")),
                    "expires_at": str(request.form.get("expires_at", "")).strip() or form_values["expires_at"],
                }
            )
            try:
                created_entry = create_par_record(config, request.form)
                flash(f"PAR `{created_entry['name']}` created and stored for reuse.", "success")
                return redirect(url_for("par_manager_page"))
            except Exception as error:  # pragma: no cover - depends on runtime services
                flash(str(error), "error")
        elif action == "delete":
            try:
                deleted_entry = delete_par_record(config, request.form.get("entry_id", ""))
                flash(f"PAR `{deleted_entry['name']}` revoked and removed.", "success")
                return redirect(url_for("par_manager_page"))
            except Exception as error:  # pragma: no cover - depends on runtime services
                flash(str(error), "error")

    return render_dashboard(
        "par_manager.html",
        page_title="PAR Manager",
        object_storage_config=config,
        par_entries=get_par_entries_for_bucket(config),
        par_target_options=PAR_TARGET_OPTIONS,
        par_access_options=PAR_ACCESS_OPTIONS,
        form_values=form_values,
    )


@app.route("/object-storage/folders", methods=["GET", "POST"])
@login_required
def folder_manager_page():
    config = load_object_storage_config()
    current_prefix = _safe_current_prefix(request.values.get("current_prefix", ""))

    if request.method == "POST":
        action = str(request.form.get("folder_action", "")).strip()
        try:
            if action == "create":
                folder_name = request.form.get("folder_name", "")
                folder_prefix = join_relative_prefixes(current_prefix, folder_name)
                config = create_managed_folder(config, folder_prefix)
                flash(f"Folder prefix `{folder_prefix}` registered.", "success")
            elif action == "rename":
                source_prefix = request.form.get("source_prefix", "")
                target_prefix = join_relative_prefixes(
                    parent_relative_prefix(source_prefix),
                    request.form.get("new_name", ""),
                )
                config, renamed_count = rename_folder(config, source_prefix, target_prefix)
                flash(
                    f"Folder `{source_prefix}` renamed to `{target_prefix}`. "
                    f"Objects moved: {renamed_count}.",
                    "success",
                )
            elif action == "delete":
                source_prefix = request.form.get("source_prefix", "")
                config, deleted_count = delete_folder(config, source_prefix)
                flash(f"Folder `{source_prefix}` deleted. Objects removed: {deleted_count}.", "success")
            return redirect(url_for("folder_manager_page", current_prefix=current_prefix))
        except Exception as error:  # pragma: no cover - depends on runtime services
            flash(str(error), "error")

    folder_state = None
    folder_error = ""
    try:
        folder_state = get_folder_browser_state(config, current_prefix)
    except Exception as error:  # pragma: no cover - depends on runtime services
        folder_error = str(error)
        folder_state = {
            "current_prefix": current_prefix,
            "current_full_prefix": "/",
            "parent_prefix": parent_relative_prefix(current_prefix),
            "folders": [],
            "objects": [],
            "breadcrumbs": [{"label": "Root", "relative_prefix": ""}],
            "managed_folder_count": len(config.get("managed_folders", [])),
        }

    return render_dashboard(
        "folder_manager.html",
        page_title="Folders",
        object_storage_config=config,
        folder_state=folder_state,
        folder_error=folder_error,
    )


@app.route("/mysql-shell/operations", methods=["GET", "POST"])
@login_required
def shell_operations_page():
    config = load_object_storage_config()
    profile = get_session_profile()
    credentials = get_session_credentials()
    requested_job_id = str(request.args.get("job_id", "")).strip()
    operation_names = {name for name, _label in SHELL_OPERATION_OPTIONS}
    shell_page = _normalize_shell_operations_page(
        request.values.get("page"),
        request.values.get("operation"),
        request.values.get("view"),
    )
    operation = _normalize_operation(request.values.get("operation", "dump-instance"))
    if shell_page in operation_names:
        operation = shell_page

    dump_pars = list_active_pars_for_purpose(config, "dump")
    load_pars = list_active_pars_for_purpose(config, "load")
    dump_par_lookup = {entry["id"]: entry for entry in dump_pars}
    load_par_lookup = {entry["id"]: entry for entry in load_pars}

    schema_error = ""
    user_schemas = []
    try:
        user_schemas = fetch_accessible_schemas(profile, credentials)
    except Exception as error:  # pragma: no cover - depends on runtime services
        schema_error = str(error)

    dump_instance_enabled_event_count = None
    try:
        dump_instance_enabled_event_count = fetch_enabled_event_count(profile, credentials)
    except Exception:  # pragma: no cover - depends on runtime services
        dump_instance_enabled_event_count = None

    selected_schemas = request.form.getlist("schemas") if request.method == "POST" else request.args.getlist("schemas")
    selected_dump_option_profile_name = _request_text("dump_option_profile_name")
    selected_load_option_profile_name = _request_text("load_option_profile_name")
    dump_option_profile_edit_name = _request_text(
        "dump_option_profile_edit_name", selected_dump_option_profile_name
    )
    load_option_profile_edit_name = _request_text(
        "load_option_profile_edit_name", selected_load_option_profile_name
    )
    option_profile_kind = _normalize_option_profile_kind(
        request.values.get("option_profile_kind"),
        default="load" if operation == "load-dump" else "dump",
    )
    dump_option_profile_json = _request_text("dump_option_profile_json")
    load_option_profile_json = _request_text("load_option_profile_json")
    dump_option_profile_entries = load_option_profiles("dump")
    load_option_profile_entries = load_option_profiles("load")
    selected_dump_option_profile = (
        get_option_profile("dump", selected_dump_option_profile_name) if selected_dump_option_profile_name else None
    )
    selected_load_option_profile = (
        get_option_profile("load", selected_load_option_profile_name) if selected_load_option_profile_name else None
    )
    option_profile_action = str(request.form.get("option_profile_action", "")).strip().lower() if request.method == "POST" else ""
    validation_action = str(request.form.get("validation_action", "")).strip().lower() if request.method == "POST" else ""
    dump_option_filter_catalog = {
        "schemas": [],
        "tables": [],
        "users": [],
        "events": [],
        "routines": [],
        "triggers": [],
        "libraries": [],
        "errors": {},
    }
    if shell_page == "option-profiles" and option_profile_kind == "dump":
        try:
            dump_option_filter_catalog = fetch_dump_filter_catalog(profile, credentials)
        except Exception as error:  # pragma: no cover - depends on server privileges
            dump_option_filter_catalog["errors"]["catalog"] = str(error)

    load_progress_default = _request_text("load_dump_progress_file")
    if not load_progress_default and load_pars:
        load_progress_default = default_progress_file(load_pars[0]["id"], "load-dump")

    form_state = {}
    form_state.update(_build_dump_form_state("dump_instance", include_users=True))
    form_state.update(_build_dump_form_state("dump_schemas"))
    form_state.update(_build_load_dump_form_state(load_progress_default))
    if profile.get("ssh_enabled") and request.values.get("load_dump_threads") is None:
        form_state["load_dump_threads"] = "1"

    operation_result = None

    if requested_job_id:
        job_access_profile_name = None if shell_page == "history" else session.get("profile_name", "")
        operation_result = build_mysqlsh_job_snapshot(
            requested_job_id,
            owner_username=session.get("mysql_username", ""),
            owner_profile_name=job_access_profile_name,
        )
        if operation_result is None:
            flash("Requested MySQL Shell job was not found.", "error")
        else:
            operation = operation_result.get("operation", operation) or operation
            if shell_page in operation_names:
                shell_page = operation
            saved_form_state = operation_result.get("form_state") or {}
            for key, value in saved_form_state.items():
                if key in form_state and key not in request.args:
                    form_state[key] = value
            if not selected_schemas:
                selected_schemas = list(operation_result.get("selected_schemas") or [])

    if not requested_job_id and (request.method == "GET" or (request.method == "POST" and not option_profile_action)):
        if selected_dump_option_profile is not None:
            _apply_dump_option_profile_values(
                form_state,
                selected_dump_option_profile["values"],
                "dump_instance",
                include_users=True,
            )
            _apply_dump_option_profile_values(form_state, selected_dump_option_profile["values"], "dump_schemas")
            dump_option_profile_edit_name = selected_dump_option_profile["name"]
        if selected_load_option_profile is not None:
            _apply_load_option_profile_values(form_state, selected_load_option_profile["values"])
            load_option_profile_edit_name = selected_load_option_profile["name"]

    form_state["load_dump_progress_file"] = normalize_progress_file_value(form_state["load_dump_progress_file"])

    if request.method == "POST":
        if option_profile_action:
            option_profile_kind = _normalize_option_profile_kind(
                request.form.get("option_profile_kind"),
                default=option_profile_kind,
            )
            try:
                if option_profile_kind == "load":
                    if option_profile_action == "apply":
                        if not selected_load_option_profile_name:
                            load_option_profile_edit_name = ""
                            selected_load_option_profile = None
                        else:
                            selected_load_option_profile = get_option_profile("load", selected_load_option_profile_name)
                            if selected_load_option_profile is None:
                                raise ValueError("Selected load option profile was not found.")
                            _apply_load_option_profile_values(form_state, selected_load_option_profile["values"])
                            load_option_profile_edit_name = selected_load_option_profile["name"]
                            load_option_profile_json = _format_option_profile_editor_json(
                                selected_load_option_profile["values"]
                            )
                            flash(f"Applied load option profile `{selected_load_option_profile['name']}`.", "success")
                    elif option_profile_action == "save":
                        target_profile_name = (load_option_profile_edit_name or selected_load_option_profile_name).strip()
                        if not target_profile_name:
                            raise ValueError("Enter a profile name to save the load options.")
                        if request.form.get("load_option_profile_json", "").strip():
                            load_profile_values = _parse_option_profile_editor_json(load_option_profile_json, "load")
                        else:
                            load_profile_values = _extract_load_option_profile_values(form_state)
                        selected_load_option_profile = save_option_profile("load", target_profile_name, load_profile_values)
                        _apply_load_option_profile_values(form_state, selected_load_option_profile["values"])
                        selected_load_option_profile_name = target_profile_name
                        load_option_profile_edit_name = target_profile_name
                        load_option_profile_json = _format_option_profile_editor_json(
                            selected_load_option_profile["values"]
                        )
                        load_option_profile_entries = load_option_profiles("load")
                        flash(f"Saved load option profile `{target_profile_name}`.", "success")
                    elif option_profile_action == "delete":
                        if not selected_load_option_profile_name:
                            raise ValueError("Select a load option profile to delete.")
                        if not delete_option_profile("load", selected_load_option_profile_name):
                            raise ValueError("Selected load option profile was not found.")
                        flash(f"Deleted load option profile `{selected_load_option_profile_name}`.", "success")
                        if load_option_profile_edit_name == selected_load_option_profile_name:
                            load_option_profile_edit_name = ""
                        selected_load_option_profile_name = ""
                        selected_load_option_profile = None
                        load_option_profile_entries = load_option_profiles("load")
                    else:
                        raise ValueError("Unsupported load option profile action.")
                else:
                    if option_profile_action == "apply":
                        if not selected_dump_option_profile_name:
                            dump_option_profile_edit_name = ""
                            selected_dump_option_profile = None
                        else:
                            selected_dump_option_profile = get_option_profile("dump", selected_dump_option_profile_name)
                            if selected_dump_option_profile is None:
                                raise ValueError("Selected dump option profile was not found.")
                            _apply_dump_option_profile_values(
                                form_state,
                                selected_dump_option_profile["values"],
                                "dump_instance",
                                include_users=True,
                            )
                            _apply_dump_option_profile_values(
                                form_state,
                                selected_dump_option_profile["values"],
                                "dump_schemas",
                            )
                            dump_option_profile_edit_name = selected_dump_option_profile["name"]
                            dump_option_profile_json = _format_option_profile_editor_json(
                                selected_dump_option_profile["values"]
                            )
                            flash(f"Applied dump option profile `{selected_dump_option_profile['name']}`.", "success")
                    elif option_profile_action == "save":
                        target_profile_name = (dump_option_profile_edit_name or selected_dump_option_profile_name).strip()
                        if not target_profile_name:
                            raise ValueError("Enter a profile name to save the dump options.")
                        dump_profile_prefix = "dump_instance" if operation in {"dump-instance", "option-profiles"} else "dump_schemas"
                        dump_profile_include_users = operation in {"dump-instance", "option-profiles"}
                        if request.form.get("dump_option_profile_json", "").strip():
                            dump_profile_values = _parse_option_profile_editor_json(dump_option_profile_json, "dump")
                        else:
                            dump_profile_values = _extract_dump_option_profile_values(
                                form_state,
                                dump_profile_prefix,
                                include_users=dump_profile_include_users,
                            )
                        selected_dump_option_profile = save_option_profile("dump", target_profile_name, dump_profile_values)
                        _apply_dump_option_profile_values(
                            form_state,
                            selected_dump_option_profile["values"],
                            "dump_instance",
                            include_users=True,
                        )
                        _apply_dump_option_profile_values(
                            form_state,
                            selected_dump_option_profile["values"],
                            "dump_schemas",
                        )
                        selected_dump_option_profile_name = target_profile_name
                        dump_option_profile_edit_name = target_profile_name
                        dump_option_profile_json = _format_option_profile_editor_json(
                            selected_dump_option_profile["values"]
                        )
                        dump_option_profile_entries = load_option_profiles("dump")
                        flash(f"Saved dump option profile `{target_profile_name}`.", "success")
                    elif option_profile_action == "delete":
                        if not selected_dump_option_profile_name:
                            raise ValueError("Select a dump option profile to delete.")
                        if not delete_option_profile("dump", selected_dump_option_profile_name):
                            raise ValueError("Selected dump option profile was not found.")
                        flash(f"Deleted dump option profile `{selected_dump_option_profile_name}`.", "success")
                        if dump_option_profile_edit_name == selected_dump_option_profile_name:
                            dump_option_profile_edit_name = ""
                        selected_dump_option_profile_name = ""
                        selected_dump_option_profile = None
                        dump_option_profile_entries = load_option_profiles("dump")
                    else:
                        raise ValueError("Unsupported dump option profile action.")
            except Exception as error:  # pragma: no cover - depends on runtime services
                flash(str(error), "error")

            form_state["load_dump_progress_file"] = normalize_progress_file_value(form_state["load_dump_progress_file"])
        elif validation_action:
            if validation_action != "dump-schemas":
                flash("Unsupported validation action.", "error")
            elif not selected_schemas:
                flash("Select at least one schema before running validation.", "error")
        elif operation != "option-profiles":
            try:
                if operation == "dump-instance":
                    par_entry = dump_par_lookup.get(form_state["dump_instance_par_id"])
                    if par_entry is None:
                        raise ValueError("Choose an active read/write PAR for dumpInstance.")
                    dump_options = _build_dump_options(form_state, "dump_instance", include_users=True)
                    dump_summary_rows = [
                        ("Operation", "util.dumpInstance"),
                        ("Target PAR", par_entry["name"]),
                        ("Output URL", par_entry["par_url"]),
                        ("Threads", str(dump_options["threads"])),
                        ("Option Count", str(len(dump_options))),
                    ]
                    if selected_dump_option_profile_name:
                        dump_summary_rows.append(("Option Profile", selected_dump_option_profile_name))
                    request_payload = build_dump_instance_request(par_entry["par_url"], dump_options)
                    operation_result = submit_mysqlsh_job(
                        profile,
                        credentials,
                        request_payload,
                        database=profile.get("database", ""),
                        operation=operation,
                        operation_name="dumpInstance",
                        owner_username=session.get("mysql_username", ""),
                        owner_profile_name=session.get("profile_name", ""),
                        options_json=json.dumps(dump_options, indent=2, sort_keys=True),
                        form_state=form_state,
                        selected_schemas=selected_schemas,
                        summary_rows=dump_summary_rows,
                    )
                elif operation == "dump-schemas":
                    par_entry = dump_par_lookup.get(form_state["dump_schemas_par_id"])
                    if par_entry is None:
                        raise ValueError("Choose an active read/write PAR for dumpSchemas.")
                    if not selected_schemas:
                        raise ValueError("Select at least one schema for dumpSchemas.")
                    dump_options = _build_dump_options(form_state, "dump_schemas")
                    dump_summary_rows = [
                        ("Operation", "util.dumpSchemas"),
                        ("Schemas", ", ".join(selected_schemas)),
                        ("Target PAR", par_entry["name"]),
                        ("Output URL", par_entry["par_url"]),
                        ("Threads", str(dump_options["threads"])),
                        ("Option Count", str(len(dump_options))),
                    ]
                    if selected_dump_option_profile_name:
                        dump_summary_rows.append(("Option Profile", selected_dump_option_profile_name))
                    request_payload = build_dump_schemas_request(selected_schemas, par_entry["par_url"], dump_options)
                    operation_result = submit_mysqlsh_job(
                        profile,
                        credentials,
                        request_payload,
                        database=profile.get("database", ""),
                        operation=operation,
                        operation_name="dumpSchemas",
                        owner_username=session.get("mysql_username", ""),
                        owner_profile_name=session.get("profile_name", ""),
                        options_json=json.dumps(dump_options, indent=2, sort_keys=True),
                        form_state=form_state,
                        selected_schemas=selected_schemas,
                        summary_rows=dump_summary_rows,
                    )
                else:
                    par_entry = load_par_lookup.get(form_state["load_dump_par_id"])
                    if par_entry is None:
                        raise ValueError("Choose an active read or read/write PAR for loadDump.")
                    progress_file = normalize_progress_file_value(form_state["load_dump_progress_file"]) or default_progress_file(
                        par_entry["id"], "load-dump"
                    )
                    form_state["load_dump_progress_file"] = progress_file
                    load_options = _build_load_dump_options(form_state)
                    load_summary_rows = [
                        ("Operation", "util.loadDump"),
                        ("Source PAR", par_entry["name"]),
                        ("Input URL", par_entry["par_url"]),
                        ("Progress File", progress_file),
                        ("Threads", str(load_options["threads"])),
                        ("Reset Progress", "Yes" if load_options["resetProgress"] else "No"),
                        ("Option Count", str(len(load_options))),
                    ]
                    if selected_load_option_profile_name:
                        load_summary_rows.append(("Option Profile", selected_load_option_profile_name))
                    request_payload = build_load_dump_request(par_entry["par_url"], load_options)
                    operation_result = submit_mysqlsh_job(
                        profile,
                        credentials,
                        request_payload,
                        database=profile.get("database", ""),
                        operation=operation,
                        operation_name="loadDump",
                        owner_username=session.get("mysql_username", ""),
                        owner_profile_name=session.get("profile_name", ""),
                        options_json=json.dumps(load_options, indent=2, sort_keys=True),
                        form_state=form_state,
                        selected_schemas=selected_schemas,
                        summary_rows=load_summary_rows,
                    )

                session["last_mysqlsh_job_id"] = operation_result["job_id"]
                redirect_values = {
                    "page": operation,
                    "operation": operation,
                    "job_id": operation_result["job_id"],
                }
                if selected_dump_option_profile_name:
                    redirect_values["dump_option_profile_name"] = selected_dump_option_profile_name
                if selected_load_option_profile_name:
                    redirect_values["load_option_profile_name"] = selected_load_option_profile_name
                flash(
                    f"MySQL Shell {operation_result['operation_name']} submitted. Job ID: {operation_result['job_id']}.",
                    "success",
                )
                return redirect(url_for("shell_operations_page", **redirect_values))
            except Exception as error:  # pragma: no cover - depends on runtime services
                flash(str(error), "error")

    if not dump_option_profile_json:
        dump_profile_values = (
            selected_dump_option_profile["values"]
            if selected_dump_option_profile is not None
            else _extract_dump_option_profile_values(form_state, "dump_instance", include_users=True)
        )
        dump_option_profile_json = _format_option_profile_editor_json(dump_profile_values)

    if not load_option_profile_json:
        load_profile_values = (
            selected_load_option_profile["values"]
            if selected_load_option_profile is not None
            else _extract_load_option_profile_values(form_state)
        )
        load_option_profile_json = _format_option_profile_editor_json(load_profile_values)

    dump_instance_validation = None
    dump_schemas_validation = None
    dump_instance_validation_scope = "All accessible schemas"
    dump_schemas_validation_scope = f"Selected schemas ({len(selected_schemas)})" if selected_schemas else ""
    dump_schemas_validation_scope_names = selected_schemas or user_schemas or None
    show_dump_schemas_validation = validation_action == "dump-schemas" and bool(selected_schemas)

    try:
        dump_instance_validation = fetch_dump_validation_summary(
            profile,
            credentials,
            schema_names=user_schemas or None,
        )
        if show_dump_schemas_validation:
            if dump_schemas_validation_scope_names == (user_schemas or None):
                dump_schemas_validation = dict(dump_instance_validation)
            else:
                dump_schemas_validation = fetch_dump_validation_summary(
                    profile,
                    credentials,
                    schema_names=dump_schemas_validation_scope_names,
                )
    except Exception:  # pragma: no cover - depends on runtime services
        dump_instance_validation = None
        dump_schemas_validation = None

    operation_history = []
    if shell_page == "history":
        operation_history = list_mysqlsh_job_history(
            owner_username=session.get("mysql_username", ""),
            owner_profile_name=None,
            limit=100,
        )

    return render_dashboard(
        "shell_operations.html",
        page_title="MySQL Shell Operations",
        object_storage_config=config,
        shell_operation_options=SHELL_OPERATION_OPTIONS,
        shell_page=shell_page,
        operation=operation,
        form_state=form_state,
        selected_schemas=selected_schemas,
        user_schemas=user_schemas,
        schema_error=schema_error,
        dump_pars=dump_pars,
        load_pars=load_pars,
        compression_options=COMPRESSION_OPTIONS,
        dump_dialect_options=DUMP_DIALECT_OPTIONS,
        dump_compatibility_options=DUMP_COMPATIBILITY_OPTIONS,
        load_analyze_tables_options=LOAD_ANALYZE_TABLES_OPTIONS,
        load_defer_table_indexes_options=LOAD_DEFER_TABLE_INDEXES_OPTIONS,
        load_handle_grant_errors_options=LOAD_HANDLE_GRANT_ERRORS_OPTIONS,
        load_update_gtid_set_options=LOAD_UPDATE_GTID_SET_OPTIONS,
        load_dump_ssh_enabled=bool(profile.get("ssh_enabled")),
        dump_option_profile_entries=dump_option_profile_entries,
        load_option_profile_entries=load_option_profile_entries,
        selected_dump_option_profile_name=selected_dump_option_profile_name,
        selected_load_option_profile_name=selected_load_option_profile_name,
        selected_dump_option_profile=selected_dump_option_profile,
        selected_load_option_profile=selected_load_option_profile,
        dump_option_profile_edit_name=dump_option_profile_edit_name,
        load_option_profile_edit_name=load_option_profile_edit_name,
        dump_option_profile_json=dump_option_profile_json,
        load_option_profile_json=load_option_profile_json,
        option_profile_kind=option_profile_kind,
        is_history_page=shell_page == "history",
        is_option_profiles_page=shell_page == "option-profiles",
        operation_history=operation_history,
        operation_result=operation_result,
        dump_instance_enabled_event_count=dump_instance_enabled_event_count,
        dump_instance_validation=dump_instance_validation,
        dump_schemas_validation=dump_schemas_validation,
        dump_instance_validation_scope=dump_instance_validation_scope,
        dump_schemas_validation_scope=dump_schemas_validation_scope,
        show_dump_schemas_validation=show_dump_schemas_validation,
        dump_option_filter_catalog=dump_option_filter_catalog,
    )


@app.route("/mysql-shell/operations/jobs/<job_id>", methods=["GET"])
@login_required
def mysqlsh_job_status_api(job_id):
    shell_page = _normalize_shell_operations_page(
        request.args.get("page"),
        request.args.get("operation"),
        request.args.get("view"),
    )
    job_access_profile_name = None if shell_page == "history" else session.get("profile_name", "")
    snapshot = build_mysqlsh_job_snapshot(
        job_id,
        owner_username=session.get("mysql_username", ""),
        owner_profile_name=job_access_profile_name,
    )
    if snapshot is None:
        return jsonify({"error": "MySQL Shell job not found."}), 404
    return jsonify(snapshot)


@app.route("/mysql-shell/operations/jobs/<job_id>/cancel", methods=["POST"])
@login_required
def mysqlsh_job_cancel(job_id):
    next_page = _normalize_shell_operations_page(
        request.form.get("page"),
        request.form.get("operation"),
        request.form.get("view"),
    )
    job_access_profile_name = None if next_page == "history" else session.get("profile_name", "")
    snapshot = cancel_mysqlsh_job(
        job_id,
        owner_username=session.get("mysql_username", ""),
        owner_profile_name=job_access_profile_name,
    )
    if snapshot is None:
        flash("MySQL Shell job not found.", "error")
        return redirect(url_for("shell_operations_page"))

    if snapshot["status"] == "cancel_requested":
        flash(f"MySQL Shell {snapshot['operation_name']} cancel requested.", "success")
    elif snapshot["status"] == "failed":
        flash(f"MySQL Shell {snapshot['operation_name']} already finished with status {snapshot['status_label']}.", "error")
    elif snapshot["status"] == "succeeded":
        flash(f"MySQL Shell {snapshot['operation_name']} already finished with status {snapshot['status_label']}.", "success")
    else:
        flash(f"MySQL Shell {snapshot['operation_name']} job canceled.", "success")
    session["last_mysqlsh_job_id"] = snapshot["job_id"]
    return redirect(
        url_for(
            "shell_operations_page",
            page=next_page,
            operation=snapshot["operation"],
            job_id=snapshot["job_id"],
        )
    )


@app.route("/mysql-shell/operations/jobs/<job_id>/cleanup", methods=["POST"])
@login_required
def mysqlsh_job_cleanup(job_id):
    next_page = _normalize_shell_operations_page(
        request.form.get("page"),
        request.form.get("operation"),
        request.form.get("view"),
    )
    job_access_profile_name = None if next_page == "history" else session.get("profile_name", "")
    snapshot = build_mysqlsh_job_snapshot(
        job_id,
        owner_username=session.get("mysql_username", ""),
        owner_profile_name=job_access_profile_name,
    )
    if snapshot is None:
        flash("MySQL Shell job not found.", "error")
        return redirect(url_for("shell_operations_page"))

    try:
        cleanup_mysqlsh_job(
            job_id,
            owner_username=session.get("mysql_username", ""),
            owner_profile_name=job_access_profile_name,
        )
    except Exception as error:  # pragma: no cover - filesystem/runtime path
        flash(str(error), "error")
        return redirect(
            url_for(
                "shell_operations_page",
                page=next_page,
                operation=snapshot["operation"],
                job_id=job_id,
            )
        )

    if session.get("last_mysqlsh_job_id") == job_id:
        session.pop("last_mysqlsh_job_id", None)
    flash(f"MySQL Shell {snapshot['operation_name']} job cleaned up.", "success")
    return redirect(url_for("shell_operations_page", page=next_page, operation=snapshot["operation"]))


@app.errorhandler(pymysql.err.OperationalError)
def handle_mysql_operational_error(error):
    if not is_logged_in():
        return f"MySQL operational error: {error}", 500
    return _redirect_to_login_for_mysql_unavailable(error)


@app.errorhandler(pymysql.err.InterfaceError)
def handle_mysql_interface_error(error):
    if not is_logged_in():
        return f"MySQL interface error: {error}", 500
    return _redirect_to_login_for_mysql_unavailable(error)


def _initialize_app_files():
    ensure_profile_store()
    ensure_option_profile_store()
    ensure_object_storage_store()
    ensure_par_store()
    ensure_runtime_dirs()
    ensure_job_store()


_initialize_app_files()


if __name__ == "__main__":
    app.run(
        debug=False,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "5000")),
    )
