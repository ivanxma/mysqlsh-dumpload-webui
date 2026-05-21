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
    "exclude_lakehouse_tables",
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
    "exclude_lakehouse_tables",
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

GTID_SERVER_UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?=:)"
)


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


def _extract_gtid_server_uuids(*gtid_values):
    uuids = []
    seen = set()
    for value in gtid_values:
        for match in GTID_SERVER_UUID_PATTERN.findall(str(value or "")):
            normalized = match.lower()
            if normalized in seen:
                continue
            uuids.append(normalized)
            seen.add(normalized)
    return uuids


def _build_load_target_gtid_context(profile, credentials):
    overview = fetch_mysql_overview(profile, credentials)
    server_uuid = str(overview.get("server_uuid", "") or "").strip().lower()
    gtid_executed = str(overview.get("gtid_executed", "") or "").strip()
    gtid_purged = str(overview.get("gtid_purged", "") or "").strip()
    gtid_uuids = _extract_gtid_server_uuids(gtid_executed, gtid_purged)
    other_uuids = [value for value in gtid_uuids if value != server_uuid] if server_uuid else list(gtid_uuids)

    if overview.get("error"):
        state = "warning"
        badge_class = "warn"
        badge_label = "Unavailable"
        message = overview["error"]
    elif not gtid_executed and not gtid_purged:
        state = "empty"
        badge_class = "good"
        badge_label = "GTID Empty"
        message = "GTID executed and GTID purged are empty on this DB System."
    elif other_uuids:
        state = "mixed"
        badge_class = "danger"
        badge_label = "GTID Warning"
        message = "WARNING: Mixing GTIDs from different Servers."
    elif server_uuid and gtid_uuids == [server_uuid]:
        state = "local"
        badge_class = "muted"
        badge_label = "Local GTID"
        message = "The Server has GTID generated only by this DB."
    else:
        state = "unknown"
        badge_class = "warn"
        badge_label = "Review GTID"
        message = "Review GTID executed and GTID purged before loading data."

    return {
        "state": state,
        "badge_class": badge_class,
        "badge_label": badge_label,
        "message": message,
        "server_host": overview.get("server_host", ""),
        "version": overview.get("version", ""),
        "server_id": overview.get("server_id", ""),
        "server_uuid": overview.get("server_uuid", ""),
        "gtid_mode": overview.get("gtid_mode", ""),
        "gtid_executed": gtid_executed,
        "gtid_purged": gtid_purged,
        "gtid_server_uuids": gtid_uuids,
        "other_server_uuids": other_uuids,
    }


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
        f"{prefix}_exclude_lakehouse_tables": _request_checkbox(f"{prefix}_exclude_lakehouse_tables"),
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


def _merge_exclude_tables(options, table_names):
    merged = []
    seen = set()
    existing_tables = options.get("excludeTables") or []
    if isinstance(existing_tables, str):
        existing_tables = parse_string_list(existing_tables)
    for table_name in list(existing_tables) + list(table_names or []):
        normalized_table_name = str(table_name or "").strip()
        if not normalized_table_name or normalized_table_name in seen:
            continue
        merged.append(normalized_table_name)
        seen.add(normalized_table_name)
    if merged:
        options["excludeTables"] = merged


def _option_list(options, key):
    value = options.get(key)
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    if isinstance(value, str):
        return parse_string_list(value)
    return []


def _dump_filter_scope_from_options(options, *, base_schema_names=None):
    return {
        "schema_names": list(base_schema_names or []),
        "include_schemas": _option_list(options, "includeSchemas"),
        "exclude_schemas": _option_list(options, "excludeSchemas"),
        "include_tables": _option_list(options, "includeTables"),
        "exclude_tables": _option_list(options, "excludeTables"),
    }


def _build_dump_options_for_scope(form_state, prefix, *, include_users=False):
    return _build_dump_options(
        form_state,
        prefix,
        include_users=include_users,
        lakehouse_tables=[],
    )


def _fetch_lakehouse_tables_for_dump_scope(profile, credentials, scope):
    return fetch_lakehouse_table_names(
        profile,
        credentials,
        schema_names=scope["schema_names"] or None,
        include_schemas=scope["include_schemas"],
        exclude_schemas=scope["exclude_schemas"],
        include_tables=scope["include_tables"],
        exclude_tables=scope["exclude_tables"],
    )


def _build_dump_validation(profile, credentials, form_state, prefix, *, base_schema_names=None, include_users=False):
    scope_options = _build_dump_options_for_scope(
        form_state,
        prefix,
        include_users=include_users,
    )
    dump_scope = _dump_filter_scope_from_options(scope_options, base_schema_names=base_schema_names)
    validation = fetch_dump_validation_summary(
        profile,
        credentials,
        schema_names=dump_scope["schema_names"] or None,
        include_schemas=dump_scope["include_schemas"],
        exclude_schemas=dump_scope["exclude_schemas"],
        include_tables=dump_scope["include_tables"],
        exclude_tables=dump_scope["exclude_tables"],
    )
    include_table_set = set(dump_scope["include_tables"])
    lakehouse_set = set(validation.get("lakehouse_tables") or [])
    validation["lakehouse_include_table_conflicts"] = sorted(include_table_set & lakehouse_set)
    validation["has_include_table_filter"] = bool(dump_scope["include_tables"])
    validation["has_include_schema_filter"] = bool(dump_scope["include_schemas"])
    validation["has_exclude_table_filter"] = bool(dump_scope["exclude_tables"])
    validation["has_exclude_schema_filter"] = bool(dump_scope["exclude_schemas"])
    return validation


def _build_dump_options(form_state, prefix, *, include_users=False, lakehouse_tables=None):
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
    if form_state.get(f"{prefix}_exclude_lakehouse_tables"):
        _merge_exclude_tables(options, lakehouse_tables or [])
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

