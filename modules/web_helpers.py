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

def _current_user_is_local_admin():
    return is_logged_in() and profile_allows_management(get_current_profile_name())


def local_admin_required(view):
    @wraps(view)
    @login_required
    def wrapped_view(*args, **kwargs):
        if not _current_user_is_local_admin():
            flash("Log in with local-admin-profile to manage application profiles.", "error")
            return redirect(url_for("overview_page"))
        return view(*args, **kwargs)

    return wrapped_view


def _profile_public_login_payload(profile):
    return {
        "name": profile.get("name", ""),
        "default_username": profile.get("default_username", ""),
    }


def _local_admin_bootstrap_required():
    return not local_admin_profile_ready()


def _redirect_to_login_for_mysql_unavailable(error):
    profile_name = get_current_profile_name()
    clear_login_state(keep_profile=True)
    flash(f"MySQL connection is unavailable: {error}", "error")
    redirect_values = {"profile": profile_name} if profile_name else {}
    return redirect(url_for("login", **redirect_values))


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not is_logged_in() or not has_server_login_state():
            flash("Log in to continue.", "error")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


def render_dashboard(template_name, **context):
    from modules.update_service import _app_version_payload, _current_version_check

    profile = get_session_profile()
    object_storage_config = context.pop("object_storage_config", None) or load_object_storage_config()
    par_entries = get_par_entries_for_bucket(object_storage_config)
    version_check = context.pop("version_check", None) or _current_version_check()
    nav_groups = []
    for group in NAV_GROUPS:
        items = []
        for item in group["items"]:
            if item["endpoint"] == "profile_page" and not _current_user_is_local_admin():
                continue
            items.append(item)
        if items:
            nav_groups.append({"label": group["label"], "items": items})
    return render_template(
        template_name,
        app_title=APP_TITLE,
        logged_in=is_logged_in(),
        current_user=get_current_username(),
        current_profile_name=get_current_profile_name(),
        connection_summary="Socket" if profile and profile.get("mode") == "socket" else (f"{profile['host'] or '-'}:{profile['port']}" if profile else "-"),
        app_version=version_check.get("app_version") or _app_version_payload()["version"],
        repo_version=version_check.get("repo_version") or "",
        update_available=bool(version_check.get("update_available")),
        version_check=version_check,
        nav_groups=nav_groups,
        current_endpoint=request.endpoint or "",
        object_storage_config=object_storage_config,
        stored_par_count=len(par_entries),
        active_par_count=len([entry for entry in par_entries if entry["is_active"]]),
        **context,
    )


__all__ = [name for name in globals() if not name.startswith("__")]
