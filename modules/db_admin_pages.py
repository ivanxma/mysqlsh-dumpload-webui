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


def register_routes(app):
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
        except MYSQL_CONNECTION_ERRORS as error:
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
            except MYSQL_CONNECTION_ERRORS as error:
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
    
    
