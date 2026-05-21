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
    @app.route("/admin/update")
    @login_required
    def update_mysql_shell_web_page():
        raw_status = _normalize_update_status()
        return render_dashboard(
            "update_mysql_shell_web.html",
            page_title="Update MySQL Shell Web",
            update_status=_public_update_status(raw_status),
            update_poll_token=str(raw_status.get("poll_token") or session.get(UPDATE_POLL_TOKEN_SESSION_KEY, "")).strip(),
            version_check=_current_version_check(),
            status_url=url_for("update_mysql_shell_web_status"),
            local_admin_bootstrap_required=_local_admin_bootstrap_required(),
            current_user_is_local_admin=_current_user_is_local_admin(),
            local_admin_profile_name=LOCAL_ADMIN_PROFILE_NAME,
        )
    
    
    @app.route("/admin/update/retrieve-version", methods=["POST"])
    @login_required
    def update_mysql_shell_web_retrieve_version():
        version_check = _check_repository_version()
        if version_check.get("update_available"):
            flash(
                "Repository version differs from the running app: "
                f"{version_check.get('app_version') or '-'} -> {version_check.get('repo_version') or '-'}.",
                "success",
            )
        elif version_check.get("error"):
            flash(f"Unable to retrieve repository version: {version_check['error']}", "error")
        else:
            flash("Repository version matches the running app.", "success")
        return redirect(url_for("update_mysql_shell_web_page"))
    
    
    @app.route("/admin/update/start", methods=["POST"])
    @login_required
    def update_mysql_shell_web_start():
        try:
            bootstrap_required = _local_admin_bootstrap_required()
            bootstrap_field_names = {"local_admin_username", "local_admin_password", "local_admin_password_confirm"}
            old_form_compatibility = bootstrap_required and not any(name in request.form for name in bootstrap_field_names)
            bootstrap_payload = {}
            if bootstrap_required and not old_form_compatibility:
                local_admin_username = str(request.form.get("local_admin_username", "localadmin")).strip() or "localadmin"
                local_admin_password = request.form.get("local_admin_password", "")
                local_admin_password_confirm = request.form.get("local_admin_password_confirm", "")
                if not local_admin_password:
                    raise RuntimeError("Temporary local admin password is required to repair local-admin-profile.")
                if local_admin_password != local_admin_password_confirm:
                    raise RuntimeError("Temporary local admin password confirmation does not match.")
                bootstrap_payload = {
                    "LOCAL_MYSQL_PROFILE_NAME": LOCAL_ADMIN_PROFILE_NAME,
                    "LOCAL_MYSQL_ADMIN_USER": local_admin_username,
                    "LOCAL_MYSQL_ADMIN_PASSWORD": local_admin_password,
                }
            elif not bootstrap_required and not _current_user_is_local_admin():
                raise RuntimeError("Log in with local-admin-profile to start application updates.")
    
            _start_update_worker(
                bootstrap_payload=bootstrap_payload,
                compatibility_code_refresh=old_form_compatibility,
            )
            flash("Update started.", "success")
        except Exception as error:
            flash(str(error), "error")
        return redirect(url_for("update_mysql_shell_web_page"))
    
    
    @app.route("/admin/update/status")
    def update_mysql_shell_web_status():
        raw_status = _read_json_file(UPDATE_STATUS_FILE)
        if not _update_status_request_authorized(raw_status):
            return jsonify({"state": "error", "message": "Log in to view update status."}), 401
        return jsonify(_public_update_status(_normalize_update_status(raw_status)))
    
    
