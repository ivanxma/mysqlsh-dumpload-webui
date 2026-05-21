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
    @app.route("/", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            profile_name = str(request.form.get("profile_name", "")).strip()
            profile = get_profile_by_name(profile_name)
            username = str(request.form.get("username", "")).strip()
            password = request.form.get("password", "")
            errors = []
            if not profile:
                errors.append("Choose a saved profile.")
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
                    if is_local_admin_profile(profile) and profile.get("force_password_change"):
                        flash("Change the local admin password before continuing.", "error")
                        return redirect(url_for("change_local_admin_password"))
                    version_check = _check_repository_version()
                    if version_check.get("update_available"):
                        flash(
                            "Application update available: "
                            f"{version_check.get('app_version') or '-'} -> {version_check.get('repo_version') or '-'}.",
                            "success",
                        )
                        return redirect(url_for("update_mysql_shell_web_page"))
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
            profiles=public_login_profiles(),
            selected_profile=_profile_public_login_payload(selected_profile),
            selected_profile_name=selected_name or selected_profile.get("name", ""),
        )
    
    
    @app.route("/logout", methods=["POST"])
    def logout():
        clear_login_state(keep_profile=False)
        flash("Logged out.", "success")
        return redirect(url_for("login"))
    
    
    @app.route("/admin/local-admin/change-password", methods=["GET", "POST"])
    @login_required
    def change_local_admin_password():
        profile = get_session_profile()
        if not is_local_admin_profile(profile):
            flash("Password changes through this page are only available for local-admin-profile.", "error")
            return redirect(url_for("overview_page"))
    
        if request.method == "POST":
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")
            if not new_password:
                flash("New password is required.", "error")
            elif new_password != confirm_password:
                flash("Password confirmation does not match.", "error")
            else:
                try:
                    credentials = get_session_credentials()
                    change_current_user_password(profile, credentials, new_password)
                    set_profile_force_password_change(profile["name"], False)
                    clear_login_state(keep_profile=False)
                    flash("Local admin password changed. Log in again with the new password.", "success")
                    return redirect(url_for("login", profile=LOCAL_ADMIN_PROFILE_NAME))
                except Exception as error:  # pragma: no cover - depends on local MySQL runtime
                    flash(f"Unable to change local admin password: {error}", "error")
    
        return render_dashboard(
            "change_local_admin_password.html",
            page_title="Change Local Admin Password",
        )
    
    
