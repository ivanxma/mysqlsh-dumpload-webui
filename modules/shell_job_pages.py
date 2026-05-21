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
    @app.route("/mysql-shell/operations/jobs/<job_id>", methods=["GET"])
    @login_required
    def mysqlsh_job_status_api(job_id):
        shell_page = _normalize_shell_operations_page(
            request.args.get("page"),
            request.args.get("operation"),
            request.args.get("view"),
        )
        job_access_profile_name = None if shell_page == "history" else get_current_profile_name()
        snapshot = build_mysqlsh_job_snapshot(
            job_id,
            owner_username=get_current_username(),
            owner_profile_name=job_access_profile_name,
        )
        if snapshot is None:
            return jsonify({"error": "MySQL Shell job not found."}), 404
        if snapshot.get("operation") == "load-dump":
            snapshot["load_target_gtid_context"] = _build_load_target_gtid_context(
                get_session_profile(),
                get_session_credentials(),
            )
        return jsonify(snapshot)
    
    
    @app.route("/mysql-shell/operations/jobs/<job_id>/cancel", methods=["POST"])
    @login_required
    def mysqlsh_job_cancel(job_id):
        next_page = _normalize_shell_operations_page(
            request.form.get("page"),
            request.form.get("operation"),
            request.form.get("view"),
        )
        job_access_profile_name = None if next_page == "history" else get_current_profile_name()
        snapshot = cancel_mysqlsh_job(
            job_id,
            owner_username=get_current_username(),
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
        set_session_value("last_mysqlsh_job_id", snapshot["job_id"])
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
        job_access_profile_name = None if next_page == "history" else get_current_profile_name()
        snapshot = build_mysqlsh_job_snapshot(
            job_id,
            owner_username=get_current_username(),
            owner_profile_name=job_access_profile_name,
        )
        if snapshot is None:
            flash("MySQL Shell job not found.", "error")
            return redirect(url_for("shell_operations_page"))
    
        try:
            cleanup_mysqlsh_job(
                job_id,
                owner_username=get_current_username(),
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
    
        if get_session_value("last_mysqlsh_job_id") == job_id:
            set_session_value("last_mysqlsh_job_id", "")
        flash(f"MySQL Shell {snapshot['operation_name']} job cleaned up.", "success")
        return redirect(url_for("shell_operations_page", page=next_page, operation=snapshot["operation"]))
    
    
    @app.route("/mysql-shell/operations/jobs/cleanup-selected", methods=["POST"])
    @login_required
    def mysqlsh_jobs_cleanup_selected():
        next_page = _normalize_shell_operations_page(
            request.form.get("page"),
            request.form.get("operation"),
            request.form.get("view"),
        )
        job_access_profile_name = None if next_page == "history" else get_current_profile_name()
        selected_job_ids = [str(item or "").strip() for item in request.form.getlist("selected_jobs")]
        selected_job_ids = [item for item in selected_job_ids if item]
        if not selected_job_ids:
            flash("Select at least one history job to clean up.", "error")
            return redirect(url_for("shell_operations_page", page=next_page))
    
        cleaned_count = 0
        failures = []
        for job_id in selected_job_ids:
            snapshot = build_mysqlsh_job_snapshot(
                job_id,
                owner_username=get_current_username(),
                owner_profile_name=job_access_profile_name,
            )
            if snapshot is None:
                failures.append(f"Job {job_id[:12]} was not found.")
                continue
            if not snapshot.get("can_cleanup"):
                failures.append(f"Job {job_id[:12]} is not ready for cleanup.")
                continue
            try:
                cleanup_mysqlsh_job(
                    job_id,
                    owner_username=get_current_username(),
                    owner_profile_name=job_access_profile_name,
                )
                if get_session_value("last_mysqlsh_job_id") == job_id:
                    set_session_value("last_mysqlsh_job_id", "")
                cleaned_count += 1
            except Exception as error:  # pragma: no cover - filesystem/runtime path
                failures.append(f"Job {job_id[:12]} cleanup failed: {error}")
    
        if cleaned_count:
            flash(f"Cleaned up {cleaned_count} job{'s' if cleaned_count != 1 else ''}.", "success")
        for message in failures:
            flash(message, "error")
        return redirect(url_for("shell_operations_page", page=next_page))
    
    
