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
    @app.route("/admin/oci-configuration", methods=["GET", "POST"])
    @login_required
    def oci_configuration_page():
        config = load_object_storage_config()
        if request.method == "POST":
            action = str(request.form.get("setup_action", "save_config")).strip()
            merged_payload = dict(config)
            merged_payload.update(request.form.to_dict())
            merged_payload["managed_folders"] = config.get("managed_folders", [])
    
            try:
                if action == "use_existing_oci_config":
                    merged_payload["config_source"] = "existing"
                    merged_payload["config_file"] = request.form.get("existing_config_file", "")
                    merged_payload["config_profile"] = request.form.get("existing_config_profile", "")
                    merged_payload["region"] = request.form.get("existing_region", "")
                    config = normalize_object_storage(merged_payload)
                    save_object_storage_config(config)
                    flash("Updated the OCI config file reference.", "success")
                elif action == "store_local_oci_config":
                    local_config = store_local_oci_config_from_upload(
                        request.form,
                        request.files.get("private_key_file"),
                    )
                    merged_payload.update(local_config)
                    config = normalize_object_storage(merged_payload)
                    save_object_storage_config(config)
                    flash("Stored OCI config and private key in the app-local runtime folder.", "success")
                else:
                    if (
                        str(merged_payload.get("config_source", "")).strip().lower() == "local"
                        and "local_config_text" in request.form
                    ):
                        save_local_oci_config_text(request.form.get("local_config_text", ""))
                    config = normalize_object_storage(merged_payload)
                    save_object_storage_config(config)
                    flash("OCI configuration saved.", "success")
            except Exception as error:
                flash(str(error), "error")
            return redirect(url_for("oci_configuration_page"))
    
        return render_dashboard(
            "oci_configuration.html",
            page_title="OCI Configuration",
            object_storage_config=config,
            oci_config_status=build_oci_config_status(config, effective_oci_config_file(config)),
        )
    
    
    @app.route("/admin/object-storage", methods=["GET", "POST"])
    @login_required
    def object_storage_settings_page():
        return redirect(url_for("oci_configuration_page"))
    
    
