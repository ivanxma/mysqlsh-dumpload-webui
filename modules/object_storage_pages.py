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
            elif action == "delete_selected":
                selected_entry_ids = [str(item or "").strip() for item in request.form.getlist("selected_pars")]
                selected_entry_ids = [item for item in selected_entry_ids if item]
                if not selected_entry_ids:
                    flash("Select at least one stored PAR to revoke.", "error")
                else:
                    revoked_count = 0
                    failures = []
                    for entry_id in selected_entry_ids:
                        try:
                            delete_par_record(config, entry_id)
                            revoked_count += 1
                        except Exception as error:  # pragma: no cover - depends on runtime services
                            failures.append(str(error))
                    if revoked_count:
                        flash(f"Revoked {revoked_count} stored PAR{'s' if revoked_count != 1 else ''}.", "success")
                    for message in failures:
                        flash(message, "error")
                    return redirect(url_for("par_manager_page"))
    
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
                elif action == "delete_selected":
                    selected_folders = [str(item or "").strip() for item in request.form.getlist("selected_folders")]
                    selected_folders = [item for item in selected_folders if item]
                    if not selected_folders:
                        flash("Select at least one folder to delete.", "error")
                    else:
                        deleted_folders = 0
                        deleted_objects = 0
                        failures = []
                        for source_prefix in selected_folders:
                            try:
                                config, deleted_count = delete_folder(config, source_prefix)
                                deleted_folders += 1
                                deleted_objects += deleted_count
                            except Exception as error:  # pragma: no cover - depends on runtime services
                                failures.append(f"{source_prefix}: {error}")
                        if deleted_folders:
                            flash(
                                f"Deleted {deleted_folders} folder{'s' if deleted_folders != 1 else ''}. "
                                f"Objects removed: {deleted_objects}.",
                                "success",
                            )
                        for message in failures:
                            flash(message, "error")
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
    
    
