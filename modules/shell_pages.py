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
    @app.route("/mysql-shell/operations", methods=["GET", "POST"])
    @login_required
    def shell_operations_page():
        config = load_object_storage_config()
        profile = get_session_profile()
        credentials = get_session_credentials()
        current_username = get_current_username()
        current_profile_name = get_current_profile_name()
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
        allowed_schema_names = set(user_schemas)
        filtered_selected_schemas = []
        ignored_selected_schemas = []
        seen_selected_schemas = set()
        for schema_name in selected_schemas:
            normalized_schema_name = str(schema_name or "").strip()
            if (
                not normalized_schema_name
                or normalized_schema_name in seen_selected_schemas
                or not is_user_schema_name(normalized_schema_name)
                or normalized_schema_name not in allowed_schema_names
            ):
                if normalized_schema_name and normalized_schema_name not in ignored_selected_schemas:
                    ignored_selected_schemas.append(normalized_schema_name)
                continue
            filtered_selected_schemas.append(normalized_schema_name)
            seen_selected_schemas.add(normalized_schema_name)
        selected_schemas = filtered_selected_schemas
        if ignored_selected_schemas and request.method == "POST":
            flash("Ignored hidden or inaccessible schemas: " + ", ".join(ignored_selected_schemas), "error")
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
            job_access_profile_name = None if shell_page == "history" else current_profile_name
            operation_result = build_mysqlsh_job_snapshot(
                requested_job_id,
                owner_username=current_username,
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
                if validation_action not in {"dump-instance", "dump-schemas"}:
                    flash("Unsupported validation action.", "error")
                elif selected_dump_option_profile is None:
                    flash("Select a dump option profile before running validation.", "error")
                elif validation_action == "dump-schemas" and not selected_schemas:
                    flash("Select at least one schema before running validation.", "error")
            elif operation != "option-profiles":
                try:
                    if operation == "dump-instance":
                        par_entry = dump_par_lookup.get(form_state["dump_instance_par_id"])
                        if par_entry is None:
                            raise ValueError("Choose an active read/write PAR for dumpInstance.")
                        lakehouse_tables = []
                        if form_state["dump_instance_exclude_lakehouse_tables"]:
                            scope_options = _build_dump_options_for_scope(
                                form_state,
                                "dump_instance",
                                include_users=True,
                            )
                            dump_scope = _dump_filter_scope_from_options(
                                scope_options,
                                base_schema_names=user_schemas,
                            )
                            lakehouse_tables = _fetch_lakehouse_tables_for_dump_scope(
                                profile,
                                credentials,
                                dump_scope,
                            )
                            if dump_scope["include_tables"] and lakehouse_tables:
                                raise ValueError(
                                    "Option conflict: includeTables overlaps with Lakehouse tables that would be excluded. "
                                    "Run validation and review the Lakehouse includeTables overlap."
                                )
                        dump_options = _build_dump_options(
                            form_state,
                            "dump_instance",
                            include_users=True,
                            lakehouse_tables=lakehouse_tables,
                        )
                        dump_summary_rows = [
                            ("Operation", "util.dumpInstance"),
                            ("Target PAR", par_entry["name"]),
                            ("Output URL", par_entry["par_url"]),
                            ("Threads", str(dump_options["threads"])),
                            ("Option Count", str(len(dump_options))),
                        ]
                        if form_state["dump_instance_exclude_lakehouse_tables"]:
                            dump_summary_rows.append(("Excluded Lakehouse Tables", str(len(lakehouse_tables))))
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
                            owner_username=current_username,
                            owner_profile_name=current_profile_name,
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
                        lakehouse_tables = []
                        if form_state["dump_schemas_exclude_lakehouse_tables"]:
                            scope_options = _build_dump_options_for_scope(form_state, "dump_schemas")
                            dump_scope = _dump_filter_scope_from_options(
                                scope_options,
                                base_schema_names=selected_schemas,
                            )
                            lakehouse_tables = _fetch_lakehouse_tables_for_dump_scope(
                                profile,
                                credentials,
                                dump_scope,
                            )
                            if dump_scope["include_tables"] and lakehouse_tables:
                                raise ValueError(
                                    "Option conflict: includeTables overlaps with Lakehouse tables that would be excluded. "
                                    "Run validation and review the Lakehouse includeTables overlap."
                                )
                        dump_options = _build_dump_options(
                            form_state,
                            "dump_schemas",
                            lakehouse_tables=lakehouse_tables,
                        )
                        dump_summary_rows = [
                            ("Operation", "util.dumpSchemas"),
                            ("Schemas", ", ".join(selected_schemas)),
                            ("Target PAR", par_entry["name"]),
                            ("Output URL", par_entry["par_url"]),
                            ("Threads", str(dump_options["threads"])),
                            ("Option Count", str(len(dump_options))),
                        ]
                        if form_state["dump_schemas_exclude_lakehouse_tables"]:
                            dump_summary_rows.append(("Excluded Lakehouse Tables", str(len(lakehouse_tables))))
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
                            owner_username=current_username,
                            owner_profile_name=current_profile_name,
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
                            owner_username=current_username,
                            owner_profile_name=current_profile_name,
                            options_json=json.dumps(load_options, indent=2, sort_keys=True),
                            form_state=form_state,
                            selected_schemas=selected_schemas,
                            summary_rows=load_summary_rows,
                        )
    
                    set_session_value("last_mysqlsh_job_id", operation_result["job_id"])
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
        dump_instance_validation_scope = (
            f"Option profile {selected_dump_option_profile_name} over all accessible schemas"
            if selected_dump_option_profile_name
            else "All accessible schemas"
        )
        dump_schemas_validation_scope = (
            f"Option profile {selected_dump_option_profile_name} over selected schemas ({len(selected_schemas)})"
            if selected_dump_option_profile_name and selected_schemas
            else (f"Selected schemas ({len(selected_schemas)})" if selected_schemas else "")
        )
        show_dump_instance_validation = validation_action == "dump-instance" and selected_dump_option_profile is not None
        show_dump_schemas_validation = (
            validation_action == "dump-schemas"
            and selected_dump_option_profile is not None
            and bool(selected_schemas)
        )
    
        try:
            if show_dump_instance_validation:
                dump_instance_validation = _build_dump_validation(
                    profile,
                    credentials,
                    form_state,
                    "dump_instance",
                    base_schema_names=user_schemas,
                    include_users=True,
                )
            if show_dump_schemas_validation:
                dump_schemas_validation = _build_dump_validation(
                    profile,
                    credentials,
                    form_state,
                    "dump_schemas",
                    base_schema_names=selected_schemas,
                )
        except Exception:  # pragma: no cover - depends on runtime services
            dump_instance_validation = None
            dump_schemas_validation = None
    
        operation_history = []
        if shell_page == "history":
            operation_history = list_mysqlsh_job_history(
                owner_username=current_username,
                owner_profile_name=None,
                limit=100,
            )
    
        load_target_gtid_context = None
        if shell_page == "load-dump" or (operation_result and operation_result.get("operation") == "load-dump"):
            load_target_gtid_context = _build_load_target_gtid_context(profile, credentials)
    
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
            load_target_gtid_context=load_target_gtid_context,
            dump_instance_enabled_event_count=dump_instance_enabled_event_count,
            dump_instance_validation=dump_instance_validation,
            dump_schemas_validation=dump_schemas_validation,
            dump_instance_validation_scope=dump_instance_validation_scope,
            dump_schemas_validation_scope=dump_schemas_validation_scope,
            show_dump_instance_validation=show_dump_instance_validation,
            show_dump_schemas_validation=show_dump_schemas_validation,
            dump_option_filter_catalog=dump_option_filter_catalog,
        )
    
    
