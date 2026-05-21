import os
import secrets

from flask import Flask

from modules.app_hooks import register_hooks
from modules.app_startup import initialize_app_files
from modules.config import (
    MYSQL_SHELL_WEB_SESSION_COOKIE_NAME,
    MYSQL_SHELL_WEB_SESSION_COOKIE_PATH,
    MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE,
    MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE,
    ROOT_DIR,
)
from modules.error_handlers import register_error_handlers


def _load_flask_secret_key():
    configured = os.environ.get("FLASK_SECRET_KEY", "").strip()
    if configured:
        return configured
    secret_file = os.environ.get("FLASK_SECRET_KEY_FILE", "").strip()
    if secret_file:
        try:
            secret_value = open(secret_file, "r", encoding="utf-8").read().strip()
            if secret_value:
                return secret_value
        except OSError:
            pass
    return secrets.token_hex(32)


def create_app():
    app = Flask("app", root_path=ROOT_DIR)
    app.config["SECRET_KEY"] = _load_flask_secret_key()
    app.config["SESSION_COOKIE_NAME"] = MYSQL_SHELL_WEB_SESSION_COOKIE_NAME
    app.config["SESSION_COOKIE_PATH"] = MYSQL_SHELL_WEB_SESSION_COOKIE_PATH
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE
    app.config["SESSION_COOKIE_SECURE"] = MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE

    initialize_app_files()
    register_hooks(app)

    from modules import (
        auth_pages,
        dashboard_pages,
        db_admin_pages,
        object_storage_pages,
        oci_pages,
        profile_pages,
        shell_job_pages,
        shell_pages,
        shell_validation_pages,
        update_pages,
    )

    for page_module in (
        auth_pages,
        dashboard_pages,
        db_admin_pages,
        profile_pages,
        oci_pages,
        update_pages,
        object_storage_pages,
        shell_pages,
        shell_validation_pages,
        shell_job_pages,
    ):
        page_module.register_routes(app)

    register_error_handlers(app)
    return app
