import os
from pathlib import Path


APP_TITLE = "MySQL Shell Web"
APP_SLUG = "mysql-shell-web"
ROOT_DIR = Path(__file__).resolve().parent.parent
PROFILE_STORE = ROOT_DIR / "profiles.json"
OPTION_PROFILE_STORE = ROOT_DIR / "mysqlsh_option_profiles.json"
OBJECT_STORAGE_STORE = ROOT_DIR / "object_storage.json"
PAR_STORE = ROOT_DIR / "par_registry.json"
RUNTIME_DIR = ROOT_DIR / "runtime"
PROGRESS_DIR = RUNTIME_DIR / "progress"
JOBS_DIR = RUNTIME_DIR / "jobs"
MYSQLSH_USER_CONFIG_HOME = RUNTIME_DIR / "mysqlsh"
SYSTEM_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}

DEFAULT_PROFILE = {
    "name": "",
    "host": "",
    "port": 3306,
    "database": "mysql",
    "ssh_enabled": False,
    "ssh_host": "",
    "ssh_port": 22,
    "ssh_user": "",
    "ssh_key_path": "",
    "ssh_config_file": "",
}

DEFAULT_OBJECT_STORAGE = {
    "region": "",
    "namespace": "",
    "bucket_name": "",
    "bucket_prefix": "",
    "config_profile": "DEFAULT",
    "config_file": "~/.oci/config",
    "managed_folders": [],
}

MYSQL_SHELL_WEB_SESSION_SCOPE_KEY = "_mysql_shell_web_session_scope"
MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE = "mysql_shell_web"
MYSQL_SHELL_WEB_SESSION_VERSION_KEY = "_mysql_shell_web_session_version"
MYSQL_SHELL_WEB_SESSION_VERSION = 1
MYSQL_SHELL_WEB_SESSION_COOKIE_NAME = (
    os.environ.get("MYSQL_SHELL_WEB_SESSION_COOKIE_NAME", "mysql_shell_web_session").strip()
    or "mysql_shell_web_session"
)
MYSQL_SHELL_WEB_SESSION_COOKIE_PATH = (
    os.environ.get("MYSQL_SHELL_WEB_SESSION_COOKIE_PATH", "/").strip() or "/"
)
MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE = (
    os.environ.get("MYSQL_SHELL_WEB_SESSION_COOKIE_SAMESITE", "Lax").strip() or "Lax"
)
MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE = os.environ.get(
    "MYSQL_SHELL_WEB_SESSION_COOKIE_SECURE",
    "",
).strip().lower() in {"1", "true", "yes", "on"}

NAV_GROUPS = [
    {
        "label": "Dashboard",
        "items": [
            {"endpoint": "overview_page", "label": "Overview"},
        ],
    },
    {
        "label": "Admin",
        "items": [
            {"endpoint": "profile_page", "label": "Profile"},
            {"endpoint": "object_storage_settings_page", "label": "Object Storage"},
        ],
    },
    {
        "label": "Object Storage",
        "items": [
            {"endpoint": "par_manager_page", "label": "PAR Manager"},
            {"endpoint": "folder_manager_page", "label": "Folders"},
        ],
    },
    {
        "label": "MySQL Shell",
        "items": [
            {"endpoint": "shell_operations_page", "label": "Operations"},
        ],
    },
]

PAR_TARGET_OPTIONS = (
    ("prefix", "Prefix"),
    ("bucket", "Bucket"),
)

PAR_ACCESS_OPTIONS = (
    ("AnyObjectReadWrite", "Read/Write"),
    ("AnyObjectRead", "Read"),
)

SHELL_OPERATION_OPTIONS = (
    ("dump-instance", "dumpInstance"),
    ("dump-schemas", "dumpSchemas"),
    ("load-dump", "loadDump"),
)
