from datetime import datetime, timezone

from modules.config import ROOT_DIR, RUNTIME_DIR

APP_STARTED_AT = datetime.now(timezone.utc).replace(microsecond=0)
UPDATE_DIR = RUNTIME_DIR / "updates"
UPDATE_STATUS_FILE = UPDATE_DIR / "mysql_shell_web_update_status.json"
UPDATE_LOG_FILE = UPDATE_DIR / "mysql_shell_web_update.log"
UPDATE_WORKER_FILE = ROOT_DIR / "mysql_shell_web_update_worker.py"
UPDATE_POLL_TOKEN_SESSION_KEY = "mysql_shell_web_update_poll_token"

MYSQL_PAGE_HEALTHCHECK_ENDPOINTS = {
    "overview_page",
    "db_admin_page",
    "db_admin_event_toggle",
    "db_admin_apply_primary_key_fix",
    "profile_page",
    "par_manager_page",
    "folder_manager_page",
    "shell_operations_page",
}
