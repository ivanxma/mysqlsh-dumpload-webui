from modules.mysqlsh_jobs import ensure_job_store
from modules.mysqlsh_runner import ensure_runtime_dirs
from modules.object_storage import ensure_object_storage_store, ensure_par_store
from modules.option_profiles import ensure_option_profile_store
from modules.profiles import ensure_profile_store, harden_profile_store_permissions


def initialize_app_files():
    ensure_profile_store()
    harden_profile_store_permissions()
    ensure_option_profile_store()
    ensure_object_storage_store()
    ensure_par_store()
    ensure_runtime_dirs()
    ensure_job_store()
