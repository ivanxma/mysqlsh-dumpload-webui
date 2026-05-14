import json
import os
import re
from pathlib import Path

from .config import DEFAULT_PROFILE, LOCAL_ADMIN_PROFILE_NAME, PROFILE_SSH_KEY_DIR, PROFILE_STORE

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _normalize_int(value, default, minimum=None):
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and normalized < minimum:
        return default
    return normalized


def normalize_profile(payload):
    mode = str(payload.get("mode", "")).strip().lower()
    if not mode:
        mode = "ssh" if str(payload.get("ssh_enabled", "")).strip().lower() in {"1", "true", "yes", "on"} else "tcp"
    if mode not in {"tcp", "socket", "ssh"}:
        mode = "tcp"
    ssh_enabled = mode == "ssh" or str(payload.get("ssh_enabled", "")).strip().lower() in {"1", "true", "yes", "on"}
    if ssh_enabled:
        mode = "ssh"
    profile_name = str(payload.get("name", "")).strip()
    profile_management = str(payload.get("profile_management", "")).strip().lower() in {"1", "true", "yes", "on"}
    if profile_name == LOCAL_ADMIN_PROFILE_NAME:
        profile_management = True
    return {
        "name": profile_name,
        "mode": mode,
        "host": str(payload.get("host", "")).strip(),
        "port": _normalize_int(payload.get("port"), DEFAULT_PROFILE["port"], minimum=1),
        "socket": str(payload.get("socket", "")).strip(),
        "database": str(payload.get("database", "")).strip() or DEFAULT_PROFILE["database"],
        "default_username": str(payload.get("default_username", "")).strip(),
        "profile_management": profile_management,
        "force_password_change": str(payload.get("force_password_change", "")).strip().lower()
        in {"1", "true", "yes", "on"},
        "ssh_enabled": ssh_enabled,
        "ssh_key_uploaded": str(payload.get("ssh_key_uploaded", "")).strip().lower()
        in {"1", "true", "yes", "on"},
        "ssh_key_id": str(payload.get("ssh_key_id", "")).strip(),
        "ssh_host": str(payload.get("ssh_host", "")).strip(),
        "ssh_port": _normalize_int(payload.get("ssh_port"), DEFAULT_PROFILE["ssh_port"], minimum=1),
        "ssh_user": str(payload.get("ssh_user", "")).strip(),
        "ssh_key_path": str(payload.get("ssh_key_path", "")).strip(),
        "ssh_config_file": str(payload.get("ssh_config_file", "")).strip(),
    }


def validate_profile(profile, *, require_name=True, require_host=True):
    errors = []
    if require_name and not profile["name"]:
        errors.append("Profile name is required.")
    if profile["mode"] == "socket":
        if not profile["socket"]:
            errors.append("MySQL socket path is required for socket profiles.")
    elif require_host and not profile["host"]:
        errors.append("MySQL host is required.")
    if profile["ssh_enabled"]:
        if not profile["ssh_host"]:
            errors.append("SSH jump host is required when SSH tunneling is enabled.")
        if not profile["ssh_user"]:
            errors.append("SSH user is required when SSH tunneling is enabled.")
        if not profile["ssh_key_path"] and not profile["ssh_key_uploaded"]:
            errors.append("SSH private key path is required when SSH tunneling is enabled.")
    return errors


def ensure_profile_store():
    if PROFILE_STORE.exists():
        return
    PROFILE_STORE.write_text(json.dumps({"profiles": []}, indent=2) + "\n", encoding="utf-8")
    harden_profile_store_permissions()


def harden_profile_store_permissions():
    try:
        if PROFILE_STORE.exists():
            os.chmod(PROFILE_STORE, 0o600)
    except OSError:
        pass
    try:
        PROFILE_SSH_KEY_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(PROFILE_SSH_KEY_DIR, 0o700)
        for path in PROFILE_SSH_KEY_DIR.rglob("*"):
            if path.is_dir():
                os.chmod(path, 0o700)
            else:
                os.chmod(path, 0o600)
    except OSError:
        pass


def load_profiles():
    ensure_profile_store()
    try:
        payload = json.loads(PROFILE_STORE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    profiles = []
    for row in payload.get("profiles", []):
        profile = normalize_profile(row)
        if profile["name"]:
            profiles.append(profile)
    return sorted(profiles, key=lambda item: item["name"].lower())


def save_profiles(profiles):
    normalized_profiles = []
    seen = set()
    for row in profiles:
        profile = normalize_profile(row)
        if not profile["name"]:
            continue
        key = profile["name"].lower()
        if key in seen:
            continue
        seen.add(key)
        normalized_profiles.append(profile)
    PROFILE_STORE.write_text(
        json.dumps({"profiles": normalized_profiles}, indent=2) + "\n",
        encoding="utf-8",
    )
    harden_profile_store_permissions()


def get_profile_by_name(profile_name):
    profile_lookup = str(profile_name or "").strip().lower()
    for profile in load_profiles():
        if profile["name"].lower() == profile_lookup:
            return profile
    return None


def public_login_profiles():
    return [{"name": profile["name"], "default_username": profile.get("default_username", "")} for profile in load_profiles()]


def is_local_admin_profile(profile):
    normalized = normalize_profile(profile or {})
    return normalized["name"] == LOCAL_ADMIN_PROFILE_NAME and normalized["mode"] == "socket"


def local_admin_profile_ready():
    profile = get_profile_by_name(LOCAL_ADMIN_PROFILE_NAME)
    return bool(profile and is_local_admin_profile(profile))


def profile_allows_management(profile_name):
    profile = get_profile_by_name(profile_name)
    return bool(profile and is_local_admin_profile(profile) and profile.get("profile_management"))


def set_profile_force_password_change(profile_name, force_password_change):
    profiles = load_profiles()
    updated = False
    for profile in profiles:
        if profile["name"].lower() == str(profile_name or "").strip().lower():
            profile["force_password_change"] = bool(force_password_change)
            updated = True
            break
    if updated:
        save_profiles(profiles)
    return updated


def safe_profile_id(value):
    normalized = str(value or "").strip()
    if not normalized or not _SAFE_ID_RE.match(normalized) or "/" in normalized or "\\" in normalized:
        raise ValueError("Profile or key id contains unsupported characters.")
    return normalized


def stored_ssh_key_path(profile_name):
    safe_name = safe_profile_id(profile_name)
    return PROFILE_SSH_KEY_DIR / safe_name / "id_key"


def resolve_stored_ssh_key_path(profile):
    normalized = normalize_profile(profile or {})
    key_id = normalized.get("ssh_key_id") or normalized.get("name")
    if not normalized.get("ssh_key_uploaded"):
        return ""
    key_path = stored_ssh_key_path(key_id)
    if not key_path.exists():
        return ""
    return str(key_path)


def store_uploaded_ssh_key(profile_name, upload):
    if upload is None or not getattr(upload, "filename", ""):
        return None
    target = stored_ssh_key_path(profile_name)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    temp_path = Path(str(target) + ".tmp")
    upload.save(temp_path)
    os.chmod(temp_path, 0o600)
    temp_path.replace(target)
    os.chmod(target, 0o600)
    return target
