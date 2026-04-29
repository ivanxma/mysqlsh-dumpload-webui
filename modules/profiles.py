import json

from .config import DEFAULT_PROFILE, PROFILE_STORE


def _normalize_int(value, default, minimum=None):
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and normalized < minimum:
        return default
    return normalized


def normalize_profile(payload):
    return {
        "name": str(payload.get("name", "")).strip(),
        "host": str(payload.get("host", "")).strip(),
        "port": _normalize_int(payload.get("port"), DEFAULT_PROFILE["port"], minimum=1),
        "database": str(payload.get("database", "")).strip() or DEFAULT_PROFILE["database"],
        "ssh_enabled": str(payload.get("ssh_enabled", "")).strip().lower() in {"1", "true", "yes", "on"},
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
    if require_host and not profile["host"]:
        errors.append("MySQL host is required.")
    if profile["ssh_enabled"]:
        if not profile["ssh_host"]:
            errors.append("SSH jump host is required when SSH tunneling is enabled.")
        if not profile["ssh_user"]:
            errors.append("SSH user is required when SSH tunneling is enabled.")
        if not profile["ssh_key_path"]:
            errors.append("SSH private key path is required when SSH tunneling is enabled.")
    return errors


def ensure_profile_store():
    if PROFILE_STORE.exists():
        return
    PROFILE_STORE.write_text(json.dumps({"profiles": []}, indent=2) + "\n", encoding="utf-8")


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


def get_profile_by_name(profile_name):
    profile_lookup = str(profile_name or "").strip().lower()
    for profile in load_profiles():
        if profile["name"].lower() == profile_lookup:
            return profile
    return None
