import os
import re

from .config import DEFAULT_OBJECT_STORAGE, LOCAL_OCI_CONFIG_FILE


OCI_PROFILE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def read_local_oci_config_text():
    try:
        return LOCAL_OCI_CONFIG_FILE.read_text(encoding="utf-8")
    except OSError:
        return ""


def save_local_oci_config_text(value):
    LOCAL_OCI_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_OCI_CONFIG_FILE.write_text(str(value or "").strip() + "\n", encoding="utf-8")
    try:
        LOCAL_OCI_CONFIG_FILE.chmod(0o600)
    except OSError:
        pass


def _safe_oci_profile_name(value):
    profile = str(value or "").strip() or DEFAULT_OBJECT_STORAGE["config_profile"]
    if not OCI_PROFILE_RE.match(profile):
        raise ValueError("OCI config profile may contain only letters, numbers, underscore, dot, and hyphen.")
    return profile


def store_local_oci_config_from_upload(payload, file_storage):
    profile = _safe_oci_profile_name(payload.get("local_config_profile"))
    tenancy_id = str(payload.get("tenancy_id", "")).strip()
    user_id = str(payload.get("user_id", "")).strip()
    fingerprint = str(payload.get("fingerprint", "")).strip()
    region = str(payload.get("local_region", "")).strip()
    if not tenancy_id or not user_id or not fingerprint or not region:
        raise ValueError("Tenancy OCID, user OCID, fingerprint, and region are required.")
    if not file_storage or not str(getattr(file_storage, "filename", "") or "").strip():
        raise ValueError("Upload the OCI API private key defined by the config profile.")

    LOCAL_OCI_CONFIG_FILE.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    key_path = LOCAL_OCI_CONFIG_FILE.parent / f"{profile}_private_key.pem"
    file_storage.stream.seek(0)
    key_path.write_bytes(file_storage.stream.read())
    try:
        key_path.chmod(0o600)
    except OSError:
        pass

    config_text = (
        f"[{profile}]\n"
        f"user={user_id}\n"
        f"fingerprint={fingerprint}\n"
        f"tenancy={tenancy_id}\n"
        f"region={region}\n"
        f"key_file={key_path}\n"
    )
    save_local_oci_config_text(config_text)
    return {
        "config_source": "local",
        "config_file": str(LOCAL_OCI_CONFIG_FILE),
        "config_profile": profile,
        "region": region,
    }


def list_oci_config_profiles(config_file):
    config_path = os.path.expanduser(str(config_file or "").strip())
    if not config_path:
        return []
    try:
        text = open(config_path, "r", encoding="utf-8").read()
    except OSError:
        return []
    profiles = []
    for line in text.splitlines():
        match = re.match(r"^\s*\[([^\]]+)\]\s*$", line)
        if match:
            profiles.append(match.group(1).strip())
    return profiles


def build_oci_config_status(config, effective_file):
    expanded_file = os.path.expanduser(effective_file)
    existing_file = str(config.get("config_file", "")).strip() or DEFAULT_OBJECT_STORAGE["config_file"]
    local_file = str(LOCAL_OCI_CONFIG_FILE)
    return {
        "config_source": str(config.get("config_source", "") or DEFAULT_OBJECT_STORAGE["config_source"]),
        "configured_file": existing_file,
        "effective_file": effective_file,
        "expanded_file": expanded_file,
        "exists": os.path.exists(expanded_file),
        "profiles": list_oci_config_profiles(effective_file),
        "existing_profiles": list_oci_config_profiles(existing_file),
        "local_profiles": list_oci_config_profiles(local_file),
        "active_profile": str(config.get("config_profile", "") or DEFAULT_OBJECT_STORAGE["config_profile"]),
        "local_config_file": local_file,
        "local_config_text": read_local_oci_config_text(),
    }
