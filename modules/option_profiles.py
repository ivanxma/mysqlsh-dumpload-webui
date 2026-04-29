import json

from .config import OPTION_PROFILE_STORE

OPTION_PROFILE_KINDS = {"dump", "load"}


def _empty_store():
    return {"dump_profiles": [], "load_profiles": []}


def _store_key(kind):
    normalized_kind = str(kind or "").strip().lower()
    if normalized_kind not in OPTION_PROFILE_KINDS:
        raise ValueError(f"Unsupported option profile type: {kind}")
    return f"{normalized_kind}_profiles"


def _normalize_value(value):
    if isinstance(value, list):
        normalized_items = []
        for item in value:
            if isinstance(item, (str, int, float, bool)) or item is None:
                normalized_items.append(item)
            else:
                normalized_items.append(str(item))
        return normalized_items
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _normalize_profile_entry(payload):
    name = str((payload or {}).get("name", "")).strip()
    values = (payload or {}).get("values", {})
    normalized_values = {}
    if isinstance(values, dict):
        for key, value in values.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            normalized_values[normalized_key] = _normalize_value(value)
    return {"name": name, "values": normalized_values}


def ensure_option_profile_store():
    if OPTION_PROFILE_STORE.exists():
        return
    OPTION_PROFILE_STORE.write_text(json.dumps(_empty_store(), indent=2) + "\n", encoding="utf-8")


def _load_store_payload():
    ensure_option_profile_store()
    try:
        payload = json.loads(OPTION_PROFILE_STORE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_store()
    if not isinstance(payload, dict):
        return _empty_store()
    normalized = _empty_store()
    for kind in OPTION_PROFILE_KINDS:
        key = _store_key(kind)
        rows = payload.get(key, [])
        normalized[key] = rows if isinstance(rows, list) else []
    return normalized


def _save_store_payload(payload):
    normalized_payload = _empty_store()
    for kind in OPTION_PROFILE_KINDS:
        key = _store_key(kind)
        normalized_payload[key] = payload.get(key, []) if isinstance(payload.get(key, []), list) else []
    OPTION_PROFILE_STORE.write_text(json.dumps(normalized_payload, indent=2) + "\n", encoding="utf-8")


def load_option_profiles(kind):
    key = _store_key(kind)
    payload = _load_store_payload()
    profiles = []
    seen = set()
    for row in payload.get(key, []):
        profile = _normalize_profile_entry(row)
        if not profile["name"]:
            continue
        name_key = profile["name"].lower()
        if name_key in seen:
            continue
        seen.add(name_key)
        profiles.append(profile)
    return sorted(profiles, key=lambda item: item["name"].lower())


def get_option_profile(kind, profile_name):
    profile_lookup = str(profile_name or "").strip().lower()
    if not profile_lookup:
        return None
    for profile in load_option_profiles(kind):
        if profile["name"].lower() == profile_lookup:
            return _normalize_profile_entry(profile)
    return None


def save_option_profile(kind, profile_name, values):
    key = _store_key(kind)
    profile = _normalize_profile_entry({"name": profile_name, "values": values})
    if not profile["name"]:
        raise ValueError("Option profile name is required.")

    payload = _load_store_payload()
    retained_rows = []
    profile_lookup = profile["name"].lower()
    for row in payload.get(key, []):
        existing = _normalize_profile_entry(row)
        if existing["name"].lower() == profile_lookup:
            continue
        retained_rows.append(existing)
    retained_rows.append(profile)
    payload[key] = sorted(retained_rows, key=lambda item: item["name"].lower())
    _save_store_payload(payload)
    return profile


def delete_option_profile(kind, profile_name):
    key = _store_key(kind)
    profile_lookup = str(profile_name or "").strip().lower()
    if not profile_lookup:
        return False

    payload = _load_store_payload()
    retained_rows = []
    deleted = False
    for row in payload.get(key, []):
        profile = _normalize_profile_entry(row)
        if profile["name"].lower() == profile_lookup:
            deleted = True
            continue
        retained_rows.append(profile)
    if deleted:
        payload[key] = retained_rows
        _save_store_payload(payload)
    return deleted
