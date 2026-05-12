from copy import deepcopy
from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4

from flask import session

from .config import (
    DEFAULT_PROFILE,
    MYSQL_SHELL_WEB_SESSION_SCOPE_KEY,
    MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE,
    MYSQL_SHELL_WEB_SESSION_VERSION,
    MYSQL_SHELL_WEB_SESSION_VERSION_KEY,
)
from .profiles import normalize_profile

SERVER_SESSION_ID_KEY = "mysql_shell_web_sid"
_SERVER_SESSIONS = {}
_SERVER_SESSION_LOCK = RLock()


def _utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_server_session():
    return {
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "connection_profile": normalize_profile(DEFAULT_PROFILE),
        "profile_name": "",
        "credentials": {"username": "", "password": ""},
        "logged_in": False,
        "values": {},
        "version_check": {},
    }


def _server_session_id():
    return str(session.get(SERVER_SESSION_ID_KEY, "")).strip()


def _ensure_server_session():
    server_session_id = _server_session_id()
    if server_session_id:
        with _SERVER_SESSION_LOCK:
            payload = _SERVER_SESSIONS.get(server_session_id)
            if payload is not None:
                payload["updated_at"] = _utc_now_iso()
                return server_session_id, payload

    server_session_id = uuid4().hex
    payload = _new_server_session()
    with _SERVER_SESSION_LOCK:
        _SERVER_SESSIONS[server_session_id] = payload
    session[SERVER_SESSION_ID_KEY] = server_session_id
    return server_session_id, payload


def _get_server_session():
    server_session_id = _server_session_id()
    if not server_session_id:
        return None
    with _SERVER_SESSION_LOCK:
        payload = _SERVER_SESSIONS.get(server_session_id)
        if payload is not None:
            payload["updated_at"] = _utc_now_iso()
        return payload


def _drop_server_session(server_session_id):
    if not server_session_id:
        return
    with _SERVER_SESSION_LOCK:
        _SERVER_SESSIONS.pop(server_session_id, None)

def prime_session_scope():
    session[MYSQL_SHELL_WEB_SESSION_SCOPE_KEY] = MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE
    session[MYSQL_SHELL_WEB_SESSION_VERSION_KEY] = MYSQL_SHELL_WEB_SESSION_VERSION
    _ensure_server_session()


def ensure_session_scope():
    if (
        session.get(MYSQL_SHELL_WEB_SESSION_SCOPE_KEY) == MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE
        and session.get(MYSQL_SHELL_WEB_SESSION_VERSION_KEY) == MYSQL_SHELL_WEB_SESSION_VERSION
    ):
        _ensure_server_session()
        return
    session.clear()
    prime_session_scope()


def get_session_profile():
    payload = (_get_server_session() or {}).get("connection_profile")
    if not payload:
        return normalize_profile(DEFAULT_PROFILE)
    return normalize_profile(payload)


def set_session_profile(profile):
    normalized = normalize_profile(profile)
    _session_id, payload = _ensure_server_session()
    payload["connection_profile"] = normalized
    payload["profile_name"] = normalized["name"]


def get_session_credentials():
    payload = _get_server_session() or {}
    credentials = payload.get("credentials") or {}
    return {
        "username": str(credentials.get("username", "")).strip(),
        "password": credentials.get("password", ""),
    }


def is_logged_in():
    payload = _get_server_session()
    return bool(payload and payload.get("logged_in"))


def has_server_login_state():
    payload = _get_server_session()
    return bool(payload and payload.get("logged_in") and payload.get("credentials"))


def get_current_username():
    return get_session_credentials()["username"]


def get_current_profile_name():
    return str((_get_server_session() or {}).get("profile_name", "")).strip()


def clear_login_state(*, keep_profile=True):
    old_server_session_id = _server_session_id()
    current_payload = _get_server_session() or {}
    profile = current_payload.get("connection_profile") if keep_profile else None
    profile_name = current_payload.get("profile_name") if keep_profile else None
    values = deepcopy(current_payload.get("values") or {}) if keep_profile else {}
    version_check = deepcopy(current_payload.get("version_check") or {}) if keep_profile else {}
    _drop_server_session(old_server_session_id)
    session.clear()
    prime_session_scope()
    _session_id, payload = _ensure_server_session()
    payload["logged_in"] = False
    payload["credentials"] = {"username": "", "password": ""}
    payload["values"] = values
    payload["version_check"] = version_check
    if keep_profile and profile:
        payload["connection_profile"] = normalize_profile(profile)
        payload["profile_name"] = profile_name or ""


def set_login_state(profile, username, password):
    set_session_profile(profile)
    _session_id, payload = _ensure_server_session()
    payload["credentials"] = {
        "username": str(username or "").strip(),
        "password": password or "",
    }
    payload["logged_in"] = True


def get_session_value(key, default=None):
    values = (_get_server_session() or {}).get("values") or {}
    return values.get(key, default)


def set_session_value(key, value):
    _session_id, payload = _ensure_server_session()
    payload.setdefault("values", {})[str(key)] = value


def pop_session_value(key, default=None):
    _session_id, payload = _ensure_server_session()
    return payload.setdefault("values", {}).pop(str(key), default)


def get_version_check():
    return deepcopy((_get_server_session() or {}).get("version_check") or {})


def set_version_check(payload):
    _session_id, server_payload = _ensure_server_session()
    server_payload["version_check"] = deepcopy(payload or {})
