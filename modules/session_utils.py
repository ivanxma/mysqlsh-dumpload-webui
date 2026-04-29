from flask import session

from .config import (
    DEFAULT_PROFILE,
    MYSQL_SHELL_WEB_SESSION_SCOPE_KEY,
    MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE,
    MYSQL_SHELL_WEB_SESSION_VERSION,
    MYSQL_SHELL_WEB_SESSION_VERSION_KEY,
)
from .profiles import normalize_profile


def prime_session_scope():
    session[MYSQL_SHELL_WEB_SESSION_SCOPE_KEY] = MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE
    session[MYSQL_SHELL_WEB_SESSION_VERSION_KEY] = MYSQL_SHELL_WEB_SESSION_VERSION


def ensure_session_scope():
    if (
        session.get(MYSQL_SHELL_WEB_SESSION_SCOPE_KEY) == MYSQL_SHELL_WEB_SESSION_SCOPE_VALUE
        and session.get(MYSQL_SHELL_WEB_SESSION_VERSION_KEY) == MYSQL_SHELL_WEB_SESSION_VERSION
    ):
        return
    session.clear()
    prime_session_scope()


def get_session_profile():
    payload = session.get("connection_profile")
    if not payload:
        return normalize_profile(DEFAULT_PROFILE)
    return normalize_profile(payload)


def set_session_profile(profile):
    normalized = normalize_profile(profile)
    session["connection_profile"] = normalized
    session["profile_name"] = normalized["name"]


def get_session_credentials():
    return {
        "username": str(session.get("mysql_username", "")).strip(),
        "password": session.get("mysql_password", ""),
    }


def is_logged_in():
    return bool(session.get("logged_in"))


def clear_login_state(*, keep_profile=True):
    profile = session.get("connection_profile") if keep_profile else None
    profile_name = session.get("profile_name") if keep_profile else None
    session.clear()
    prime_session_scope()
    if keep_profile and profile:
        session["connection_profile"] = normalize_profile(profile)
        session["profile_name"] = profile_name or ""


def set_login_state(profile, username, password):
    set_session_profile(profile)
    session["mysql_username"] = str(username or "").strip()
    session["mysql_password"] = password or ""
    session["logged_in"] = True
