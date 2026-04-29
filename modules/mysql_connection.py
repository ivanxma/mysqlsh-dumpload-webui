import os
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .config import SYSTEM_SCHEMAS

try:
    import paramiko
except ImportError:  # pragma: no cover - optional dependency at runtime
    paramiko = None


def _patch_paramiko_for_sshtunnel():
    if paramiko is None or hasattr(paramiko, "DSSKey"):
        return

    class _UnsupportedDSSKey:
        @classmethod
        def from_private_key_file(cls, *args, **kwargs):
            raise paramiko.SSHException(
                "DSA private keys are not supported by the installed Paramiko version."
            )

        @classmethod
        def from_private_key(cls, *args, **kwargs):
            raise paramiko.SSHException(
                "DSA private keys are not supported by the installed Paramiko version."
            )

    # sshtunnel 0.4 expects paramiko.DSSKey to exist, but Paramiko 4 removed it.
    paramiko.DSSKey = _UnsupportedDSSKey


_patch_paramiko_for_sshtunnel()

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:  # pragma: no cover - optional dependency at runtime
    SSHTunnelForwarder = None


@contextmanager
def mysql_endpoint(profile):
    tunnel = None
    target_host = profile["host"]
    target_port = profile["port"]

    if profile["ssh_enabled"]:
        if SSHTunnelForwarder is None:
            raise RuntimeError("SSH tunneling requires the `sshtunnel` package.")
        if not profile["ssh_host"] or not profile["ssh_user"] or not profile["ssh_key_path"]:
            raise ValueError("SSH-enabled profiles require jump host, SSH user, and private key path.")
        expanded_key_path = os.path.expanduser(profile["ssh_key_path"])
        if not os.path.exists(expanded_key_path):
            raise ValueError(f"SSH private key does not exist: {expanded_key_path}")

        tunnel = SSHTunnelForwarder(
            (profile["ssh_host"], profile["ssh_port"]),
            ssh_username=profile["ssh_user"],
            ssh_pkey=expanded_key_path,
            remote_bind_address=(profile["host"], profile["port"]),
        )
        tunnel.start()
        target_host = "127.0.0.1"
        target_port = tunnel.local_bind_port

    try:
        yield {"host": target_host, "port": target_port}
    finally:
        if tunnel is not None:
            tunnel.stop()


@contextmanager
def mysql_connection(profile, credentials, *, database_override=None, connect_timeout=5, autocommit=True):
    if not credentials["username"]:
        raise ValueError("No active MySQL username is stored in the current session.")
    if not profile["host"]:
        raise ValueError("The selected profile does not have a MySQL host configured.")

    connection = None
    with mysql_endpoint(profile) as endpoint:
        connection = pymysql.connect(
            host=endpoint["host"],
            port=endpoint["port"],
            user=credentials["username"],
            password=credentials["password"],
            database=database_override or profile["database"] or None,
            connect_timeout=connect_timeout,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=autocommit,
        )
        try:
            yield connection
        finally:
            if connection is not None:
                connection.close()


def test_mysql_connection(profile, credentials):
    with mysql_connection(profile, credentials, connect_timeout=5):
        return True


def fetch_accessible_schemas(profile, credentials):
    with mysql_connection(profile, credentials, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name AS name
                FROM information_schema.schemata
                WHERE schema_name NOT IN (%s, %s, %s, %s)
                ORDER BY schema_name
                """,
                tuple(sorted(SYSTEM_SCHEMAS)),
            )
            return [row["name"] for row in cursor.fetchall()]


def fetch_mysql_overview(profile, credentials):
    overview = {
        "connected": False,
        "error": "",
        "version": "",
        "server_host": "",
        "current_schema": profile.get("database", ""),
        "schema_count": 0,
        "schemas": [],
    }
    try:
        with mysql_connection(profile, credentials, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT VERSION() AS version, @@hostname AS server_host, DATABASE() AS current_schema")
                row = cursor.fetchone() or {}
                cursor.execute("SELECT COUNT(*) AS schema_count FROM information_schema.schemata")
                schema_count_row = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT schema_name AS name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN (%s, %s, %s, %s)
                    ORDER BY schema_name
                    LIMIT 12
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                overview.update(
                    {
                        "connected": True,
                        "version": str(row.get("version", "")).strip(),
                        "server_host": str(row.get("server_host", "")).strip(),
                        "current_schema": str(row.get("current_schema", "") or profile.get("database", "")).strip(),
                        "schema_count": int(schema_count_row.get("schema_count", 0) or 0),
                        "schemas": [item["name"] for item in cursor.fetchall()],
                    }
                )
    except Exception as error:  # pragma: no cover - depends on runtime services
        overview["error"] = str(error)
    return overview
