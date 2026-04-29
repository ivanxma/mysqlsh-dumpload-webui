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
        expanded_config_path = os.path.expanduser(str(profile.get("ssh_config_file", "")).strip())
        if expanded_config_path and not os.path.exists(expanded_config_path):
            raise ValueError(f"SSH config file does not exist: {expanded_config_path}")

        tunnel_options = {
            "ssh_address_or_host": (profile["ssh_host"], profile["ssh_port"]),
            "ssh_username": profile["ssh_user"],
            "ssh_pkey": expanded_key_path,
            "remote_bind_address": (profile["host"], profile["port"]),
            "set_keepalive": 30.0,
        }
        if expanded_config_path:
            tunnel_options["ssh_config_file"] = expanded_config_path

        tunnel = SSHTunnelForwarder(**tunnel_options)
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


def _string_value(value):
    return str(value or "").strip()


def _int_value(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _fetch_replication_issues(cursor):
    try:
        cursor.execute("SHOW REPLICA STATUS")
        rows = cursor.fetchall() or []
    except Exception:
        return []

    issues = []
    for row in rows:
        io_errno = _int_value(row.get("Last_IO_Errno"))
        sql_errno = _int_value(row.get("Last_SQL_Errno"))
        io_error = _string_value(row.get("Last_IO_Error"))
        sql_error = _string_value(row.get("Last_SQL_Error"))
        if not any([io_errno, sql_errno, io_error, sql_error]):
            continue

        source_host = _string_value(row.get("Source_Host") or row.get("Master_Host"))
        source_port = _string_value(row.get("Source_Port") or row.get("Master_Port"))
        source_display = source_host or "-"
        if source_host and source_port:
            source_display = f"{source_host}:{source_port}"

        issue_messages = []
        if io_errno or io_error:
            issue_messages.append(f"Receiver [{io_errno or 0}]: {io_error or 'Unknown receiver error'}")
        if sql_errno or sql_error:
            issue_messages.append(f"Applier [{sql_errno or 0}]: {sql_error or 'Unknown applier error'}")

        issues.append(
            {
                "channel_name": _string_value(row.get("Channel_Name")) or "default",
                "source": source_display,
                "io_state": _string_value(row.get("Replica_IO_Running") or row.get("Slave_IO_Running")) or "-",
                "sql_state": _string_value(row.get("Replica_SQL_Running") or row.get("Slave_SQL_Running")) or "-",
                "last_io_error_timestamp": _string_value(row.get("Last_IO_Error_Timestamp")),
                "last_sql_error_timestamp": _string_value(row.get("Last_SQL_Error_Timestamp")),
                "error_summary": " | ".join(issue_messages),
            }
        )
    return issues


def _fetch_applier_issues(cursor):
    try:
        cursor.execute(
            """
            SELECT
              CHANNEL_NAME,
              WORKER_ID,
              THREAD_ID,
              SERVICE_STATE,
              LAST_ERROR_NUMBER,
              LAST_ERROR_MESSAGE,
              LAST_ERROR_TIMESTAMP,
              LAST_APPLIED_TRANSACTION,
              APPLYING_TRANSACTION
            FROM performance_schema.replication_applier_status_by_worker
            """
        )
        rows = cursor.fetchall() or []
    except Exception:
        return []

    issues = []
    for row in rows:
        last_error_number = _int_value(row.get("LAST_ERROR_NUMBER"))
        last_error_message = _string_value(row.get("LAST_ERROR_MESSAGE"))
        if not (last_error_number or last_error_message):
            continue

        issues.append(
            {
                "channel_name": _string_value(row.get("CHANNEL_NAME")) or "default",
                "worker_id": _string_value(row.get("WORKER_ID")) or "-",
                "thread_id": _string_value(row.get("THREAD_ID")) or "-",
                "service_state": _string_value(row.get("SERVICE_STATE")) or "-",
                "last_error_number": last_error_number,
                "last_error_message": last_error_message or "Unknown applier error",
                "last_error_timestamp": _string_value(row.get("LAST_ERROR_TIMESTAMP")),
                "last_applied_transaction": _string_value(row.get("LAST_APPLIED_TRANSACTION")),
                "applying_transaction": _string_value(row.get("APPLYING_TRANSACTION")),
            }
        )
    return issues


def _format_event_schedule(row):
    event_type = _string_value(row.get("EVENT_TYPE")).upper()
    execute_at = _string_value(row.get("EXECUTE_AT"))
    interval_value = _string_value(row.get("INTERVAL_VALUE"))
    interval_field = _string_value(row.get("INTERVAL_FIELD"))
    starts = _string_value(row.get("STARTS"))
    ends = _string_value(row.get("ENDS"))

    if event_type == "ONE TIME":
        return f"AT {execute_at}" if execute_at else "One time"

    schedule_parts = []
    every = " ".join(part for part in [interval_value, interval_field] if part).strip()
    if every:
        schedule_parts.append(f"EVERY {every}")
    if starts:
        schedule_parts.append(f"STARTS {starts}")
    if ends:
        schedule_parts.append(f"ENDS {ends}")
    return " ".join(schedule_parts) or "Recurring"


def _fetch_event_overview(cursor):
    try:
        cursor.execute(
            """
            SELECT
              EVENT_SCHEMA,
              EVENT_NAME,
              STATUS,
              EVENT_TYPE,
              EXECUTE_AT,
              INTERVAL_VALUE,
              INTERVAL_FIELD,
              STARTS,
              ENDS,
              LAST_EXECUTED
            FROM information_schema.events
            ORDER BY EVENT_SCHEMA, EVENT_NAME
            """
        )
        rows = cursor.fetchall() or []
    except Exception:
        return {"event_count": 0, "events": []}

    events = []
    for row in rows:
        status = _string_value(row.get("STATUS")) or "-"
        normalized_status = status.upper()
        if normalized_status == "ENABLED":
            status_badge_class = "good"
        elif normalized_status in {"DISABLED", "SLAVESIDE_DISABLED"}:
            status_badge_class = "warn"
        else:
            status_badge_class = "muted"

        events.append(
            {
                "event_schema": _string_value(row.get("EVENT_SCHEMA")) or "-",
                "event_name": _string_value(row.get("EVENT_NAME")) or "-",
                "status": status,
                "status_badge_class": status_badge_class,
                "schedule": _format_event_schedule(row),
                "last_executed": _string_value(row.get("LAST_EXECUTED")),
            }
        )

    return {"event_count": len(events), "events": events}


def fetch_mysql_overview(profile, credentials):
    overview = {
        "connected": False,
        "error": "",
        "version": "",
        "server_host": "",
        "server_id": "",
        "server_uuid": "",
        "current_schema": profile.get("database", ""),
        "event_scheduler": "",
        "gtid_mode": "",
        "gtid_executed": "",
        "gtid_purged": "",
        "schema_count": 0,
        "schemas": [],
        "event_count": 0,
        "events": [],
        "replication_issues": [],
        "applier_issues": [],
    }
    try:
        with mysql_connection(profile, credentials, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      VERSION() AS version,
                      @@hostname AS server_host,
                      @@GLOBAL.server_id AS server_id,
                      @@GLOBAL.server_uuid AS server_uuid,
                      DATABASE() AS current_schema,
                      @@GLOBAL.event_scheduler AS event_scheduler,
                      @@GLOBAL.gtid_mode AS gtid_mode,
                      @@GLOBAL.gtid_executed AS gtid_executed,
                      @@GLOBAL.gtid_purged AS gtid_purged
                    """
                )
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
                schemas = [item["name"] for item in cursor.fetchall()]
                replication_issues = _fetch_replication_issues(cursor)
                applier_issues = _fetch_applier_issues(cursor)
                event_overview = _fetch_event_overview(cursor)
                overview.update(
                    {
                        "connected": True,
                        "version": _string_value(row.get("version")),
                        "server_host": _string_value(row.get("server_host")),
                        "server_id": _string_value(row.get("server_id")),
                        "server_uuid": _string_value(row.get("server_uuid")),
                        "current_schema": _string_value(row.get("current_schema") or profile.get("database", "")),
                        "event_scheduler": _string_value(row.get("event_scheduler")),
                        "gtid_mode": _string_value(row.get("gtid_mode")),
                        "gtid_executed": _string_value(row.get("gtid_executed")),
                        "gtid_purged": _string_value(row.get("gtid_purged")),
                        "schema_count": int(schema_count_row.get("schema_count", 0) or 0),
                        "schemas": schemas,
                        "event_count": event_overview["event_count"],
                        "events": event_overview["events"],
                        "replication_issues": replication_issues,
                        "applier_issues": applier_issues,
                    }
                )
    except Exception as error:  # pragma: no cover - depends on runtime services
        overview["error"] = str(error)
    return overview
