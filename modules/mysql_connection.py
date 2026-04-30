import os
import re
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .config import SYSTEM_SCHEMAS

AUTO_FIX_PRIMARY_KEY_COLUMN = "my_row_id"
_IDENTIFIER_TOKEN_RE = re.compile(r"`([^`]+)`|([A-Za-z_][A-Za-z0-9_$]*)")

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


def _format_account_filter_value(user_name, host_name):
    escaped_user = _string_value(user_name).replace("'", "''")
    escaped_host = _string_value(host_name).replace("'", "''")
    return f"'{escaped_user}'@'{escaped_host}'"


def fetch_dump_filter_catalog(profile, credentials):
    catalog = {
        "schemas": [],
        "tables": [],
        "users": [],
        "events": [],
        "routines": [],
        "triggers": [],
        "libraries": [],
        "errors": {},
    }

    with mysql_connection(profile, credentials, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    SELECT schema_name AS schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN (%s, %s, %s, %s)
                    ORDER BY schema_name
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["schemas"] = [
                    {"value": _string_value(row.get("schema_name")), "label": _string_value(row.get("schema_name"))}
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["schemas"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name
                    FROM information_schema.tables
                    WHERE TABLE_TYPE = 'BASE TABLE'
                      AND TABLE_SCHEMA NOT IN (%s, %s, %s, %s)
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["tables"] = [
                    {
                        "value": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('table_name'))}",
                        "label": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('table_name'))}",
                    }
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name")) and _string_value(row.get("table_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["tables"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT User AS user_name, Host AS host_name
                    FROM mysql.user
                    ORDER BY User, Host
                    """
                )
                seen_users = set()
                for row in cursor.fetchall() or []:
                    user_name = _string_value(row.get("user_name"))
                    host_name = _string_value(row.get("host_name"))
                    if not user_name and not host_name:
                        continue
                    value = _format_account_filter_value(user_name, host_name)
                    if value in seen_users:
                        continue
                    seen_users.add(value)
                    catalog["users"].append({"value": value, "label": value})
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["users"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT EVENT_SCHEMA AS schema_name, EVENT_NAME AS event_name
                    FROM information_schema.events
                    WHERE EVENT_SCHEMA NOT IN (%s, %s, %s, %s)
                    ORDER BY EVENT_SCHEMA, EVENT_NAME
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["events"] = [
                    {
                        "value": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('event_name'))}",
                        "label": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('event_name'))}",
                    }
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name")) and _string_value(row.get("event_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["events"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT ROUTINE_SCHEMA AS schema_name, ROUTINE_NAME AS routine_name, ROUTINE_TYPE AS routine_type
                    FROM information_schema.routines
                    WHERE ROUTINE_SCHEMA NOT IN (%s, %s, %s, %s)
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_TYPE, ROUTINE_NAME
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["routines"] = [
                    {
                        "value": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('routine_name'))}",
                        "label": (
                            f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('routine_name'))} "
                            f"({_string_value(row.get('routine_type')) or 'ROUTINE'})"
                        ),
                    }
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name")) and _string_value(row.get("routine_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["routines"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT TRIGGER_SCHEMA AS schema_name, TRIGGER_NAME AS trigger_name
                    FROM information_schema.triggers
                    WHERE TRIGGER_SCHEMA NOT IN (%s, %s, %s, %s)
                    ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["triggers"] = [
                    {
                        "value": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('trigger_name'))}",
                        "label": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('trigger_name'))}",
                    }
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name")) and _string_value(row.get("trigger_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on server privileges
                catalog["errors"]["triggers"] = str(error)

            try:
                cursor.execute(
                    """
                    SELECT LIBRARY_SCHEMA AS schema_name, LIBRARY_NAME AS library_name
                    FROM information_schema.libraries
                    WHERE LIBRARY_SCHEMA NOT IN (%s, %s, %s, %s)
                    ORDER BY LIBRARY_SCHEMA, LIBRARY_NAME
                    """,
                    tuple(sorted(SYSTEM_SCHEMAS)),
                )
                catalog["libraries"] = [
                    {
                        "value": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('library_name'))}",
                        "label": f"{_string_value(row.get('schema_name'))}.{_string_value(row.get('library_name'))}",
                    }
                    for row in (cursor.fetchall() or [])
                    if _string_value(row.get("schema_name")) and _string_value(row.get("library_name"))
                ]
            except Exception as error:  # pragma: no cover - depends on runtime services
                catalog["errors"]["libraries"] = str(error)

    return catalog


def _string_value(value):
    return str(value or "").strip()


def _int_value(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _quote_identifier(value):
    identifier = _string_value(value)
    if not identifier:
        raise ValueError("Event schema and event name are required.")
    return f"`{identifier.replace('`', '``')}`"


def _split_grouped_values(value, *, separator="\n"):
    return [item for item in (_string_value(part) for part in _string_value(value).split(separator)) if item]


def _merge_column_names(*groups):
    merged = []
    seen = set()
    for group in groups:
        for raw_name in group or []:
            column_name = _string_value(raw_name)
            normalized = column_name.lower()
            if not column_name or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(column_name)
    return merged


def _extract_partition_columns(expression, table_columns):
    raw_expression = _string_value(expression)
    if not raw_expression or not table_columns:
        return []

    columns_by_name = {column.lower(): column for column in table_columns if _string_value(column)}
    partition_columns = []
    seen = set()

    for match in _IDENTIFIER_TOKEN_RE.finditer(raw_expression):
        token = match.group(1) or match.group(2)
        if not token:
            continue

        normalized_token = token.lower()
        column_name = columns_by_name.get(normalized_token)
        if not column_name or normalized_token in seen:
            continue

        if match.group(1) is None:
            remainder = raw_expression[match.end() :]
            if remainder.lstrip().startswith("("):
                continue

        seen.add(normalized_token)
        partition_columns.append(column_name)

    return partition_columns


def _format_column_list(column_names):
    return ", ".join(_quote_identifier(column_name) for column_name in column_names or [])


def _resolve_partition_columns(row):
    table_columns = _split_grouped_values(row.get("table_columns"))
    is_partitioned = _int_value(row.get("is_partitioned")) > 0
    partition_expression = _string_value(row.get("partition_expression"))
    subpartition_expression = _string_value(row.get("subpartition_expression"))
    partition_columns = _merge_column_names(
        _extract_partition_columns(partition_expression, table_columns),
        _extract_partition_columns(subpartition_expression, table_columns),
    )
    partition_columns_resolved = not is_partitioned or bool(partition_columns)
    return {
        "is_partitioned": is_partitioned,
        "partition_expression": partition_expression,
        "subpartition_expression": subpartition_expression,
        "partition_columns": partition_columns,
        "partition_columns_resolved": partition_columns_resolved,
    }


def _normalize_schema_names(schema_names):
    normalized_names = []
    seen = set()
    for raw_name in schema_names or []:
        schema_name = _string_value(raw_name)
        if not schema_name or schema_name in SYSTEM_SCHEMAS or schema_name in seen:
            continue
        seen.add(schema_name)
        normalized_names.append(schema_name)
    return normalized_names


def _schema_filter_clause(column_name, schema_names):
    normalized_names = _normalize_schema_names(schema_names)
    if not normalized_names:
        return "", ()

    placeholders = ", ".join(["%s"] * len(normalized_names))
    return f" AND {column_name} IN ({placeholders})", tuple(normalized_names)


def _fetch_enabled_event_count_value(cursor, schema_names=None):
    schema_filter_sql, schema_filter_params = _schema_filter_clause("EVENT_SCHEMA", schema_names)
    cursor.execute(
        f"""
        SELECT COUNT(*) AS enabled_event_count
        FROM information_schema.events
        WHERE STATUS = 'ENABLED'
          AND EVENT_SCHEMA NOT IN (%s, %s, %s, %s)
          {schema_filter_sql}
        """,
        tuple(sorted(SYSTEM_SCHEMAS)) + schema_filter_params,
    )
    row = cursor.fetchone() or {}
    return _int_value(row.get("enabled_event_count"))


def _fetch_tables_without_primary_key_count(cursor, schema_names=None):
    schema_filter_sql, schema_filter_params = _schema_filter_clause("t.TABLE_SCHEMA", schema_names)
    cursor.execute(
        f"""
        SELECT COUNT(*) AS tables_without_primary_key_count
        FROM information_schema.tables t
        LEFT JOIN information_schema.table_constraints tc
          ON tc.TABLE_SCHEMA = t.TABLE_SCHEMA
         AND tc.TABLE_NAME = t.TABLE_NAME
         AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_SCHEMA NOT IN (%s, %s, %s, %s)
          {schema_filter_sql}
          AND tc.CONSTRAINT_NAME IS NULL
        """,
        tuple(sorted(SYSTEM_SCHEMAS)) + schema_filter_params,
    )
    row = cursor.fetchone() or {}
    return _int_value(row.get("tables_without_primary_key_count"))


def _fetch_table_engine_counts(cursor, schema_names=None):
    schema_filter_sql, schema_filter_params = _schema_filter_clause("t.TABLE_SCHEMA", schema_names)
    cursor.execute(
        f"""
        SELECT
          SUM(CASE WHEN UPPER(COALESCE(t.ENGINE, '')) = 'INNODB' THEN 1 ELSE 0 END) AS innodb_table_count,
          SUM(CASE WHEN UPPER(COALESCE(t.ENGINE, '')) = 'LAKEHOUSE' THEN 1 ELSE 0 END) AS lakehouse_table_count,
          SUM(
            CASE
              WHEN UPPER(COALESCE(t.CREATE_OPTIONS, '')) LIKE '%%SECONDARY_ENGINE=RAPID%%' THEN 1
              ELSE 0
            END
          ) AS rapid_secondary_engine_table_count
        FROM information_schema.tables t
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_SCHEMA NOT IN (%s, %s, %s, %s)
          {schema_filter_sql}
        """,
        tuple(sorted(SYSTEM_SCHEMAS)) + schema_filter_params,
    )
    row = cursor.fetchone() or {}
    return {
        "innodb_table_count": _int_value(row.get("innodb_table_count")),
        "lakehouse_table_count": _int_value(row.get("lakehouse_table_count")),
        "rapid_secondary_engine_table_count": _int_value(row.get("rapid_secondary_engine_table_count")),
    }


def fetch_enabled_event_count(profile, credentials):
    with mysql_connection(profile, credentials, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            return _fetch_enabled_event_count_value(cursor)


def fetch_dump_validation_summary(profile, credentials, *, schema_names=None):
    with mysql_connection(profile, credentials, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            engine_counts = _fetch_table_engine_counts(
                cursor,
                schema_names=schema_names,
            )
            return {
                "tables_without_primary_key_count": _fetch_tables_without_primary_key_count(
                    cursor,
                    schema_names=schema_names,
                ),
                "enabled_event_count": _fetch_enabled_event_count_value(
                    cursor,
                    schema_names=schema_names,
                ),
                "innodb_table_count": engine_counts["innodb_table_count"],
                "lakehouse_table_count": engine_counts["lakehouse_table_count"],
                "rapid_secondary_engine_table_count": engine_counts["rapid_secondary_engine_table_count"],
            }


def _fetch_primary_key_rows(cursor, *, table_schema="", table_name=""):
    sql = """
        SELECT
          t.TABLE_SCHEMA,
          t.TABLE_NAME,
          GROUP_CONCAT(
            CASE WHEN pk.CONSTRAINT_NAME = 'PRIMARY' THEN c.COLUMN_NAME END
            ORDER BY c.ORDINAL_POSITION
            SEPARATOR ', '
          ) AS primary_key_columns,
          GROUP_CONCAT(
            CASE WHEN c.EXTRA LIKE '%%auto_increment%%' THEN c.COLUMN_NAME END
            ORDER BY c.ORDINAL_POSITION
            SEPARATOR ', '
          ) AS auto_increment_columns,
          GROUP_CONCAT(
            c.COLUMN_NAME
            ORDER BY c.ORDINAL_POSITION
            SEPARATOR '\n'
          ) AS table_columns,
          MAX(CASE WHEN c.EXTRA LIKE '%%auto_increment%%' THEN 1 ELSE 0 END) AS has_auto_increment,
          MAX(CASE WHEN c.COLUMN_NAME = %s THEN 1 ELSE 0 END) AS has_my_row_id,
          COALESCE(partitions.is_partitioned, 0) AS is_partitioned,
          partitions.partition_expression,
          partitions.subpartition_expression
        FROM information_schema.tables t
        JOIN information_schema.columns c
          ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
         AND c.TABLE_NAME = t.TABLE_NAME
        LEFT JOIN information_schema.key_column_usage pk
          ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND pk.TABLE_NAME = c.TABLE_NAME
         AND pk.COLUMN_NAME = c.COLUMN_NAME
         AND pk.CONSTRAINT_NAME = 'PRIMARY'
        LEFT JOIN (
          SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            MAX(CASE WHEN PARTITION_NAME IS NOT NULL OR SUBPARTITION_NAME IS NOT NULL THEN 1 ELSE 0 END) AS is_partitioned,
            MAX(PARTITION_EXPRESSION) AS partition_expression,
            MAX(SUBPARTITION_EXPRESSION) AS subpartition_expression
          FROM information_schema.partitions
          GROUP BY TABLE_SCHEMA, TABLE_NAME
        ) partitions
          ON partitions.TABLE_SCHEMA = t.TABLE_SCHEMA
         AND partitions.TABLE_NAME = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_SCHEMA NOT IN (%s, %s, %s, %s)
    """
    params = [AUTO_FIX_PRIMARY_KEY_COLUMN, *sorted(SYSTEM_SCHEMAS)]

    normalized_schema = _string_value(table_schema)
    if normalized_schema:
        sql += " AND t.TABLE_SCHEMA = %s"
        params.append(normalized_schema)

    normalized_table = _string_value(table_name)
    if normalized_table:
        sql += " AND t.TABLE_NAME = %s"
        params.append(normalized_table)

    sql += """
        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    """
    cursor.execute(sql, tuple(params))
    return cursor.fetchall() or []


def _normalize_primary_key_row(row):
    table_schema = _string_value(row.get("TABLE_SCHEMA"))
    table_name = _string_value(row.get("TABLE_NAME"))
    primary_key_columns = _string_value(row.get("primary_key_columns"))
    auto_increment_columns = _string_value(row.get("auto_increment_columns"))
    auto_increment_column = auto_increment_columns.split(", ", 1)[0] if auto_increment_columns else ""
    has_primary_key = bool(primary_key_columns)
    has_my_row_id = _int_value(row.get("has_my_row_id")) > 0
    partition_info = _resolve_partition_columns(row)
    partition_columns = partition_info["partition_columns"]
    partition_columns_display = _format_column_list(partition_columns) or "-"
    required_primary_key_columns = _merge_column_names(
        [auto_increment_column or AUTO_FIX_PRIMARY_KEY_COLUMN],
        partition_columns,
    )
    required_primary_key_columns_display = _format_column_list(required_primary_key_columns) or "-"
    fix_error = ""

    if has_primary_key:
        fix_strategy = "Already has a PRIMARY KEY"
        can_apply_fix = False
    elif not partition_info["partition_columns_resolved"]:
        fix_strategy = "Partitioned table; review partition columns manually before adding a PRIMARY KEY"
        can_apply_fix = False
        fix_error = (
            f"Table `{table_schema}`.`{table_name}` is partitioned, but its partition columns "
            "could not be resolved automatically. Review the table manually."
        )
    elif auto_increment_column:
        fix_strategy = f"Add PRIMARY KEY on {required_primary_key_columns_display}"
        can_apply_fix = True
    elif has_my_row_id:
        fix_strategy = f"`{AUTO_FIX_PRIMARY_KEY_COLUMN}` already exists; review manually"
        can_apply_fix = False
        fix_error = (
            f"Table `{table_schema}`.`{table_name}` already contains "
            f"`{AUTO_FIX_PRIMARY_KEY_COLUMN}`. Review the table manually."
        )
    else:
        if partition_columns:
            fix_strategy = (
                f"Add invisible AUTO_INCREMENT column `{AUTO_FIX_PRIMARY_KEY_COLUMN}` "
                f"and PRIMARY KEY on {required_primary_key_columns_display}"
            )
        else:
            fix_strategy = f"Add invisible AUTO_INCREMENT PRIMARY KEY column `{AUTO_FIX_PRIMARY_KEY_COLUMN}`"
        can_apply_fix = True

    return {
        "table_schema": table_schema,
        "table_name": table_name,
        "object_name": f"{table_schema}.{table_name}" if table_schema and table_name else table_name or "-",
        "primary_key_columns": primary_key_columns or "-",
        "auto_increment_column": auto_increment_column,
        "auto_increment_column_display": auto_increment_column or "-",
        "has_primary_key": has_primary_key,
        "has_my_row_id": has_my_row_id,
        "is_partitioned": partition_info["is_partitioned"],
        "partition_columns": partition_columns,
        "partition_columns_display": partition_columns_display,
        "required_primary_key_columns": required_primary_key_columns,
        "required_primary_key_columns_display": required_primary_key_columns_display,
        "fix_error": fix_error,
        "fix_strategy": fix_strategy,
        "can_apply_fix": can_apply_fix,
    }


def _fetch_primary_key_check(cursor):
    rows = _fetch_primary_key_rows(cursor)
    databases_by_name = {}
    tables_with_primary_key = []
    tables_without_primary_key = []

    for row in rows:
        entry = _normalize_primary_key_row(row)
        database_entry = databases_by_name.setdefault(
            entry["table_schema"],
            {
                "schema_name": entry["table_schema"],
                "table_count": 0,
                "tables_with_primary_key_count": 0,
                "tables_without_primary_key_count": 0,
            },
        )
        database_entry["table_count"] += 1

        if entry["has_primary_key"]:
            database_entry["tables_with_primary_key_count"] += 1
            tables_with_primary_key.append(entry)
        else:
            database_entry["tables_without_primary_key_count"] += 1
            tables_without_primary_key.append(entry)

    databases = [databases_by_name[name] for name in sorted(databases_by_name)]
    return {
        "database_count": len(databases),
        "tables_with_primary_key_count": len(tables_with_primary_key),
        "tables_without_primary_key_count": len(tables_without_primary_key),
        "databases": databases,
        "tables_with_primary_key": tables_with_primary_key,
        "tables_without_primary_key": tables_without_primary_key,
    }


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
                "event_schema": _string_value(row.get("EVENT_SCHEMA")),
                "event_name": _string_value(row.get("EVENT_NAME")),
                "status": status,
                "status_badge_class": status_badge_class,
                "is_enabled": normalized_status == "ENABLED",
                "is_disabled": normalized_status in {"DISABLED", "SLAVESIDE_DISABLED"},
                "schedule": _format_event_schedule(row),
                "last_executed": _string_value(row.get("LAST_EXECUTED")),
            }
        )

    return {"event_count": len(events), "events": events}


def set_event_status(profile, credentials, event_schema, event_name, *, enabled):
    action = "ENABLE" if enabled else "DISABLE"
    statement = f"ALTER EVENT {_quote_identifier(event_schema)}.{_quote_identifier(event_name)} {action}"
    with mysql_connection(profile, credentials, connect_timeout=5, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)


def fetch_db_admin_overview(profile, credentials):
    overview = {
        "error": "",
        "event_scheduler": "",
        "event_count": 0,
        "events": [],
        "primary_key_error": "",
        "primary_key_check": {
            "database_count": 0,
            "tables_with_primary_key_count": 0,
            "tables_without_primary_key_count": 0,
            "databases": [],
            "tables_with_primary_key": [],
            "tables_without_primary_key": [],
        },
    }
    try:
        with mysql_connection(profile, credentials, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT @@GLOBAL.event_scheduler AS event_scheduler")
                row = cursor.fetchone() or {}
                event_overview = _fetch_event_overview(cursor)
                overview["event_scheduler"] = _string_value(row.get("event_scheduler"))
                overview["event_count"] = event_overview["event_count"]
                overview["events"] = event_overview["events"]
                try:
                    overview["primary_key_check"] = _fetch_primary_key_check(cursor)
                except Exception as error:  # pragma: no cover - depends on server privileges
                    overview["primary_key_error"] = str(error)
    except Exception as error:  # pragma: no cover - depends on runtime services
        overview["error"] = str(error)
    return overview


def apply_primary_key_fix(profile, credentials, table_schema, table_name):
    normalized_schema = _string_value(table_schema)
    normalized_table = _string_value(table_name)
    if not normalized_schema or not normalized_table:
        raise ValueError("Table schema and table name are required.")

    with mysql_connection(profile, credentials, connect_timeout=5, autocommit=True) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_primary_key_rows(
                cursor,
                table_schema=normalized_schema,
                table_name=normalized_table,
            )
            if not rows:
                raise ValueError("Table not found or is not a base table.")

            table_info = _normalize_primary_key_row(rows[0])
            if table_info["has_primary_key"]:
                raise ValueError(
                    f"Table `{normalized_schema}`.`{normalized_table}` already has a PRIMARY KEY."
                )
            if not table_info["can_apply_fix"]:
                raise ValueError(
                    table_info["fix_error"]
                    or f"Table `{normalized_schema}`.`{normalized_table}` cannot be fixed automatically."
                )

            primary_key_columns_sql = _format_column_list(table_info["required_primary_key_columns"])

            if table_info["auto_increment_column"]:
                statement = (
                    f"ALTER TABLE {_quote_identifier(normalized_schema)}.{_quote_identifier(normalized_table)} "
                    f"ADD PRIMARY KEY ({primary_key_columns_sql})"
                )
                message = f"Added PRIMARY KEY on {table_info['required_primary_key_columns_display']}."
            else:
                statement = (
                    f"ALTER TABLE {_quote_identifier(normalized_schema)}.{_quote_identifier(normalized_table)} "
                    f"ADD COLUMN `{AUTO_FIX_PRIMARY_KEY_COLUMN}` "
                    "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, "
                    f"ADD PRIMARY KEY ({primary_key_columns_sql})"
                )
                if table_info["partition_columns"]:
                    message = (
                        f"Added invisible AUTO_INCREMENT column `{AUTO_FIX_PRIMARY_KEY_COLUMN}` "
                        f"and PRIMARY KEY on {table_info['required_primary_key_columns_display']}."
                    )
                else:
                    message = (
                        f"Added invisible AUTO_INCREMENT PRIMARY KEY column `{AUTO_FIX_PRIMARY_KEY_COLUMN}`."
                    )

            cursor.execute(statement)
            return {
                "table_schema": normalized_schema,
                "table_name": normalized_table,
                "message": message,
            }


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
