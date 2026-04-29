import json


COMPRESSION_OPTIONS = [
    ("zstd;level=1", "zstd level 1"),
    ("zstd;level=8", "zstd level 8"),
    ("gzip;level=1", "gzip level 1"),
    ("gzip;level=8", "gzip level 8"),
    ("none", "none"),
]

DUMP_DIALECT_OPTIONS = [
    ("default", "default"),
    ("csv", "csv"),
    ("tsv", "tsv"),
    ("csv-unix", "csv-unix"),
    ("csv-rfc-unix", "csv-rfc-unix"),
]

DUMP_COMPATIBILITY_OPTIONS = [
    ("create_invisible_pks", "create_invisible_pks"),
    ("force_innodb", "force_innodb"),
    ("force_non_standard_fks", "force_non_standard_fks"),
    ("ignore_missing_pks", "ignore_missing_pks"),
    ("ignore_wildcard_grants", "ignore_wildcard_grants"),
    ("lock_invalid_accounts", "lock_invalid_accounts"),
    ("skip_invalid_accounts", "skip_invalid_accounts"),
    ("strip_definers", "strip_definers"),
    ("strip_invalid_grants", "strip_invalid_grants"),
    ("strip_restricted_grants", "strip_restricted_grants"),
    ("strip_tablespaces", "strip_tablespaces"),
    ("unescape_wildcard_grants", "unescape_wildcard_grants"),
]

LOAD_ANALYZE_TABLES_OPTIONS = [
    ("off", "off"),
    ("on", "on"),
    ("histogram", "histogram"),
]

LOAD_DEFER_TABLE_INDEXES_OPTIONS = [
    ("off", "off"),
    ("fulltext", "fulltext"),
    ("all", "all"),
]

LOAD_HANDLE_GRANT_ERRORS_OPTIONS = [
    ("abort", "abort"),
    ("drop_account", "drop_account"),
    ("ignore", "ignore"),
]

LOAD_UPDATE_GTID_SET_OPTIONS = [
    ("off", "off"),
    ("replace", "replace"),
    ("append", "append"),
]


def normalize_select(value, allowed_values, default):
    normalized = str(value or "").strip()
    return normalized if normalized in set(allowed_values) else default


def normalize_multiselect(values, allowed_values):
    allowed_lookup = {str(item).strip(): str(item).strip() for item in allowed_values}
    normalized = []
    seen = set()
    for value in values or []:
        candidate = allowed_lookup.get(str(value or "").strip())
        if not candidate or candidate in seen:
            continue
        normalized.append(candidate)
        seen.add(candidate)
    return normalized


def parse_string_list(value):
    normalized_text = str(value or "").replace("\r", "")
    if not normalized_text.strip():
        return []

    entries = []
    seen = set()
    for line in normalized_text.replace(",", "\n").splitlines():
        candidate = line.strip()
        if not candidate or candidate in seen:
            continue
        entries.append(candidate)
        seen.add(candidate)
    return entries


def parse_sql_statement_list(value):
    normalized_text = str(value or "").replace("\r", "")
    if not normalized_text.strip():
        return []
    return [line.strip() for line in normalized_text.splitlines() if line.strip()]


def parse_json_options(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(f"Advanced options JSON is invalid: {error.msg}.") from error
    if not isinstance(parsed, dict):
        raise ValueError("Advanced options JSON must be a JSON object.")
    return parsed

