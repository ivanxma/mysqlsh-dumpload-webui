import json
import os
from datetime import datetime, timezone
from uuid import uuid4

from .config import (
    DEFAULT_OBJECT_STORAGE,
    LOCAL_OCI_CONFIG_FILE,
    OBJECT_STORAGE_STORE,
    PAR_ACCESS_OPTIONS,
    PAR_STORE,
)
from .oci_configuration import list_oci_config_profiles

try:
    import oci
except ImportError:  # pragma: no cover - optional dependency at runtime
    oci = None


def normalize_relative_prefix(value):
    raw_value = str(value or "").replace("\\", "/").strip()
    if not raw_value:
        return ""

    parts = []
    for segment in raw_value.split("/"):
        normalized_segment = segment.strip()
        if not normalized_segment:
            continue
        if normalized_segment in {".", ".."}:
            raise ValueError("Folder names cannot contain '.' or '..' path segments.")
        parts.append(normalized_segment)

    if not parts:
        return ""
    return "/".join(parts) + "/"


def join_relative_prefixes(*parts):
    combined = []
    for part in parts:
        normalized = normalize_relative_prefix(part)
        if normalized:
            combined.extend(segment for segment in normalized.strip("/").split("/") if segment)
    if not combined:
        return ""
    return "/".join(combined) + "/"


def parent_relative_prefix(prefix):
    normalized = normalize_relative_prefix(prefix)
    if not normalized:
        return ""
    parts = normalized.strip("/").split("/")
    if len(parts) <= 1:
        return ""
    return "/".join(parts[:-1]) + "/"


def ensure_object_storage_store():
    if OBJECT_STORAGE_STORE.exists():
        return
    OBJECT_STORAGE_STORE.write_text(json.dumps(DEFAULT_OBJECT_STORAGE, indent=2) + "\n", encoding="utf-8")


def _normalize_config_source(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"local", "app", "application"}:
        return "local"
    return "existing"


def effective_oci_config_file(config):
    if _normalize_config_source(config.get("config_source")) == "local":
        return str(LOCAL_OCI_CONFIG_FILE)
    return str(config.get("config_file", "")).strip() or DEFAULT_OBJECT_STORAGE["config_file"]


def normalize_object_storage(payload):
    managed_folders = payload.get("managed_folders", [])
    if not isinstance(managed_folders, list):
        managed_folders = []

    normalized_folders = []
    seen = set()
    for item in managed_folders:
        try:
            normalized = normalize_relative_prefix(item)
        except ValueError:
            continue
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_folders.append(normalized)

    config_source = _normalize_config_source(payload.get("config_source", DEFAULT_OBJECT_STORAGE["config_source"]))
    config_file = str(payload.get("config_file", "")).strip() or DEFAULT_OBJECT_STORAGE["config_file"]
    if config_source == "local":
        config_file = str(LOCAL_OCI_CONFIG_FILE)

    return {
        "config_source": config_source,
        "region": str(payload.get("region", "")).strip(),
        "namespace": str(payload.get("namespace", "")).strip(),
        "bucket_name": str(payload.get("bucket_name", "")).strip(),
        "bucket_prefix": normalize_relative_prefix(payload.get("bucket_prefix", "")),
        "config_profile": str(payload.get("config_profile", "")).strip() or DEFAULT_OBJECT_STORAGE["config_profile"],
        "config_file": config_file,
        "managed_folders": sorted(normalized_folders),
    }


def load_object_storage_config():
    ensure_object_storage_store()
    try:
        payload = json.loads(OBJECT_STORAGE_STORE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return dict(DEFAULT_OBJECT_STORAGE)
    return normalize_object_storage(payload)


def save_object_storage_config(payload):
    OBJECT_STORAGE_STORE.write_text(
        json.dumps(normalize_object_storage(payload), indent=2) + "\n",
        encoding="utf-8",
    )


def ensure_par_store():
    if PAR_STORE.exists():
        return
    PAR_STORE.write_text(json.dumps({"pars": []}, indent=2) + "\n", encoding="utf-8")


def _parse_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed.astimezone(timezone.utc)


def format_datetime_local(value):
    parsed = _parse_time(value)
    if parsed is None:
        return ""
    return parsed.astimezone().strftime("%Y-%m-%dT%H:%M")


def _serialize_time(value):
    parsed = _parse_time(value)
    if parsed is None:
        return ""
    return parsed.isoformat()


def _annotate_par_entry(entry):
    annotated = dict(entry)
    expires_at = _parse_time(entry.get("expires_at"))
    now = datetime.now(timezone.utc)
    annotated["expires_at"] = _serialize_time(expires_at)
    annotated["created_at"] = _serialize_time(entry.get("created_at"))
    annotated["is_active"] = bool(expires_at and expires_at > now)
    annotated["status_label"] = "Active" if annotated["is_active"] else "Expired"
    annotated["expires_at_local"] = format_datetime_local(expires_at)
    target_type = str(entry.get("target_type", "prefix")).strip().lower()
    annotated["target_type"] = target_type if target_type in {"prefix", "bucket"} else "prefix"
    annotated["target_display"] = "Bucket" if annotated["target_type"] == "bucket" else (
        entry.get("object_name") or entry.get("relative_prefix") or "/"
    )
    return annotated


def _normalize_par_entry(payload):
    allowed_access_types = {value for value, _label in PAR_ACCESS_OPTIONS}
    target_type = str(payload.get("target_type", "prefix")).strip().lower()
    if target_type not in {"prefix", "bucket"}:
        target_type = "prefix"
    access_type = str(payload.get("access_type", "AnyObjectReadWrite")).strip()
    if access_type not in allowed_access_types:
        access_type = "AnyObjectReadWrite"

    relative_prefix = ""
    try:
        relative_prefix = normalize_relative_prefix(payload.get("relative_prefix", ""))
    except ValueError:
        relative_prefix = ""

    return {
        "id": str(payload.get("id", "")).strip() or str(uuid4()),
        "par_id": str(payload.get("par_id", "")).strip(),
        "name": str(payload.get("name", "")).strip(),
        "namespace": str(payload.get("namespace", "")).strip(),
        "bucket_name": str(payload.get("bucket_name", "")).strip(),
        "target_type": target_type,
        "relative_prefix": relative_prefix,
        "object_name": str(payload.get("object_name", "")).strip(),
        "access_type": access_type,
        "bucket_listing_action": str(payload.get("bucket_listing_action", "")).strip(),
        "created_at": _serialize_time(payload.get("created_at")),
        "expires_at": _serialize_time(payload.get("expires_at")),
        "par_url": str(payload.get("par_url", "")).strip(),
        "raw_par_url": str(payload.get("raw_par_url", "")).strip(),
    }


def load_par_entries():
    ensure_par_store()
    try:
        payload = json.loads(PAR_STORE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    entries = [_annotate_par_entry(_normalize_par_entry(item)) for item in payload.get("pars", [])]
    return sorted(entries, key=lambda item: item.get("created_at", ""), reverse=True)


def save_par_entries(entries):
    normalized_entries = [_normalize_par_entry(item) for item in entries]
    PAR_STORE.write_text(
        json.dumps({"pars": normalized_entries}, indent=2) + "\n",
        encoding="utf-8",
    )


def get_par_entry_by_id(entry_id):
    entry_lookup = str(entry_id or "").strip()
    for entry in load_par_entries():
        if entry["id"] == entry_lookup:
            return entry
    return None


def get_par_entries_for_bucket(config):
    namespace = str(config.get("namespace", "")).strip()
    bucket_name = str(config.get("bucket_name", "")).strip()
    if not namespace or not bucket_name:
        return []
    return [
        entry
        for entry in load_par_entries()
        if entry["namespace"] == namespace and entry["bucket_name"] == bucket_name
    ]


def list_active_pars_for_purpose(config, purpose):
    allowed_access = {"AnyObjectRead", "AnyObjectReadWrite"} if purpose == "load" else {"AnyObjectReadWrite"}
    return [
        entry
        for entry in get_par_entries_for_bucket(config)
        if entry["is_active"]
        and entry["access_type"] in allowed_access
        and entry["bucket_listing_action"] == "ListObjects"
        and entry["target_type"] in {"bucket", "prefix"}
    ]


def _require_bucket_config(config):
    missing = [key for key in ("namespace", "bucket_name") if not str(config.get(key, "")).strip()]
    if missing:
        raise ValueError(f"Object Storage is missing: {', '.join(missing)}")


def _get_oci_config(config):
    if oci is None:
        raise RuntimeError("The `oci` package is required for Object Storage integration.")

    config_file = os.path.expanduser(effective_oci_config_file(config))
    profile_name = str(config.get("config_profile", "") or DEFAULT_OBJECT_STORAGE["config_profile"])
    try:
        oci_config = oci.config.from_file(file_location=config_file, profile_name=profile_name)
    except FileNotFoundError as error:
        raise ValueError(f"OCI config file not found: {config_file}") from error
    except Exception as error:  # pragma: no cover - depends on OCI environment
        raise ValueError(f"Unable to load OCI config `{profile_name}` from `{config_file}`: {error}") from error

    region_override = str(config.get("region", "")).strip()
    if region_override:
        oci_config["region"] = region_override
    return oci_config


def _get_object_storage_client(config):
    _require_bucket_config(config)
    oci_config = _get_oci_config(config)
    return oci.object_storage.ObjectStorageClient(oci_config)


def _build_par_url(client, access_uri):
    endpoint = client.base_client.endpoint.rstrip("/")
    uri = str(access_uri or "").strip()
    if not uri.startswith("/"):
        uri = "/" + uri
    return endpoint + uri


def _list_all_objects(client, namespace, bucket_name, prefix):
    objects = []
    start = None
    while True:
        response = client.list_objects(
            namespace_name=namespace,
            bucket_name=bucket_name,
            prefix=prefix or None,
            start=start,
            fields="name,size,timeCreated,timeModified",
        )
        objects.extend(response.data.objects or [])
        start = response.data.next_start_with
        if not start:
            break
    return objects


def _prefix_has_any_content(client, namespace, bucket_name, prefix):
    response = client.list_objects(
        namespace_name=namespace,
        bucket_name=bucket_name,
        prefix=prefix or None,
        delimiter="/",
        limit=1,
        fields="name",
    )
    return bool((response.data.objects or []) or (response.data.prefixes or []))


def create_par_record(config, payload):
    client = _get_object_storage_client(config)
    namespace = config["namespace"]
    bucket_name = config["bucket_name"]

    name = str(payload.get("name", "")).strip()
    if not name:
        raise ValueError("PAR name is required.")

    target_type = str(payload.get("target_type", "prefix")).strip().lower()
    if target_type not in {"prefix", "bucket"}:
        raise ValueError("PAR target type must be bucket or prefix.")

    relative_prefix = normalize_relative_prefix(payload.get("relative_prefix", ""))
    object_name = ""
    if target_type == "prefix":
        object_name = join_relative_prefixes(config.get("bucket_prefix", ""), relative_prefix)
        if not object_name:
            raise ValueError("Prefix PARs require either a bucket prefix or a folder prefix.")

    access_type = str(payload.get("access_type", "AnyObjectReadWrite")).strip()
    allowed_access_types = {value for value, _label in PAR_ACCESS_OPTIONS}
    if access_type not in allowed_access_types:
        raise ValueError("Unsupported PAR access type.")

    expires_at = _parse_time(payload.get("expires_at"))
    if expires_at is None:
        raise ValueError("PAR expiration is required.")
    if expires_at <= datetime.now(timezone.utc):
        raise ValueError("PAR expiration must be in the future.")

    allow_listing = str(payload.get("allow_listing", "")).strip().lower() in {"1", "true", "yes", "on"}
    details = oci.object_storage.models.CreatePreauthenticatedRequestDetails(
        name=name,
        access_type=access_type,
        time_expires=expires_at,
        bucket_listing_action="ListObjects" if allow_listing else None,
        object_name=object_name or None,
    )
    response = client.create_preauthenticated_request(
        namespace_name=namespace,
        bucket_name=bucket_name,
        create_preauthenticated_request_details=details,
    )
    created = response.data
    raw_par_url = _build_par_url(client, created.access_uri)
    par_url = raw_par_url
    if target_type == "prefix" and object_name:
        par_url = raw_par_url.rstrip("/") + "/" + object_name

    new_entry = _normalize_par_entry(
        {
            "id": str(uuid4()),
            "par_id": created.id,
            "name": created.name or name,
            "namespace": namespace,
            "bucket_name": bucket_name,
            "target_type": target_type,
            "relative_prefix": relative_prefix,
            "object_name": created.object_name or object_name,
            "access_type": created.access_type or access_type,
            "bucket_listing_action": created.bucket_listing_action or ("ListObjects" if allow_listing else ""),
            "created_at": created.time_created or datetime.now(timezone.utc),
            "expires_at": created.time_expires or expires_at,
            "par_url": par_url,
            "raw_par_url": raw_par_url,
        }
    )
    entries = load_par_entries()
    entries.append(new_entry)
    save_par_entries(entries)
    return _annotate_par_entry(new_entry)


def delete_par_record(config, entry_id):
    entry = get_par_entry_by_id(entry_id)
    if entry is None:
        raise ValueError("PAR entry was not found.")

    client = _get_object_storage_client(config)
    try:
        client.delete_preauthenticated_request(
            namespace_name=entry["namespace"],
            bucket_name=entry["bucket_name"],
            par_id=entry["par_id"],
        )
    except Exception as error:  # pragma: no cover - depends on OCI environment
        raise ValueError(f"Unable to revoke PAR in OCI: {error}") from error

    remaining_entries = [row for row in load_par_entries() if row["id"] != entry["id"]]
    save_par_entries(remaining_entries)
    return entry


def create_managed_folder(config, relative_prefix):
    folder_prefix = normalize_relative_prefix(relative_prefix)
    if not folder_prefix:
        raise ValueError("Folder name is required.")

    updated_config = normalize_object_storage(config)
    managed_folders = set(updated_config["managed_folders"])
    if folder_prefix in managed_folders:
        raise ValueError("That folder is already registered.")

    managed_folders.add(folder_prefix)
    updated_config["managed_folders"] = sorted(managed_folders)
    save_object_storage_config(updated_config)
    return updated_config


def rename_folder(config, source_prefix, target_prefix):
    source_relative = normalize_relative_prefix(source_prefix)
    target_relative = normalize_relative_prefix(target_prefix)
    if not source_relative or not target_relative:
        raise ValueError("Both the source folder and the target folder are required.")
    if source_relative == target_relative:
        raise ValueError("Choose a different target folder name.")
    if source_relative.startswith(target_relative) or target_relative.startswith(source_relative):
        raise ValueError("Folder rename must stay outside the source folder hierarchy.")

    updated_config = normalize_object_storage(config)
    client = _get_object_storage_client(updated_config)
    namespace = updated_config["namespace"]
    bucket_name = updated_config["bucket_name"]

    source_object_prefix = join_relative_prefixes(updated_config.get("bucket_prefix", ""), source_relative)
    target_object_prefix = join_relative_prefixes(updated_config.get("bucket_prefix", ""), target_relative)

    if _prefix_has_any_content(client, namespace, bucket_name, target_object_prefix):
        raise ValueError("The target folder already exists in Object Storage.")

    renamed_count = 0
    source_objects = _list_all_objects(client, namespace, bucket_name, source_object_prefix)
    for item in source_objects:
        new_name = target_object_prefix + item.name[len(source_object_prefix):]
        details = oci.object_storage.models.RenameObjectDetails(
            source_name=item.name,
            new_name=new_name,
            new_obj_if_none_match_e_tag="*",
        )
        client.rename_object(namespace_name=namespace, bucket_name=bucket_name, rename_object_details=details)
        renamed_count += 1

    managed_folders = []
    for item in updated_config["managed_folders"]:
        if item == source_relative or item.startswith(source_relative):
            managed_folders.append(target_relative + item[len(source_relative):])
        else:
            managed_folders.append(item)
    updated_config["managed_folders"] = sorted({normalize_relative_prefix(item) for item in managed_folders if item})
    save_object_storage_config(updated_config)
    return updated_config, renamed_count


def delete_folder(config, source_prefix):
    source_relative = normalize_relative_prefix(source_prefix)
    if not source_relative:
        raise ValueError("Choose a folder to delete.")

    updated_config = normalize_object_storage(config)
    client = _get_object_storage_client(updated_config)
    namespace = updated_config["namespace"]
    bucket_name = updated_config["bucket_name"]
    object_prefix = join_relative_prefixes(updated_config.get("bucket_prefix", ""), source_relative)

    deleted_count = 0
    for item in _list_all_objects(client, namespace, bucket_name, object_prefix):
        client.delete_object(namespace_name=namespace, bucket_name=bucket_name, object_name=item.name)
        deleted_count += 1

    updated_config["managed_folders"] = [
        item for item in updated_config["managed_folders"] if not (item == source_relative or item.startswith(source_relative))
    ]
    save_object_storage_config(updated_config)
    return updated_config, deleted_count


def get_folder_browser_state(config, current_prefix=""):
    updated_config = normalize_object_storage(config)
    current_relative = normalize_relative_prefix(current_prefix)
    client = _get_object_storage_client(updated_config)
    namespace = updated_config["namespace"]
    bucket_name = updated_config["bucket_name"]
    bucket_root_prefix = updated_config.get("bucket_prefix", "")
    current_object_prefix = join_relative_prefixes(bucket_root_prefix, current_relative)

    response = client.list_objects(
        namespace_name=namespace,
        bucket_name=bucket_name,
        prefix=current_object_prefix or None,
        delimiter="/",
        fields="name,size,timeCreated,timeModified",
    )

    actual_children = set()
    for prefix in response.data.prefixes or []:
        if current_object_prefix:
            suffix = prefix[len(current_object_prefix):]
        else:
            suffix = prefix
        child_name = suffix.split("/", 1)[0].strip()
        if child_name:
            actual_children.add(join_relative_prefixes(current_relative, child_name))

    managed_children = set()
    for prefix in updated_config["managed_folders"]:
        if current_relative and not prefix.startswith(current_relative):
            continue
        suffix = prefix[len(current_relative):] if current_relative else prefix
        child_name = suffix.split("/", 1)[0].strip()
        if child_name:
            managed_children.add(join_relative_prefixes(current_relative, child_name))

    folder_rows = []
    for prefix in sorted(actual_children | managed_children):
        folder_rows.append(
            {
                "name": prefix[len(current_relative):].strip("/"),
                "relative_prefix": prefix,
                "full_prefix": join_relative_prefixes(bucket_root_prefix, prefix),
                "from_object_storage": prefix in actual_children,
                "from_registry": prefix in updated_config["managed_folders"] or any(
                    item.startswith(prefix) for item in updated_config["managed_folders"]
                ),
            }
        )

    object_rows = []
    for item in response.data.objects or []:
        relative_name = item.name[len(current_object_prefix):] if current_object_prefix else item.name
        if not relative_name or "/" in relative_name.strip("/"):
            continue
        if relative_name.endswith("/") and not item.size:
            continue
        object_rows.append(
            {
                "name": relative_name,
                "size": int(item.size or 0),
                "time_created": _serialize_time(item.time_created),
            }
        )

    breadcrumbs = [{"label": "Root", "relative_prefix": ""}]
    breadcrumb_prefix = ""
    if current_relative:
        for segment in current_relative.strip("/").split("/"):
            breadcrumb_prefix = join_relative_prefixes(breadcrumb_prefix, segment)
            breadcrumbs.append({"label": segment, "relative_prefix": breadcrumb_prefix})

    return {
        "current_prefix": current_relative,
        "current_full_prefix": current_object_prefix or "/",
        "parent_prefix": parent_relative_prefix(current_relative),
        "folders": folder_rows,
        "objects": object_rows,
        "breadcrumbs": breadcrumbs,
        "managed_folder_count": len(updated_config["managed_folders"]),
    }
