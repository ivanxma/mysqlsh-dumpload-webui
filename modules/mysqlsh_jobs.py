import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from .config import JOBS_DIR, ROOT_DIR
from .mysqlsh_runner import (
    build_mysqlsh_command,
    build_mysqlsh_execution_request,
    ensure_runtime_dirs,
    evaluate_mysqlsh_execution,
    get_mysqlsh_status,
    resolve_progress_file_path,
)

JOB_ACTIVE_STATUSES = {"submitted", "starting", "running", "cancel_requested"}
JOB_CANCELABLE_STATUSES = {"submitted", "starting", "running"}
JOB_FINAL_STATUSES = {"succeeded", "failed", "canceled"}
JOB_WORKER_MODULE = "modules.mysqlsh_job_worker"
JOB_TAIL_CHARS = 48000
PERCENT_PATTERN = re.compile(r"(?<!\d)(100(?:\.0+)?|\d{1,2}(?:\.\d+)?)%")


def ensure_job_store():
    ensure_runtime_dirs()
    JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _utc_now():
    return datetime.now(timezone.utc)


def _iso_now():
    return _utc_now().isoformat()


def _format_timestamp(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    try:
        return datetime.fromisoformat(raw_value).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        return raw_value


def _format_duration(seconds):
    if seconds is None:
        return ""
    total_seconds = max(0, int(round(float(seconds))))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    if minutes:
        return f"{minutes:d}:{secs:02d}"
    return f"{secs} seconds"


def _status_badge_class(status):
    return {
        "submitted": "muted",
        "starting": "warn",
        "running": "warn",
        "cancel_requested": "warn",
        "canceled": "muted",
        "failed": "warn",
        "succeeded": "good",
    }.get(str(status or "").strip(), "muted")


def _calculate_duration_seconds(started_at, finished_at):
    raw_started = str(started_at or "").strip()
    if not raw_started:
        return None
    try:
        started = datetime.fromisoformat(raw_started)
        ended = datetime.fromisoformat(str(finished_at).strip()) if str(finished_at or "").strip() else _utc_now()
        return round((ended - started).total_seconds(), 2)
    except ValueError:
        return None


def _summary_lookup(metadata):
    lookup = {}
    for row in metadata.get("summary_rows", []) or []:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            continue
        label = str(row[0] or "").strip()
        value = str(row[1] or "").strip()
        if label and label not in lookup:
            lookup[label] = value
    return lookup


def _build_history_summary_text(metadata):
    lookup = _summary_lookup(metadata)
    parts = []
    schemas = lookup.get("Schemas")
    if schemas:
        parts.append(f"Schemas: {schemas}")

    par_name = lookup.get("Source PAR") or lookup.get("Target PAR")
    if par_name:
        parts.append(f"PAR: {par_name}")

    threads = lookup.get("Threads")
    if threads:
        parts.append(f"Threads: {threads}")

    reset_progress = lookup.get("Reset Progress")
    if reset_progress:
        parts.append(f"Reset: {reset_progress}")

    progress_file = lookup.get("Progress File")
    if not parts and progress_file:
        parts.append(f"Progress: {progress_file}")

    return " | ".join(part for part in parts if part)


def _job_dir(job_id):
    return JOBS_DIR / str(job_id or "").strip()


def _job_metadata_path(job_id):
    return _job_dir(job_id) / "job.json"


def _atomic_write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(temp_path, path)


def load_mysqlsh_job_metadata(job_id=None, metadata_path=None):
    resolved_path = Path(metadata_path) if metadata_path is not None else _job_metadata_path(job_id)
    if not resolved_path.exists():
        return None
    with open(resolved_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_metadata_path"] = str(resolved_path)
    return payload


def save_mysqlsh_job_metadata(metadata, *, metadata_path=None):
    resolved_path = Path(metadata_path) if metadata_path is not None else Path(metadata["_metadata_path"])
    payload = dict(metadata)
    payload.pop("_metadata_path", None)
    _atomic_write_json(resolved_path, payload)
    payload["_metadata_path"] = str(resolved_path)
    return payload


def update_mysqlsh_job_metadata(job_id=None, metadata_path=None, **updates):
    metadata = load_mysqlsh_job_metadata(job_id=job_id, metadata_path=metadata_path)
    if metadata is None:
        return None
    metadata.update(updates)
    return save_mysqlsh_job_metadata(metadata, metadata_path=metadata.get("_metadata_path"))


def _job_owner_matches(metadata, owner_username, owner_profile_name):
    expected_username = str(metadata.get("owner_username", "")).strip()
    expected_profile = str(metadata.get("owner_profile_name", "")).strip()
    if owner_username is not None and expected_username and expected_username != str(owner_username or "").strip():
        return False
    if owner_profile_name is not None and expected_profile and expected_profile != str(owner_profile_name or "").strip():
        return False
    return True


def _extract_progress_file(executable_payload):
    function_name = str(executable_payload.get("function_name", "")).strip()
    args = executable_payload.get("args", [])
    options = None
    if function_name == "dump_instance" and len(args) >= 2:
        options = args[1]
    elif function_name == "dump_schemas" and len(args) >= 3:
        options = args[2]
    elif function_name == "load_dump" and len(args) >= 2:
        options = args[1]
    if isinstance(options, dict):
        return str(options.get("progressFile", "")).strip()
    return ""


def remove_mysqlsh_job_request_file(metadata):
    request_path = str(metadata.get("request_path", "")).strip()
    if not request_path:
        return
    try:
        Path(request_path).unlink()
    except OSError:
        pass


def _reap_process(process):
    try:
        process.wait()
    except Exception:
        pass


def submit_mysqlsh_job(
    profile,
    credentials,
    request_payload,
    *,
    database="",
    operation="",
    operation_name="mysqlsh",
    summary_rows=None,
    options_json="",
    form_state=None,
    selected_schemas=None,
    owner_username="",
    owner_profile_name="",
):
    ensure_job_store()
    mysqlsh_status = get_mysqlsh_status()
    if not mysqlsh_status["available"]:
        raise RuntimeError(mysqlsh_status["error"] or "mysqlsh is not available.")

    executable_payload = build_mysqlsh_execution_request(
        profile,
        credentials,
        request_payload,
        database=database,
    )
    job_id = uuid.uuid4().hex
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)

    request_path = job_dir / "request.json"
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"
    worker_log_path = job_dir / "worker.log"
    metadata_path = _job_metadata_path(job_id)

    _atomic_write_json(request_path, executable_payload)
    os.chmod(request_path, 0o600)
    command = build_mysqlsh_command(
        mysqlsh_status["binary"],
        profile,
        credentials,
        str(request_path),
        database=database,
    )

    metadata = {
        "job_id": job_id,
        "status": "submitted",
        "operation": str(operation or "").strip(),
        "operation_name": operation_name,
        "owner_username": str(owner_username or "").strip(),
        "owner_profile_name": str(owner_profile_name or "").strip(),
        "submitted_at": _iso_now(),
        "started_at": "",
        "finished_at": "",
        "returncode": None,
        "succeeded": False,
        "error": "",
        "error_type": "",
        "database": str(database or "").strip(),
        "mysqlsh_binary": mysqlsh_status["binary"],
        "command": command,
        "command_preview": shlex.join(command),
        "script_text": str(request_payload.get("display_text", "")),
        "options_json": str(options_json or ""),
        "summary_rows": list(summary_rows or []),
        "request_path": str(request_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "worker_log_path": str(worker_log_path),
        "progress_file": _extract_progress_file(executable_payload),
        "worker_pid": None,
        "process_group_id": None,
        "mysqlsh_pid": None,
        "retry_count": 0,
        "form_state": dict(form_state or {}),
        "selected_schemas": list(selected_schemas or []),
    }
    metadata = save_mysqlsh_job_metadata(metadata, metadata_path=metadata_path)

    worker_command = [sys.executable, "-m", JOB_WORKER_MODULE, str(metadata_path)]
    worker_stderr_handle = open(worker_log_path, "a", encoding="utf-8")
    try:
        worker = subprocess.Popen(
            worker_command,
            cwd=str(ROOT_DIR),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=worker_stderr_handle,
            text=True,
            start_new_session=True,
        )
        threading.Thread(target=_reap_process, args=(worker,), daemon=True).start()
    except Exception:
        worker_stderr_handle.close()
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    finally:
        if not worker_stderr_handle.closed:
            worker_stderr_handle.close()

    metadata = update_mysqlsh_job_metadata(
        metadata_path=metadata_path,
        status="starting",
        worker_pid=worker.pid,
        process_group_id=worker.pid,
    )
    return build_mysqlsh_job_snapshot(
        metadata["job_id"],
        owner_username=owner_username,
        owner_profile_name=owner_profile_name,
    )


def _read_text_tail(path, max_chars=JOB_TAIL_CHARS):
    resolved_path = Path(path)
    if not resolved_path.exists() or not resolved_path.is_file():
        return ""
    size = resolved_path.stat().st_size
    with open(resolved_path, "rb") as handle:
        if size > max_chars:
            handle.seek(-max_chars, os.SEEK_END)
            data = handle.read().decode("utf-8", errors="replace")
            return f"[showing last {max_chars} characters]\n{data}"
        return handle.read().decode("utf-8", errors="replace")


def _read_text(path):
    resolved_path = Path(path)
    if not resolved_path.exists() or not resolved_path.is_file():
        return ""
    with open(resolved_path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _as_float(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _normalize_percent(value):
    numeric = _as_float(value)
    if numeric is None:
        return None
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    if 0.0 <= numeric <= 100.0:
        return round(numeric, 1)
    return None


def _extract_progress_from_json(payload):
    percent = None
    info_lines = []

    def walk(node, path=()):
        nonlocal percent
        if isinstance(node, dict):
            lowered = {str(key).lower(): value for key, value in node.items()}
            for key, value in node.items():
                lower_key = str(key).lower()
                if percent is None and any(token in lower_key for token in ("percent", "percentage", "pct")):
                    percent = _normalize_percent(value)
                if isinstance(value, str) and any(token in lower_key for token in ("status", "state", "phase", "stage", "message", "detail")):
                    line = f"{'.'.join(path + (str(key),))}: {value}"
                    if line not in info_lines:
                        info_lines.append(line)
            ratio_pairs = (
                ("current", "total"),
                ("completed", "total"),
                ("done", "total"),
                ("processed", "total"),
                ("step", "steps"),
            )
            if percent is None:
                for current_key, total_key in ratio_pairs:
                    current_value = _as_float(lowered.get(current_key))
                    total_value = _as_float(lowered.get(total_key))
                    if current_value is not None and total_value and total_value > 0:
                        percent = round((current_value / total_value) * 100.0, 1)
                        break
            for key, value in node.items():
                walk(value, path + (str(key),))
        elif isinstance(node, list):
            for index, item in enumerate(node):
                walk(item, path + (str(index),))

    walk(payload)
    return percent, info_lines[:6]


def _extract_percent_from_text(*texts):
    for text in reversed(texts):
        matches = PERCENT_PATTERN.findall(text or "")
        if matches:
            return round(float(matches[-1]), 1)
    return None


def _build_progress_snapshot(metadata, stdout_text, stderr_text):
    percent = None
    info_lines = []
    progress_file = str(metadata.get("progress_file", "")).strip()
    if progress_file:
        progress_path = resolve_progress_file_path(progress_file)
        if progress_path.exists() and progress_path.is_file():
            try:
                with open(progress_path, "r", encoding="utf-8") as handle:
                    progress_payload = json.load(handle)
                percent, info_lines = _extract_progress_from_json(progress_payload)
            except (OSError, json.JSONDecodeError):
                pass

    if percent is None:
        percent = _extract_percent_from_text(stdout_text, stderr_text)

    if not info_lines:
        error_text = str(metadata.get("error", "")).strip()
        if error_text:
            info_lines.append(error_text)

    status = str(metadata.get("status", "submitted")).strip()
    if status == "succeeded":
        percent = 100.0

    if percent is None:
        label = {
            "submitted": "Queued for execution",
            "starting": "Starting mysqlsh",
            "running": "Running",
            "cancel_requested": "Cancel requested",
            "canceled": "Canceled",
            "failed": "Failed",
            "succeeded": "Completed",
        }.get(status, status.replace("_", " ").title())
    elif status == "succeeded":
        label = "Completed"
    else:
        label = f"{percent:.1f}%"

    return {
        "percent": percent,
        "label": label,
        "info": "\n".join(info_lines[:6]).strip(),
        "indeterminate": percent is None and status in JOB_ACTIVE_STATUSES,
    }


def _reconcile_result_payload(metadata, stdout_text, stderr_text):
    status = str(metadata.get("status", "")).strip()
    if status == "canceled" or status in JOB_ACTIVE_STATUSES:
        return metadata

    execution_state = evaluate_mysqlsh_execution(metadata.get("returncode"), stdout_text, stderr_text)
    if execution_state["succeeded"]:
        return metadata

    if (
        str(metadata.get("status", "")).strip() == "failed"
        and str(metadata.get("error", "")).strip() == str(execution_state["error"] or "").strip()
        and str(metadata.get("error_type", "")).strip() == str(execution_state["error_type"] or "").strip()
    ):
        return metadata

    return save_mysqlsh_job_metadata(
        {
            **metadata,
            "status": "failed",
            "succeeded": False,
            "error": execution_state["error"],
            "error_type": execution_state["error_type"],
            "finished_at": metadata.get("finished_at") or _iso_now(),
        },
        metadata_path=metadata.get("_metadata_path"),
    )


def _pid_exists(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, ValueError, TypeError):
        return False
    except PermissionError:
        return True
    return True


def _pid_matches(pid, required_fragment=""):
    if not _pid_exists(pid):
        return False
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(int(pid))],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return True
    command = (result.stdout or "").strip()
    if result.returncode != 0 or not command:
        return True
    if not required_fragment:
        return True
    return required_fragment in command


def _worker_process_is_active(metadata):
    worker_pid = metadata.get("worker_pid")
    if not worker_pid:
        return False
    return _pid_matches(worker_pid, JOB_WORKER_MODULE)


def _mysqlsh_process_is_active(metadata):
    mysqlsh_pid = metadata.get("mysqlsh_pid")
    if not mysqlsh_pid:
        return False
    binary_name = Path(str(metadata.get("mysqlsh_binary", "mysqlsh"))).name or "mysqlsh"
    return _pid_matches(mysqlsh_pid, binary_name)


def _job_processes_are_active(metadata):
    return _worker_process_is_active(metadata) or _mysqlsh_process_is_active(metadata)


def _reconcile_job_state(metadata):
    status = str(metadata.get("status", "")).strip()
    if status in JOB_FINAL_STATUSES:
        return metadata
    if _job_processes_are_active(metadata):
        return metadata

    finished_at = str(metadata.get("finished_at", "")).strip()
    if status == "cancel_requested":
        finished_at = finished_at or _iso_now()
        return save_mysqlsh_job_metadata(
            {
                **metadata,
                "status": "canceled",
                "finished_at": finished_at,
                "error": metadata.get("error") or "Canceled by user.",
            },
            metadata_path=metadata.get("_metadata_path"),
        )

    if not finished_at:
        return save_mysqlsh_job_metadata(
            {
                **metadata,
                "status": "failed",
                "finished_at": _iso_now(),
                "error": metadata.get("error") or "Job worker exited unexpectedly.",
                "error_type": metadata.get("error_type") or "WorkerExitError",
            },
            metadata_path=metadata.get("_metadata_path"),
        )
    return metadata


def build_mysqlsh_job_snapshot(job_id, *, owner_username="", owner_profile_name=""):
    metadata = load_mysqlsh_job_metadata(job_id=job_id)
    if metadata is None or not _job_owner_matches(metadata, owner_username, owner_profile_name):
        return None

    metadata = _reconcile_job_state(metadata)
    stdout_text = _read_text_tail(metadata.get("stdout_path", ""))
    stderr_text = _read_text_tail(metadata.get("stderr_path", ""))
    metadata = _reconcile_result_payload(metadata, stdout_text, stderr_text)
    progress = _build_progress_snapshot(metadata, stdout_text, stderr_text)

    status = str(metadata.get("status", "submitted")).strip()
    started_at = str(metadata.get("started_at", "")).strip()
    finished_at = str(metadata.get("finished_at", "")).strip()
    submitted_at = str(metadata.get("submitted_at", "")).strip()

    duration_seconds = _calculate_duration_seconds(started_at, finished_at)

    summary_rows = [
        ("Job ID", metadata["job_id"]),
        ("Status", status.replace("_", " ").title()),
        ("Submitted", _format_timestamp(submitted_at)),
    ]
    if started_at:
        summary_rows.append(("Started", _format_timestamp(started_at)))
    if finished_at:
        summary_rows.append(("Finished", _format_timestamp(finished_at)))
    summary_rows.extend(list(metadata.get("summary_rows", [])))
    if metadata.get("retry_count"):
        summary_rows.append(("Retry Count", str(metadata.get("retry_count"))))

    badge_class = _status_badge_class(status)

    return {
        "job_id": metadata["job_id"],
        "operation": metadata.get("operation", ""),
        "operation_name": metadata.get("operation_name", ""),
        "status": status,
        "status_label": status.replace("_", " ").title(),
        "status_badge_class": badge_class,
        "is_active": status in JOB_ACTIVE_STATUSES,
        "can_cancel": status in JOB_CANCELABLE_STATUSES,
        "can_cleanup": status in JOB_FINAL_STATUSES,
        "succeeded": bool(metadata.get("succeeded")),
        "returncode": metadata.get("returncode"),
        "duration_seconds": duration_seconds,
        "duration_text": _format_duration(duration_seconds),
        "summary_rows": summary_rows,
        "options_json": metadata.get("options_json", ""),
        "stdout": stdout_text,
        "stderr": stderr_text,
        "script_text": metadata.get("script_text", ""),
        "command_preview": metadata.get("command_preview", ""),
        "progress_percent": progress["percent"],
        "progress_label": progress["label"],
        "progress_info": progress["info"],
        "progress_indeterminate": progress["indeterminate"],
        "error": metadata.get("error", ""),
        "form_state": metadata.get("form_state", {}),
        "selected_schemas": metadata.get("selected_schemas", []),
    }


def list_mysqlsh_job_history(*, owner_username="", owner_profile_name="", operation="", limit=25):
    ensure_job_store()
    resolved_operation = str(operation or "").strip()
    items = []

    for metadata_path in JOBS_DIR.glob("*/job.json"):
        metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path)
        if metadata is None or not _job_owner_matches(metadata, owner_username, owner_profile_name):
            continue
        if resolved_operation and str(metadata.get("operation", "")).strip() != resolved_operation:
            continue

        metadata = _reconcile_job_state(metadata)
        status = str(metadata.get("status", "submitted")).strip()
        started_at = str(metadata.get("started_at", "")).strip()
        finished_at = str(metadata.get("finished_at", "")).strip()
        submitted_at = str(metadata.get("submitted_at", "")).strip()
        duration_seconds = _calculate_duration_seconds(started_at, finished_at)

        items.append(
            {
                "job_id": metadata["job_id"],
                "operation": str(metadata.get("operation", "")).strip(),
                "operation_name": str(metadata.get("operation_name", "")).strip(),
                "owner_profile_name": str(metadata.get("owner_profile_name", "")).strip(),
                "database": str(metadata.get("database", "")).strip(),
                "status": status,
                "status_label": status.replace("_", " ").title(),
                "status_badge_class": _status_badge_class(status),
                "submitted_at": submitted_at,
                "submitted_at_local": _format_timestamp(submitted_at),
                "finished_at": finished_at,
                "finished_at_local": _format_timestamp(finished_at),
                "duration_seconds": duration_seconds,
                "duration_text": _format_duration(duration_seconds),
                "retry_count": int(metadata.get("retry_count") or 0),
                "summary_text": _build_history_summary_text(metadata),
                "error": str(metadata.get("error", "")).strip(),
                "can_cleanup": status in JOB_FINAL_STATUSES,
            }
        )

    items.sort(key=lambda item: (item.get("submitted_at") or "", item.get("job_id") or ""), reverse=True)
    return items[: max(1, int(limit or 25))]


def cancel_mysqlsh_job(job_id, *, owner_username="", owner_profile_name=""):
    metadata = load_mysqlsh_job_metadata(job_id=job_id)
    if metadata is None or not _job_owner_matches(metadata, owner_username, owner_profile_name):
        return None

    status = str(metadata.get("status", "")).strip()
    if status in JOB_FINAL_STATUSES:
        return build_mysqlsh_job_snapshot(job_id, owner_username=owner_username, owner_profile_name=owner_profile_name)

    metadata = save_mysqlsh_job_metadata(
        {
            **metadata,
            "status": "cancel_requested",
            "error": "Cancel requested by user.",
        },
        metadata_path=metadata.get("_metadata_path"),
    )

    metadata_path = metadata.get("_metadata_path")
    mysqlsh_pid = metadata.get("mysqlsh_pid")
    process_group_id = metadata.get("process_group_id") or metadata.get("worker_pid")

    sent_signal = False
    if mysqlsh_pid and _mysqlsh_process_is_active(metadata):
        try:
            os.kill(int(mysqlsh_pid), signal.SIGTERM)
            sent_signal = True
        except (ProcessLookupError, PermissionError):
            pass
    if not sent_signal and process_group_id:
        try:
            os.killpg(int(process_group_id), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

    deadline = time.time() + 5.0
    latest_metadata = metadata
    while time.time() < deadline:
        latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
        if not _job_processes_are_active(latest_metadata):
            break
        time.sleep(0.2)

    latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
    if _mysqlsh_process_is_active(latest_metadata):
        latest_mysqlsh_pid = latest_metadata.get("mysqlsh_pid") or mysqlsh_pid
        try:
            os.kill(int(latest_mysqlsh_pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

        kill_deadline = time.time() + 2.0
        while time.time() < kill_deadline:
            latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
            if not _job_processes_are_active(latest_metadata):
                break
            time.sleep(0.1)

    latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
    if _worker_process_is_active(latest_metadata) and not _mysqlsh_process_is_active(latest_metadata):
        settle_deadline = time.time() + 2.0
        while time.time() < settle_deadline:
            latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
            if not _worker_process_is_active(latest_metadata):
                break
            time.sleep(0.1)

    latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
    if process_group_id and _job_processes_are_active(latest_metadata):
        try:
            os.killpg(int(process_group_id), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

        kill_deadline = time.time() + 2.0
        while time.time() < kill_deadline:
            latest_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
            if not _job_processes_are_active(latest_metadata):
                break
            time.sleep(0.1)

    final_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or latest_metadata
    if _job_processes_are_active(final_metadata):
        return build_mysqlsh_job_snapshot(
            final_metadata["job_id"],
            owner_username=owner_username,
            owner_profile_name=owner_profile_name,
        )

    final_status = str(final_metadata.get("status", "")).strip()
    if final_status not in JOB_FINAL_STATUSES:
        final_metadata = update_mysqlsh_job_metadata(
            metadata_path=metadata_path,
            status="canceled",
            finished_at=final_metadata.get("finished_at") or _iso_now(),
            succeeded=False,
            error="Canceled by user.",
        )
    return build_mysqlsh_job_snapshot(
        final_metadata["job_id"],
        owner_username=owner_username,
        owner_profile_name=owner_profile_name,
    )


def cleanup_mysqlsh_job(job_id, *, owner_username="", owner_profile_name=""):
    metadata = load_mysqlsh_job_metadata(job_id=job_id)
    if metadata is None or not _job_owner_matches(metadata, owner_username, owner_profile_name):
        return None
    if str(metadata.get("status", "")).strip() in JOB_ACTIVE_STATUSES:
        raise ValueError("Cancel the running job before cleaning up its files.")

    shutil.rmtree(_job_dir(job_id), ignore_errors=True)
    return metadata
