import json
import os
import sys
import tempfile
import traceback
from copy import deepcopy
from pathlib import Path
from urllib.parse import unquote, urlsplit
from datetime import datetime, timezone

from modules.config import ROOT_DIR
from modules.mysql_connection import mysql_endpoint
from modules.mysqlsh_jobs import load_mysqlsh_job_metadata, remove_mysqlsh_job_request_file, save_mysqlsh_job_metadata
from modules.mysqlsh_runner import evaluate_mysqlsh_execution, mysqlsh_env

import subprocess


def _iso_now():
    return datetime.now(timezone.utc).isoformat()


def _merge_job_metadata(metadata_path, **updates):
    current_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path)
    if current_metadata is None:
        raise ValueError(f"Job metadata was not found: {metadata_path}")

    current_status = str(current_metadata.get("status", "")).strip()
    next_status = str(updates.get("status", "")).strip()
    if current_status in {"cancel_requested", "canceled"} and next_status in {"starting", "running"}:
        updates["status"] = current_status

    return save_mysqlsh_job_metadata(
        {
            **current_metadata,
            **updates,
        },
        metadata_path=metadata_path,
    )


def _append_log_line(path, message):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(message.rstrip())
        handle.write("\n")


def _load_request_payload(metadata):
    with open(metadata["request_path"], "r", encoding="utf-8") as handle:
        return json.load(handle)


def _parse_ssh_target(ssh_target):
    normalized_target = str(ssh_target or "").strip()
    if not normalized_target:
        return None
    if "@" not in normalized_target:
        raise ValueError(f"SSH target is missing user information: {normalized_target}")

    ssh_user, host_port = normalized_target.rsplit("@", 1)
    ssh_host = host_port
    ssh_port = 22
    if ":" in host_port:
        ssh_host, raw_port = host_port.rsplit(":", 1)
        ssh_port = int(raw_port)
    return {
        "ssh_user": ssh_user,
        "ssh_host": ssh_host,
        "ssh_port": ssh_port,
    }


def _build_tunnel_profile(request_payload):
    connection_options = (request_payload or {}).get("connection_options") or {}
    ssh_details = _parse_ssh_target(connection_options.get("ssh"))
    if ssh_details is None:
        return None

    if str(connection_options.get("uri", "")).strip():
        parsed_uri = urlsplit(str(connection_options["uri"]).strip())
        db_host = parsed_uri.hostname or ""
        db_port = int(parsed_uri.port or 3306)
    else:
        db_host = str(connection_options.get("host", "")).strip()
        db_port = int(connection_options.get("port") or 3306)

    return {
        "name": "",
        "host": db_host,
        "port": db_port,
        "database": "",
        "ssh_enabled": True,
        "ssh_host": ssh_details["ssh_host"],
        "ssh_port": ssh_details["ssh_port"],
        "ssh_user": ssh_details["ssh_user"],
        "ssh_key_path": str(connection_options.get("ssh-identity-file", "")).strip(),
        "ssh_config_file": str(connection_options.get("ssh-config-file", "")).strip(),
    }


def _rewrite_connection_options_for_local_endpoint(connection_options, host, port):
    rewritten_options = deepcopy(connection_options or {})
    rewritten_options.pop("ssh", None)
    rewritten_options.pop("ssh-identity-file", None)
    rewritten_options.pop("ssh-config-file", None)

    if str(rewritten_options.get("uri", "")).strip():
        parsed_uri = urlsplit(str(rewritten_options["uri"]).strip())
        rewritten_options.pop("uri", None)
        rewritten_options["scheme"] = parsed_uri.scheme or "mysql"
        rewritten_options["host"] = str(host)
        rewritten_options["port"] = int(port)
        if parsed_uri.username is not None:
            rewritten_options["user"] = unquote(parsed_uri.username)
        if parsed_uri.password is not None:
            rewritten_options["password"] = unquote(parsed_uri.password)
        schema_name = str(parsed_uri.path or "").lstrip("/")
        if schema_name:
            rewritten_options["schema"] = unquote(schema_name)
        return rewritten_options

    rewritten_options["host"] = str(host)
    rewritten_options["port"] = int(port)
    return rewritten_options


def _build_runtime_request_payload(request_payload, endpoint):
    runtime_payload = deepcopy(request_payload)
    runtime_payload["connection_options"] = _rewrite_connection_options_for_local_endpoint(
        runtime_payload.get("connection_options") or {},
        endpoint["host"],
        endpoint["port"],
    )
    return runtime_payload


class _MysqlshRuntimeCommand:
    def __init__(self, metadata, request_payload):
        self.metadata = metadata
        self.request_payload = request_payload
        self.runtime_request_path = None
        self.tunnel_context = None
        self.tunnel_endpoint = None

    def __enter__(self):
        tunnel_profile = _build_tunnel_profile(self.request_payload)
        if tunnel_profile is None:
            return list(self.metadata["command"])

        self.tunnel_context = mysql_endpoint(tunnel_profile)
        self.tunnel_endpoint = self.tunnel_context.__enter__()
        runtime_payload = _build_runtime_request_payload(self.request_payload, self.tunnel_endpoint)
        request_dir = Path(self.metadata["request_path"]).resolve().parent
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            prefix="mysql-shell-runtime-",
            dir=str(request_dir),
            delete=False,
        ) as handle:
            json.dump(runtime_payload, handle, indent=2, sort_keys=True)
            self.runtime_request_path = handle.name
        os.chmod(self.runtime_request_path, 0o600)
        _append_log_line(
            self.metadata["stderr_path"],
            (
                "NOTE: Opening app-managed SSH tunnel to "
                f"{tunnel_profile['ssh_host']}:{tunnel_profile['ssh_port']} for mysqlsh execution."
            ),
        )

        runtime_command = list(self.metadata["command"])
        runtime_command[-1] = self.runtime_request_path
        return runtime_command

    def __exit__(self, exc_type, exc, tb):
        if self.runtime_request_path:
            try:
                Path(self.runtime_request_path).unlink()
            except OSError:
                pass
        if self.tunnel_context is not None:
            self.tunnel_context.__exit__(exc_type, exc, tb)
        return False


def _save_request_payload(metadata, payload):
    with open(metadata["request_path"], "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.chmod(metadata["request_path"], 0o600)


def _read_text(path):
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _get_load_dump_options(request_payload):
    args = request_payload.get("args", [])
    if str(request_payload.get("function_name", "")).strip() != "load_dump":
        return None
    if len(args) < 2 or not isinstance(args[1], dict):
        return None
    return args[1]


def _should_retry_load_dump(metadata, request_payload, execution_state, stderr_text):
    if execution_state["succeeded"]:
        return None

    connection_options = request_payload.get("connection_options") or {}
    if "ssh" not in connection_options:
        return None

    options = _get_load_dump_options(request_payload)
    if options is None:
        return None
    if not str(options.get("progressFile", "")).strip():
        return None

    error_text = "\n".join(
        part for part in [execution_state.get("error", ""), stderr_text] if str(part).strip()
    )
    if "MySQL Error 2013" not in error_text or "Lost connection to MySQL server during query" not in error_text:
        return None

    try:
        current_threads = int(options.get("threads") or 0)
    except (TypeError, ValueError):
        current_threads = 0
    retry_count = int(metadata.get("retry_count") or 0)
    last_retry_mode = str(metadata.get("last_retry_mode", "")).strip()

    if current_threads > 1 and retry_count == 0:
        return {"mode": "reduce_threads", "threads": current_threads}
    if retry_count == 0:
        return {"mode": "resume_same_options", "threads": max(current_threads, 1)}
    if retry_count == 1 and last_retry_mode == "reduce_threads":
        return {"mode": "resume_same_options", "threads": max(current_threads, 1)}

    return None


def _prepare_load_dump_retry(metadata_path, metadata, request_payload, retry_plan):
    retry_payload = deepcopy(request_payload)
    options = _get_load_dump_options(retry_payload)
    retry_mode = str((retry_plan or {}).get("mode", "")).strip()
    current_threads = int((retry_plan or {}).get("threads") or 1)

    if retry_mode == "reduce_threads":
        options["threads"] = 1
        if "backgroundThreads" in options:
            options["backgroundThreads"] = 1
        _save_request_payload(metadata, retry_payload)
        retry_note = (
            "NOTE: SSH-backed loadDump lost the MySQL connection during parallel data load. "
            "Retrying once with threads=1 and the existing progress file."
        )
        retry_error = (
            f"Retrying loadDump with threads=1 after connection loss (previous threads={current_threads})."
        )
    else:
        retry_note = (
            "NOTE: SSH-backed loadDump lost the MySQL connection during data load. "
            "Retrying once with the existing progress file and the same load options."
        )
        retry_error = (
            "Retrying loadDump after SSH connection loss with the existing progress file "
            f"(threads={current_threads})."
        )

    _append_log_line(metadata["stderr_path"], retry_note)
    return _merge_job_metadata(
        metadata_path,
        status="running",
        mysqlsh_pid=None,
        retry_count=int(metadata.get("retry_count") or 0) + 1,
        last_retry_mode=retry_mode,
        error=retry_error,
        error_type="MySQLLoadDumpRetry",
    )


def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing job metadata path.")

    metadata_path = sys.argv[1]
    metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path)
    if metadata is None:
        raise ValueError(f"Job metadata was not found: {metadata_path}")

    metadata = _merge_job_metadata(
        metadata_path,
        status="starting",
        worker_pid=metadata.get("worker_pid") or None,
        process_group_id=metadata.get("process_group_id") or None,
    )

    execution_state = {"succeeded": True, "error": "", "error_type": ""}
    returncode = None
    while True:
        if str(metadata.get("status", "")).strip() in {"cancel_requested", "canceled"}:
            execution_state = {
                "succeeded": False,
                "error": metadata.get("error") or "Canceled by user.",
                "error_type": metadata.get("error_type", ""),
            }
            break

        request_payload = _load_request_payload(metadata)

        with open(metadata["stdout_path"], "a", encoding="utf-8") as stdout_handle, open(
            metadata["stderr_path"], "a", encoding="utf-8"
        ) as stderr_handle:
            with _MysqlshRuntimeCommand(metadata, request_payload) as runtime_command:
                mysqlsh_process = subprocess.Popen(
                    runtime_command,
                    cwd=str(ROOT_DIR),
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    text=True,
                    env=mysqlsh_env(),
                )
                metadata = _merge_job_metadata(
                    metadata_path,
                    status="running",
                    started_at=metadata.get("started_at") or _iso_now(),
                    mysqlsh_pid=mysqlsh_process.pid,
                )

                returncode = mysqlsh_process.wait()

        stdout_text = _read_text(metadata["stdout_path"])
        stderr_text = _read_text(metadata["stderr_path"])
        execution_state = evaluate_mysqlsh_execution(returncode, stdout_text, stderr_text)

        retry_plan = _should_retry_load_dump(metadata, request_payload, execution_state, stderr_text)
        if retry_plan:
            metadata = _prepare_load_dump_retry(metadata_path, metadata, request_payload, retry_plan)
            if str(metadata.get("status", "")).strip() in {"cancel_requested", "canceled"}:
                execution_state = {
                    "succeeded": False,
                    "error": metadata.get("error") or "Canceled by user.",
                    "error_type": metadata.get("error_type", ""),
                }
                break
            continue
        break

    finished_at = _iso_now()
    current_metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path) or metadata
    status = str(current_metadata.get("status", "")).strip()
    if status in {"cancel_requested", "canceled"}:
        final_status = "canceled"
        succeeded = False
        error = current_metadata.get("error") or "Canceled by user."
        error_type = current_metadata.get("error_type", "")
    else:
        final_status = "succeeded" if execution_state["succeeded"] else "failed"
        succeeded = execution_state["succeeded"]
        if succeeded:
            error = ""
            error_type = ""
        else:
            error = execution_state["error"] or current_metadata.get("error", "")
            error_type = execution_state["error_type"] or current_metadata.get("error_type", "")

    save_mysqlsh_job_metadata(
        {
            **current_metadata,
            "status": final_status,
            "finished_at": finished_at,
            "returncode": returncode,
            "succeeded": succeeded,
            "error": error,
            "error_type": error_type,
        },
        metadata_path=metadata_path,
    )
    remove_mysqlsh_job_request_file(current_metadata)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pragma: no cover - detached worker path
        metadata_path = sys.argv[1] if len(sys.argv) > 1 else None
        if metadata_path:
            metadata = load_mysqlsh_job_metadata(metadata_path=metadata_path)
            if metadata is not None:
                save_mysqlsh_job_metadata(
                    {
                        **metadata,
                        "status": "failed",
                        "finished_at": _iso_now(),
                        "succeeded": False,
                        "error": str(error),
                        "error_type": type(error).__name__,
                    },
                    metadata_path=metadata_path,
                )
                with open(metadata["stderr_path"], "a", encoding="utf-8") as stderr_handle:
                    stderr_handle.write(traceback.format_exc())
                    stderr_handle.write("\n")
                remove_mysqlsh_job_request_file(metadata)
        raise
