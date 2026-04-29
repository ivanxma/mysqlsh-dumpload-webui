import json
import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from .config import MYSQLSH_USER_CONFIG_HOME, PROGRESS_DIR, ROOT_DIR

PYTHON_RUNNER_MODULE = "modules.mysqlsh_python_runner"
MYSQLSH_RESULT_START = "MYSQL_SHELL_WEB_RESULT_START"
MYSQLSH_RESULT_END = "MYSQL_SHELL_WEB_RESULT_END"


def ensure_runtime_dirs():
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    MYSQLSH_USER_CONFIG_HOME.mkdir(parents=True, exist_ok=True)


def _mysqlsh_env():
    ensure_runtime_dirs()
    env = os.environ.copy()
    env["MYSQLSH_USER_CONFIG_HOME"] = str(MYSQLSH_USER_CONFIG_HOME)
    pythonpath_entries = [str(ROOT_DIR)]
    existing_pythonpath = str(env.get("PYTHONPATH", "")).strip()
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env.setdefault("TERM", "dumb")
    return env


def mysqlsh_env():
    return _mysqlsh_env()


def resolve_mysqlsh_binary():
    configured_binary = str(os.environ.get("MYSQLSH_BINARY", "mysqlsh")).strip() or "mysqlsh"
    resolved_binary = shutil.which(configured_binary)
    if resolved_binary:
        return resolved_binary
    return configured_binary


def get_mysqlsh_status():
    binary = resolve_mysqlsh_binary()
    resolved = shutil.which(binary) if os.path.basename(binary) == binary else binary
    if not resolved or not os.path.exists(resolved):
        return {
            "available": False,
            "binary": binary,
            "version": "",
            "error": "mysqlsh was not found in PATH.",
        }

    try:
        result = subprocess.run(
            [resolved, "--version"],
            capture_output=True,
            text=True,
            env=_mysqlsh_env(),
            cwd=str(ROOT_DIR),
            check=False,
        )
    except Exception as error:  # pragma: no cover - depends on runtime services
        return {
            "available": False,
            "binary": resolved,
            "version": "",
            "error": str(error),
        }

    output = (result.stdout or result.stderr or "").strip()
    return {
        "available": result.returncode == 0,
        "binary": resolved,
        "version": output,
        "error": "" if result.returncode == 0 else output,
    }


def _build_mysqlsh_ssh_target(profile):
    ssh_host = str(profile.get("ssh_host", "")).strip()
    ssh_user = str(profile.get("ssh_user", "")).strip()
    ssh_key_path = os.path.expanduser(str(profile.get("ssh_key_path", "")).strip())
    ssh_port = int(profile.get("ssh_port") or 22)

    if not ssh_host or not ssh_user or not ssh_key_path:
        raise ValueError("SSH-enabled profiles require jump host, SSH user, and private key path.")
    if not os.path.exists(ssh_key_path):
        raise ValueError(f"SSH private key does not exist: {ssh_key_path}")

    return f"{ssh_user}@{ssh_host}:{ssh_port}", ssh_key_path


def _expand_optional_existing_path(path_value, label):
    normalized_value = os.path.expanduser(str(path_value or "").strip())
    if not normalized_value:
        return ""
    if not os.path.exists(normalized_value):
        raise ValueError(f"{label} does not exist: {normalized_value}")
    return normalized_value


def _build_mysql_uri(username, password, host, port, database=""):
    authority = quote(str(username), safe="")
    if password:
        authority += f":{quote(str(password), safe='')}"
    authority += f"@{host}:{int(port)}"

    normalized_database = str(database or "").strip()
    if normalized_database:
        return f"mysql://{authority}/{quote(normalized_database, safe='')}"
    return f"mysql://{authority}"


def build_mysqlsh_connection_options(profile, credentials, *, database=""):
    if not credentials.get("username"):
        raise ValueError("No active MySQL username is stored in the current session.")
    if not profile.get("host"):
        raise ValueError("The selected profile does not have a MySQL host configured.")

    normalized_database = str(database or profile.get("database", "")).strip()

    if profile.get("ssh_enabled"):
        ssh_target, ssh_key_path = _build_mysqlsh_ssh_target(profile)
        connection_options = {
            "uri": _build_mysql_uri(
                credentials["username"],
                credentials.get("password", ""),
                profile["host"],
                profile["port"],
                normalized_database,
            ),
            "ssh": ssh_target,
            "ssh-identity-file": ssh_key_path,
        }
        ssh_config_file = _expand_optional_existing_path(profile.get("ssh_config_file", ""), "SSH config file")
        if ssh_config_file:
            connection_options["ssh-config-file"] = ssh_config_file
        return connection_options

    connection_options = {
        "scheme": "mysql",
        "user": str(credentials["username"]),
        "password": str(credentials.get("password", "")),
        "host": str(profile["host"]),
        "port": int(profile["port"]),
    }

    if normalized_database:
        connection_options["schema"] = normalized_database

    return connection_options


def _build_mysqlsh_command(mysqlsh_binary, profile, credentials, request_path, *, database=""):
    command = [
        mysqlsh_binary,
        "--py",
        "--no-wizard",
    ]

    command.extend(
        [
            "--pym",
            PYTHON_RUNNER_MODULE,
            request_path,
        ]
    )
    return command


def build_mysqlsh_command(mysqlsh_binary, profile, credentials, request_path, *, database=""):
    return _build_mysqlsh_command(mysqlsh_binary, profile, credentials, request_path, database=database)


def build_mysqlsh_execution_request(profile, credentials, request_payload, *, database=""):
    if not isinstance(request_payload, dict):
        raise ValueError("MySQL Shell request payload must be a dictionary.")
    if not request_payload.get("function_name"):
        raise ValueError("MySQL Shell request payload is missing function_name.")

    return {
        "function_name": request_payload["function_name"],
        "args": request_payload.get("args", []),
        "kwargs": request_payload.get("kwargs", {}),
        "connection_options": build_mysqlsh_connection_options(
            profile,
            credentials,
            database=database,
        ),
    }


def extract_mysqlsh_result_payload(stdout_text):
    rendered_stdout = str(stdout_text or "")
    end_index = rendered_stdout.rfind(MYSQLSH_RESULT_END)
    if end_index < 0:
        return None

    start_index = rendered_stdout.rfind(MYSQLSH_RESULT_START, 0, end_index)
    if start_index < 0:
        return None

    payload_text = rendered_stdout[start_index + len(MYSQLSH_RESULT_START) : end_index].strip()
    if not payload_text:
        return None

    try:
        return json.loads(payload_text)
    except json.JSONDecodeError:
        return None


def _last_nonempty_line(*texts):
    for text in texts:
        for line in reversed(str(text or "").splitlines()):
            candidate = line.strip()
            if candidate:
                return candidate
    return ""


def evaluate_mysqlsh_execution(returncode, stdout_text, stderr_text):
    payload = extract_mysqlsh_result_payload(stdout_text)
    succeeded = int(returncode or 0) == 0
    error = ""
    error_type = ""

    if isinstance(payload, dict):
        payload_status = str(payload.get("status", "")).strip().lower()
        if payload_status == "error":
            succeeded = False
            error = str(payload.get("error", "")).strip()
            error_type = str(payload.get("error_type", "")).strip()

    if not error and not succeeded:
        error = _last_nonempty_line(stderr_text, stdout_text)

    return {
        "succeeded": succeeded,
        "error": error,
        "error_type": error_type,
        "result_payload": payload,
    }


def normalize_progress_file_value(path_value):
    normalized_value = str(path_value or "").strip()
    if not normalized_value:
        return ""

    expanded_path = Path(os.path.expanduser(normalized_value))
    if not expanded_path.is_absolute():
        return normalized_value

    try:
        root_path = ROOT_DIR.resolve(strict=False)
        relative_path = expanded_path.resolve(strict=False).relative_to(root_path)
        return str(relative_path)
    except ValueError:
        return normalized_value


def resolve_progress_file_path(path_value):
    normalized_value = str(path_value or "").strip()
    if not normalized_value:
        return None

    expanded_path = Path(os.path.expanduser(normalized_value))
    if expanded_path.is_absolute():
        return expanded_path
    return ROOT_DIR / expanded_path


def _render_python_literal(value):
    if isinstance(value, bool):
        return "True" if value else "False"
    if value is None:
        return "None"
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, dict):
        if not value:
            return "{}"

        lines = ["{"]
        items = sorted(value.items(), key=lambda item: str(item[0]))
        for index, (key, item_value) in enumerate(items):
            rendered_key = _render_python_literal(key)
            rendered_value_lines = _render_python_literal(item_value).splitlines()
            lines.append(f"  {rendered_key}: {rendered_value_lines[0]}")
            if len(rendered_value_lines) > 1:
                lines.extend(f"  {line}" for line in rendered_value_lines[1:])
            if index < len(items) - 1:
                lines[-1] += ","
        lines.append("}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"

        lines = ["["]
        for index, item in enumerate(value):
            rendered_item_lines = _render_python_literal(item).splitlines()
            lines.append(f"  {rendered_item_lines[0]}")
            if len(rendered_item_lines) > 1:
                lines.extend(f"  {line}" for line in rendered_item_lines[1:])
            if index < len(value) - 1:
                lines[-1] += ","
        lines.append("]")
        return "\n".join(lines)
    if isinstance(value, tuple):
        if not value:
            return "()"

        lines = ["("]
        for index, item in enumerate(value):
            rendered_item_lines = _render_python_literal(item).splitlines()
            lines.append(f"  {rendered_item_lines[0]}")
            if len(rendered_item_lines) > 1:
                lines.extend(f"  {line}" for line in rendered_item_lines[1:])
            if index < len(value) - 1:
                lines[-1] += ","
        lines.append(")")
        return "\n".join(lines)
    return repr(value)


def _render_python_argument(value):
    return textwrap.indent(_render_python_literal(value), "    ")


def _render_python_keyword_argument(key, value):
    rendered_value_lines = _render_python_literal(value).splitlines()
    if len(rendered_value_lines) == 1:
        return f"    {key}={rendered_value_lines[0]}"
    return "\n".join(
        [f"    {key}={rendered_value_lines[0]}"] + [f"    {line}" for line in rendered_value_lines[1:]]
    )


def _build_python_request(function_name, *args, **kwargs):
    request_payload = {
        "function_name": function_name,
        "args": list(args),
        "kwargs": kwargs,
    }
    rendered_arguments = ",\n".join(_render_python_argument(argument) for argument in args)
    if kwargs:
        for key, value in kwargs.items():
            rendered_arguments = (
                f"{rendered_arguments},\n{_render_python_keyword_argument(key, value)}"
                if rendered_arguments
                else _render_python_keyword_argument(key, value)
            )

    request_payload["display_text"] = "\n".join(
        [
            "shell.options.useWizards = False",
            f"result = util.{function_name}(",
            rendered_arguments,
            ")",
            "print(result)",
        ]
    )
    return request_payload


def build_dump_instance_request(output_url, options):
    return _build_python_request("dump_instance", output_url, options)


def build_dump_schemas_request(schema_names, output_url, options):
    return _build_python_request("dump_schemas", schema_names, output_url, options)


def build_load_dump_request(source_url, options):
    return _build_python_request("load_dump", source_url, options)


def default_progress_file(par_id, operation_name):
    ensure_runtime_dirs()
    safe_operation = "".join(character if character.isalnum() else "-" for character in str(operation_name or "load-dump"))
    safe_par_id = "".join(character for character in str(par_id or "par") if character.isalnum())[:12] or "par"
    return normalize_progress_file_value(str(PROGRESS_DIR / f"{safe_operation}-{safe_par_id}.json"))


def execute_mysqlsh_request(profile, credentials, request_payload, *, database="", operation_name="mysqlsh"):
    ensure_runtime_dirs()
    mysqlsh_status = get_mysqlsh_status()
    if not mysqlsh_status["available"]:
        raise RuntimeError(mysqlsh_status["error"] or "mysqlsh is not available.")

    request_path = None
    started_at = datetime.now(timezone.utc)
    executable_payload = build_mysqlsh_execution_request(
        profile,
        credentials,
        request_payload,
        database=database,
    )
    display_text = str(request_payload.get("display_text", json.dumps(executable_payload, indent=2, sort_keys=True)))
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        prefix="mysql-shell-web-",
        dir=str(PROGRESS_DIR),
        delete=False,
    ) as handle:
        json.dump(executable_payload, handle, indent=2, sort_keys=True)
        request_path = handle.name
    os.chmod(request_path, 0o600)

    try:
        command = _build_mysqlsh_command(
            mysqlsh_status["binary"],
            profile,
            credentials,
            request_path,
            database=database,
        )
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=_mysqlsh_env(),
            cwd=str(ROOT_DIR),
            stdin=subprocess.DEVNULL,
            check=False,
        )
    finally:
        if request_path:
            try:
                Path(request_path).unlink()
            except OSError:
                pass

    finished_at = datetime.now(timezone.utc)
    execution_state = evaluate_mysqlsh_execution(result.returncode, result.stdout or "", result.stderr or "")
    return {
        "operation_name": operation_name,
        "returncode": result.returncode,
        "succeeded": execution_state["succeeded"],
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
        "error": execution_state["error"],
        "error_type": execution_state["error_type"],
        "result_payload": execution_state["result_payload"],
        "script_text": display_text,
        "command_preview": shlex.join(command),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 2),
    }
