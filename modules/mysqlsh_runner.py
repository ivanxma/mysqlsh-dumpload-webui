import json
import os
import shlex
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from .config import MYSQLSH_USER_CONFIG_HOME, PROGRESS_DIR, ROOT_DIR
from .mysql_connection import mysql_endpoint


def ensure_runtime_dirs():
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    MYSQLSH_USER_CONFIG_HOME.mkdir(parents=True, exist_ok=True)


def _mysqlsh_env():
    ensure_runtime_dirs()
    env = os.environ.copy()
    env["MYSQLSH_USER_CONFIG_HOME"] = str(MYSQLSH_USER_CONFIG_HOME)
    env.setdefault("TERM", "dumb")
    return env


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


def _build_mysql_uri(username, host, port, database=""):
    uri = f"mysql://{quote(str(username), safe='')}@{host}:{int(port)}"
    normalized_database = str(database or "").strip()
    if normalized_database:
        uri += "/" + quote(normalized_database, safe="")
    return uri


def _js_script_for_call(invocation):
    return "\n".join(
        [
            "shell.options.useWizards = false;",
            f"const result = {invocation};",
            "print('MYSQL_SHELL_WEB_RESULT_START');",
            "print(JSON.stringify({status: 'ok', result: result}, null, 2));",
            "print('MYSQL_SHELL_WEB_RESULT_END');",
        ]
    )


def build_dump_instance_script(output_url, options):
    invocation = f"util.dumpInstance({json.dumps(output_url)}, {json.dumps(options, sort_keys=True)})"
    return _js_script_for_call(invocation)


def build_dump_schemas_script(schema_names, output_url, options):
    invocation = (
        f"util.dumpSchemas({json.dumps(schema_names)}, {json.dumps(output_url)}, "
        f"{json.dumps(options, sort_keys=True)})"
    )
    return _js_script_for_call(invocation)


def build_load_dump_script(source_url, options):
    invocation = f"util.loadDump({json.dumps(source_url)}, {json.dumps(options, sort_keys=True)})"
    return _js_script_for_call(invocation)


def default_progress_file(par_id, operation_name):
    ensure_runtime_dirs()
    safe_operation = "".join(character if character.isalnum() else "-" for character in str(operation_name or "load-dump"))
    safe_par_id = "".join(character for character in str(par_id or "par") if character.isalnum())[:12] or "par"
    return str(PROGRESS_DIR / f"{safe_operation}-{safe_par_id}.json")


def execute_mysqlsh_script(profile, credentials, script_text, *, database="", operation_name="mysqlsh"):
    ensure_runtime_dirs()
    mysqlsh_status = get_mysqlsh_status()
    if not mysqlsh_status["available"]:
        raise RuntimeError(mysqlsh_status["error"] or "mysqlsh is not available.")

    script_path = None
    started_at = datetime.now(timezone.utc)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".js",
        prefix="mysql-shell-web-",
        dir=str(PROGRESS_DIR),
        delete=False,
    ) as handle:
        handle.write(script_text)
        script_path = handle.name

    try:
        with mysql_endpoint(profile) as endpoint:
            uri = _build_mysql_uri(
                credentials["username"],
                endpoint["host"],
                endpoint["port"],
                database or profile.get("database", ""),
            )
            command = [
                mysqlsh_status["binary"],
                "--js",
                "--no-wizard",
                "--uri",
                uri,
                "--passwords-from-stdin",
                "--file",
                script_path,
            ]
            result = subprocess.run(
                command,
                input=f"{credentials['password']}\n",
                capture_output=True,
                text=True,
                env=_mysqlsh_env(),
                cwd=str(ROOT_DIR),
                check=False,
            )
    finally:
        if script_path:
            try:
                Path(script_path).unlink()
            except OSError:
                pass

    finished_at = datetime.now(timezone.utc)
    return {
        "operation_name": operation_name,
        "returncode": result.returncode,
        "succeeded": result.returncode == 0,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
        "script_text": script_text,
        "command_preview": shlex.join(command),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 2),
    }
