import json
import sys
import traceback

RESULT_START = "MYSQL_SHELL_WEB_RESULT_START"
RESULT_END = "MYSQL_SHELL_WEB_RESULT_END"


def _print_result(payload):
    print(RESULT_START)
    print(json.dumps(payload, indent=2, sort_keys=True))
    print(RESULT_END)


def _load_request():
    if len(sys.argv) < 2:
        raise ValueError("Missing MySQL Shell request payload path.")

    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        return json.load(handle)


def _serialize_result(value):
    try:
        json.dumps(value)
    except TypeError:
        return repr(value)
    return value


def _open_request_session(request_payload):
    existing_session = shell.get_session()
    if existing_session is not None and existing_session.is_open():
        return existing_session

    connection_options = request_payload.get("connection_options") or {}
    if connection_options:
        return shell.connect(connection_options)

    if existing_session is None:
        raise ValueError("MySQL Shell request payload is missing connection options.")
    return existing_session


def main():
    request_payload = _load_request()
    function_name = str(request_payload.get("function_name", "")).strip()
    if not function_name:
        raise ValueError("MySQL Shell request payload is missing function_name.")

    args = request_payload.get("args", [])
    kwargs = request_payload.get("kwargs", {})
    session = _open_request_session(request_payload)
    try:
        shell.options.useWizards = False
        operation = getattr(util, function_name)
        result = operation(*args, **kwargs)
        _print_result(
            {
                "function_name": function_name,
                "result": _serialize_result(result),
                "status": "ok",
            }
        )
    finally:
        try:
            if session is not None and session.is_open():
                session.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pragma: no cover - executed inside mysqlsh
        _print_result(
            {
                "error": str(error),
                "error_type": type(error).__name__,
                "status": "error",
                "traceback": traceback.format_exc(),
            }
        )
        raise
