import os

from modules.app_factory import create_app
from modules.legacy_app import (
    ensure_object_storage_store,
    ensure_par_store,
    ensure_profile_store,
    ensure_runtime_dirs,
)


app = create_app()


if __name__ == "__main__":
    app.run(
        debug=False,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "5000")),
    )
