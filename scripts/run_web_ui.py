from pathlib import Path
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import uvicorn
from factor_lab.webui_app import app


if __name__ == "__main__":
    host = os.getenv("WEB_UI_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_UI_PORT", "8765"))
    uvicorn.run(app, host=host, port=port)
