"""
Lead Dataset Builder - local entry point.
Run: python app.py  or  flask --app app run
Vercel uses api/app.py instead.
"""
import sys
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from api.app import app  # noqa: E402

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, port=5000)
