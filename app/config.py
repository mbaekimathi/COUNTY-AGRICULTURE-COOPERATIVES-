import os
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
# Always prefer repo-root `.env` (cwd differs under IDE / task runners / subprocesses).
load_dotenv(BASE_DIR / ".env")
load_dotenv()


def _mysql_from_env():
    """Resolve MySQL settings from DATABASE_URL (hosting) or MYSQL_* variables."""
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if url.startswith("mysql+pymysql://"):
        url = "mysql://" + url.split("mysql+pymysql://", 1)[1]
    if url.startswith("mysql://") or url.startswith("mariadb://"):
        parsed = urlparse(url)
        path_db = (parsed.path or "").lstrip("/").split("?", 1)[0]
        return {
            "MYSQL_HOST": parsed.hostname or "127.0.0.1",
            "MYSQL_PORT": int(parsed.port or 3306),
            "MYSQL_USER": unquote(parsed.username or ""),
            "MYSQL_PASSWORD": unquote(parsed.password or ""),
            "MYSQL_DATABASE": path_db or os.environ.get("MYSQL_DATABASE", "meru_cooperatives"),
        }
    return {
        "MYSQL_HOST": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "MYSQL_PORT": int(os.environ.get("MYSQL_PORT", "3306")),
        "MYSQL_USER": os.environ.get("MYSQL_USER", "root"),
        "MYSQL_PASSWORD": os.environ.get("MYSQL_PASSWORD", ""),
        "MYSQL_DATABASE": os.environ.get("MYSQL_DATABASE", "meru_cooperatives"),
    }


_DB = _mysql_from_env()


class Config:
    SECRET_KEY = os.environ.get("FLASK_SECRET_KEY") or "dev-only-change-in-production"
    PERMANENT_SESSION_LIFETIME = timedelta(days=14)
    # After this many minutes without any request (or session heartbeat), session hours pause.
    SESSION_ACTIVITY_IDLE_MINUTES = int(os.environ.get("SESSION_ACTIVITY_IDLE_MINUTES", "15"))

    MYSQL_HOST = _DB["MYSQL_HOST"]
    MYSQL_PORT = _DB["MYSQL_PORT"]
    MYSQL_USER = _DB["MYSQL_USER"]
    MYSQL_PASSWORD = _DB["MYSQL_PASSWORD"]
    MYSQL_DATABASE = _DB["MYSQL_DATABASE"]

    # Set SESSION_COOKIE_SECURE=1 when the site is served only over HTTPS (production).
    SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "").lower() in ("1", "true", "yes")
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")

    UPLOAD_FOLDER = BASE_DIR / "app" / "static" / "uploads" / "profiles"
    # Total request size limit (form fields + files). Farmers registration can include
    # profile photo plus a national ID PDF/image; keep this comfortably above typical scans.
    MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH_MB", "25")) * 1024 * 1024
    ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}

    # Browser Maps JavaScript API key (restrict by HTTP referrer in Google Cloud Console).
    GOOGLE_MAPS_API_KEY = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
