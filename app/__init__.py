import os
from pathlib import Path

from flask import Flask
from werkzeug.exceptions import RequestEntityTooLarge

from app.config import Config
from app.db import ensure_database_and_schema

# Resolve paths from this package directory so static/CSS always loads (avoids
# wrong root_path when the import name is generic, e.g. package named "app").
_PKG_DIR = Path(__file__).resolve().parent
_APP_CSS = _PKG_DIR / "static" / "css" / "app.css"


def _app_css_version() -> str:
    try:
        return str(int(_APP_CSS.stat().st_mtime))
    except OSError:
        return "0"


def create_app(config_class=Config):
    app = Flask(
        __name__,
        static_folder=str(_PKG_DIR / "static"),
        template_folder=str(_PKG_DIR / "templates"),
    )
    app.config.from_object(config_class)

    if os.environ.get("TRUST_PROXY_HEADERS", "").lower() in ("1", "true", "yes"):
        from werkzeug.middleware.proxy_fix import ProxyFix

        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    ensure_database_and_schema(app)

    from app.routes.auth import bp as auth_bp
    from app.routes.main import bp as main_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp, url_prefix="/auth")

    @app.context_processor
    def inject_csrf():
        from app.csrf import fresh_csrf_token

        return {"csrf_token": fresh_csrf_token(), "static_asset_version": _app_css_version()}

    @app.template_filter("initials")
    def initials_filter(value):
        if not value or not str(value).strip():
            return "?"
        parts = str(value).strip().split()
        if len(parts) >= 2:
            return (parts[0][0] + parts[-1][0]).upper()
        word = parts[0]
        if len(word) == 1:
            return word[0].upper()
        return (word[0] + word[1]).upper()

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_too_large(_err):
        # Keep users on the same page (Flask will otherwise respond with a bare 413).
        from flask import flash, redirect, request

        max_mb = int(app.config.get("MAX_CONTENT_LENGTH", 0) / (1024 * 1024)) if app.config.get("MAX_CONTENT_LENGTH") else 0
        hint = f" (max {max_mb} MB)" if max_mb else ""
        flash(f"Upload too large{hint}. Please upload a smaller file (compress photo/PDF) and try again.", "error")
        return redirect(request.referrer or "/"), 303

    return app
