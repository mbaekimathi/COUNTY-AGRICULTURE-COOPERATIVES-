import os
import socket

from app import create_app

app = create_app()


def _guess_lan_ipv4():
    """Best-effort local LAN address for 'open on phone' hints (same Wi‑Fi)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.3)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return None


if __name__ == "__main__":
    host = os.environ.get("FLASK_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_PORT", "5000"))

    if host == "0.0.0.0":
        lan = _guess_lan_ipv4()
        print(f"* Listening on all interfaces — phone (same Wi‑Fi): http://{lan or '<your-PC-IPv4>'}:{port}")
        print("* Tip: set FLASK_HOST=127.0.0.1 to listen on this PC only.")

    app.run(debug=True, host=host, port=port)
