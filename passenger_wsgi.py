"""
WSGI entry for Phusion Passenger (cPanel "Setup Python App").
Passenger loads this file and expects a global named ``application``.
"""
from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from run import app as application  # noqa: E402
