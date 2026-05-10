"""Kenya-oriented mobile normalization shared by employees and suppliers."""

import re


def normalize_ke_phone(raw: str) -> str:
    """Strip non-digits; normalize common Kenyan mobiles to 254…."""
    s = re.sub(r"\D+", "", (raw or "").strip())
    if not s:
        return ""
    if s.startswith("254"):
        return s
    if s.startswith("0") and len(s) >= 10:
        return "254" + s[1:]
    if len(s) == 9 and s[0] == "7":
        return "254" + s
    return s


def employee_phone_error(normalized: str) -> str | None:
    if not normalized:
        return "Phone number is required."
    if len(normalized) < 9 or len(normalized) > 15:
        return "Enter a valid mobile number (e.g. 07XXXXXXXX or 2547XXXXXXXX)."
    return None
