from secrets import token_urlsafe

from flask import abort, request, session


def fresh_csrf_token() -> str:
    token = token_urlsafe(32)
    session["_csrf"] = token
    return token


def validate_csrf() -> None:
    if request.form.get("csrf_token") != session.get("_csrf"):
        abort(400)
