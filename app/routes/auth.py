import re
from pathlib import Path

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    current_app,
)
from pymysql.err import IntegrityError
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from app.csrf import validate_csrf
from app.db import get_connection

bp = Blueprint("auth", __name__)


def _allowed_file(filename: str) -> bool:
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower()
        in current_app.config["ALLOWED_EXTENSIONS"]
    )


@bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html")

    validate_csrf()

    full_name = (request.form.get("full_name") or "").strip().upper()
    email = (request.form.get("email") or "").strip().lower()
    national_id = (request.form.get("national_id") or "").strip().upper()
    login_code = (request.form.get("login_code") or "").strip()
    password = request.form.get("password") or ""
    confirm = request.form.get("confirm_password") or ""

    errors = []
    if len(full_name) < 2:
        errors.append("Full name is required.")
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        errors.append("Enter a valid email address.")
    if len(national_id) < 5:
        errors.append("National ID must be at least 5 characters.")
    if not re.fullmatch(r"\d{6}", login_code):
        errors.append("Login code must be exactly 6 digits.")
    if len(password) < 6:
        errors.append("Password must be at least 6 characters (digits, letters, or words).")
    if password != confirm:
        errors.append("Password and confirmation do not match.")

    file = request.files.get("profile_photo")
    if file and file.filename:
        if not _allowed_file(file.filename):
            errors.append("Profile photo must be PNG, JPG, JPEG, WebP, or GIF.")

    photo_rel = None
    if errors:
        for e in errors:
            flash(e, "error")
        return (
            render_template(
                "register.html",
                form=request.form,
            ),
            422,
        )

    if file and file.filename:
        ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
        from uuid import uuid4

        fname = f"{uuid4().hex}.{ext}"
        upload_root = Path(current_app.config["UPLOAD_FOLDER"])
        upload_root.mkdir(parents=True, exist_ok=True)
        dest = upload_root / fname
        file.save(dest)
        photo_rel = f"uploads/profiles/{fname}"

    pwd_hash = generate_password_hash(password)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO employees (
                    full_name, email, national_id, login_code,
                    password_hash, profile_photo, role, status
                ) VALUES (%s, %s, %s, %s, %s, %s, 'employee', 'pending_approval')
                """,
                (full_name, email, national_id, login_code, pwd_hash, photo_rel),
            )
    except IntegrityError as exc:
        msg = str(exc).lower()
        if "uq_employees_email" in msg or "email" in msg:
            flash("That email is already registered.", "error")
        elif "uq_employees_national_id" in msg or "national_id" in msg:
            flash("That national ID is already registered.", "error")
        elif "uq_employees_login_code" in msg or "login_code" in msg:
            flash("That 6-digit login code is already taken.", "error")
        else:
            flash("Registration failed: duplicate value.", "error")
        return render_template(
            "register.html",
            form=request.form,
        ), 409
    finally:
        conn.close()

    flash(
        "Account created. Your role is Employee and status is Pending approval. "
        "You can sign in once an administrator activates your account.",
        "success",
    )
    return redirect(url_for("auth.login"))


@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")

    validate_csrf()

    login_code = (request.form.get("login_code") or "").strip()
    password = request.form.get("password") or ""

    if not re.fullmatch(r"\d{6}", login_code):
        flash("Enter your 6-digit login code.", "error")
        return render_template("login.html"), 422

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, full_name, password_hash, role, status, profile_photo
                FROM employees
                WHERE login_code = %s
                LIMIT 1
                """,
                (login_code,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not check_password_hash(row["password_hash"], password):
        flash("Invalid login code or password.", "error")
        return render_template("login.html"), 401

    status = row["status"]
    if status == "pending_approval":
        flash(
            "Your account is pending approval. You cannot sign in yet.",
            "error",
        )
        return render_template("login.html"), 403
    if status == "suspended":
        flash("Your account is suspended. Contact an administrator.", "error")
        return render_template("login.html"), 403

    session.clear()
    session["employee_id"] = row["id"]
    session["employee_name"] = row["full_name"]
    session["employee_role"] = row["role"]
    session["employee_status"] = row["status"]
    session["employee_photo"] = row.get("profile_photo") or ""
    session.permanent = True
    flash("Welcome back.", "success")
    return redirect(url_for("main.dashboard", role=row["role"]))


@bp.route("/logout", methods=["POST"])
def logout():
    validate_csrf()
    session.clear()
    flash("You have been signed out.", "success")
    return redirect(url_for("auth.login"))
