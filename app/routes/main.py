import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename

from app.db import get_connection
from app.csrf import validate_csrf

bp = Blueprint("main", __name__)

ALLOWED_ROLES = frozenset(
    {
        "administrator",
        "manager",
        "health_officer",
        "sales",
        "it_support",
        "storage",
        "employee",
    }
)

PRIVILEGED_MODULE_ROLES = frozenset({"administrator", "manager", "it_support"})

# 1 ha ≈ 2.47105 acres (international)
HECTARES_TO_ACRES = 2.47105381467


def _normalize_supplier_phone(raw: str) -> str:
    """Strip non-digits; normalize common Kenyan mobiles to 254… for deduplication."""
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


def _upsert_supplier_row(cur, name: str, contact_display: str) -> None:
    """Register or update supplier directory row keyed by normalized phone (≥9 digits)."""
    norm = _normalize_supplier_phone(contact_display)
    if len(norm) < 9:
        return
    cur.execute(
        """
        INSERT INTO suppliers (name, contact, contact_normalized)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
          name = VALUES(name),
          contact = VALUES(contact),
          updated_at = CURRENT_TIMESTAMP
        """,
        (name, contact_display, norm),
    )


def _farmer_land_acres(land_size, land_size_unit) -> float | None:
    """Normalize stored land to acres for distribution UI."""
    if land_size is None:
        return None
    v = float(land_size)
    u = (land_size_unit or "").lower()
    if u == "hectares":
        return round(v * HECTARES_TO_ACRES, 2)
    return round(v, 2)


def _group_active_sessions_by_farmer(rows: list) -> list:
    """One summary per farmer: nested active sessions, totals vs recorded land."""
    by_id: dict[int, dict] = {}
    order: list[int] = []

    def _start_for_sort(s: dict):
        d = s.get("session_started_on")
        if d is None:
            return date(1970, 1, 1)
        if isinstance(d, date):
            return d
        try:
            return date.fromisoformat(str(d)[:10])
        except (TypeError, ValueError):
            return date(1970, 1, 1)

    for r in rows:
        fid = int(r["farmer_id"])
        if fid not in by_id:
            order.append(fid)
            by_id[fid] = {
                "farmer_id": fid,
                "farmer_code": r.get("farmer_code"),
                "membership_number": r.get("membership_number"),
                "full_name": r.get("full_name"),
                "phone_number": r.get("phone_number"),
                "county": r.get("county"),
                "ward": r.get("ward"),
                "farmer_status": r.get("farmer_status"),
                "land_size": r.get("land_size"),
                "land_size_unit": r.get("land_size_unit"),
                "sessions": [],
            }
        parts = []
        if r.get("product_crop_code"):
            parts.append(str(r["product_crop_code"]).strip())
        if r.get("product_crop_name"):
            parts.append(str(r["product_crop_name"]).strip())
        crop_display = " — ".join(parts) if parts else (r.get("crop_or_activity") or "—")
        try:
            acres = float(r.get("acreage_used_acres") or 0)
        except (TypeError, ValueError):
            acres = 0.0
        by_id[fid]["sessions"].append(
            {
                "session_id": r.get("session_id"),
                "crop_display": crop_display,
                "season_name": r.get("season_name"),
                "session_started_on": r.get("session_started_on"),
                "session_ended_on": r.get("session_ended_on"),
                "acreage_used_acres": acres,
                "registered_by_name": r.get("registered_by_name"),
            }
        )

    out: list[dict] = []
    for fid in order:
        g = by_id[fid]
        sess = g["sessions"]
        sess.sort(key=_start_for_sort, reverse=True)
        total_sess = round(sum(s["acreage_used_acres"] for s in sess), 4)
        land = _farmer_land_acres(g.get("land_size"), g.get("land_size_unit"))
        remaining = None
        over_by = None
        if land is not None:
            diff = round(land - total_sess, 4)
            if diff >= -0.01:
                remaining = max(0.0, diff)
            else:
                over_by = round(total_sess - land, 4)
        g["sessions_total_acres"] = total_sess
        g["land_total_acres"] = land
        g["land_remaining_acres"] = remaining
        g["land_over_by_acres"] = over_by
        out.append(g)

    out.sort(key=lambda x: str(x.get("full_name") or "").upper())
    return out


def _parse_per_acre_amount(val) -> float | None:
    """Positive numeric amount from products.package_size (string or number)."""
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    if not s:
        return None
    try:
        x = float(s)
    except ValueError:
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if not m:
            return None
        try:
            x = float(m.group(1))
        except ValueError:
            return None
    if x <= 0:
        return None
    return x


def _parse_dist_acres_input(raw) -> float | None:
    """Acres to use from distribution form; must be positive if present."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", ".")
    if not s:
        return None
    try:
        x = float(s)
    except ValueError:
        return None
    if x <= 0:
        return None
    return x


def _crop_average_yield_from_form(
    errors: list, stock_uom: str | None
) -> tuple[str | None, str | None, str | None]:
    """Optional harvest per 1 acre; unit matches product unit_of_measure."""
    amt = (request.form.get("average_yield_per_acre") or "").strip().upper() or None
    uom = (stock_uom or "").strip().upper() or None
    if not amt:
        return None, None, None
    if not uom:
        errors.append("Average yield per acre: set unit of measure (above) first.")
        return None, None, None
    return amt, uom, f"{amt} {uom}/ACRE"


def _parse_growth_duration_to_days(raw: str | None) -> int:
    """Interpret products.growth_duration into days for session end calculation."""
    if raw is None:
        return 90
    s = str(raw).strip().upper()
    if not s:
        return 90
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 90
    n = float(m.group(1))
    if "MONTH" in s:
        return max(1, int(round(n * 30)))
    if "WEEK" in s:
        return max(1, int(round(n * 7)))
    return max(1, int(round(n)))


def _iso_date_plus_days(start_iso: str, days: int) -> str:
    d0 = date.fromisoformat(start_iso.strip())
    d1 = d0 + timedelta(days=max(0, int(days)))
    return d1.isoformat()


def _refresh_farmer_farming_land(cur, farmer_id: int) -> None:
    """Set farmers.farming_session_land from active sessions vs recorded farm size (acres)."""
    cur.execute(
        "SELECT land_size, land_size_unit FROM farmers WHERE id=%s LIMIT 1",
        (farmer_id,),
    )
    row = cur.fetchone()
    if not row:
        return
    total = _farmer_land_acres(row.get("land_size"), row.get("land_size_unit"))
    cur.execute(
        """
        SELECT COALESCE(SUM(acreage_used_acres), 0) AS s
        FROM farming_sessions
        WHERE farmer_id=%s AND status='active'
        """,
        (farmer_id,),
    )
    committed = float((cur.fetchone() or {}).get("s") or 0)
    if total is None or total <= 0:
        flag = "partial" if committed > 0 else "none"
    elif committed + 1e-6 >= total:
        flag = "full"
    elif committed > 0:
        flag = "partial"
    else:
        flag = "none"
    cur.execute(
        "UPDATE farmers SET farming_session_land=%s WHERE id=%s",
        (flag, farmer_id),
    )

MODULES = [
    (
        "crops_session",
        "Crops in session",
        "Crop products with at least one active farming registration.",
        "leaf",
    ),
    ("farmers", "Farmers in session", "Active farming sessions per farmer: crops, acres, and totals vs recorded land.", "users"),
    ("administration", "Administration", "HR, administration, staff records, and assignments.", "briefcase"),
    ("finance", "Finance", "Receipts, payments, budgeting, and financial tracking.", "wallet"),
    ("warehouse", "Warehouse", "Stock, warehousing, issuing, and inventory.", "boxes"),
    ("logistics", "Logistics", "Deliveries, dispatch, routes, and transport operations.", "truck"),
    ("processing_value_addition", "Processing & Value addition", "Processing, packaging, and value chain activities.", "plus_circle"),
    ("sales_marketing", "Sales and marketing", "Sales orders, invoicing, outreach, and customer transactions.", "shopping_cart"),
    ("communication", "Communication", "Messages, notices, and stakeholder updates.", "megaphone"),
    ("health", "Health management", "Health checks, compliance, and clinic workflows.", "heart"),
    ("analytics", "Analytics", "Dashboards and operational insights.", "bar_chart"),
    ("reports", "Reports", "Generate and download reports.", "file_text"),
    ("loans_credit", "Loans and credit", "Loan applications, approvals, and repayments.", "hand_coins"),
    ("governance", "Membership & Cooperative Governance", "Members, governance records, and compliance.", "users_cog"),
    ("smart_farming", "Smart Farming Integration", "IoT, sensors, and smart farming connections.", "cpu"),
    ("company_settings", "Company settings", "Organization-wide configuration and preferences.", "sliders"),
]


def _require_session_role(role: str) -> str:
    if not session.get("employee_id"):
        return ""
    session_role = session.get("employee_role")
    if not session_role or session_role not in ALLOWED_ROLES:
        session.clear()
        return ""
    if role not in ALLOWED_ROLES:
        abort(404)
    if session_role != role:
        return session_role
    return session_role


@bp.route("/styles/app.css")
def app_stylesheet():
    """Serve CSS via a named route so styles load even if /static is blocked or misconfigured."""
    return send_from_directory(
        current_app.static_folder,
        "css/app.css",
        mimetype="text/css",
        max_age=0,
    )


@bp.route("/")
def index():
    if session.get("employee_id"):
        role = session.get("employee_role")
        if not role or role not in ALLOWED_ROLES:
            session.clear()
            return redirect(url_for("auth.login"))
        return redirect(url_for("main.dashboard", role=role))
    return redirect(url_for("auth.login"))


@bp.route("/dashboard")
def dashboard_legacy():
    """Old URL — redirect to role-scoped dashboard."""
    if not session.get("employee_id"):
        return redirect(url_for("auth.login"))
    role = session.get("employee_role")
    if not role or role not in ALLOWED_ROLES:
        session.clear()
        return redirect(url_for("auth.login"))
    return redirect(url_for("main.dashboard", role=role))


@bp.route("/<role>/dashboard")
def dashboard(role):
    if not session.get("employee_id"):
        return redirect(url_for("auth.login"))
    session_role = session.get("employee_role")
    if not session_role or session_role not in ALLOWED_ROLES:
        session.clear()
        return redirect(url_for("auth.login"))
    if role not in ALLOWED_ROLES:
        abort(404)
    if session_role != role:
        return redirect(url_for("main.dashboard", role=session_role))
    return render_template(
        "dashboard.html",
        employee_name=session.get("employee_name"),
        employee_role=session.get("employee_role"),
        employee_status=session.get("employee_status"),
    )


@bp.route("/<role>/module/farmers/manage", methods=["GET"])
def farmers_manage_page(role):
    """Full farmer directory: register, edit, suspend, activate, delete."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.farmers_manage_page", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, farmer_code, membership_number, full_name, national_id, phone_number, county, ward, location, village,
                       land_size, land_size_unit, status, farming_session_land, created_at
                FROM farmers
                ORDER BY created_at DESC
                LIMIT 500
                """
            )
            farmers = cur.fetchall()
    finally:
        conn.close()
    return render_template(
        "module_farmers.html",
        module_key="farmers",
        module_title="Farmer management",
        module_desc="Register, edit, suspend, activate, or delete farmers. New farmers default to pending approval.",
        module_icon="users",
        farmers=farmers,
    )


@bp.route("/<role>/module/products/management")
def products_management(role):
    """Full product catalogue: register, edit, suspend, delete."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.products_management", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, crop_code, product_type, crop_name, scientific_name, crop_category, crop_variety,
                       planting_season, growth_duration, water_requirement,
                       average_yield_range, average_yield_per_acre, average_yield_uom,
                       unit_of_measure, package_size,
                       crop_image, status, created_at
                FROM products
                ORDER BY product_type ASC, crop_category ASC, crop_name ASC, created_at DESC
                LIMIT 500
                """
            )
            products = cur.fetchall()
    finally:
        conn.close()

    return render_template(
        "module_products.html",
        module_key="products",
        module_title="Product management",
        module_desc="Full catalogue: register new products, edit, suspend, activate, or delete.",
        module_icon="package",
        products=products,
    )


@bp.route("/<role>/module/farmers/sessions/crop/<int:product_id>")
def farmers_sessions_for_crop(role, product_id: int):
    """Active farming sessions for a single crop product (for drill-in from Crops in session)."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.farmers_sessions_for_crop", role=session_role, product_id=product_id))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            cur.execute(
                """
                SELECT fs.id AS session_id,
                       fs.farmer_id,
                       f.farmer_code, f.membership_number, f.full_name, f.phone_number, f.county, f.ward,
                       f.land_size, f.land_size_unit,
                       f.status AS farmer_status,
                       fs.season_name, fs.session_started_on, fs.session_ended_on, fs.acreage_used_acres,
                       fs.crop_or_activity, fs.status AS session_status, fs.land_area_notes,
                       p.crop_name AS product_crop_name, p.crop_code AS product_crop_code,
                       e.full_name AS registered_by_name
                FROM farming_sessions fs
                INNER JOIN farmers f ON f.id = fs.farmer_id
                LEFT JOIN products p ON p.id = fs.product_id
                LEFT JOIN employees e ON e.id = fs.registered_by_employee_id
                WHERE fs.status = 'active' AND fs.product_id = %s
                ORDER BY f.full_name ASC, fs.session_started_on DESC
                LIMIT 500
                """,
                (int(product_id),),
            )
            farmer_sessions_flat = cur.fetchall()
    finally:
        conn.close()

    farmer_session_groups = _group_active_sessions_by_farmer(farmer_sessions_flat)
    header_title = f"{product['crop_code'] or ''} — {product['crop_name'] or 'Crop'}"
    return render_template(
        "module_farmers_one_crop.html",
        module_key="farmers",
        module_title=header_title,
        module_desc="Farmers with an active registration for this crop. Session acres vs land on file.",
        module_icon="leaf",
        product=product,
        farmer_session_groups=farmer_session_groups,
    )


@bp.route("/<role>/module/crops_session/sessions/crop/<int:product_id>")
def crops_session_sessions_for_crop(role, product_id: int):
    """Same as farmers_sessions_for_crop, but keeps user within the crops_session URL space."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for("main.crops_session_sessions_for_crop", role=session_role, product_id=product_id)
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            cur.execute(
                """
                SELECT fs.id AS session_id,
                       fs.farmer_id,
                       f.farmer_code, f.membership_number, f.full_name, f.phone_number, f.county, f.ward,
                       f.land_size, f.land_size_unit,
                       f.status AS farmer_status,
                       fs.season_name, fs.session_started_on, fs.session_ended_on, fs.acreage_used_acres,
                       fs.crop_or_activity, fs.status AS session_status, fs.land_area_notes,
                       p.crop_name AS product_crop_name, p.crop_code AS product_crop_code,
                       e.full_name AS registered_by_name
                FROM farming_sessions fs
                INNER JOIN farmers f ON f.id = fs.farmer_id
                LEFT JOIN products p ON p.id = fs.product_id
                LEFT JOIN employees e ON e.id = fs.registered_by_employee_id
                WHERE fs.status = 'active' AND fs.product_id = %s
                ORDER BY f.full_name ASC, fs.session_started_on DESC
                LIMIT 500
                """,
                (int(product_id),),
            )
            farmer_sessions_flat = cur.fetchall()
    finally:
        conn.close()

    farmer_session_groups = _group_active_sessions_by_farmer(farmer_sessions_flat)
    header_title = f"{product['crop_code'] or ''} — {product['crop_name'] or 'Crop'}"
    return render_template(
        "module_farmers_one_crop.html",
        module_key="crops_session",
        module_title=header_title,
        module_desc="Farmers with an active registration for this crop. Session acres vs land on file.",
        module_icon="leaf",
        product=product,
        farmer_session_groups=farmer_session_groups,
    )


@bp.route("/<role>/module/crops_session/sessions/crop/<int:product_id>/required_products")
def crops_session_crop_required_products(role, product_id: int):
    """Totals of inputs/products implied by scheduled activities × active session acreage for this crop."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for(
                "main.crops_session_crop_required_products",
                role=session_role,
                product_id=product_id,
            )
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, crop_category, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            cur.execute(
                """
                SELECT COUNT(DISTINCT farmer_id) AS farmers_count,
                       COALESCE(SUM(acreage_used_acres), 0) AS total_session_acres
                FROM farming_sessions
                WHERE status = 'active' AND product_id = %s
                """,
                (int(product_id),),
            )
            sess_stats = cur.fetchone() or {}

            cur.execute(
                """
                SELECT
                  'PRODUCT' AS row_kind,
                  ep.id AS equipment_product_id,
                  ep.crop_code AS item_code,
                  ep.crop_name AS item_name,
                  ep.crop_category AS item_category,
                  ep.product_type AS item_type,
                  COALESCE(NULLIF(TRIM(ep.unit_of_measure), ''), 'UNIT') AS unit_of_measure,
                  SUM(fs.acreage_used_acres * COALESCE(fa.equipment_units_per_acre, 0)) AS total_qty_required,
                  COUNT(DISTINCT fs.farmer_id) AS farmers_reached,
                  COUNT(DISTINCT fa.id) AS activities_count
                FROM farm_activities fa
                INNER JOIN farming_sessions fs
                  ON fs.product_id = fa.product_id AND fs.status = 'active'
                INNER JOIN products ep ON ep.id = fa.equipment_product_id
                WHERE fa.product_id = %s
                  AND fa.activity_status = 'ACTIVE'
                  AND fa.equipment_product_id IS NOT NULL
                  AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
                GROUP BY ep.id, ep.crop_code, ep.crop_name, ep.crop_category, ep.product_type,
                         COALESCE(NULLIF(TRIM(ep.unit_of_measure), ''), 'UNIT')
                ORDER BY ep.crop_name ASC, ep.crop_code ASC
                """,
                (int(product_id),),
            )
            product_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                  'MANUAL' AS row_kind,
                  fa.id AS activity_id,
                  NULL AS equipment_product_id,
                  '' AS item_code,
                  TRIM(CONCAT(
                    fa.activity_name,
                    CASE
                      WHEN fa.equipment_tools IS NOT NULL AND TRIM(fa.equipment_tools) <> ''
                      THEN CONCAT(' — ', TRIM(fa.equipment_tools))
                      ELSE ''
                    END
                  )) AS item_name,
                  NULL AS item_category,
                  fa.activity_type AS item_type,
                  COALESCE(NULLIF(TRIM(fa.equipment_unit_of_measure), ''), 'UNIT') AS unit_of_measure,
                  SUM(fs.acreage_used_acres * COALESCE(fa.equipment_units_per_acre, 0)) AS total_qty_required,
                  COUNT(DISTINCT fs.farmer_id) AS farmers_reached,
                  1 AS activities_count
                FROM farm_activities fa
                INNER JOIN farming_sessions fs
                  ON fs.product_id = fa.product_id AND fs.status = 'active'
                WHERE fa.product_id = %s
                  AND fa.activity_status = 'ACTIVE'
                  AND fa.equipment_product_id IS NULL
                  AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
                GROUP BY fa.id, fa.activity_name, fa.equipment_tools, fa.activity_type,
                         COALESCE(NULLIF(TRIM(fa.equipment_unit_of_measure), ''), 'UNIT')
                ORDER BY fa.activity_name ASC
                """,
                (int(product_id),),
            )
            manual_rows = cur.fetchall()

    finally:
        conn.close()

    requirement_rows = list(product_rows or []) + list(manual_rows or [])
    header_title = f"{product['crop_code'] or ''} — {product['crop_name'] or 'Crop'}"
    try:
        farmers_n = int(sess_stats.get("farmers_count") or 0)
    except Exception:
        farmers_n = 0
    try:
        acres_total = float(sess_stats.get("total_session_acres") or 0)
    except Exception:
        acres_total = 0.0

    return render_template(
        "module_crops_session_required_products.html",
        module_key="crops_session",
        module_title=header_title,
        module_desc=(
            "Estimated inputs from active scheduled activities multiplied by each farmer’s "
            "registered session acreage for this crop."
        ),
        module_icon="boxes",
        product=product,
        requirement_rows=requirement_rows,
        farmers_in_crop_sessions=farmers_n,
        total_session_acres=round(acres_total, 4),
    )


def _crop_product_session_guard(cur, product_id: int):
    cur.execute(
        "SELECT id, crop_code, crop_name, crop_category, product_type, status FROM products WHERE id=%s LIMIT 1",
        (int(product_id),),
    )
    crop_product = cur.fetchone()
    if (
        not crop_product
        or crop_product.get("status") != "active"
        or (str(crop_product.get("product_type") or "").upper() != "CROP")
    ):
        return None
    return crop_product


def _fetch_farmer_requirements_for_crop_equipment(cur, crop_pid: int, equip_pid: int, farmer_ids: list[int] | None):
    """Farmer rows with qty_required for stock product equip_pid used on crop_pid activities."""
    extra = ""
    params: list = [int(crop_pid), int(equip_pid), int(crop_pid)]
    if farmer_ids is not None:
        if not farmer_ids:
            return []
        ph = ",".join(["%s"] * len(farmer_ids))
        extra = f" AND f.id IN ({ph})"
        params.extend(int(x) for x in farmer_ids)
    cur.execute(
        f"""
        SELECT
          q.farmer_id,
          q.farmer_code,
          q.membership_number,
          q.full_name,
          q.phone_number,
          COALESCE(ac.total_crop_acres, 0) AS farmer_crop_acres,
          q.qty_required
        FROM (
          SELECT
            f.id AS farmer_id,
            f.farmer_code,
            f.membership_number,
            f.full_name,
            f.phone_number,
            SUM(fs.acreage_used_acres * COALESCE(fa.equipment_units_per_acre, 0)) AS qty_required
          FROM farm_activities fa
          INNER JOIN farming_sessions fs
            ON fs.product_id = fa.product_id AND fs.status = 'active'
          INNER JOIN farmers f ON f.id = fs.farmer_id
          WHERE fa.product_id = %s
            AND fa.equipment_product_id = %s
            AND fa.activity_status = 'ACTIVE'
            AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
            {extra}
          GROUP BY f.id, f.farmer_code, f.membership_number, f.full_name, f.phone_number
        ) q
        LEFT JOIN (
          SELECT farmer_id, SUM(acreage_used_acres) AS total_crop_acres
          FROM farming_sessions
          WHERE product_id = %s AND status = 'active'
          GROUP BY farmer_id
        ) ac ON ac.farmer_id = q.farmer_id
        ORDER BY q.full_name ASC
        LIMIT 500
        """,
        tuple(params),
    )
    return cur.fetchall()


def _farmer_distribution_totals_for_product(cur, stock_product_id: int, farmer_ids: list[int]) -> dict[int, float]:
    """Sum qty already stocked out to each farmer via warehouse distributions for this product SKU."""
    if not farmer_ids:
        return {}
    ph = ",".join(["%s"] * len(farmer_ids))
    cur.execute(
        f"""
        SELECT pdr.recipient_id AS farmer_id,
               SUM(COALESCE(pdr.quantity, 0)) AS allocated_qty
        FROM product_distribution_recipients pdr
        INNER JOIN product_distributions pd ON pd.id = pdr.distribution_id
        WHERE pdr.recipient_type = 'FARMER'
          AND pd.product_id = %s
          AND pdr.recipient_id IN ({ph})
        GROUP BY pdr.recipient_id
        """,
        tuple([int(stock_product_id)] + [int(x) for x in farmer_ids]),
    )
    out: dict[int, float] = {}
    for row in cur.fetchall() or []:
        try:
            out[int(row["farmer_id"])] = float(row.get("allocated_qty") or 0)
        except Exception:
            pass
    return out


def _fetch_farmer_allocated_product_rows(cur, farmer_id: int) -> list:
    """Warehouse distribution totals per product SKU for one farmer."""
    cur.execute(
        """
        SELECT pd.product_id,
               MAX(p.crop_code) AS crop_code,
               MAX(p.crop_name) AS crop_name,
               MAX(p.product_type) AS product_type,
               MAX(p.unit_of_measure) AS unit_of_measure,
               SUM(COALESCE(pdr.quantity, 0)) AS allocated_quantity
        FROM product_distribution_recipients pdr
        INNER JOIN product_distributions pd ON pd.id = pdr.distribution_id
        LEFT JOIN products p ON p.id = pd.product_id
        WHERE pdr.recipient_type = 'FARMER'
          AND pdr.recipient_id = %s
        GROUP BY pd.product_id
        HAVING SUM(COALESCE(pdr.quantity, 0)) > 0
        ORDER BY MAX(p.crop_name) ASC, pd.product_id ASC
        """,
        (int(farmer_id),),
    )
    return cur.fetchall()


def _enrich_crop_issue_rows(rows: list, alloc_map: dict[int, float]) -> float:
    """Attach qty_already_issued, qty_remaining_need, qty_issue_suggested; return sum of suggested issue."""
    grand_issue = 0.0
    for row in rows or []:
        fid = int(row["farmer_id"])
        try:
            rq = float(row.get("qty_required") or 0)
        except Exception:
            rq = 0.0
        issued = round(float(alloc_map.get(fid, 0)), 2)
        rem = max(0.0, round(rq - issued, 4))
        sugg = round(rem, 2)
        row["qty_already_issued"] = issued
        row["qty_remaining_need"] = rem
        row["qty_issue_suggested"] = sugg
        row["qty_over_issued"] = round(max(0.0, issued - rq), 2) if issued > rq + 1e-6 else 0.0
        grand_issue += sugg
    return round(grand_issue, 2)


def _distribution_sale_to_farmers(cur, stock_product_id: int, per_farmer: dict[int, float], movement_note: str) -> tuple[int, float]:
    """Insert distribution, recipients, stock OUT, decrement inventory. Returns (distribution id, total qty issued)."""
    cleaned: dict[int, float] = {}
    for k, v in (per_farmer or {}).items():
        q = round(float(v), 2)
        if q > 0:
            cleaned[int(k)] = q
    if not cleaned:
        raise ValueError("No quantity to issue.")
    total_qty = round(sum(cleaned.values()), 2)
    if total_qty <= 0:
        raise ValueError("No quantity to issue.")

    cur.execute(
        "INSERT INTO product_inventory (product_id, quantity) VALUES (%s, 0) ON DUPLICATE KEY UPDATE product_id=product_id",
        (int(stock_product_id),),
    )
    cur.execute(
        "SELECT quantity FROM product_inventory WHERE product_id=%s LIMIT 1",
        (int(stock_product_id),),
    )
    inv = cur.fetchone() or {}
    available = float(inv.get("quantity") or 0)
    if total_qty > available:
        raise ValueError(f"Insufficient stock. Available: {available:.2f} — needed: {total_qty:.2f}")

    recipients_farmer_count = len(cleaned)
    cur.execute(
        """
        INSERT INTO product_distributions (
          product_id, quantity_per_recipient, total_quantity, recipients_count, note, created_by_employee_id
        ) VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (
            int(stock_product_id),
            None,
            float(total_qty),
            int(recipients_farmer_count),
            movement_note[:512] if movement_note else None,
            session.get("employee_id"),
        ),
    )
    dist_id = int(cur.lastrowid)

    if cleaned:
        placeholders = ",".join(["%s"] * len(cleaned))
        cur.execute(f"SELECT id, full_name FROM farmers WHERE id IN ({placeholders})", tuple(cleaned.keys()))
        farmer_rows = cur.fetchall()
        for r in farmer_rows:
            fid = int(r["id"])
            qty = cleaned.get(fid, 0)
            if qty <= 0:
                continue
            cur.execute(
                """
                INSERT INTO product_distribution_recipients (distribution_id, recipient_type, recipient_id, recipient_name, quantity)
                VALUES (%s,'FARMER',%s,%s,%s)
                """,
                (
                    dist_id,
                    fid,
                    (r.get("full_name") or "").strip().upper() or None,
                    qty,
                ),
            )

    cur.execute(
        "UPDATE product_inventory SET quantity = quantity - %s WHERE product_id=%s",
        (total_qty, int(stock_product_id)),
    )
    cur.execute(
        """
        INSERT INTO product_stock_movements (
          product_id, movement_type, quantity, stock_out_reason, note, created_by_employee_id
        ) VALUES (%s,'OUT',%s,'SALE',%s,%s)
        """,
        (
            int(stock_product_id),
            total_qty,
            f"DISTRIBUTION #{dist_id}" + (f" — {movement_note}" if movement_note else ""),
            session.get("employee_id"),
        ),
    )
    return dist_id, total_qty


@bp.route(
    "/<role>/module/crops_session/sessions/crop/<int:product_id>/required_products/product/<int:equipment_product_id>",
    methods=["GET", "POST"],
)
def crops_session_crop_required_product_farmers(role, product_id: int, equipment_product_id: int):
    """Per-farmer quantities for one catalogue input linked from crop activities."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for(
                "main.crops_session_crop_required_product_farmers",
                role=session_role,
                product_id=product_id,
                equipment_product_id=equipment_product_id,
            )
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    if request.method == "POST":
        validate_csrf()
        note_extra = (request.form.get("stock_out_note") or "").strip().upper() or None

        raw_ids = []
        for x in request.form.getlist("farmer_stock"):
            xs = str(x).strip()
            if xs.isdigit():
                raw_ids.append(int(xs))
        selected_ids = list(dict.fromkeys(raw_ids))

        redir = url_for(
            "main.crops_session_crop_required_product_farmers",
            role=session_role,
            product_id=product_id,
            equipment_product_id=equipment_product_id,
        )

        if not selected_ids:
            flash("Select at least one farmer to stock out.", "error")
            return redirect(redir)

        conn_post = get_connection()
        try:
            with conn_post.cursor() as cur:
                crop_product = _crop_product_session_guard(cur, product_id)
                if not crop_product:
                    abort(404)

                cur.execute(
                    """
                    SELECT id, crop_code, crop_name, crop_category, product_type,
                           COALESCE(NULLIF(TRIM(unit_of_measure), ''), 'UNIT') AS unit_of_measure
                    FROM products WHERE id=%s LIMIT 1
                    """,
                    (int(equipment_product_id),),
                )
                input_product = cur.fetchone()
                if not input_product:
                    abort(404)

                cur.execute(
                    """
                    SELECT 1 AS ok
                    FROM farm_activities fa
                    WHERE fa.product_id = %s
                      AND fa.equipment_product_id = %s
                      AND fa.activity_status = 'ACTIVE'
                      AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
                    LIMIT 1
                    """,
                    (int(product_id), int(equipment_product_id)),
                )
                if not cur.fetchone():
                    abort(404)

                picked_rows = _fetch_farmer_requirements_for_crop_equipment(
                    cur, int(product_id), int(equipment_product_id), selected_ids
                )
                fid_list = [int(r["farmer_id"]) for r in (picked_rows or [])]
                alloc_post = _farmer_distribution_totals_for_product(cur, int(equipment_product_id), fid_list)
        finally:
            conn_post.close()

        allowed = {int(r["farmer_id"]) for r in (picked_rows or [])}
        skipped = [fid for fid in selected_ids if fid not in allowed]
        per_farmer: dict[int, float] = {}
        for r in picked_rows or []:
            fid = int(r["farmer_id"])
            try:
                qv = float(r.get("qty_required") or 0)
            except Exception:
                qv = 0.0
            issued = round(float(alloc_post.get(fid, 0)), 2)
            remaining = max(0.0, round(qv - issued, 4))
            issue_amt = round(remaining, 2)
            if issue_amt > 0:
                per_farmer[fid] = issue_amt

        if skipped:
            flash(
                "Some selected farmers were skipped (no computed requirement for this item on this crop).",
                "warning",
            )

        if not per_farmer:
            flash(
                "No remaining quantity to issue for the selected farmers "
                "(already stocked out up to the computed requirement, or zero remaining).",
                "error",
            )
            return redirect(redir)

        crop_label = (crop_product.get("crop_code") or "").strip()
        inp_label = (input_product.get("crop_code") or "").strip()
        dist_note = (
            f"CROP SESSION STOCK — crop {crop_label} — input {inp_label}"
            + (f" — {note_extra}" if note_extra else "")
        )

        conn_do = None
        try:
            conn_do = get_connection()
            with conn_do.cursor() as cur:
                dist_id, tot = _distribution_sale_to_farmers(
                    cur,
                    int(equipment_product_id),
                    per_farmer,
                    dist_note,
                )
        except ValueError as e:
            flash(str(e), "error")
            return redirect(redir)
        finally:
            if conn_do is not None:
                conn_do.close()

        flash(
            (
                f"Stock out recorded (distribution #{dist_id}). Issued {tot:.2f} "
                f"{(input_product.get('unit_of_measure') or 'UNIT').upper()} to {len(per_farmer)} farmer(s)."
            ),
            "success",
        )
        return redirect(redir)

    conn = get_connection()
    stock_qty_available = 0.0
    try:
        with conn.cursor() as cur:
            crop_product = _crop_product_session_guard(cur, product_id)
            if not crop_product:
                abort(404)

            cur.execute(
                """
                SELECT id, crop_code, crop_name, crop_category, product_type,
                       COALESCE(NULLIF(TRIM(unit_of_measure), ''), 'UNIT') AS unit_of_measure
                FROM products WHERE id=%s LIMIT 1
                """,
                (int(equipment_product_id),),
            )
            input_product = cur.fetchone()
            if not input_product:
                abort(404)

            cur.execute(
                """
                SELECT 1 AS ok
                FROM farm_activities fa
                WHERE fa.product_id = %s
                  AND fa.equipment_product_id = %s
                  AND fa.activity_status = 'ACTIVE'
                  AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
                LIMIT 1
                """,
                (int(product_id), int(equipment_product_id)),
            )
            if not cur.fetchone():
                abort(404)

            farmer_rows = _fetch_farmer_requirements_for_crop_equipment(
                cur, int(product_id), int(equipment_product_id), None
            )
            fid_list = [int(r["farmer_id"]) for r in (farmer_rows or [])]
            alloc_map = _farmer_distribution_totals_for_product(cur, int(equipment_product_id), fid_list)
            grand_issue_suggested = _enrich_crop_issue_rows(list(farmer_rows or []), alloc_map)

            cur.execute(
                "SELECT COALESCE(quantity, 0) AS q FROM product_inventory WHERE product_id=%s LIMIT 1",
                (int(equipment_product_id),),
            )
            invr = cur.fetchone()
            try:
                stock_qty_available = float((invr or {}).get("q") or 0)
            except Exception:
                stock_qty_available = 0.0
    finally:
        conn.close()

    grand_qty = 0.0
    for row in farmer_rows or []:
        try:
            grand_qty += float(row.get("qty_required") or 0)
        except Exception:
            pass

    return render_template(
        "module_crops_session_required_product_detail.html",
        module_key="crops_session",
        module_title=f"{crop_product.get('crop_code') or ''} — Requirements by farmer",
        module_desc=(
            f"Quantities of {input_product.get('crop_code') or ''} {input_product.get('crop_name') or ''} "
            "from scheduled activities multiplied by each farmer’s session acreage."
        ),
        module_icon="users",
        crop_product=crop_product,
        input_product=input_product,
        manual_activity=None,
        manual_activity_label="",
        detail_kind="PRODUCT",
        unit_display=input_product.get("unit_of_measure") or "UNIT",
        farmer_rows=farmer_rows or [],
        grand_qty_required=round(grand_qty, 4),
        grand_issue_suggested=grand_issue_suggested,
        stock_qty_available=round(stock_qty_available, 4),
        stock_out_enabled=True,
    )


@bp.route(
    "/<role>/module/crops_session/sessions/crop/<int:product_id>/required_products/manual/<int:activity_id>"
)
def crops_session_crop_required_manual_farmers(role, product_id: int, activity_id: int):
    """Per-farmer quantities for one manual (non-stock) activity line."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for(
                "main.crops_session_crop_required_manual_farmers",
                role=session_role,
                product_id=product_id,
                activity_id=activity_id,
            )
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            crop_product = _crop_product_session_guard(cur, product_id)
            if not crop_product:
                abort(404)

            cur.execute(
                """
                SELECT id, activity_type, activity_name, equipment_tools,
                       COALESCE(NULLIF(TRIM(equipment_unit_of_measure), ''), 'UNIT') AS unit_of_measure
                FROM farm_activities
                WHERE id = %s AND product_id = %s
                  AND equipment_product_id IS NULL
                  AND activity_status = 'ACTIVE'
                  AND COALESCE(equipment_units_per_acre, 0) <> 0
                LIMIT 1
                """,
                (int(activity_id), int(product_id)),
            )
            activity_row = cur.fetchone()
            if not activity_row:
                abort(404)

            cur.execute(
                """
                SELECT
                  q.farmer_id,
                  q.farmer_code,
                  q.membership_number,
                  q.full_name,
                  q.phone_number,
                  COALESCE(ac.total_crop_acres, 0) AS farmer_crop_acres,
                  q.qty_required
                FROM (
                  SELECT
                    f.id AS farmer_id,
                    f.farmer_code,
                    f.membership_number,
                    f.full_name,
                    f.phone_number,
                    SUM(fs.acreage_used_acres * COALESCE(fa.equipment_units_per_acre, 0)) AS qty_required
                  FROM farm_activities fa
                  INNER JOIN farming_sessions fs
                    ON fs.product_id = fa.product_id AND fs.status = 'active'
                  INNER JOIN farmers f ON f.id = fs.farmer_id
                  WHERE fa.product_id = %s
                    AND fa.id = %s
                    AND fa.equipment_product_id IS NULL
                    AND fa.activity_status = 'ACTIVE'
                    AND COALESCE(fa.equipment_units_per_acre, 0) <> 0
                  GROUP BY f.id, f.farmer_code, f.membership_number, f.full_name, f.phone_number
                ) q
                LEFT JOIN (
                  SELECT farmer_id, SUM(acreage_used_acres) AS total_crop_acres
                  FROM farming_sessions
                  WHERE product_id = %s AND status = 'active'
                  GROUP BY farmer_id
                ) ac ON ac.farmer_id = q.farmer_id
                ORDER BY q.full_name ASC
                LIMIT 500
                """,
                (int(product_id), int(activity_id), int(product_id)),
            )
            farmer_rows = cur.fetchall()
    finally:
        conn.close()

    act_label = (activity_row.get("activity_name") or "").strip()
    tools = (activity_row.get("equipment_tools") or "").strip()
    if tools:
        act_label = f"{act_label} — {tools}" if act_label else tools

    grand_qty = 0.0
    for row in farmer_rows or []:
        try:
            grand_qty += float(row.get("qty_required") or 0)
        except Exception:
            pass

    return render_template(
        "module_crops_session_required_product_detail.html",
        module_key="crops_session",
        module_title=f"{crop_product.get('crop_code') or ''} — Requirements by farmer",
        module_desc=f"Manual input line for this crop session: {act_label or 'Activity'}.",
        module_icon="users",
        crop_product=crop_product,
        input_product=None,
        manual_activity=activity_row,
        manual_activity_label=act_label or "Activity",
        detail_kind="MANUAL",
        unit_display=activity_row.get("unit_of_measure") or "UNIT",
        farmer_rows=farmer_rows or [],
        grand_qty_required=round(grand_qty, 4),
        grand_issue_suggested=0.0,
        stock_qty_available=0.0,
        stock_out_enabled=False,
    )


@bp.route("/<role>/module/crops_session/sessions/crop/<int:product_id>/activities", methods=["GET", "POST"])
def crops_session_crop_activities(role, product_id: int):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for(
                "main.crops_session_crop_activities",
                role=session_role,
                product_id=product_id,
            )
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    if request.method == "POST":
        validate_csrf()

    activity_type = (request.form.get("activity_type") or "").strip().upper()
    activity_name = (request.form.get("activity_name") or "").strip()
    activity_description = (request.form.get("activity_description") or "").strip()
    equipment_tools = (request.form.get("equipment_tools") or "").strip()
    equipment_product_id_raw = (request.form.get("equipment_product_id") or "").strip()
    units_per_acre_raw = (request.form.get("equipment_units_per_acre") or "").strip()
    equipment_manual_unit_price_raw = (request.form.get("equipment_manual_unit_price") or "").strip()
    estimated_cost_raw = (request.form.get("estimated_cost") or "").strip()
    scheduled_day_raw = (request.form.get("scheduled_day") or "").strip()
    preferred_time = (request.form.get("preferred_time") or "").strip().upper()

    allowed_types = {"MECHANICAL", "CHEMICAL", "MANUAL", "IRRIGATION", "HARVESTING", "MONITORING"}
    allowed_times = {"MORNING", "AFTERNOON", "EVENING", "NIGHT"}

    edit_activity_row = None
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            edit_activity_id = request.args.get("edit", type=int)
            if edit_activity_id:
                cur.execute(
                    """
                    SELECT id, activity_type, activity_name, activity_description,
                           equipment_tools, equipment_product_id, equipment_units_per_acre,
                           equipment_unit_price, estimated_cost, scheduled_day, preferred_time,
                           activity_status
                    FROM farm_activities
                    WHERE id = %s AND product_id = %s
                    LIMIT 1
                    """,
                    (int(edit_activity_id), int(product_id)),
                )
                edit_activity_row = cur.fetchone()
                if edit_activity_id and not edit_activity_row:
                    flash("That activity could not be opened for editing.", "error")

            if request.method == "POST":
                activity_action = (request.form.get("activity_action") or "schedule").strip().lower()

                def _activity_belongs(aid: int) -> bool:
                    cur.execute(
                        "SELECT id FROM farm_activities WHERE id = %s AND product_id = %s LIMIT 1",
                        (int(aid), int(product_id)),
                    )
                    return cur.fetchone() is not None

                if activity_action == "delete":
                    try:
                        del_id = int(request.form.get("activity_id") or "0")
                    except Exception:
                        del_id = 0
                    if not del_id or not _activity_belongs(del_id):
                        flash("Activity not found or already removed.", "error")
                    else:
                        cur.execute(
                            "DELETE FROM farm_activities WHERE id = %s AND product_id = %s LIMIT 1",
                            (del_id, int(product_id)),
                        )
                        flash("Activity deleted.", "success")
                    return redirect(
                        url_for(
                            "main.crops_session_crop_activities",
                            role=session_role,
                            product_id=product_id,
                        )
                    )

                if activity_action == "suspend":
                    try:
                        sid = int(request.form.get("activity_id") or "0")
                    except Exception:
                        sid = 0
                    if not sid or not _activity_belongs(sid):
                        flash("Activity not found.", "error")
                    else:
                        cur.execute(
                            """
                            UPDATE farm_activities
                            SET activity_status = 'SUSPENDED'
                            WHERE id = %s AND product_id = %s AND activity_status = 'ACTIVE'
                            LIMIT 1
                            """,
                            (sid, int(product_id)),
                        )
                        flash("Activity suspended.", "success")
                    return redirect(
                        url_for(
                            "main.crops_session_crop_activities",
                            role=session_role,
                            product_id=product_id,
                        )
                    )

                if activity_action == "resume":
                    try:
                        rid = int(request.form.get("activity_id") or "0")
                    except Exception:
                        rid = 0
                    if not rid or not _activity_belongs(rid):
                        flash("Activity not found.", "error")
                    else:
                        cur.execute(
                            """
                            UPDATE farm_activities
                            SET activity_status = 'ACTIVE'
                            WHERE id = %s AND product_id = %s AND activity_status = 'SUSPENDED'
                            LIMIT 1
                            """,
                            (rid, int(product_id)),
                        )
                        flash("Activity resumed.", "success")
                    return redirect(
                        url_for(
                            "main.crops_session_crop_activities",
                            role=session_role,
                            product_id=product_id,
                        )
                    )

                update_activity_id: int | None = None
                if activity_action == "update":
                    try:
                        update_activity_id = int(request.form.get("activity_id") or "0")
                    except Exception:
                        update_activity_id = None
                    if not update_activity_id or not _activity_belongs(update_activity_id):
                        flash("Cannot update this activity.", "error")
                        return redirect(
                            url_for(
                                "main.crops_session_crop_activities",
                                role=session_role,
                                product_id=product_id,
                            )
                        )

                errors = []
                if activity_type not in allowed_types:
                    errors.append("Activity type is invalid.")
                if not activity_name:
                    errors.append("Activity name is required.")
                if preferred_time not in allowed_times:
                    errors.append("Preferred time is invalid.")

                try:
                    scheduled_day = int(scheduled_day_raw)
                    if scheduled_day < 0:
                        raise ValueError()
                except Exception:
                    scheduled_day = None
                    errors.append("Scheduled day must be a number (0 or higher).")

                # Equipment from stock (optional)
                equipment_product_id: int | None = None
                equipment_uom: str | None = None
                equipment_unit_price: float | None = None
                equipment_units_per_acre: float | None = None
                equipment_cost_per_acre: float | None = None
                equipment_label: str | None = None

                if equipment_product_id_raw:
                    try:
                        eq_id = int(equipment_product_id_raw)
                    except Exception:
                        eq_id = 0
                        errors.append("Selected equipment / tool is invalid.")
                    if eq_id:
                        cur.execute(
                            """
                            SELECT p.id, p.crop_code, p.crop_name, p.crop_category,
                                   p.product_type, p.unit_of_measure, p.package_size, p.status
                            FROM products p
                            WHERE p.id = %s LIMIT 1
                            """,
                            (eq_id,),
                        )
                        eq_row = cur.fetchone()
                        if (
                            not eq_row
                            or eq_row.get("status") != "active"
                            or (str(eq_row.get("product_type") or "").upper() == "CROP")
                        ):
                            errors.append("Selected equipment / tool is not available in stock.")
                        else:
                            equipment_product_id = int(eq_row["id"])
                            equipment_uom = (eq_row.get("unit_of_measure") or "").strip() or None
                            equipment_label = (
                                f"{eq_row['crop_code']} — {eq_row['crop_name']}"
                                f" ({eq_row.get('crop_category') or eq_row['product_type']})"
                            )
                            # Average buying price across ALL stock-in movements for this item.
                            cur.execute(
                                """
                                SELECT AVG(buying_price) AS avg_price
                                FROM product_stock_movements
                                WHERE product_id = %s AND movement_type = 'IN'
                                  AND buying_price IS NOT NULL
                                """,
                                (equipment_product_id,),
                            )
                            price_row = cur.fetchone()
                            if price_row and price_row.get("avg_price") is not None:
                                try:
                                    equipment_unit_price = round(float(price_row["avg_price"]), 2)
                                except Exception:
                                    equipment_unit_price = None

                            # Units per acre: auto-fetch from products.package_size,
                            # but a non-empty form value overrides.
                            product_units_per_acre = _parse_per_acre_amount(eq_row.get("package_size"))
                            if units_per_acre_raw:
                                try:
                                    upa = float(units_per_acre_raw)
                                    if upa < 0:
                                        raise ValueError()
                                    equipment_units_per_acre = upa
                                except Exception:
                                    errors.append(
                                        "Units of measure per acre must be a positive number."
                                    )
                            elif product_units_per_acre is not None:
                                equipment_units_per_acre = product_units_per_acre
                            else:
                                errors.append(
                                    "Selected item has no 'amount per acre' set in Products. "
                                    "Set it on the product or enter Units per acre here."
                                )

                            if (
                                equipment_units_per_acre is not None
                                and equipment_unit_price is not None
                            ):
                                equipment_cost_per_acre = round(
                                    equipment_units_per_acre * equipment_unit_price, 2
                                )

                            if not equipment_tools:
                                equipment_tools = equipment_label or ""

                elif not equipment_product_id_raw:
                    # No stock item: allow manual equipment text plus optional units / unit price.
                    if units_per_acre_raw:
                        try:
                            upa_m = float(units_per_acre_raw)
                            if upa_m < 0:
                                raise ValueError()
                            equipment_units_per_acre = upa_m
                        except Exception:
                            errors.append("Units per acre must be a positive number.")
                    if equipment_manual_unit_price_raw:
                        try:
                            mp = float(equipment_manual_unit_price_raw)
                            if mp < 0:
                                raise ValueError()
                            equipment_unit_price = round(mp, 2)
                        except Exception:
                            errors.append("Manual avg unit price must be a positive number.")
                    if (
                        equipment_units_per_acre is not None
                        and equipment_unit_price is not None
                    ):
                        equipment_cost_per_acre = round(
                            equipment_units_per_acre * equipment_unit_price, 2
                        )

                estimated_cost = None
                if estimated_cost_raw:
                    try:
                        estimated_cost = float(estimated_cost_raw)
                        if estimated_cost < 0:
                            raise ValueError()
                    except Exception:
                        errors.append("Estimated cost must be a positive number.")
                elif equipment_cost_per_acre is not None:
                    estimated_cost = equipment_cost_per_acre

                if errors:
                    for e in errors:
                        flash(e, "error")
                elif activity_action == "update" and update_activity_id:
                    cur.execute(
                        """
                        UPDATE farm_activities SET
                            activity_type=%s, activity_name=%s, activity_description=%s,
                            equipment_tools=%s, equipment_product_id=%s, equipment_unit_of_measure=%s,
                            equipment_units_per_acre=%s, equipment_unit_price=%s, equipment_cost_per_acre=%s,
                            estimated_cost=%s, scheduled_day=%s, preferred_time=%s
                        WHERE id=%s AND product_id=%s
                        LIMIT 1
                        """,
                        (
                            activity_type,
                            activity_name,
                            activity_description or None,
                            equipment_tools or None,
                            equipment_product_id,
                            equipment_uom,
                            equipment_units_per_acre,
                            equipment_unit_price,
                            equipment_cost_per_acre,
                            estimated_cost,
                            int(scheduled_day),
                            preferred_time,
                            int(update_activity_id),
                            int(product_id),
                        ),
                    )
                    flash("Activity updated.", "success")
                    return redirect(
                        url_for("main.crops_session_crop_activities", role=session_role, product_id=product_id)
                    )
                elif activity_action == "schedule":
                    cur.execute(
                        """
                        INSERT INTO farm_activities (
                            product_id, activity_type, activity_name, activity_description,
                            equipment_tools, equipment_product_id, equipment_unit_of_measure,
                            equipment_units_per_acre, equipment_unit_price, equipment_cost_per_acre,
                            estimated_cost, scheduled_day, preferred_time,
                            created_by_employee_id
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            int(product_id),
                            activity_type,
                            activity_name,
                            activity_description or None,
                            equipment_tools or None,
                            equipment_product_id,
                            equipment_uom,
                            equipment_units_per_acre,
                            equipment_unit_price,
                            equipment_cost_per_acre,
                            estimated_cost,
                            int(scheduled_day),
                            preferred_time,
                            session.get("employee_id"),
                        ),
                    )
                    flash("Activity scheduled.", "success")
                    return redirect(
                        url_for("main.crops_session_crop_activities", role=session_role, product_id=product_id)
                    )
                elif not errors:
                    flash("Invalid activity action.", "error")

            cur.execute(
                """
                SELECT a.id, a.activity_type, a.activity_name, a.activity_description,
                       a.equipment_tools, a.equipment_product_id, a.equipment_unit_of_measure,
                       a.equipment_units_per_acre, a.equipment_unit_price, a.equipment_cost_per_acre,
                       a.estimated_cost, a.scheduled_day, a.preferred_time, a.created_at,
                       a.activity_status, a.completed_on,
                       p.crop_code AS equipment_code, p.crop_name AS equipment_name,
                       p.crop_category AS equipment_category, p.product_type AS equipment_type
                FROM farm_activities a
                LEFT JOIN products p ON p.id = a.equipment_product_id
                WHERE a.product_id = %s
                ORDER BY a.scheduled_day ASC, a.created_at DESC
                LIMIT 500
                """,
                (int(product_id),),
            )
            activities = cur.fetchall()

            # Stock items (equipment / tools / inputs) for the picker.
            # Includes:
            #   - package_size (the products' "amount per acre" string)
            #   - average buying price across ALL stock-in movements
            cur.execute(
                """
                SELECT p.id, p.crop_code, p.product_type, p.crop_name, p.crop_category,
                       p.unit_of_measure, p.package_size,
                       COALESCE(i.quantity, 0) AS stock_quantity,
                       (
                         SELECT AVG(m.buying_price)
                         FROM product_stock_movements m
                         WHERE m.product_id = p.id
                           AND m.movement_type = 'IN'
                           AND m.buying_price IS NOT NULL
                       ) AS avg_unit_price
                FROM products p
                LEFT JOIN product_inventory i ON i.product_id = p.id
                WHERE p.status = 'active'
                  AND UPPER(p.product_type) <> 'CROP'
                ORDER BY p.crop_category ASC, p.product_type ASC, p.crop_name ASC
                LIMIT 1000
                """
            )
            stock_rows = cur.fetchall()
    finally:
        conn.close()

    equipment_picker_items = []
    for it in stock_rows:
        uom = (it.get("unit_of_measure") or "") or ""
        stock_qty = float(it.get("stock_quantity") or 0)
        avg_price = it.get("avg_unit_price")
        try:
            avg_price_f = float(avg_price) if avg_price is not None else None
        except Exception:
            avg_price_f = None
        units_per_acre_default = _parse_per_acre_amount(it.get("package_size"))
        equipment_picker_items.append(
            {
                "id": int(it["id"]),
                "code": it.get("crop_code") or "",
                "name": it.get("crop_name") or "",
                "category": it.get("crop_category") or "",
                "type": it.get("product_type") or "",
                "label": (
                    f"{it['crop_code']} — {it['product_type']} — {it['crop_name']} "
                    f"({it.get('crop_category') or 'GENERAL'}) — {uom or 'UNIT'}"
                ),
                "uom": uom,
                "stock": f"{stock_qty:.2f}",
                "package_size": (it.get("package_size") or "") or "",
                "units_per_acre": units_per_acre_default,
                "unit_price": (None if avg_price_f is None else round(avg_price_f, 2)),
            }
        )

    header_title = f"{product['crop_code'] or ''} — {product['crop_name'] or 'Crop'}"
    edit_mode_id = int(edit_activity_row["id"]) if edit_activity_row else None
    if (
        not edit_mode_id
        and request.method == "POST"
        and (request.form.get("activity_action") or "").strip().lower() == "update"
    ):
        try:
            edit_mode_id = int(request.form.get("activity_id") or "0") or None
        except Exception:
            edit_mode_id = None

    form_values = {
        "activity_type": activity_type,
        "activity_name": activity_name,
        "activity_description": activity_description,
        "equipment_tools": equipment_tools,
        "equipment_product_id": equipment_product_id_raw,
        "equipment_units_per_acre": units_per_acre_raw,
        "equipment_manual_unit_price": equipment_manual_unit_price_raw,
        "estimated_cost": estimated_cost_raw,
        "scheduled_day": scheduled_day_raw,
        "preferred_time": preferred_time,
    }
    if request.method == "GET" and edit_activity_row:
        er = edit_activity_row
        ep_id = er.get("equipment_product_id")
        upa = er.get("equipment_units_per_acre")
        ec = er.get("estimated_cost")
        eup = er.get("equipment_unit_price")
        form_values = {
            "activity_type": (er.get("activity_type") or ""),
            "activity_name": (er.get("activity_name") or ""),
            "activity_description": (er.get("activity_description") or ""),
            "equipment_tools": (er.get("equipment_tools") or ""),
            "equipment_product_id": str(ep_id) if ep_id else "",
            "equipment_units_per_acre": "" if upa is None else str(upa),
            "equipment_manual_unit_price": (
                ""
                if ep_id
                else ("" if eup is None else str(eup))
            ),
            "estimated_cost": "" if ec is None else str(ec),
            "scheduled_day": "" if er.get("scheduled_day") is None else str(er.get("scheduled_day")),
            "preferred_time": (er.get("preferred_time") or ""),
        }

    show_form = (
        request.method == "POST"
        or bool(edit_activity_row)
        or bool(edit_mode_id)
        or any(
            [
                bool(activity_type),
                bool(activity_name),
                bool(activity_description),
                bool(equipment_tools),
                bool(equipment_product_id_raw),
                bool(units_per_acre_raw),
                bool(equipment_manual_unit_price_raw),
                bool(estimated_cost_raw),
                bool(scheduled_day_raw),
                bool(preferred_time),
            ]
        )
    )
    return render_template(
        "module_crop_activities.html",
        module_key="crops_session",
        module_title=f"{header_title} — Activities",
        module_desc="Create and schedule farm activities by crop session day (days since session start).",
        module_icon="leaf",
        product=product,
        activities=activities,
        show_form=show_form,
        edit_mode_id=edit_mode_id,
        equipment_picker_items=equipment_picker_items,
        form_values=form_values,
    )


@bp.route("/<role>/module/farmers/<int:farmer_id>/crop/<int:product_id>", methods=["GET", "POST"])
def farmer_crop_session_detail(role, farmer_id: int, product_id: int):
    """Farmer profile in context of one crop: active farming session(s) for that product."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for(
                "main.farmer_crop_session_detail",
                role=session_role,
                farmer_id=farmer_id,
                product_id=product_id,
            )
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    if request.method == "POST":
        validate_csrf()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, crop_category, crop_image, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            cur.execute(
                """
                SELECT id, farmer_code, membership_number, full_name, national_id, phone_number,
                       county, ward, location, village, land_size, land_size_unit, status, profile_photo
                FROM farmers WHERE id=%s LIMIT 1
                """,
                (int(farmer_id),),
            )
            farmer = cur.fetchone()
            if not farmer:
                abort(404)

            cur.execute(
                """
                SELECT fs.id AS session_id, fs.season_name, fs.session_started_on, fs.session_ended_on,
                       fs.acreage_used_acres, fs.crop_or_activity, fs.land_area_notes, fs.notes, fs.status,
                       e.full_name AS registered_by_name
                FROM farming_sessions fs
                LEFT JOIN employees e ON e.id = fs.registered_by_employee_id
                WHERE fs.farmer_id=%s AND fs.product_id=%s AND fs.status='active'
                ORDER BY fs.session_started_on DESC
                LIMIT 50
                """,
                (int(farmer_id), int(product_id)),
            )
            crop_sessions = cur.fetchall()
            farm_acres = 0.0
            for s in (crop_sessions or []):
                try:
                    farm_acres += float(s.get("acreage_used_acres") or 0)
                except Exception:
                    pass

            # Session start reference for scheduled_day -> calendar date.
            session_start = None
            if crop_sessions:
                # Use the latest active session (already sorted DESC).
                session_start = crop_sessions[0].get("session_started_on")
            if session_start and not isinstance(session_start, date):
                try:
                    session_start = date.fromisoformat(str(session_start)[:10])
                except Exception:
                    session_start = None

            if request.method == "POST":
                action = (request.form.get("activity_action") or "").strip().lower()
                if action == "complete_activity":
                    try:
                        aid = int(request.form.get("activity_id") or "0")
                    except Exception:
                        aid = 0
                    done_date_raw = (request.form.get("done_date") or "").strip()
                    note = (request.form.get("completion_note") or "").strip()
                    if not aid:
                        flash("Select a valid activity.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))
                    if not note:
                        flash("Completion note is required.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))
                    try:
                        done_date = date.fromisoformat(done_date_raw)
                    except Exception:
                        done_date = None
                        flash("Select a valid completion date.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))

                    cur.execute(
                        """
                        SELECT id, scheduled_day
                        FROM farm_activities
                        WHERE id=%s AND product_id=%s
                        LIMIT 1
                        """,
                        (int(aid), int(product_id)),
                    )
                    arow = cur.fetchone()
                    if not arow:
                        flash("Activity not found.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))

                    scheduled_day = int(arow.get("scheduled_day") or 0)
                    if not session_start:
                        flash("Cannot complete activity: session start date is missing.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))

                    scheduled_on = session_start + timedelta(days=scheduled_day)
                    today = date.today()
                    if today < scheduled_on:
                        flash("This activity is not yet due (scheduled day not reached).", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))
                    if done_date < scheduled_on:
                        flash("Done date cannot be before the scheduled day.", "error")
                        return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))

                    cur.execute(
                        """
                        INSERT INTO farm_activity_completions (
                          activity_id, product_id, farmer_id,
                          completed_on, completion_note, completed_by_employee_id
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          completed_on = VALUES(completed_on),
                          completion_note = VALUES(completion_note),
                          completed_by_employee_id = VALUES(completed_by_employee_id)
                        """,
                        (int(aid), int(product_id), int(farmer_id), done_date, note, session.get("employee_id")),
                    )
                    flash("Activity marked as done.", "success")
                    return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))
                elif action:
                    flash("Unknown action.", "error")
                    return redirect(url_for("main.farmer_crop_session_detail", role=session_role, farmer_id=farmer_id, product_id=product_id))

            cur.execute(
                """
                SELECT a.id, a.activity_type, a.activity_name, a.activity_description,
                       a.equipment_tools, a.equipment_product_id, a.equipment_unit_of_measure,
                       a.equipment_units_per_acre, a.equipment_unit_price, a.equipment_cost_per_acre,
                       a.estimated_cost, a.scheduled_day, a.preferred_time, a.created_at,
                       a.activity_status,
                       fac.completed_on, fac.completion_note, fac.completed_by_employee_id,
                       p.crop_code AS equipment_code, p.crop_name AS equipment_name,
                       p.crop_category AS equipment_category, p.product_type AS equipment_type
                FROM farm_activities a
                LEFT JOIN products p ON p.id = a.equipment_product_id
                LEFT JOIN farm_activity_completions fac
                  ON fac.activity_id = a.id
                 AND fac.farmer_id = %s
                WHERE a.product_id = %s
                ORDER BY a.scheduled_day ASC, a.created_at DESC
                LIMIT 500
                """,
                (int(farmer_id), int(product_id)),
            )
            activities = cur.fetchall()

            # Compute progress labels for this farmer's timeline.
            today = date.today()
            for a in (activities or []):
                if session_start:
                    try:
                        sd = int(a.get("scheduled_day") or 0)
                    except Exception:
                        sd = 0
                    scheduled_on = session_start + timedelta(days=sd)
                    a["scheduled_on"] = scheduled_on.isoformat()
                    if a.get("completed_on"):
                        a["progress_status"] = "DONE"
                    elif a.get("activity_status") == "SUSPENDED":
                        a["progress_status"] = "SUSPENDED"
                    elif today < scheduled_on:
                        a["progress_status"] = "UPCOMING"
                    elif today == scheduled_on:
                        a["progress_status"] = "PENDING"
                    else:
                        a["progress_status"] = "OVERDUE"
                else:
                    a["scheduled_on"] = None
                    a["progress_status"] = "NOT_YET"
    finally:
        conn.close()

    page_title = f"{farmer.get('full_name') or 'Farmer'} — {product.get('crop_code') or ''} {product.get('crop_name') or ''}".strip()
    return render_template(
        "module_farmer_crop_detail.html",
        module_key="farmers",
        module_title=page_title,
        module_desc="This farmer’s active registration and details for the selected crop.",
        module_icon="user",
        product=product,
        farmer=farmer,
        crop_sessions=crop_sessions,
        activities=activities,
        farm_acres=round(float(farm_acres or 0), 4),
    )


@bp.route("/<role>/module/farmers/<int:farmer_id>/sessions")
def farmer_all_active_sessions(role, farmer_id: int):
    """All farming sessions for one farmer (from Farmers in session list)."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for("main.farmer_all_active_sessions", role=session_role, farmer_id=farmer_id)
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, farmer_code, membership_number, full_name, national_id, phone_number,
                       county, ward, location, village, land_size, land_size_unit, status, profile_photo
                FROM farmers WHERE id=%s LIMIT 1
                """,
                (int(farmer_id),),
            )
            farmer = cur.fetchone()
            if not farmer:
                abort(404)

            cur.execute(
                """
                SELECT fs.id AS session_id,
                       fs.farmer_id,
                       fs.product_id,
                       f.farmer_code, f.membership_number, f.full_name, f.phone_number, f.county, f.ward,
                       f.land_size, f.land_size_unit,
                       f.status AS farmer_status,
                       fs.season_name, fs.session_started_on, fs.session_ended_on, fs.acreage_used_acres,
                       fs.crop_or_activity, fs.status AS session_status, fs.land_area_notes,
                       p.crop_name AS product_crop_name, p.crop_code AS product_crop_code,
                       e.full_name AS registered_by_name
                FROM farming_sessions fs
                INNER JOIN farmers f ON f.id = fs.farmer_id
                LEFT JOIN products p ON p.id = fs.product_id
                LEFT JOIN employees e ON e.id = fs.registered_by_employee_id
                WHERE fs.farmer_id = %s
                ORDER BY fs.session_started_on DESC
                LIMIT 500
                """,
                (int(farmer_id),),
            )
            farmer_sessions_flat = cur.fetchall()
    finally:
        conn.close()

    active_session_acres = 0.0
    for r in farmer_sessions_flat:
        if str(r.get("session_status") or "").lower() != "active":
            continue
        try:
            active_session_acres += float(r.get("acreage_used_acres") or 0)
        except (TypeError, ValueError):
            pass
    active_session_acres = round(active_session_acres, 4)

    land_acres = _farmer_land_acres(farmer.get("land_size"), farmer.get("land_size_unit"))
    left_acres = None
    over_by_acres = None
    if land_acres is not None:
        diff = round(land_acres - active_session_acres, 4)
        if diff >= -0.01:
            left_acres = max(0.0, diff)
        else:
            over_by_acres = round(active_session_acres - land_acres, 4)

    farm_summary = {
        "total_farm_acres": land_acres,
        "session_acres": active_session_acres,
        "left_acres": left_acres,
        "over_by_acres": over_by_acres,
    }

    title_name = (farmer.get("full_name") or "Farmer").strip()
    return render_template(
        "module_farmer_all_sessions.html",
        module_key="farmers",
        module_title=f"{title_name} — Sessions",
        module_desc="",
        module_icon="leaf",
        farmer_id=int(farmer_id),
        farmer=farmer,
        farm_summary=farm_summary,
        session_rows=farmer_sessions_flat,
    )


@bp.route("/<role>/module/farmers/<int:farmer_id>/products")
def farmer_allocated_products(role, farmer_id: int):
    """Warehouse allocations (My products) for one farmer."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(
            url_for("main.farmer_allocated_products", role=session_role, farmer_id=farmer_id)
        )
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, farmer_code, membership_number, full_name, national_id, phone_number,
                       county, ward, location, village, land_size, land_size_unit, status, profile_photo
                FROM farmers WHERE id=%s LIMIT 1
                """,
                (int(farmer_id),),
            )
            farmer = cur.fetchone()
            if not farmer:
                abort(404)
            allocated_product_rows = _fetch_farmer_allocated_product_rows(cur, int(farmer_id))
    finally:
        conn.close()

    title_name = (farmer.get("full_name") or "Farmer").strip()
    return render_template(
        "module_farmer_my_products.html",
        module_key="farmers",
        module_title=f"{title_name} — My products",
        module_desc="Totals from warehouse distributions issued to this farmer.",
        module_icon="package",
        farmer_id=int(farmer_id),
        farmer=farmer,
        allocated_product_rows=allocated_product_rows,
    )


@bp.route("/<role>/module/products")
def module_products_legacy_redirect(role):
    """Old URL; crops-in-session list now lives under /module/crops_session."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.module_products_legacy_redirect", role=session_role))
    return redirect(url_for("main.module_page", role=session_role, module_key="crops_session"))


@bp.route("/<role>/module/<module_key>")
def module_page(role, module_key):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.module_page", role=session_role, module_key=module_key))

    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    module = next((m for m in MODULES if m[0] == module_key), None)
    if not module:
        abort(404)

    key, title, desc, icon = module
    if key == "administration":
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, full_name, email, national_id, login_code, role, status, created_at
                    FROM employees
                    ORDER BY created_at DESC
                    LIMIT 500
                    """
                )
                employees = cur.fetchall()
        finally:
            conn.close()
        return render_template(
            "module_hr.html",
            module_key=key,
            module_title=title,
            module_desc=desc,
            module_icon=icon,
            employees=employees,
        )

    if key == "farmers":
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT fs.id AS session_id,
                           fs.farmer_id,
                           f.farmer_code, f.membership_number, f.full_name, f.phone_number, f.county, f.ward,
                           f.land_size, f.land_size_unit,
                           f.status AS farmer_status,
                           fs.season_name, fs.session_started_on, fs.session_ended_on, fs.acreage_used_acres,
                           fs.crop_or_activity, fs.status AS session_status, fs.land_area_notes,
                           p.crop_name AS product_crop_name, p.crop_code AS product_crop_code,
                           e.full_name AS registered_by_name
                    FROM farming_sessions fs
                    INNER JOIN farmers f ON f.id = fs.farmer_id
                    LEFT JOIN products p ON p.id = fs.product_id
                    LEFT JOIN employees e ON e.id = fs.registered_by_employee_id
                    WHERE fs.status = 'active'
                    ORDER BY f.full_name ASC, fs.session_started_on DESC
                    LIMIT 500
                    """
                )
                farmer_sessions_flat = cur.fetchall()
        finally:
            conn.close()
        farmer_session_groups = _group_active_sessions_by_farmer(farmer_sessions_flat)
        return render_template(
            "module_farmers_sessions.html",
            module_key=key,
            module_title=title,
            module_desc=desc,
            module_icon=icon,
            farmer_session_groups=farmer_session_groups,
        )

    if key == "crops_session":
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.crop_code, p.product_type, p.crop_name, p.scientific_name, p.crop_category, p.crop_variety,
                           p.planting_season, p.growth_duration, p.water_requirement,
                           p.average_yield_range, p.average_yield_per_acre, p.average_yield_uom,
                           p.unit_of_measure, p.package_size,
                           p.crop_image, p.status, p.created_at,
                           (
                             SELECT COUNT(*) FROM farming_sessions fs
                             WHERE fs.product_id = p.id AND fs.status = 'active'
                           ) AS active_session_count,
                           (
                             SELECT COUNT(DISTINCT fs.farmer_id) FROM farming_sessions fs
                             WHERE fs.product_id = p.id AND fs.status = 'active'
                           ) AS farmers_in_session_count
                    FROM products p
                    WHERE p.status = 'active'
                      AND p.product_type = 'CROP'
                      AND EXISTS (
                        SELECT 1 FROM farming_sessions fs
                        WHERE fs.product_id = p.id AND fs.status = 'active'
                      )
                    ORDER BY p.crop_category ASC, p.crop_name ASC, p.created_at DESC
                    LIMIT 500
                    """
                )
                products_in_session = cur.fetchall()
        finally:
            conn.close()
        return render_template(
            "module_products_sessions.html",
            module_key=key,
            module_title=title,
            module_desc=desc,
            module_icon=icon,
            products_in_session=products_in_session,
        )

    return render_template("module_page.html", module_key=key, module_title=title, module_desc=desc, module_icon=icon)


@bp.route("/<role>/module/finance/crops_session")
def finance_crops_session(role):
    """Finance view: crops currently in active sessions (counts + acres)."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.finance_crops_session", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.id, p.crop_code, p.crop_name, p.crop_category,
                  sess.farmers_in_session_count,
                  sess.active_session_count,
                  sess.total_acres_in_session,
                  (sess.total_acres_in_session * COALESCE(act.estimated_cost_per_acre_sum, 0)) AS estimated_total_amount,
                  COALESCE(used.used_total_amount, 0) AS used_total_amount
                FROM products p
                INNER JOIN (
                  SELECT
                    product_id,
                    COUNT(*) AS active_session_count,
                    COUNT(DISTINCT farmer_id) AS farmers_in_session_count,
                    COALESCE(SUM(acreage_used_acres), 0) AS total_acres_in_session
                  FROM farming_sessions
                  WHERE status = 'active'
                  GROUP BY product_id
                ) sess ON sess.product_id = p.id
                LEFT JOIN (
                  SELECT
                    product_id,
                    COALESCE(SUM(CASE
                      WHEN activity_status = 'ACTIVE'
                      THEN COALESCE(equipment_cost_per_acre, estimated_cost)
                      ELSE 0
                    END), 0) AS estimated_cost_per_acre_sum
                  FROM farm_activities
                  GROUP BY product_id
                ) act ON act.product_id = p.id
                LEFT JOIN (
                  SELECT
                    fac.product_id,
                    COALESCE(SUM(fs.acreage_used_acres * COALESCE(a.equipment_cost_per_acre, a.estimated_cost)), 0) AS used_total_amount
                  FROM farm_activity_completions fac
                  INNER JOIN farming_sessions fs
                    ON fs.status = 'active'
                   AND fs.product_id = fac.product_id
                   AND fs.farmer_id = fac.farmer_id
                  INNER JOIN farm_activities a
                    ON a.id = fac.activity_id
                   AND a.product_id = fac.product_id
                  GROUP BY fac.product_id
                ) used ON used.product_id = p.id
                WHERE p.status = 'active'
                  AND UPPER(p.product_type) = 'CROP'
                ORDER BY estimated_total_amount DESC, sess.total_acres_in_session DESC, p.crop_name ASC
                LIMIT 500
                """
            )
            crops = cur.fetchall()
    finally:
        conn.close()

    return render_template(
        "finance_crops_session.html",
        module_key="finance",
        module_title="Crops in session finance",
        module_desc="Finance view of all crops currently in active farming sessions.",
        module_icon="wallet",
        crops=crops,
    )


@bp.route("/<role>/module/finance/crops_session/<int:product_id>")
def finance_crops_session_crop(role, product_id: int):
    """Finance view for one crop: activities + totals across all active sessions."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.finance_crops_session_crop", role=session_role, product_id=product_id))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, crop_code, crop_name, crop_category, product_type, status FROM products WHERE id=%s LIMIT 1",
                (int(product_id),),
            )
            product = cur.fetchone()
            if (
                not product
                or product.get("status") != "active"
                or (str(product.get("product_type") or "").upper() != "CROP")
            ):
                abort(404)

            cur.execute(
                """
                SELECT
                  COUNT(*) AS active_session_count,
                  COUNT(DISTINCT farmer_id) AS farmers_in_session_count,
                  COALESCE(SUM(acreage_used_acres), 0) AS total_acres_in_session
                FROM farming_sessions
                WHERE product_id = %s AND status = 'active'
                """,
                (int(product_id),),
            )
            stats = cur.fetchone() or {}
            try:
                total_acres = float(stats.get("total_acres_in_session") or 0)
            except Exception:
                total_acres = 0.0

            cur.execute(
                """
                SELECT a.id, a.activity_type, a.activity_name, a.activity_description,
                       a.equipment_tools, a.equipment_product_id, a.equipment_unit_of_measure,
                       a.equipment_units_per_acre, a.equipment_unit_price, a.equipment_cost_per_acre,
                       a.estimated_cost, a.scheduled_day, a.preferred_time, a.created_at,
                       a.activity_status,
                       COALESCE(used.completed_farmers_count, 0) AS completed_farmers_count,
                       COALESCE(used.used_acres, 0) AS used_acres,
                       p.crop_code AS equipment_code, p.crop_name AS equipment_name,
                       p.crop_category AS equipment_category, p.product_type AS equipment_type
                FROM farm_activities a
                LEFT JOIN products p ON p.id = a.equipment_product_id
                LEFT JOIN (
                  SELECT
                    fac.activity_id,
                    COUNT(DISTINCT fac.farmer_id) AS completed_farmers_count,
                    COALESCE(SUM(fs.acreage_used_acres), 0) AS used_acres
                  FROM farm_activity_completions fac
                  INNER JOIN farming_sessions fs
                    ON fs.status='active'
                   AND fs.product_id = fac.product_id
                   AND fs.farmer_id = fac.farmer_id
                  WHERE fac.product_id = %s
                  GROUP BY fac.activity_id
                ) used ON used.activity_id = a.id
                WHERE a.product_id = %s
                ORDER BY a.scheduled_day ASC, a.created_at DESC
                LIMIT 500
                """,
                (int(product_id), int(product_id)),
            )
            activities = cur.fetchall()
    finally:
        conn.close()

    # Pre-calc totals per activity for the template.
    grand_total_cost = 0.0
    grand_total_used_cost = 0.0
    for a in (activities or []):
        units = a.get("equipment_units_per_acre")
        cost_pa = a.get("equipment_cost_per_acre")
        if cost_pa is None:
            cost_pa = a.get("estimated_cost")
        try:
            units_f = float(units) if units is not None else None
        except Exception:
            units_f = None
        try:
            cost_pa_f = float(cost_pa) if cost_pa is not None else None
        except Exception:
            cost_pa_f = None

        a["units_per_acre_f"] = units_f
        a["cost_per_acre_f"] = cost_pa_f
        a["total_units_all_farmers"] = None if units_f is None else round(units_f * total_acres, 4)
        a["total_cost_all_farmers"] = None if cost_pa_f is None else round(cost_pa_f * total_acres, 2)
        try:
            used_acres = float(a.get("used_acres") or 0)
        except Exception:
            used_acres = 0.0
        a["total_cost_used_all_farmers"] = None if cost_pa_f is None else round(cost_pa_f * used_acres, 2)
        if a["total_cost_all_farmers"] is not None:
            grand_total_cost += float(a["total_cost_all_farmers"] or 0)
        if a["total_cost_used_all_farmers"] is not None:
            grand_total_used_cost += float(a["total_cost_used_all_farmers"] or 0)

    page_title = f"{product.get('crop_code') or ''} — {product.get('crop_name') or ''} — Finance".strip(" —")
    return render_template(
        "finance_crops_session_crop.html",
        module_key="finance",
        module_title=page_title,
        module_desc="Activities and totals across all farmers in session.",
        module_icon="wallet",
        product=product,
        stats=stats,
        total_acres=round(total_acres, 4),
        activities=activities,
        grand_total_cost=round(grand_total_cost, 2),
        grand_total_used_cost=round(grand_total_used_cost, 2),
    )


@bp.route("/<role>/module/finance/product_purchases")
def finance_product_purchases(role):
    """Finance view: product purchase analytics from stock-in movements."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.finance_product_purchases", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.id,
                  p.crop_code,
                  p.product_type,
                  p.crop_name,
                  p.crop_category,
                  p.unit_of_measure,
                  COALESCE(i.quantity, 0) AS stock_quantity,
                  s.avg_buying_price,
                  (COALESCE(i.quantity, 0) * COALESCE(s.avg_buying_price, 0)) AS est_stock_value
                FROM products p
                LEFT JOIN product_inventory i ON i.product_id = p.id
                LEFT JOIN (
                  SELECT
                    product_id,
                    AVG(CASE WHEN movement_type='IN' THEN buying_price ELSE NULL END) AS avg_buying_price
                  FROM product_stock_movements
                  GROUP BY product_id
                ) s ON s.product_id = p.id
                WHERE p.status = 'active'
                ORDER BY p.product_type ASC, p.crop_name ASC, p.crop_code ASC
                LIMIT 2000
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    product_count = len(rows or [])
    total_est_value = 0.0
    for r in rows or []:
        try:
            total_est_value += float(r.get("est_stock_value") or 0)
        except (TypeError, ValueError):
            pass

    return render_template(
        "finance_product_purchases.html",
        module_key="finance",
        module_title="Product purchase finance",
        module_desc="Purchase analytics across all products based on stock-in movements.",
        module_icon="wallet",
        rows=rows,
        product_count=product_count,
        total_est_value=round(total_est_value, 2),
    )


@bp.route("/<role>/module/finance/product_purchases/<int:product_id>")
def finance_product_purchase_detail(role, product_id: int):
    """Finance view: one product purchase analytics + recent transactions."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.finance_product_purchase_detail", role=session_role, product_id=product_id))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.crop_code, p.product_type, p.crop_name, p.crop_category, p.unit_of_measure, p.status
                FROM products p
                WHERE p.id=%s
                LIMIT 1
                """,
                (int(product_id),),
            )
            product = cur.fetchone()
            if not product or product.get("status") != "active":
                abort(404)

            cur.execute(
                "SELECT COALESCE(quantity,0) AS quantity FROM product_inventory WHERE product_id=%s LIMIT 1",
                (int(product_id),),
            )
            inv = cur.fetchone() or {}

            cur.execute(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN movement_type='IN' THEN quantity ELSE 0 END), 0) AS total_in_qty,
                  COALESCE(SUM(CASE WHEN movement_type='OUT' THEN quantity ELSE 0 END), 0) AS total_out_qty,
                  AVG(CASE WHEN movement_type='IN' THEN buying_price ELSE NULL END) AS avg_buying_price,
                  COALESCE(SUM(CASE WHEN movement_type='IN' THEN quantity * COALESCE(buying_price, 0) ELSE 0 END), 0) AS total_spent,
                  MAX(CASE WHEN movement_type='IN' THEN created_at ELSE NULL END) AS last_in_date,
                  (
                    SELECT m2.buying_price
                    FROM product_stock_movements m2
                    WHERE m2.product_id = %s
                      AND m2.movement_type = 'IN'
                      AND m2.buying_price IS NOT NULL
                    ORDER BY m2.created_at DESC, m2.id DESC
                    LIMIT 1
                  ) AS last_buying_price,
                  COUNT(DISTINCT CASE WHEN movement_type='IN' THEN supplier_name ELSE NULL END) AS suppliers_count
                FROM product_stock_movements
                WHERE product_id=%s
                """,
                (int(product_id), int(product_id)),
            )
            stats = cur.fetchone() or {}

            cur.execute(
                """
                SELECT m.id, m.movement_type, m.quantity, m.buying_price,
                       m.supplier_name, m.supplier_contact,
                       m.stock_out_reason, m.note, m.created_at,
                       e.full_name AS created_by_name,
                       GROUP_CONCAT(
                         CASE
                           WHEN pdr.recipient_type = 'FARMER'
                           THEN CONCAT(
                             REPLACE(REPLACE(COALESCE(pdr.recipient_name, f.full_name, 'Farmer'), '\t', ' '), '|', '/'),
                             '\t',
                             CAST(ROUND(COALESCE(pdr.quantity, 0), 4) AS CHAR)
                           )
                           ELSE NULL
                         END
                         ORDER BY COALESCE(pdr.recipient_name, f.full_name) ASC
                         SEPARATOR '|||'
                       ) AS allocation_detail
                FROM product_stock_movements m
                LEFT JOIN employees e ON e.id = m.created_by_employee_id
                LEFT JOIN product_distributions pd
                  ON pd.product_id = m.product_id
                 AND m.movement_type = 'OUT'
                 AND m.note LIKE CONCAT('DISTRIBUTION #', pd.id, '%%')
                LEFT JOIN product_distribution_recipients pdr
                  ON pdr.distribution_id = pd.id
                 AND pdr.recipient_type = 'FARMER'
                LEFT JOIN farmers f
                  ON f.id = pdr.recipient_id
                WHERE m.product_id=%s
                GROUP BY m.id
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 1000
                """,
                (int(product_id),),
            )
            transactions = cur.fetchall()
    finally:
        conn.close()

    try:
        stock_qty = float(inv.get("quantity") or 0)
    except Exception:
        stock_qty = 0.0
    try:
        avg_price = float(stats.get("avg_buying_price")) if stats.get("avg_buying_price") is not None else None
    except Exception:
        avg_price = None
    est_stock_value = None if avg_price is None else round(stock_qty * avg_price, 2)

    return render_template(
        "finance_product_purchase_detail.html",
        module_key="finance",
        module_title=f"{product.get('crop_code')} — {product.get('crop_name')} — Purchase finance",
        module_desc="Detailed purchase analytics and transactions for this item.",
        module_icon="wallet",
        product=product,
        stock_qty=stock_qty,
        stats=stats,
        est_stock_value=est_stock_value,
        transactions=transactions,
    )


@bp.route("/<role>/module/farmers/farming-session", methods=["GET", "POST"])
def farmer_farming_session(role):
    """Register farming sessions: crop-driven season & duration; acreage per farmer; land cover flags."""
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.farmer_farming_session", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    def _load_farmer_rows():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT f.id, f.farmer_code, f.membership_number, f.full_name, f.status,
                           f.land_size, f.land_size_unit,
                           COALESCE(
                               SUM(
                                   CASE WHEN fs.status = 'active' THEN fs.acreage_used_acres ELSE 0 END
                               ),
                               0
                           ) AS committed_acres
                    FROM farmers f
                    LEFT JOIN farming_sessions fs ON fs.farmer_id = f.id AND fs.status = 'active'
                    WHERE f.status = 'active'
                    GROUP BY f.id, f.farmer_code, f.membership_number, f.full_name, f.status,
                             f.land_size, f.land_size_unit
                    ORDER BY f.full_name ASC
                    LIMIT 1000
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        for fr in rows:
            ta = _farmer_land_acres(fr.get("land_size"), fr.get("land_size_unit"))
            committed = float(fr.get("committed_acres") or 0)
            fr["total_acres"] = ta
            fr["committed_acres_display"] = round(committed, 4)
            if ta is not None:
                fr["available_acres"] = round(max(0.0, ta - committed), 4)
            else:
                fr["available_acres"] = None
        return rows

    def _load_crop_products():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, crop_code, crop_name, planting_season, growth_duration
                    FROM products
                    WHERE status = 'active' AND product_type = 'CROP'
                    ORDER BY crop_name ASC, crop_code ASC
                    LIMIT 500
                    """
                )
                raw = cur.fetchall()
        finally:
            conn.close()
        out = []
        for r in raw:
            out.append(
                {
                    **r,
                    "growth_days": _parse_growth_duration_to_days(r.get("growth_duration")),
                }
            )
        return out

    def _parse_date(raw: str) -> str | None:
        s = (raw or "").strip()
        return s if s else None

    def _redirect_back_to_form(farmer_ids: list[int] | None):
        base = url_for("main.farmer_farming_session", role=session_role)
        if not farmer_ids:
            return redirect(base)
        q = urlencode([("farmer_id", i) for i in farmer_ids])
        return redirect(f"{base}?{q}")

    if request.method == "POST":
        validate_csrf()
        raw_farmer_ids = request.form.getlist("farmer_id")
        farmer_ids: list[int] = []
        seen: set[int] = set()
        for x in raw_farmer_ids:
            s = str(x).strip()
            if s.isdigit():
                n = int(s)
                if n not in seen:
                    seen.add(n)
                    farmer_ids.append(n)

        if not farmer_ids:
            flash("Select at least one farmer.", "error")
            return redirect(url_for("main.farmer_farming_session", role=session_role))

        pid_raw = (request.form.get("product_id") or "").strip()
        if not pid_raw.isdigit():
            flash("Select a registered crop.", "error")
            return _redirect_back_to_form(farmer_ids)

        started = _parse_date(request.form.get("session_started_on") or "")
        if not started:
            flash("Session start date is required.", "error")
            return _redirect_back_to_form(farmer_ids)

        land_notes = ((request.form.get("land_area_notes") or "").strip().upper() or None)
        notes = ((request.form.get("notes") or "").strip().upper() or None)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, planting_season, growth_duration, crop_name
                    FROM products
                    WHERE id=%s AND status='active' AND product_type='CROP'
                    LIMIT 1
                    """,
                    (int(pid_raw),),
                )
                pr = cur.fetchone()
                if not pr:
                    flash("Invalid crop selection.", "error")
                    return _redirect_back_to_form(farmer_ids)

                growth_days = _parse_growth_duration_to_days(pr.get("growth_duration"))
                ended = _iso_date_plus_days(started, growth_days)
                season = ((pr.get("planting_season") or "").strip().upper() or None)
                crop_label = ((pr.get("crop_name") or "").strip().upper() or None)

                ph = ",".join(["%s"] * len(farmer_ids))
                cur.execute(
                    f"""
                    SELECT f.id, f.land_size, f.land_size_unit,
                           COALESCE(
                               SUM(
                                   CASE WHEN fs.status = 'active' THEN fs.acreage_used_acres ELSE 0 END
                               ),
                               0
                           ) AS committed_acres
                    FROM farmers f
                    LEFT JOIN farming_sessions fs ON fs.farmer_id = f.id AND fs.status = 'active'
                    WHERE f.id IN ({ph}) AND f.status = 'active'
                    GROUP BY f.id, f.land_size, f.land_size_unit
                    """,
                    tuple(farmer_ids),
                )
                fmap = {int(r["id"]): r for r in cur.fetchall()}
                if len(fmap) != len(farmer_ids):
                    flash("One or more farmers are invalid or not active.", "error")
                    return _redirect_back_to_form(farmer_ids)

                per_acres: dict[int, float] = {}
                for fid in farmer_ids:
                    raw_a = request.form.get(f"farmer_session_acres_{fid}")
                    try:
                        a = float(str(raw_a).strip().replace(",", "."))
                    except (TypeError, ValueError):
                        a = -1.0
                    if a <= 0:
                        flash("Enter a positive acreage for each selected farmer.", "error")
                        return _redirect_back_to_form(farmer_ids)

                    fm = fmap[fid]
                    committed = float(fm.get("committed_acres") or 0)
                    total = _farmer_land_acres(fm.get("land_size"), fm.get("land_size_unit"))
                    available = max(0.0, total - committed) if total is not None else None
                    if available is not None and a > available + 0.02:
                        flash(
                            "Acreage for one or more farmers exceeds available planting land on record.",
                            "error",
                        )
                        return _redirect_back_to_form(farmer_ids)
                    per_acres[fid] = round(a, 4)

                emp_id = session.get("employee_id")
                insert_sql = """
                    INSERT INTO farming_sessions (
                      farmer_id, product_id, season_name, session_started_on, session_ended_on,
                      acreage_used_acres, crop_or_activity, land_area_notes, notes, status,
                      registered_by_employee_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                for fid in farmer_ids:
                    cur.execute(
                        insert_sql,
                        (
                            fid,
                            int(pid_raw),
                            season,
                            started,
                            ended,
                            per_acres[fid],
                            crop_label,
                            land_notes,
                            notes,
                            "active",
                            emp_id,
                        ),
                    )
                    _refresh_farmer_farming_land(cur, fid)
        finally:
            conn.close()

        n = len(farmer_ids)
        flash(
            f"Farming session registered for {n} farmer{'s' if n != 1 else ''}. "
            "Land session status updated from acreage vs farm size.",
            "success",
        )
        return redirect(url_for("main.module_page", role=session_role, module_key="farmers"))

    raw_fid_params = request.args.getlist("farmer_id")
    pre_ids: list[int] = []
    pre_seen: set[int] = set()
    for x in raw_fid_params:
        s = str(x).strip()
        if s.isdigit():
            n = int(s)
            if n not in pre_seen:
                pre_seen.add(n)
                pre_ids.append(n)

    farmers_list = _load_farmer_rows()
    active_ids = {int(f["id"]) for f in farmers_list}
    selected_farmer_ids = [i for i in pre_ids if i in active_ids]
    crop_products = _load_crop_products()

    return render_template(
        "farmer_farming_session.html",
        module_key="farmers",
        module_title="Farmers management",
        module_desc="Manage farmers, registrations, and records.",
        module_icon="users",
        farmers_for_select=farmers_list,
        selected_farmer_ids=selected_farmer_ids,
        crop_products=crop_products,
    )


@bp.route("/<role>/module/warehouse/<section_key>")
def warehouse_section(role, section_key):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.warehouse_section", role=session_role, section_key=section_key))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    section_key = (section_key or "").strip().lower()
    sections = {
        "stock": ("Stock management", "Manage stock items, quantities, and movements.", "boxes"),
        "distribution": ("Distribution section", "Track issuing, dispatch, and distribution records.", "truck"),
        "audits": ("Audits", "Stock takes, variance checks, and audit trails.", "shield"),
    }
    sec = sections.get(section_key)
    if not sec:
        abort(404)
    title, desc, icon = sec
    return render_template(
        "warehouse_section.html",
        module_key="warehouse",
        module_title="Warehouse",
        module_desc="Stock, warehousing, issuing, and inventory.",
        module_icon="boxes",
        section_key=section_key,
        section_title=title,
        section_desc=desc,
        section_icon=icon,
    )


@bp.route("/<role>/module/warehouse/stock", methods=["GET", "POST"])
def warehouse_stock(role):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.warehouse_stock", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    if request.method == "POST":
        validate_csrf()
        action = (request.form.get("action") or "").strip().lower()
        product_id = (request.form.get("product_id") or "").strip()

        if not product_id.isdigit():
            flash("Select a valid item.", "error")
            return redirect(url_for("main.warehouse_stock", role=session_role))

        qty_raw = (request.form.get("quantity") or "").strip()
        try:
            qty = float(qty_raw)
        except ValueError:
            qty = -1
        if qty <= 0:
            flash("Quantity must be a positive number.", "error")
            return redirect(url_for("main.warehouse_stock", role=session_role))

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Ensure product exists and is active
                cur.execute("SELECT id, crop_name, status FROM products WHERE id=%s LIMIT 1", (int(product_id),))
                p = cur.fetchone()
                if not p or p.get("status") != "active":
                    flash("Item not found or not active.", "error")
                    return redirect(url_for("main.warehouse_stock", role=session_role))

                # Ensure inventory row exists
                cur.execute(
                    "INSERT INTO product_inventory (product_id, quantity) VALUES (%s, 0) ON DUPLICATE KEY UPDATE product_id=product_id",
                    (int(product_id),),
                )

                if action == "stock_in":
                    buying_price_raw = (request.form.get("buying_price") or "").strip()
                    try:
                        buying_price = float(buying_price_raw)
                    except ValueError:
                        buying_price = -1
                    supplier_name = (request.form.get("supplier_name") or "").strip().upper()
                    supplier_contact = (request.form.get("supplier_contact") or "").strip().upper()

                    if buying_price < 0:
                        flash("Buying price must be a number (0 or more).", "error")
                        return redirect(url_for("main.warehouse_stock", role=session_role))
                    if not supplier_name:
                        flash("Supplier name is required.", "error")
                        return redirect(url_for("main.warehouse_stock", role=session_role))
                    if not supplier_contact:
                        flash("Supplier contact number is required.", "error")
                        return redirect(url_for("main.warehouse_stock", role=session_role))

                    _upsert_supplier_row(cur, supplier_name, supplier_contact)

                    cur.execute(
                        "UPDATE product_inventory SET quantity = quantity + %s WHERE product_id=%s",
                        (qty, int(product_id)),
                    )
                    cur.execute(
                        """
                        INSERT INTO product_stock_movements (
                            product_id, movement_type, quantity,
                            buying_price, supplier_name, supplier_contact,
                            created_by_employee_id
                        ) VALUES (%s,'IN',%s,%s,%s,%s,%s)
                        """,
                        (int(product_id), qty, buying_price, supplier_name, supplier_contact, session.get("employee_id")),
                    )
                    flash("Stock in recorded.", "success")

                elif action == "stock_out":
                    reason = (request.form.get("reason") or "").strip().upper()
                    note = (request.form.get("note") or "").strip().upper() or None
                    allowed = {"SALE", "DAMAGE", "EXPIRED", "TRANSFER", "SAMPLE", "ADJUSTMENT", "OTHER"}
                    if reason not in allowed:
                        flash("Select a valid stock out reason.", "error")
                        return redirect(url_for("main.warehouse_stock", role=session_role))

                    cur.execute("SELECT quantity FROM product_inventory WHERE product_id=%s LIMIT 1", (int(product_id),))
                    inv = cur.fetchone() or {}
                    available = float(inv.get("quantity") or 0)
                    if qty > available:
                        flash(f"Insufficient stock. Available: {available:.2f}", "error")
                        return redirect(url_for("main.warehouse_stock", role=session_role))

                    cur.execute(
                        "UPDATE product_inventory SET quantity = quantity - %s WHERE product_id=%s",
                        (qty, int(product_id)),
                    )
                    cur.execute(
                        """
                        INSERT INTO product_stock_movements (
                            product_id, movement_type, quantity,
                            stock_out_reason, note,
                            created_by_employee_id
                        ) VALUES (%s,'OUT',%s,%s,%s,%s)
                        """,
                        (int(product_id), qty, reason, note, session.get("employee_id")),
                    )
                    flash("Stock out recorded.", "success")
                else:
                    flash("Unknown action.", "error")
        finally:
            conn.close()

        return redirect(url_for("main.warehouse_stock", role=session_role))

    # GET: list active items + quantities
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.crop_code, p.product_type, p.crop_name, p.crop_category, p.crop_variety,
                       p.unit_of_measure,
                       p.status,
                       COALESCE(i.quantity, 0) AS stock_quantity,
                       EXISTS (
                         SELECT 1 FROM farming_sessions fs
                         WHERE fs.product_id = p.id AND fs.status = 'active'
                       ) AS in_session
                FROM products p
                LEFT JOIN product_inventory i ON i.product_id = p.id
                WHERE p.status = 'active'
                ORDER BY EXISTS (
                         SELECT 1 FROM farming_sessions fs
                         WHERE fs.product_id = p.id AND fs.status = 'active'
                       ) DESC,
                       p.product_type ASC, p.crop_category ASC, p.crop_name ASC
                LIMIT 1000
                """
            )
            items = cur.fetchall()
    finally:
        conn.close()

    picker_items = []
    for it in items:
        uom = (it.get("unit_of_measure") or "") or ""
        stock_qty = float(it.get("stock_quantity") or 0)
        picker_items.append(
            {
                "id": int(it["id"]),
                "label": (
                    f"{it['crop_code']} — {it['product_type']} — {it['crop_name']} "
                    f"({it['crop_category']}) — {uom or 'UNIT'}"
                ),
                "uom": uom,
                "stock": f"{stock_qty:.2f}",
                "in_session": bool(it.get("in_session")),
            }
        )

    return render_template(
        "warehouse_stock.html",
        module_key="warehouse",
        module_title="Warehouse",
        module_desc="Stock, warehousing, issuing, and inventory.",
        module_icon="boxes",
        section_key="stock",
        section_title="Stock management",
        section_desc="Stock in and stock out items. Quantities update instantly.",
        section_icon="boxes",
        items=items,
        picker_items=picker_items,
    )


@bp.route("/<role>/module/warehouse/stock/transactions")
def warehouse_stock_transactions(role):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.warehouse_stock_transactions", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id,
                       m.movement_type,
                       m.quantity,
                       m.buying_price,
                       m.supplier_name,
                       m.supplier_contact,
                       m.stock_out_reason,
                       m.note,
                       m.created_at,
                       m.created_by_employee_id,
                       p.crop_code,
                       p.product_type,
                       p.crop_name,
                       p.crop_category,
                       p.crop_variety,
                       p.unit_of_measure,
                       e.full_name AS created_by_name
                FROM product_stock_movements m
                INNER JOIN products p ON p.id = m.product_id
                LEFT JOIN employees e ON e.id = m.created_by_employee_id
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 5000
                """
            )
            transactions = cur.fetchall()
    finally:
        conn.close()

    return render_template(
        "warehouse_stock_transactions.html",
        module_key="warehouse",
        module_title="Warehouse",
        module_desc="Stock, warehousing, issuing, and inventory.",
        module_icon="boxes",
        section_key="stock_transactions",
        section_title="Stock transactions",
        section_desc="Audit trail of all stock movements (ins, outs, and distribution-linked outs).",
        section_icon="file_text",
        transactions=transactions,
    )


@bp.route("/<role>/module/warehouse/distribution", methods=["GET", "POST"])
def warehouse_distribution(role):
    session_role = _require_session_role(role)
    if not session_role:
        return redirect(url_for("auth.login"))
    if session_role != role:
        return redirect(url_for("main.warehouse_distribution", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Products that have at least one active farming registration
            cur.execute(
                """
                SELECT DISTINCT p.id, p.crop_code, p.product_type, p.crop_name, p.crop_category, p.unit_of_measure,
                       p.package_size,
                       COALESCE(i.quantity, 0) AS stock_quantity
                FROM products p
                INNER JOIN farming_sessions fs ON fs.product_id = p.id AND fs.status = 'active'
                LEFT JOIN product_inventory i ON i.product_id = p.id
                WHERE p.status='active'
                ORDER BY p.product_type ASC, p.crop_category ASC, p.crop_name ASC
                LIMIT 1000
                """
            )
            products = cur.fetchall()

            cur.execute(
                """
                SELECT fs.id AS farming_session_id,
                       fs.farmer_id,
                       fs.product_id,
                       fs.acreage_used_acres AS session_acres,
                       f.farmer_code, f.full_name, f.phone_number, f.sub_county, f.ward, f.status,
                       f.land_size, f.land_size_unit,
                       p.crop_code AS reg_crop_code,
                       p.crop_name AS reg_crop_name,
                       p.crop_category AS reg_crop_category,
                       p.unit_of_measure AS reg_unit_of_measure,
                       COALESCE(dsum.alloc_qty, 0) AS crop_allocated_qty
                FROM farming_sessions fs
                INNER JOIN farmers f ON f.id = fs.farmer_id AND f.status = 'active'
                INNER JOIN products p ON p.id = fs.product_id AND p.status = 'active'
                LEFT JOIN (
                    SELECT pdr.recipient_id AS farmer_id,
                           pd.product_id,
                           SUM(COALESCE(pdr.quantity, 0)) AS alloc_qty
                    FROM product_distribution_recipients pdr
                    INNER JOIN product_distributions pd ON pd.id = pdr.distribution_id
                    WHERE pdr.recipient_type = 'FARMER'
                    GROUP BY pdr.recipient_id, pd.product_id
                ) dsum ON dsum.farmer_id = fs.farmer_id AND dsum.product_id = fs.product_id
                WHERE fs.status = 'active'
                ORDER BY f.full_name ASC, p.crop_name ASC
                LIMIT 2000
                """
            )
            distribution_sessions = cur.fetchall()
            for row in distribution_sessions:
                row["land_acres"] = _farmer_land_acres(row.get("land_size"), row.get("land_size_unit"))
                sa = row.get("session_acres")
                row["session_acres_display"] = float(sa) if sa is not None else 0.0
                row["crop_allocated_qty"] = float(row.get("crop_allocated_qty") or 0)

            farmer_session_sum = defaultdict(float)
            for row in distribution_sessions:
                farmer_session_sum[int(row["farmer_id"])] += float(row.get("session_acres") or 0)
            for row in distribution_sessions:
                fid = int(row["farmer_id"])
                mine = float(row.get("session_acres") or 0)
                other_active = farmer_session_sum[fid] - mine
                land = row["land_acres"]
                if land is not None:
                    row["max_dist_acres"] = round(max(0.0, float(land) - other_active), 4)
                else:
                    row["max_dist_acres"] = None

    finally:
        conn.close()

    if request.method == "POST":
        validate_csrf()

        product_id = (request.form.get("product_id") or "").strip()
        if not product_id.isdigit():
            flash("Select a valid product.", "error")
            return redirect(url_for("main.warehouse_distribution", role=session_role))

        session_ids_int = [int(x) for x in request.form.getlist("distribution_session_ids") if str(x).strip().isdigit()]
        if not session_ids_int:
            flash("Select at least one registered crop session to distribute to.", "error")
            return redirect(url_for("main.warehouse_distribution", role=session_role))

        note = (request.form.get("note") or "").strip().upper() or None

        qty_mode = (request.form.get("distribution_qty_mode") or "land").strip().lower()
        if qty_mode not in {"land", "manual"}:
            qty_mode = "land"

        def _qty_for_session(sid: str) -> float:
            raw = (request.form.get(f"farmer_qty_{sid}") or "").strip()
            return float(raw)

        per_farmer: dict[int, float] = {}
        errors: list[str] = []
        total_qty = 0.0

        per_acre: float | None = None
        if qty_mode == "land":
            conn_pa = get_connection()
            try:
                with conn_pa.cursor() as cur:
                    cur.execute(
                        "SELECT package_size FROM products WHERE id=%s AND status='active' LIMIT 1",
                        (int(product_id),),
                    )
                    pr = cur.fetchone()
                    if pr:
                        per_acre = _parse_per_acre_amount(pr.get("package_size"))
            finally:
                conn_pa.close()
            if per_acre is None:
                errors.append("Land-based mode requires an amount per acre on the product (set under Products).")

        sess_rows: list = []
        by_farmer_sessions: dict[int, list[tuple[int, float]]] = defaultdict(list)
        conn_sess = get_connection()
        try:
            with conn_sess.cursor() as cur:
                ph = ",".join(["%s"] * len(session_ids_int))
                cur.execute(
                    f"""
                    SELECT fs.id AS farming_session_id, fs.farmer_id, fs.product_id, fs.acreage_used_acres,
                           f.land_size, f.land_size_unit
                    FROM farming_sessions fs
                    INNER JOIN farmers f ON f.id = fs.farmer_id AND f.status = 'active'
                    INNER JOIN products p ON p.id = fs.product_id AND p.status = 'active'
                    WHERE fs.status = 'active' AND fs.id IN ({ph})
                    """,
                    tuple(session_ids_int),
                )
                sess_rows = cur.fetchall()
                farmer_ids_set = {int(r["farmer_id"]) for r in sess_rows}
                if farmer_ids_set:
                    phf = ",".join(["%s"] * len(farmer_ids_set))
                    cur.execute(
                        f"""
                        SELECT id, farmer_id, acreage_used_acres
                        FROM farming_sessions
                        WHERE status = 'active' AND farmer_id IN ({phf})
                        """,
                        tuple(farmer_ids_set),
                    )
                    for srow in cur.fetchall():
                        by_farmer_sessions[int(srow["farmer_id"])].append(
                            (int(srow["id"]), float(srow["acreage_used_acres"] or 0))
                        )
        finally:
            conn_sess.close()

        found_ids = {int(r["farming_session_id"]) for r in sess_rows}
        if found_ids != set(session_ids_int):
            errors.append("One or more selected registrations are invalid or no longer active.")

        if not errors:
            for r in sess_rows:
                if int(r["product_id"]) != int(product_id):
                    errors.append(
                        "Each selection must match the product chosen above (registered crop for that farmer)."
                    )
                    break

        pending: defaultdict[int, float] = defaultdict(float)
        session_land_updates: list[tuple[int, int, float]] = []
        if not errors:
            for r in sess_rows:
                sid = str(int(r["farming_session_id"]))
                sid_int = int(r["farming_session_id"])
                fid = int(r["farmer_id"])
                sess_stored = float(r["acreage_used_acres"] or 0)

                acres_typed = _parse_dist_acres_input(request.form.get(f"session_dist_acres_{sid}"))
                eff_acres = acres_typed if acres_typed is not None else sess_stored

                other_active = sum(
                    ac for oid, ac in by_farmer_sessions.get(fid, []) if oid != sid_int
                )
                land = _farmer_land_acres(r.get("land_size"), r.get("land_size_unit"))
                if land is not None:
                    max_allowed = max(0.0, float(land) - other_active)
                    if eff_acres > max_allowed + 0.02:
                        errors.append(
                            "Acres to use for at least one farmer exceed available land on record "
                            f"(after other active sessions, max is {max_allowed:.2f} ac for that registration)."
                        )
                        break
                if eff_acres <= 0:
                    errors.append(
                        "Enter session acres greater than zero (or use the allocated session acres shown)."
                    )
                    break

                if qty_mode == "land":
                    line_qty = round(float(eff_acres) * float(per_acre), 4)
                    session_land_updates.append((sid_int, fid, round(float(eff_acres), 4)))
                else:
                    try:
                        line_qty = _qty_for_session(sid)
                    except Exception:
                        line_qty = -1
                    if line_qty <= 0:
                        errors.append(
                            "Each selected registration needs a valid quantity greater than zero."
                        )
                        break

                pending[fid] += line_qty

            if not errors:
                per_farmer = dict(pending)
                total_qty = sum(per_farmer.values())

        if errors:
            for e in errors:
                flash(e, "error")
            return redirect(url_for("main.warehouse_distribution", role=session_role))

        recipients_farmer_count = len(per_farmer)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Ensure inventory row exists
                cur.execute(
                    "INSERT INTO product_inventory (product_id, quantity) VALUES (%s, 0) ON DUPLICATE KEY UPDATE product_id=product_id",
                    (int(product_id),),
                )
                cur.execute("SELECT quantity FROM product_inventory WHERE product_id=%s LIMIT 1", (int(product_id),))
                inv = cur.fetchone() or {}
                available = float(inv.get("quantity") or 0)
                if total_qty > available:
                    flash(f"Insufficient stock. Available: {available:.2f} — Needed: {total_qty:.2f}", "error")
                    return redirect(url_for("main.warehouse_distribution", role=session_role))

                for sid_int, _fid, eff in session_land_updates:
                    cur.execute(
                        """
                        UPDATE farming_sessions
                        SET acreage_used_acres=%s
                        WHERE id=%s AND status='active'
                        """,
                        (float(eff), sid_int),
                    )
                for uf in {f for _, f, _ in session_land_updates}:
                    _refresh_farmer_farming_land(cur, uf)

                # Create distribution record
                cur.execute(
                    """
                    INSERT INTO product_distributions (
                      product_id, quantity_per_recipient, total_quantity, recipients_count, note, created_by_employee_id
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        int(product_id),
                        None,
                        float(total_qty),
                        int(recipients_farmer_count),
                        note,
                        session.get("employee_id"),
                    ),
                )
                dist_id = int(cur.lastrowid)

                farmer_rows = []
                if per_farmer:
                    placeholders = ",".join(["%s"] * len(per_farmer))
                    cur.execute(f"SELECT id, full_name FROM farmers WHERE id IN ({placeholders})", tuple(per_farmer.keys()))
                    farmer_rows = cur.fetchall()

                for r in farmer_rows:
                    cur.execute(
                        """
                        INSERT INTO product_distribution_recipients (distribution_id, recipient_type, recipient_id, recipient_name, quantity)
                        VALUES (%s,'FARMER',%s,%s,%s)
                        """,
                        (
                            dist_id,
                            int(r["id"]),
                            (r.get("full_name") or "").strip().upper() or None,
                            float(per_farmer.get(int(r["id"]), 0)),
                        ),
                    )

                cur.execute(
                    "UPDATE product_inventory SET quantity = quantity - %s WHERE product_id=%s",
                    (total_qty, int(product_id)),
                )
                cur.execute(
                    """
                    INSERT INTO product_stock_movements (
                      product_id, movement_type, quantity, stock_out_reason, note, created_by_employee_id
                    ) VALUES (%s,'OUT',%s,'SALE',%s,%s)
                    """,
                    (
                        int(product_id),
                        total_qty,
                        f"DISTRIBUTION #{dist_id}" + (f" — {note}" if note else ""),
                        session.get("employee_id"),
                    ),
                )

        finally:
            conn.close()

        flash(
            (
                f"Distribution saved as sale. Stock reduced; transaction recorded (SALE). Farmers: {recipients_farmer_count}. Total qty issued: {total_qty:.2f}."
                + (" Registered session acres were updated to match this issue." if session_land_updates else "")
            ),
            "success",
        )
        return redirect(url_for("main.warehouse_distribution", role=session_role))

    return render_template(
        "warehouse_distribution.html",
        module_key="warehouse",
        module_title="Warehouse",
        module_desc="Stock, warehousing, issuing, and inventory.",
        module_icon="boxes",
        section_key="distribution",
        section_title="Distribution section",
        section_desc="Distribute using session acres × amount per acre; saving updates registered session acreage system-wide.",
        section_icon="truck",
        products=products,
        distribution_sessions=distribution_sessions,
    )


def _ensure_privileged(role: str):
    session_role = _require_session_role(role)
    if not session_role:
        return "", redirect(url_for("auth.login"))
    if session_role != role:
        return session_role, redirect(url_for("main.dashboard", role=session_role))
    if session_role not in PRIVILEGED_MODULE_ROLES:
        abort(403)
    return session_role, None


@bp.route("/api/locations")
def api_locations():
    """
    Return distinct location values for a given ward.

    Used to populate the "Location" dropdown in farmers registration.
    """
    if not session.get("employee_id"):
        return jsonify({"locations": []}), 401

    ward = (request.args.get("ward") or "").strip().upper()
    if not ward:
        return jsonify({"locations": []})

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT location
                FROM farmers
                WHERE ward = %s
                  AND location IS NOT NULL
                  AND location <> ''
                ORDER BY location ASC
                LIMIT 250
                """,
                (ward,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return jsonify({"locations": [r["location"] for r in rows if r.get("location")]})


@bp.route("/api/farmers/<int:farmer_id>")
def api_farmer(farmer_id: int):
    session_role = session.get("employee_role") or ""
    if not session.get("employee_id"):
        return jsonify({"error": "unauthorized"}), 401
    if session_role not in PRIVILEGED_MODULE_ROLES:
        return jsonify({"error": "forbidden"}), 403

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM farmers WHERE id=%s LIMIT 1", (int(farmer_id),))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "not_found"}), 404

    # Make dates JSON friendly
    dob = row.get("date_of_birth")
    if dob is not None:
        row["date_of_birth"] = dob.isoformat()
    reg_date = row.get("registration_date")
    if reg_date is not None:
        row["registration_date"] = reg_date.isoformat()

    return jsonify({"farmer": row})


@bp.route("/api/products/<int:product_id>")
def api_product(product_id: int):
    session_role = session.get("employee_role") or ""
    if not session.get("employee_id"):
        return jsonify({"error": "unauthorized"}), 401
    if session_role not in PRIVILEGED_MODULE_ROLES:
        return jsonify({"error": "forbidden"}), 403

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM products WHERE id=%s LIMIT 1", (int(product_id),))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "not_found"}), 404

    return jsonify({"product": row})


@bp.route("/api/suppliers/lookup")
def api_suppliers_lookup():
    """Match warehouse suppliers by phone or name while user types (stock-in form)."""
    session_role = session.get("employee_role") or ""
    if not session.get("employee_id"):
        return jsonify({"error": "unauthorized", "found": False}), 401
    if session_role not in PRIVILEGED_MODULE_ROLES:
        return jsonify({"error": "forbidden", "found": False}), 403

    phone = (request.args.get("phone") or "").strip()
    name = (request.args.get("name") or "").strip()
    norm = _normalize_supplier_phone(phone)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if len(norm) >= 9:
                cur.execute(
                    "SELECT id, name, contact FROM suppliers WHERE contact_normalized = %s LIMIT 1",
                    (norm,),
                )
                row = cur.fetchone()
                if row:
                    return jsonify(
                        {
                            "found": True,
                            "supplier": {
                                "id": int(row["id"]),
                                "name": row["name"],
                                "contact": row["contact"],
                            },
                            "match": "phone",
                        }
                    )

            if len(name) >= 2:
                cur.execute(
                    "SELECT id, name, contact FROM suppliers WHERE UPPER(name) = UPPER(%s) LIMIT 1",
                    (name,),
                )
                row = cur.fetchone()
                if row:
                    return jsonify(
                        {
                            "found": True,
                            "supplier": {
                                "id": int(row["id"]),
                                "name": row["name"],
                                "contact": row["contact"],
                            },
                            "match": "name_exact",
                        }
                    )

                cur.execute(
                    """
                    SELECT id, name, contact FROM suppliers
                    WHERE UPPER(name) LIKE UPPER(CONCAT(%s, %s))
                    ORDER BY CHAR_LENGTH(name) ASC, name ASC
                    """,
                    (name, "%"),
                )
                rows = cur.fetchall()
                if len(rows) == 1:
                    r = rows[0]
                    return jsonify(
                        {
                            "found": True,
                            "supplier": {
                                "id": int(r["id"]),
                                "name": r["name"],
                                "contact": r["contact"],
                            },
                            "match": "name_prefix",
                        }
                    )
    finally:
        conn.close()

    return jsonify({"found": False})


@bp.route("/<role>/module/administration/employees", methods=["POST"])
def hr_employees_action(role):
    session_role, resp = _ensure_privileged(role)
    if resp:
        return resp
    validate_csrf()

    action = (request.form.get("action") or "").strip()
    employee_id = (request.form.get("employee_id") or "").strip()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if action == "create":
                full_name = (request.form.get("full_name") or "").strip().upper()
                email = (request.form.get("email") or "").strip().lower()
                national_id = (request.form.get("national_id") or "").strip().upper()
                login_code = (request.form.get("login_code") or "").strip()
                password = request.form.get("password") or ""
                role_in = (request.form.get("role") or "employee").strip()

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
                    errors.append("Password must be at least 6 characters.")
                if role_in not in ALLOWED_ROLES:
                    role_in = "employee"
                if errors:
                    for e in errors:
                        flash(e, "error")
                    return redirect(url_for("main.module_page", role=session_role, module_key="administration"))

                cur.execute(
                    """
                    INSERT INTO employees (full_name, email, national_id, login_code, password_hash, role, status)
                    VALUES (%s,%s,%s,%s,%s,%s,'pending_approval')
                    """,
                    (full_name, email, national_id, login_code, generate_password_hash(password), role_in),
                )
                flash("Employee registered (pending approval).", "success")

            elif action == "update" and employee_id.isdigit():
                full_name = (request.form.get("full_name") or "").strip().upper()
                email = (request.form.get("email") or "").strip().lower()
                national_id = (request.form.get("national_id") or "").strip().upper()
                login_code = (request.form.get("login_code") or "").strip()
                role_in = (request.form.get("role") or "employee").strip()

                if role_in not in ALLOWED_ROLES:
                    role_in = "employee"
                if len(full_name) < 2 or not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email) or not re.fullmatch(
                    r"\d{6}", login_code
                ):
                    flash("Please provide valid employee details.", "error")
                    return redirect(url_for("main.module_page", role=session_role, module_key="administration"))

                cur.execute(
                    """
                    UPDATE employees
                    SET full_name=%s, email=%s, national_id=%s, login_code=%s, role=%s
                    WHERE id=%s
                    """,
                    (full_name, email, national_id, login_code, role_in, int(employee_id)),
                )
                flash("Employee updated.", "success")

            elif action == "suspend" and employee_id.isdigit():
                cur.execute("UPDATE employees SET status='suspended' WHERE id=%s", (int(employee_id),))
                flash("Employee suspended.", "success")

            elif action == "activate" and employee_id.isdigit():
                cur.execute("UPDATE employees SET status='active' WHERE id=%s", (int(employee_id),))
                flash("Employee activated.", "success")

            elif action == "delete" and employee_id.isdigit():
                cur.execute("DELETE FROM employees WHERE id=%s", (int(employee_id),))
                flash("Employee deleted.", "success")
            else:
                flash("Unknown action.", "error")
    except Exception as exc:
        msg = str(exc).lower()
        if "1062" in msg or "duplicate" in msg:
            flash("Duplicate value: email / national id / login code already exists.", "error")
        else:
            flash("Action failed.", "error")
    finally:
        conn.close()

    return redirect(url_for("main.module_page", role=session_role, module_key="administration"))


@bp.route("/<role>/module/farmers", methods=["POST"])
def farmers_action(role):
    session_role, resp = _ensure_privileged(role)
    if resp:
        return resp
    validate_csrf()

    action = (request.form.get("action") or "").strip()
    farmer_id = (request.form.get("farmer_id") or "").strip()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if action == "create":
                # Basic personal info
                full_name = (request.form.get("full_name") or "").strip().upper()
                national_id = (request.form.get("national_id") or "").strip().upper()
                phone_number = (request.form.get("phone_number") or "").strip().upper()
                alt_phone_number = (request.form.get("alt_phone_number") or "").strip().upper() or None
                gender = (request.form.get("gender") or "").strip().lower()
                date_of_birth = (request.form.get("date_of_birth") or "").strip() or None

                # Location
                county = (request.form.get("county") or "").strip().upper()
                sub_county = (request.form.get("sub_county") or "").strip().upper()
                ward = (request.form.get("ward") or "").strip().upper()
                location = (request.form.get("location") or "").strip().upper() or None
                village = (request.form.get("village") or "").strip().upper() or None

                # Farm info
                farm_name = (request.form.get("farm_name") or "").strip().upper()
                farm_location = (request.form.get("farm_location") or "").strip().upper() or None
                land_size_raw = (request.form.get("land_size") or "").strip()
                land_size = land_size_raw or None
                land_size_unit = (request.form.get("land_size_unit") or "").strip().lower() or None
                ownership_type = (request.form.get("ownership_type") or "").strip().lower() or None
                lease_period_value = (request.form.get("lease_period_value") or "").strip() or None
                lease_period_unit = (request.form.get("lease_period_unit") or "").strip().lower() or None

                # Coop info
                membership_number = (request.form.get("membership_number") or "").strip().upper()
                cooperative_name = (request.form.get("cooperative_name") or "").strip().upper()
                collection_center = (request.form.get("collection_center") or "").strip().upper() or None
                # Field officer should be the signed-in user (session)
                field_officer = (session.get("employee_name") or "").strip().upper() or None

                # Payment
                mpesa_number = (request.form.get("mpesa_number") or "").strip().upper()
                bank_account = (request.form.get("bank_account") or "").strip().upper() or None
                preferred_payment_method = (request.form.get("preferred_payment_method") or "").strip().lower()

                # Emergency
                next_of_kin_name = (request.form.get("next_of_kin_name") or "").strip().upper()
                next_of_kin_phone = (request.form.get("next_of_kin_phone") or "").strip().upper()
                next_of_kin_relationship = (request.form.get("next_of_kin_relationship") or "").strip().upper() or None

                # System info
                gps_coordinates = (request.form.get("gps_coordinates") or "").strip().upper() or None
                registration_consent = (request.form.get("registration_consent") or "").strip()

                errors = []
                if len(full_name) < 2:
                    errors.append("Full name is required.")
                if len(national_id) < 5:
                    errors.append("National ID is required.")
                if len(phone_number) < 7:
                    errors.append("Phone number is required.")
                if not date_of_birth:
                    errors.append("Date of birth is required.")
                if gender and gender not in {"male", "female", "other"}:
                    errors.append("Gender must be Male, Female or Other.")
                if land_size_unit and land_size_unit not in {"acres", "hectares"}:
                    errors.append("Land size unit must be Acres or Hectares.")
                if land_size:
                    try:
                        float(land_size)
                    except ValueError:
                        errors.append("Land size must be a number.")
                if ownership_type and ownership_type not in {"owned", "leased", "family_land", "cooperative_land"}:
                    errors.append("Farm ownership type is invalid.")
                if ownership_type == "leased":
                    if not lease_period_value or not str(lease_period_value).isdigit() or int(lease_period_value) <= 0:
                        errors.append("Lease period value is required (a positive number).")
                    if lease_period_unit not in {"months", "years"}:
                        errors.append("Lease period unit must be Months or Years.")
                else:
                    lease_period_value = None
                    lease_period_unit = None
                if not mpesa_number:
                    errors.append("M-Pesa number is required.")
                if preferred_payment_method and preferred_payment_method not in {"mpesa", "bank", "sacco"}:
                    errors.append("Preferred payment method must be M-Pesa, Bank, or SACCO.")
                if registration_consent.lower() not in {"yes", "on", "true", "1"}:
                    errors.append("Registration consent is required.")

                file = request.files.get("profile_photo")
                photo_rel = None
                if file and file.filename:
                    if not _allowed_file(file.filename):
                        errors.append("Profile photo must be PNG, JPG, JPEG, WebP, or GIF.")

                id_file = request.files.get("national_id_upload")
                id_rel = None
                if id_file and id_file.filename:
                    ext = secure_filename(id_file.filename).rsplit(".", 1)[-1].lower()
                    if ext not in {"png", "jpg", "jpeg", "webp", "gif", "pdf"}:
                        errors.append("National ID upload must be an image or PDF.")

                if errors:
                    for e in errors:
                        flash(e, "error")
                    return redirect(url_for("main.farmers_manage_page", role=session_role))

                # Auto-fill farm name if left blank: LASTNAME FARM
                if not farm_name:
                    parts = [p for p in full_name.split() if p.strip()]
                    last = parts[-1] if parts else "FARM"
                    farm_name = f"{last} FARM"

                if file and file.filename:
                    ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "farmers"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    file.save(dest)
                    photo_rel = f"uploads/farmers/{fname}"

                if id_file and id_file.filename:
                    ext = secure_filename(id_file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "farmer_ids"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    id_file.save(dest)
                    id_rel = f"uploads/farmer_ids/{fname}"

                # Insert with temp code, then update to Fxxxxxx
                cur.execute(
                    """
                    INSERT INTO farmers (
                        farmer_code, status, full_name, national_id, phone_number, alt_phone_number, gender, date_of_birth, profile_photo, national_id_upload, registration_consent,
                        county, sub_county, ward, location, village,
                        farm_name, farm_location, land_size, land_size_unit, ownership_type, lease_period_value, lease_period_unit,
                        cooperative_name, membership_number, collection_center, field_officer,
                        mpesa_number, bank_account, preferred_payment_method,
                        next_of_kin_name, next_of_kin_phone, next_of_kin_relationship,
                        gps_coordinates, registered_by_employee_id
                    ) VALUES (
                        %s, 'pending_approval', %s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s
                    )
                    """,
                    (
                        "TEMP",
                        full_name,
                        national_id,
                        phone_number,
                        alt_phone_number,
                        gender or None,
                        date_of_birth,
                        photo_rel,
                        id_rel,
                        1,
                        county,
                        sub_county,
                        ward,
                        location,
                        village,
                        farm_name,
                        farm_location,
                        land_size,
                        land_size_unit,
                        ownership_type,
                        int(lease_period_value) if lease_period_value else None,
                        lease_period_unit,
                        cooperative_name,
                        membership_number or None,
                        collection_center,
                        field_officer,
                        mpesa_number,
                        bank_account,
                        preferred_payment_method,
                        next_of_kin_name,
                        next_of_kin_phone,
                        next_of_kin_relationship,
                        gps_coordinates,
                        int(session.get("employee_id")),
                    ),
                )
                new_id = cur.lastrowid
                code = f"F{int(new_id):06d}"
                # Simple + memorable, still unique (based on auto-increment id)
                auto_membership = membership_number or f"M{int(new_id):06d}"
                cur.execute(
                    "UPDATE farmers SET farmer_code=%s, membership_number=%s, registration_date=COALESCE(registration_date, CURDATE()) WHERE id=%s",
                    (code, auto_membership, int(new_id)),
                )
                flash("Farmer registered (pending approval).", "success")

            elif action == "update" and farmer_id.isdigit():
                # Basic personal info
                full_name = (request.form.get("full_name") or "").strip().upper()
                national_id = (request.form.get("national_id") or "").strip().upper()
                phone_number = (request.form.get("phone_number") or "").strip().upper()
                alt_phone_number = (request.form.get("alt_phone_number") or "").strip().upper() or None
                gender = (request.form.get("gender") or "").strip().lower() or None
                date_of_birth = (request.form.get("date_of_birth") or "").strip() or None

                # Location
                county = (request.form.get("county") or "").strip().upper()
                sub_county = (request.form.get("sub_county") or "").strip().upper()
                ward = (request.form.get("ward") or "").strip().upper()
                location = (request.form.get("location") or "").strip().upper() or None
                village = (request.form.get("village") or "").strip().upper() or None

                # Farm info
                farm_name = (request.form.get("farm_name") or "").strip().upper() or None
                farm_location = (request.form.get("farm_location") or "").strip().upper() or None
                land_size_raw = (request.form.get("land_size") or "").strip()
                land_size = land_size_raw or None
                land_size_unit = (request.form.get("land_size_unit") or "").strip().lower() or None
                ownership_type = (request.form.get("ownership_type") or "").strip().lower() or None
                lease_period_value = (request.form.get("lease_period_value") or "").strip() or None
                lease_period_unit = (request.form.get("lease_period_unit") or "").strip().lower() or None

                # Coop info
                membership_number = (request.form.get("membership_number") or "").strip().upper() or None
                cooperative_name = (request.form.get("cooperative_name") or "").strip().upper() or None
                collection_center = (request.form.get("collection_center") or "").strip().upper() or None

                # Payment
                mpesa_number = (request.form.get("mpesa_number") or "").strip().upper() or None
                bank_account = (request.form.get("bank_account") or "").strip().upper() or None
                preferred_payment_method = (request.form.get("preferred_payment_method") or "").strip().lower() or None

                # Emergency
                next_of_kin_name = (request.form.get("next_of_kin_name") or "").strip().upper() or None
                next_of_kin_phone = (request.form.get("next_of_kin_phone") or "").strip().upper() or None
                next_of_kin_relationship = (request.form.get("next_of_kin_relationship") or "").strip().upper() or None

                # System info
                gps_coordinates = (request.form.get("gps_coordinates") or "").strip().upper() or None

                errors = []
                if len(full_name) < 2:
                    errors.append("Full name is required.")
                if len(national_id) < 5:
                    errors.append("National ID is required.")
                if len(phone_number) < 7:
                    errors.append("Phone number is required.")
                if gender and gender not in {"male", "female", "other"}:
                    errors.append("Gender must be Male, Female or Other.")
                if land_size_unit and land_size_unit not in {"acres", "hectares"}:
                    errors.append("Land size unit must be Acres or Hectares.")
                if land_size:
                    try:
                        float(land_size)
                    except ValueError:
                        errors.append("Land size must be a number.")
                if ownership_type and ownership_type not in {"owned", "leased", "family_land", "cooperative_land"}:
                    errors.append("Farm ownership type is invalid.")
                if ownership_type == "leased":
                    if not lease_period_value or not str(lease_period_value).isdigit() or int(lease_period_value) <= 0:
                        errors.append("Lease period value is required (a positive number).")
                    if lease_period_unit not in {"months", "years"}:
                        errors.append("Lease period unit must be Months or Years.")
                else:
                    lease_period_value = None
                    lease_period_unit = None

                # Optional file updates
                file = request.files.get("profile_photo")
                photo_rel = None
                if file and file.filename:
                    if not _allowed_file(file.filename):
                        errors.append("Profile photo must be PNG, JPG, JPEG, WebP, or GIF.")

                id_file = request.files.get("national_id_upload")
                id_rel = None
                if id_file and id_file.filename:
                    ext = secure_filename(id_file.filename).rsplit(".", 1)[-1].lower()
                    if ext not in {"png", "jpg", "jpeg", "webp", "gif", "pdf"}:
                        errors.append("National ID upload must be an image or PDF.")

                if errors:
                    for e in errors:
                        flash(e, "error")
                    return redirect(url_for("main.farmers_manage_page", role=session_role))

                if file and file.filename:
                    ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "farmers"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    file.save(dest)
                    photo_rel = f"uploads/farmers/{fname}"

                if id_file and id_file.filename:
                    ext = secure_filename(id_file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "farmer_ids"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    id_file.save(dest)
                    id_rel = f"uploads/farmer_ids/{fname}"

                fields = {
                    "full_name": full_name,
                    "national_id": national_id,
                    "phone_number": phone_number,
                    "alt_phone_number": alt_phone_number,
                    "gender": gender,
                    "date_of_birth": date_of_birth,
                    "county": county,
                    "sub_county": sub_county,
                    "ward": ward,
                    "location": location,
                    "village": village,
                    "farm_name": farm_name,
                    "farm_location": farm_location,
                    "land_size": land_size,
                    "land_size_unit": land_size_unit,
                    "ownership_type": ownership_type,
                    "lease_period_value": int(lease_period_value) if lease_period_value else None,
                    "lease_period_unit": lease_period_unit,
                    "mpesa_number": mpesa_number,
                    "bank_account": bank_account,
                    "preferred_payment_method": preferred_payment_method,
                    "next_of_kin_name": next_of_kin_name,
                    "next_of_kin_phone": next_of_kin_phone,
                    "next_of_kin_relationship": next_of_kin_relationship,
                    "gps_coordinates": gps_coordinates,
                }
                if photo_rel is not None:
                    fields["profile_photo"] = photo_rel
                if id_rel is not None:
                    fields["national_id_upload"] = id_rel

                set_sql = ", ".join([f"{k}=%s" for k in fields.keys()])
                params = list(fields.values()) + [int(farmer_id)]
                cur.execute(f"UPDATE farmers SET {set_sql} WHERE id=%s", params)
                flash("Farmer updated.", "success")

            elif action == "suspend" and farmer_id.isdigit():
                cur.execute("UPDATE farmers SET status='suspended' WHERE id=%s", (int(farmer_id),))
                flash("Farmer suspended.", "success")

            elif action == "activate" and farmer_id.isdigit():
                cur.execute("UPDATE farmers SET status='active' WHERE id=%s", (int(farmer_id),))
                flash("Farmer activated.", "success")

            elif action == "delete" and farmer_id.isdigit():
                cur.execute("DELETE FROM farmers WHERE id=%s", (int(farmer_id),))
                flash("Farmer deleted.", "success")
            else:
                flash("Unknown action.", "error")
    except Exception as exc:
        msg = str(exc).lower()
        if "1062" in msg or "duplicate" in msg:
            flash("Duplicate value: national id or farmer code already exists.", "error")
        else:
            flash("Action failed.", "error")
    finally:
        conn.close()

    return redirect(url_for("main.farmers_manage_page", role=session_role))


def _allowed_file(filename: str) -> bool:
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower()
        in current_app.config["ALLOWED_EXTENSIONS"]
    )


@bp.route("/<role>/module/products", methods=["POST"])
def products_action(role):
    session_role, resp = _ensure_privileged(role)
    if resp:
        return resp
    validate_csrf()

    action = (request.form.get("action") or "").strip()
    product_id = (request.form.get("product_id") or "").strip()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if action == "create":
                product_type = (request.form.get("product_type") or "CROP").strip().upper()
                product_name = (request.form.get("product_name") or request.form.get("crop_name") or "").strip().upper()

                # Shared
                crop_description = (request.form.get("crop_description") or request.form.get("product_description") or "").strip().upper()
                brand = (request.form.get("brand") or "").strip().upper() or None
                manufacturer = (request.form.get("manufacturer") or "").strip().upper() or None
                unit_of_measure = (request.form.get("unit_of_measure") or "").strip().upper() or None
                package_size = (request.form.get("package_size") or "").strip().upper() or None

                # Crop-specific
                scientific_name = (request.form.get("scientific_name") or "").strip().upper()
                crop_category = (request.form.get("crop_category") or "").strip().upper()
                crop_variety = (request.form.get("crop_variety") or "").strip().upper()
                planting_season = (request.form.get("planting_season") or "").strip().upper()
                growth_duration = (request.form.get("growth_duration") or "").strip().upper()
                water_requirement = (request.form.get("water_requirement") or "").strip().upper()

                # Agrochemicals
                active_ingredient = (request.form.get("active_ingredient") or "").strip().upper() or None
                formulation = (request.form.get("formulation") or "").strip().upper() or None
                application_rate = (request.form.get("application_rate") or "").strip().upper() or None
                target_use = (request.form.get("target_use") or "").strip().upper() or None
                safety_notes = (request.form.get("safety_notes") or "").strip().upper() or None

                # Equipment
                equipment_model = (request.form.get("equipment_model") or "").strip().upper() or None
                power_source = (request.form.get("power_source") or "").strip().upper() or None
                capacity = (request.form.get("capacity") or "").strip().upper() or None
                warranty_period = (request.form.get("warranty_period") or "").strip().upper() or None

                errors = []
                average_yield_per_acre = None
                average_yield_uom = None
                average_yield_range = None
                if product_type not in {"CROP", "HERBICIDE", "FERTILIZER", "EQUIPMENT"}:
                    errors.append("Product category is invalid.")
                if not product_name:
                    errors.append("Product name is required.")
                if not crop_description:
                    errors.append("Description is required.")

                if product_type == "CROP":
                    if not scientific_name:
                        errors.append("Scientific name is required.")
                    if not crop_category:
                        errors.append("Crop category is required.")
                    if not crop_variety:
                        errors.append("Crop variety is required.")
                    if not planting_season:
                        errors.append("Planting season is required.")
                    if not growth_duration:
                        errors.append("Growth duration is required.")
                    if not water_requirement:
                        errors.append("Water requirement is required.")
                    average_yield_per_acre, average_yield_uom, average_yield_range = _crop_average_yield_from_form(
                        errors, unit_of_measure
                    )
                elif product_type in {"HERBICIDE", "FERTILIZER"}:
                    if not active_ingredient:
                        errors.append("Active ingredient / composition is required.")
                    if not formulation:
                        errors.append("Formulation is required.")
                    if not application_rate:
                        errors.append("Application rate is required.")
                    if not target_use:
                        errors.append("Target use is required.")
                    if _parse_per_acre_amount(package_size) is None:
                        errors.append(
                            "Amount per acre must be a positive number for herbicides and fertilizers "
                            "(how many stock units—above—are applied per one acre). "
                            "Warehouse land distribution calculates quantity as farmer session acres × this amount. "
                            "Put mixing or spray concentration guidance only in Application rate."
                        )
                    # Fill required legacy crop fields for non-crops
                    scientific_name = "N/A"
                    crop_category = product_type
                    crop_variety = formulation or "N/A"
                    planting_season = "ALL YEAR"
                    growth_duration = "N/A"
                    water_requirement = "N/A"
                else:  # EQUIPMENT
                    if not equipment_model:
                        errors.append("Model is required.")
                    if not power_source:
                        errors.append("Power source is required.")
                    if not capacity:
                        errors.append("Capacity is required.")
                    scientific_name = "N/A"
                    crop_category = product_type
                    crop_variety = equipment_model or "N/A"
                    planting_season = "ALL YEAR"
                    growth_duration = "N/A"
                    water_requirement = "N/A"

                file = request.files.get("crop_image")
                image_rel = None
                if file and file.filename:
                    if not _allowed_file(file.filename):
                        errors.append("Crop image must be PNG, JPG, JPEG, WebP, or GIF.")

                if errors:
                    for e in errors:
                        flash(e, "error")
                    return redirect(url_for("main.products_management", role=session_role))

                if file and file.filename:
                    ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "products"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    file.save(dest)
                    image_rel = f"uploads/products/{fname}"

                cur.execute(
                    """
                    INSERT INTO products (
                        crop_code, status, product_type, crop_name, scientific_name, crop_category, crop_variety,
                        crop_description, crop_image,
                        planting_season, growth_duration, water_requirement, average_yield_range,
                        average_yield_per_acre, average_yield_uom,
                        brand, manufacturer, unit_of_measure, package_size,
                        active_ingredient, formulation, application_rate, target_use, safety_notes,
                        equipment_model, power_source, capacity, warranty_period
                    ) VALUES (
                        %s, 'active', %s, %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s
                    )
                    """,
                    (
                        "TEMP",
                        product_type,
                        product_name,
                        scientific_name,
                        crop_category,
                        crop_variety,
                        crop_description,
                        image_rel,
                        planting_season,
                        growth_duration,
                        water_requirement,
                        average_yield_range,
                        average_yield_per_acre,
                        average_yield_uom,
                        brand,
                        manufacturer,
                        unit_of_measure,
                        package_size,
                        active_ingredient,
                        formulation,
                        application_rate,
                        target_use,
                        safety_notes,
                        equipment_model,
                        power_source,
                        capacity,
                        warranty_period,
                    ),
                )
                new_id = cur.lastrowid
                prefix = {"CROP": "CR", "HERBICIDE": "HB", "FERTILIZER": "FZ", "EQUIPMENT": "EQ"}.get(product_type, "PR")
                code = f"{prefix}{int(new_id):06d}"
                cur.execute("UPDATE products SET crop_code=%s WHERE id=%s", (code, int(new_id)))
                flash(f"Product registered. Product Code: {code}", "success")

            elif action == "update" and product_id.isdigit():
                product_type = (request.form.get("product_type") or "CROP").strip().upper()
                product_name = (request.form.get("product_name") or request.form.get("crop_name") or "").strip().upper()

                crop_description = (request.form.get("crop_description") or request.form.get("product_description") or "").strip().upper()
                brand = (request.form.get("brand") or "").strip().upper() or None
                manufacturer = (request.form.get("manufacturer") or "").strip().upper() or None
                unit_of_measure = (request.form.get("unit_of_measure") or "").strip().upper() or None
                package_size = (request.form.get("package_size") or "").strip().upper() or None

                scientific_name = (request.form.get("scientific_name") or "").strip().upper()
                crop_category = (request.form.get("crop_category") or "").strip().upper()
                crop_variety = (request.form.get("crop_variety") or "").strip().upper()
                planting_season = (request.form.get("planting_season") or "").strip().upper()
                growth_duration = (request.form.get("growth_duration") or "").strip().upper()
                water_requirement = (request.form.get("water_requirement") or "").strip().upper()

                active_ingredient = (request.form.get("active_ingredient") or "").strip().upper() or None
                formulation = (request.form.get("formulation") or "").strip().upper() or None
                application_rate = (request.form.get("application_rate") or "").strip().upper() or None
                target_use = (request.form.get("target_use") or "").strip().upper() or None
                safety_notes = (request.form.get("safety_notes") or "").strip().upper() or None

                equipment_model = (request.form.get("equipment_model") or "").strip().upper() or None
                power_source = (request.form.get("power_source") or "").strip().upper() or None
                capacity = (request.form.get("capacity") or "").strip().upper() or None
                warranty_period = (request.form.get("warranty_period") or "").strip().upper() or None

                errors = []
                average_yield_per_acre = None
                average_yield_uom = None
                average_yield_range = None
                if product_type not in {"CROP", "HERBICIDE", "FERTILIZER", "EQUIPMENT"}:
                    errors.append("Product category is invalid.")
                if not product_name:
                    errors.append("Product name is required.")
                if not crop_description:
                    errors.append("Description is required.")

                if product_type == "CROP":
                    if not scientific_name:
                        errors.append("Scientific name is required.")
                    if not crop_category:
                        errors.append("Crop category is required.")
                    if not crop_variety:
                        errors.append("Crop variety is required.")
                    if not planting_season:
                        errors.append("Planting season is required.")
                    if not growth_duration:
                        errors.append("Growth duration is required.")
                    if not water_requirement:
                        errors.append("Water requirement is required.")
                    average_yield_per_acre, average_yield_uom, average_yield_range = _crop_average_yield_from_form(
                        errors, unit_of_measure
                    )
                elif product_type in {"HERBICIDE", "FERTILIZER"}:
                    if not active_ingredient:
                        errors.append("Active ingredient / composition is required.")
                    if not formulation:
                        errors.append("Formulation is required.")
                    if not application_rate:
                        errors.append("Application rate is required.")
                    if not target_use:
                        errors.append("Target use is required.")
                    if _parse_per_acre_amount(package_size) is None:
                        errors.append(
                            "Amount per acre must be a positive number for herbicides and fertilizers "
                            "(how many stock units—above—are applied per one acre). "
                            "Warehouse land distribution calculates quantity as farmer session acres × this amount. "
                            "Put mixing or spray concentration guidance only in Application rate."
                        )
                    scientific_name = "N/A"
                    crop_category = product_type
                    crop_variety = formulation or "N/A"
                    planting_season = "ALL YEAR"
                    growth_duration = "N/A"
                    water_requirement = "N/A"
                else:
                    if not equipment_model:
                        errors.append("Model is required.")
                    if not power_source:
                        errors.append("Power source is required.")
                    if not capacity:
                        errors.append("Capacity is required.")
                    scientific_name = "N/A"
                    crop_category = product_type
                    crop_variety = equipment_model or "N/A"
                    planting_season = "ALL YEAR"
                    growth_duration = "N/A"
                    water_requirement = "N/A"

                file = request.files.get("crop_image")
                image_rel = None
                if file and file.filename:
                    if not _allowed_file(file.filename):
                        errors.append("Crop image must be PNG, JPG, JPEG, WebP, or GIF.")

                if errors:
                    for e in errors:
                        flash(e, "error")
                    return redirect(url_for("main.products_management", role=session_role))

                if file and file.filename:
                    ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
                    from uuid import uuid4

                    fname = f"{uuid4().hex}.{ext}"
                    upload_root = Path(current_app.config["UPLOAD_FOLDER"]).parent / "products"
                    upload_root.mkdir(parents=True, exist_ok=True)
                    dest = upload_root / fname
                    file.save(dest)
                    image_rel = f"uploads/products/{fname}"

                fields = {
                    "product_type": product_type,
                    "crop_name": product_name,
                    "scientific_name": scientific_name,
                    "crop_category": crop_category,
                    "crop_variety": crop_variety,
                    "crop_description": crop_description,
                    "planting_season": planting_season,
                    "growth_duration": growth_duration,
                    "water_requirement": water_requirement,
                    "average_yield_range": average_yield_range,
                    "average_yield_per_acre": average_yield_per_acre,
                    "average_yield_uom": average_yield_uom,
                    "brand": brand,
                    "manufacturer": manufacturer,
                    "unit_of_measure": unit_of_measure,
                    "package_size": package_size,
                    "active_ingredient": active_ingredient,
                    "formulation": formulation,
                    "application_rate": application_rate,
                    "target_use": target_use,
                    "safety_notes": safety_notes,
                    "equipment_model": equipment_model,
                    "power_source": power_source,
                    "capacity": capacity,
                    "warranty_period": warranty_period,
                }
                if image_rel is not None:
                    fields["crop_image"] = image_rel

                set_sql = ", ".join([f"{k}=%s" for k in fields.keys()])
                params = list(fields.values()) + [int(product_id)]
                cur.execute(f"UPDATE products SET {set_sql} WHERE id=%s", params)
                flash("Product updated.", "success")

            elif action == "suspend" and product_id.isdigit():
                cur.execute("UPDATE products SET status='suspended' WHERE id=%s", (int(product_id),))
                flash("Product suspended.", "success")

            elif action == "activate" and product_id.isdigit():
                cur.execute("UPDATE products SET status='active' WHERE id=%s", (int(product_id),))
                flash("Product activated.", "success")

            elif action == "delete" and product_id.isdigit():
                cur.execute("DELETE FROM products WHERE id=%s", (int(product_id),))
                flash("Product deleted.", "success")
            else:
                flash("Unknown action.", "error")
    except Exception as exc:
        msg = str(exc).lower()
        if "1062" in msg or "duplicate" in msg:
            flash("Duplicate value: crop code already exists.", "error")
        else:
            flash("Action failed.", "error")
    finally:
        conn.close()

    return redirect(url_for("main.products_management", role=session_role))


@bp.route("/<role>/profile", methods=["GET", "POST"])
def profile(role):
    if not session.get("employee_id"):
        return redirect(url_for("auth.login"))
    session_role = session.get("employee_role")
    if not session_role or session_role not in ALLOWED_ROLES:
        session.clear()
        return redirect(url_for("auth.login"))
    if role not in ALLOWED_ROLES:
        abort(404)
    if session_role != role:
        return redirect(url_for("main.profile", role=session_role))

    if request.method == "POST":
        validate_csrf()

        full_name = (request.form.get("full_name") or "").strip().upper()
        email = (request.form.get("email") or "").strip().lower()
        national_id = (request.form.get("national_id") or "").strip().upper()
        login_code = (request.form.get("login_code") or "").strip()

        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        errors = []
        if len(full_name) < 2:
            errors.append("Full name is required.")
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
            errors.append("Enter a valid email address.")
        if len(national_id) < 5:
            errors.append("National ID must be at least 5 characters.")
        if not re.fullmatch(r"\d{6}", login_code):
            errors.append("Login code must be exactly 6 digits.")

        update_password = bool(new_password.strip() or confirm_password.strip())
        if update_password:
            if len(new_password) < 6:
                errors.append("New password must be at least 6 characters.")
            if new_password != confirm_password:
                errors.append("New password and confirmation do not match.")

        file = request.files.get("profile_photo")
        photo_rel = None
        if file and file.filename:
            if not _allowed_file(file.filename):
                errors.append("Profile photo must be PNG, JPG, JPEG, WebP, or GIF.")

        if errors:
            for e in errors:
                flash(e, "error")
            # Re-fetch employee row to re-render with current state
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, full_name, email, national_id, login_code, role, status,
                               profile_photo, created_at
                        FROM employees
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (session["employee_id"],),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
            return render_template("profile.html", employee=row, form=request.form), 422

        # Save new photo (optional)
        if file and file.filename:
            ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
            from uuid import uuid4

            fname = f"{uuid4().hex}.{ext}"
            upload_root = Path(current_app.config["UPLOAD_FOLDER"])
            upload_root.mkdir(parents=True, exist_ok=True)
            dest = upload_root / fname
            file.save(dest)
            photo_rel = f"uploads/profiles/{fname}"

        # Persist updates
        fields = {
            "full_name": full_name,
            "email": email,
            "national_id": national_id,
            "login_code": login_code,
        }
        if photo_rel is not None:
            fields["profile_photo"] = photo_rel
        if update_password:
            fields["password_hash"] = generate_password_hash(new_password)

        set_sql = ", ".join([f"{k}=%s" for k in fields.keys()])
        params = list(fields.values()) + [session["employee_id"]]

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE employees SET {set_sql} WHERE id=%s", params)
        except Exception as exc:
            msg = str(exc).lower()
            if "1062" in msg or "duplicate" in msg:
                if "uq_employees_email" in msg or "email" in msg:
                    flash("That email is already registered.", "error")
                elif "uq_employees_national_id" in msg or "national_id" in msg:
                    flash("That national ID is already registered.", "error")
                elif "uq_employees_login_code" in msg or "login_code" in msg:
                    flash("That 6-digit login code is already taken.", "error")
                else:
                    flash("Update failed: duplicate value.", "error")
                # Re-fetch for render
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, full_name, email, national_id, login_code, role, status,
                               profile_photo, created_at
                        FROM employees
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (session["employee_id"],),
                    )
                    row = cur.fetchone()
                return render_template("profile.html", employee=row, form=request.form), 409
            raise
        finally:
            conn.close()

        # Refresh session header values
        session["employee_name"] = full_name
        if photo_rel is not None:
            session["employee_photo"] = photo_rel
        flash("Profile updated successfully.", "success")
        return redirect(url_for("main.profile", role=session_role))

    # GET
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, full_name, email, national_id, login_code, role, status,
                       profile_photo, created_at
                FROM employees
                WHERE id = %s
                LIMIT 1
                """,
                (session["employee_id"],),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        session.clear()
        return redirect(url_for("auth.login"))

    session["employee_name"] = row["full_name"]
    session["employee_photo"] = row.get("profile_photo") or ""

    return render_template("profile.html", employee=row)


@bp.route("/<role>/settings")
def settings(role):
    if not session.get("employee_id"):
        return redirect(url_for("auth.login"))
    session_role = session.get("employee_role")
    if not session_role or session_role not in ALLOWED_ROLES:
        session.clear()
        return redirect(url_for("auth.login"))
    if role not in ALLOWED_ROLES:
        abort(404)
    if session_role != role:
        return redirect(url_for("main.settings", role=session_role))
    return render_template(
        "settings.html",
        employee_name=session.get("employee_name"),
        employee_role=session.get("employee_role"),
    )
