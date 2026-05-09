"""
Record a stock-IN movement for many products at once (same rules as Warehouse → Stock).

- Upserts product_inventory and inserts product_stock_movements (movement_type IN).
- Default: active products only. Use --include-inactive for every row in products.
- Quantities are added on top of current inventory (like repeated stock-in forms).

Examples:
  python scripts/stock_in_all_products.py --dry-run
  python scripts/stock_in_all_products.py --quantity 500 --buying-price 120
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # noqa: E402
from app.db import get_connection  # noqa: E402


def _normalize_supplier_phone(raw: str) -> str:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk stock-in for all (or eligible) products.")
    parser.add_argument("--quantity", type=float, default=100.0, help="Quantity to add per product")
    parser.add_argument("--buying-price", type=float, default=1.0, help="Buying price per unit for movement row")
    parser.add_argument(
        "--supplier-name",
        default="MERU COOPERATIVES BULK STOCK-IN",
        help="Supplier name stored on movement (uppercase)",
    )
    parser.add_argument(
        "--supplier-contact",
        default="0712345678",
        help="Supplier phone (Kenyan-style ok)",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include products where status is not active",
    )
    parser.add_argument(
        "--only-zero",
        action="store_true",
        help="Only products with no inventory row or quantity 0",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; no DB writes")
    args = parser.parse_args()

    if args.quantity <= 0:
        print("--quantity must be positive.", file=sys.stderr)
        return 2
    if args.buying_price < 0:
        print("--buying-price must be 0 or more.", file=sys.stderr)
        return 2

    supplier_name = args.supplier_name.strip().upper()
    supplier_contact = args.supplier_contact.strip().upper()
    if not supplier_name or not supplier_contact:
        print("Supplier name and contact are required.", file=sys.stderr)
        return 2

    app = create_app()
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                parts = []
                if not args.include_inactive:
                    parts.append("p.status = 'active'")
                if args.only_zero:
                    parts.append("(i.product_id IS NULL OR COALESCE(i.quantity, 0) = 0)")
                where_sql = " AND ".join(parts) if parts else "1=1"

                cur.execute(
                    f"""
                    SELECT p.id, p.crop_code, p.crop_name, COALESCE(i.quantity, 0) AS q
                    FROM products p
                    LEFT JOIN product_inventory i ON i.product_id = p.id
                    WHERE {where_sql}
                    ORDER BY p.id ASC
                    """,
                )
                rows = cur.fetchall()

                if args.dry_run:
                    print(f"Dry run: would stock-in {len(rows)} product(s), +{args.quantity} each.")
                    return 0

                _upsert_supplier_row(cur, supplier_name, supplier_contact)
                n = 0
                for r in rows:
                    pid = int(r["id"])
                    cur.execute(
                        "INSERT INTO product_inventory (product_id, quantity) VALUES (%s, 0) "
                        "ON DUPLICATE KEY UPDATE product_id=product_id",
                        (pid,),
                    )
                    cur.execute(
                        "UPDATE product_inventory SET quantity = quantity + %s WHERE product_id=%s",
                        (args.quantity, pid),
                    )
                    cur.execute(
                        """
                        INSERT INTO product_stock_movements (
                            product_id, movement_type, quantity,
                            buying_price, supplier_name, supplier_contact,
                            created_by_employee_id
                        ) VALUES (%s,'IN',%s,%s,%s,%s,NULL)
                        """,
                        (pid, args.quantity, args.buying_price, supplier_name, supplier_contact),
                    )
                    n += 1
                conn.commit()
                print(f"Stock-in recorded for {n} product(s): +{args.quantity} units @ KES {args.buying_price:.2f}.")
        except Exception as ex:
            conn.rollback()
            print(f"Error: {ex}", file=sys.stderr)
            return 1
        finally:
            conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
