from __future__ import annotations

import sys
from pathlib import Path

# Allow running as a script from /scripts on Windows.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # noqa: E402
from app.db import get_connection  # noqa: E402


def main() -> int:
    app = create_app()
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Prefer seeded-style crop codes (CR...) if duplicates exist.
                cur.execute(
                    """
                    SELECT id, crop_code, crop_name
                    FROM products
                    WHERE status='active'
                      AND UPPER(product_type)='CROP'
                      AND UPPER(crop_name)='BEANS'
                    ORDER BY (crop_code LIKE 'CR%') DESC, id ASC
                    LIMIT 1
                    """
                )
                beans = cur.fetchone()
                if not beans:
                    raise RuntimeError("BEANS crop not found in products table.")
                beans_id = int(beans["id"])

                def _pid(code: str) -> int | None:
                    cur.execute(
                        "SELECT id FROM products WHERE status='active' AND crop_code=%s LIMIT 1",
                        (code,),
                    )
                    r = cur.fetchone()
                    return (int(r["id"]) if r else None)

                # Stock items (store-linked)
                dap_id = _pid("FZ000020")  # DAP
                roundup_id = _pid("HB000017")  # ROUNDUP
                sprayer_id = _pid("EQ000023")  # KNAPSACK SPRAYER

                rows: list[dict] = [
                    # STORE-LINKED (equipment_product_id set)
                    dict(
                        activity_type="CHEMICAL",
                        activity_name="Pre-emergence weed control",
                        activity_description="Apply herbicide to control weeds before emergence.",
                        equipment_tools="ROUNDUP",
                        equipment_product_id=roundup_id,
                        equipment_unit_of_measure="LITER",
                        equipment_units_per_acre=1.0,
                        equipment_unit_price=650.0,
                        equipment_cost_per_acre=650.0,
                        estimated_cost=650.0,
                        scheduled_day=7,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="CHEMICAL",
                        activity_name="Top dressing (DAP)",
                        activity_description="Apply DAP fertilizer as top dressing.",
                        equipment_tools="DAP",
                        equipment_product_id=dap_id,
                        equipment_unit_of_measure="KG",
                        equipment_units_per_acre=25.0,
                        equipment_unit_price=85.0,
                        equipment_cost_per_acre=2125.0,
                        estimated_cost=2125.0,
                        scheduled_day=21,
                        preferred_time="AFTERNOON",
                    ),
                    dict(
                        activity_type="MECHANICAL",
                        activity_name="Sprayer calibration",
                        activity_description="Calibrate knapsack sprayer before chemical application.",
                        equipment_tools="KNAPSACK SPRAYER",
                        equipment_product_id=sprayer_id,
                        equipment_unit_of_measure="UNIT",
                        equipment_units_per_acre=0.05,
                        equipment_unit_price=4500.0,
                        equipment_cost_per_acre=225.0,
                        estimated_cost=225.0,
                        scheduled_day=6,
                        preferred_time="MORNING",
                    ),
                    # MANUAL (no stock link)
                    dict(
                        activity_type="MANUAL",
                        activity_name="Land preparation (manual)",
                        activity_description="Clear field, remove debris, prepare seedbed.",
                        equipment_tools="Panga, hoe, rake",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=3000.0,
                        scheduled_day=0,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="MANUAL",
                        activity_name="Planting",
                        activity_description="Plant beans using recommended spacing.",
                        equipment_tools="String line, dibbler, seed container",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=1500.0,
                        scheduled_day=1,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="MANUAL",
                        activity_name="Weeding (first)",
                        activity_description="First weeding to reduce competition.",
                        equipment_tools="Hoe, gloves",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=1200.0,
                        scheduled_day=14,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="MONITORING",
                        activity_name="Pest scouting",
                        activity_description="Check for aphids and bean fly; record observations.",
                        equipment_tools="Notebook, phone camera",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=0.0,
                        scheduled_day=10,
                        preferred_time="EVENING",
                    ),
                    dict(
                        activity_type="HARVESTING",
                        activity_name="Harvesting",
                        activity_description="Harvest mature pods; dry and store properly.",
                        equipment_tools="Sacks, tarpaulin",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=2500.0,
                        scheduled_day=85,
                        preferred_time="MORNING",
                    ),
                ]

                inserted = 0
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO farm_activities (
                          product_id, activity_type, activity_name, activity_description,
                          equipment_tools, equipment_product_id, equipment_unit_of_measure,
                          equipment_units_per_acre, equipment_unit_price, equipment_cost_per_acre,
                          estimated_cost, scheduled_day, preferred_time,
                          created_by_employee_id
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
                        """,
                        (
                            beans_id,
                            r["activity_type"],
                            r["activity_name"],
                            r["activity_description"],
                            r["equipment_tools"],
                            r["equipment_product_id"],
                            r["equipment_unit_of_measure"],
                            r["equipment_units_per_acre"],
                            r["equipment_unit_price"],
                            r["equipment_cost_per_acre"],
                            r["estimated_cost"],
                            r["scheduled_day"],
                            r["preferred_time"],
                        ),
                    )
                    inserted += 1

            conn.commit()
            print(
                f"Inserted {inserted} BEANS activities into product_id={beans_id} ({beans.get('crop_code')})."
            )
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

