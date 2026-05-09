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
                # Find a crop under category TUBER (e.g., POTATO). If none, fall back to crop name match.
                cur.execute(
                    """
                    SELECT id, crop_code, crop_name, crop_category
                    FROM products
                    WHERE status='active'
                      AND UPPER(product_type)='CROP'
                      AND UPPER(crop_category)='TUBER'
                    ORDER BY id ASC
                    LIMIT 5
                    """
                )
                tubers = cur.fetchall()
                if not tubers:
                    cur.execute(
                        """
                        SELECT id, crop_code, crop_name, crop_category
                        FROM products
                        WHERE status='active'
                          AND UPPER(product_type)='CROP'
                          AND (UPPER(crop_name) LIKE '%%POTATO%%' OR UPPER(crop_name) LIKE '%%CASSAVA%%')
                        ORDER BY id ASC
                        LIMIT 5
                        """
                    )
                    tubers = cur.fetchall()
                if not tubers:
                    raise RuntimeError("No TUBER crop found (crop_category='TUBER', or POTATO/CASSAVA name match).")

                # Prefer seeded-style crop codes (CR...) if duplicates exist.
                tubers.sort(key=lambda r: (0 if str(r.get("crop_code") or "").startswith("CR") else 1, int(r["id"])))
                tuber = tubers[0]
                tuber_id = int(tuber["id"])

                cur.execute("SELECT COUNT(*) AS n FROM farm_activities WHERE product_id=%s", (tuber_id,))
                existing = int((cur.fetchone() or {}).get("n") or 0)
                if existing > 0:
                    print(f"Skipped: {existing} activity(ies) already exist for product_id={tuber_id} ({tuber.get('crop_code')}).")
                    return 0

                def _pid(code: str) -> int | None:
                    cur.execute(
                        "SELECT id FROM products WHERE status='active' AND crop_code=%s LIMIT 1",
                        (code,),
                    )
                    r = cur.fetchone()
                    return (int(r["id"]) if r else None)

                # Stock items (store-linked)
                dap_id = _pid("FZ000020")  # DAP
                npk_id = _pid("FZ000022")  # NPK 17:17:17
                roundup_id = _pid("HB000017")  # ROUNDUP
                sprayer_id = _pid("EQ000023")  # KNAPSACK SPRAYER
                hoe_id = _pid("EQ000025")  # HAND HOE

                rows: list[dict] = [
                    # STORE-LINKED examples
                    dict(
                        activity_type="MECHANICAL",
                        activity_name="Land preparation (hoe)",
                        activity_description="Prepare ridges/beds for tuber planting.",
                        equipment_tools="HAND HOE",
                        equipment_product_id=hoe_id,
                        equipment_unit_of_measure="UNIT",
                        equipment_units_per_acre=0.25,
                        equipment_unit_price=800.0,
                        equipment_cost_per_acre=200.0,
                        estimated_cost=200.0,
                        scheduled_day=0,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="CHEMICAL",
                        activity_name="Fertilizer application (DAP)",
                        activity_description="Apply DAP at planting (or basal) as recommended.",
                        equipment_tools="DAP",
                        equipment_product_id=dap_id,
                        equipment_unit_of_measure="KG",
                        equipment_units_per_acre=50.0,
                        equipment_unit_price=85.0,
                        equipment_cost_per_acre=4250.0,
                        estimated_cost=4250.0,
                        scheduled_day=1,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="CHEMICAL",
                        activity_name="Herbicide (weed control)",
                        activity_description="Apply herbicide as recommended for early weed control.",
                        equipment_tools="ROUNDUP",
                        equipment_product_id=roundup_id,
                        equipment_unit_of_measure="LITER",
                        equipment_units_per_acre=1.5,
                        equipment_unit_price=650.0,
                        equipment_cost_per_acre=975.0,
                        estimated_cost=975.0,
                        scheduled_day=5,
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
                        scheduled_day=4,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="CHEMICAL",
                        activity_name="Top dressing (NPK)",
                        activity_description="Apply NPK as top dressing to support tuber bulking.",
                        equipment_tools="NPK 17:17:17",
                        equipment_product_id=npk_id,
                        equipment_unit_of_measure="KG",
                        equipment_units_per_acre=50.0,
                        equipment_unit_price=95.0,
                        equipment_cost_per_acre=4750.0,
                        estimated_cost=4750.0,
                        scheduled_day=30,
                        preferred_time="MORNING",
                    ),
                    # MANUAL equipment/tools examples
                    dict(
                        activity_type="MANUAL",
                        activity_name="Planting",
                        activity_description="Plant tuber seed pieces/cuttings at recommended spacing and depth.",
                        equipment_tools="String line, planting knife, seed container",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=2500.0,
                        scheduled_day=1,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="MANUAL",
                        activity_name="Ridging / earthing up",
                        activity_description="Earth up around plants to support tuber development and reduce greening.",
                        equipment_tools="Hoe, rake, gloves",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=1800.0,
                        scheduled_day=21,
                        preferred_time="MORNING",
                    ),
                    dict(
                        activity_type="MONITORING",
                        activity_name="Pest and disease scouting",
                        activity_description="Monitor for pests/diseases and record observations.",
                        equipment_tools="Notebook, phone camera",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=0.0,
                        scheduled_day=14,
                        preferred_time="EVENING",
                    ),
                    dict(
                        activity_type="HARVESTING",
                        activity_name="Harvesting",
                        activity_description="Harvest mature tubers; sort and store.",
                        equipment_tools="Sacks, crates, tarpaulin",
                        equipment_product_id=None,
                        equipment_unit_of_measure=None,
                        equipment_units_per_acre=None,
                        equipment_unit_price=None,
                        equipment_cost_per_acre=None,
                        estimated_cost=3500.0,
                        scheduled_day=90,
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
                            tuber_id,
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
                f"Inserted {inserted} TUBER activities into product_id={tuber_id} "
                f"({tuber.get('crop_code')} — {tuber.get('crop_name')})."
            )
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

