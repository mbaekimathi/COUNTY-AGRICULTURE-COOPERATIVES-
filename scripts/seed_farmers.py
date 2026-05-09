from __future__ import annotations

import argparse
import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# Allow running as a script from /scripts on Windows.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # noqa: E402
from app.db import get_connection  # noqa: E402


FIRST_NAMES = [
    "JOHN",
    "MARY",
    "PETER",
    "GRACE",
    "JAMES",
    "ANNE",
    "DAVID",
    "JANE",
    "SAMUEL",
    "JOYCE",
    "MICHAEL",
    "SARAH",
    "DANIEL",
    "RUTH",
    "PAUL",
    "LUCY",
    "ROBERT",
    "MARGARET",
    "GEORGE",
    "PATRICIA",
]

LAST_NAMES = [
    "MUTUA",
    "MWITI",
    "KIRIMI",
    "MURITHI",
    "NJERU",
    "KARIUKI",
    "MUGO",
    "MUNENE",
    "KARANJA",
    "KIMANI",
    "KIBET",
    "CHEPKOECH",
    "ODHIAMBO",
    "OKELLO",
    "WANGARI",
    "WAMBUA",
    "KILONZO",
    "KABIRU",
    "MWENDA",
    "GICHERU",
]

WARDS = [
    "NYAKI WEST",
    "NYAKI EAST",
    "TOWN",
    "MUNICIPAL",
    "KIANJAI",
    "MUTHAMBI",
    "KANGETA",
    "KIEGOI",
    "MAUA",
    "KATHERA",
]


@dataclass(frozen=True)
class SubCountyPlan:
    sub_county: str
    count: int


PLAN: list[SubCountyPlan] = [
    SubCountyPlan("IMENTI NORTH", 50),
    SubCountyPlan("IMENTI SOUTH", 25),
    SubCountyPlan("IGEMBE", 30),
    SubCountyPlan("TIGANIA", 30),
    # The rest (200 - 135 = 65). We'll keep them in MERU COUNTY under IMENTI CENTRAL.
    SubCountyPlan("IMENTI CENTRAL", 65),
]


def _rand_dob(rng: random.Random) -> date:
    # Adults: 18–60 years old roughly
    today = date.today()
    max_age_days = 60 * 365
    min_age_days = 18 * 365
    days = rng.randint(min_age_days, max_age_days)
    return today - timedelta(days=days)


def _make_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _sample_land(rng: random.Random) -> tuple[float, str]:
    """Random but realistic land size; unit is acres or hectares."""
    unit = rng.choice(("acres", "hectares"))
    if unit == "acres":
        size = round(rng.uniform(0.25, 48.0), 2)
    else:
        size = round(rng.uniform(0.05, 20.0), 2)
    return size, unit


def _land_for_farmer_id(farmer_id: int) -> tuple[float, str]:
    """Deterministic land sample for backfilling existing rows."""
    r = random.Random(20260507 + farmer_id * 31)
    return _sample_land(r)


def backfill_land_sizes() -> int:
    app = create_app()
    updated = 0
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM farmers WHERE land_size IS NULL")
                rows = cur.fetchall()
                for row in rows:
                    fid = int(row["id"])
                    land_size, land_unit = _land_for_farmer_id(fid)
                    cur.execute(
                        "UPDATE farmers SET land_size=%s, land_size_unit=%s WHERE id=%s AND land_size IS NULL",
                        (land_size, land_unit, fid),
                    )
                    updated += cur.rowcount
        finally:
            conn.close()
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed or backfill dummy farmers.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Set land_size / land_size_unit on existing rows where land_size IS NULL (deterministic per id).",
    )
    args = parser.parse_args()

    if args.backfill:
        n = backfill_land_sizes()
        print(f"Backfilled land size on {n} farmer row(s).")
        return 0

    rng = random.Random(20260507)  # deterministic seed

    app = create_app()
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                inserted = 0

                # Ensure national_id uniqueness by starting above current max.
                cur.execute("SELECT COALESCE(MAX(id), 0) AS mx FROM farmers")
                base = int(cur.fetchone()["mx"]) + 1

                for p in PLAN:
                    for _ in range(p.count):
                        full_name = _make_name(rng).upper()
                        national_id = f"ID{base + inserted:07d}".upper()
                        phone_number = f"+2547{rng.randint(10000000, 99999999)}"

                        county = "MERU"
                        sub_county = p.sub_county.upper()
                        ward = rng.choice(WARDS).upper()
                        location = f"LOCATION {rng.randint(1, 20)}"
                        village = f"VILLAGE {rng.randint(1, 40)}"

                        land_size, land_size_unit = _sample_land(rng)

                        cooperative_name = "MERU COOPERATIVES"
                        mpesa_number = phone_number
                        next_of_kin_name = _make_name(rng).upper()
                        next_of_kin_phone = f"+2547{rng.randint(10000000, 99999999)}"

                        cur.execute(
                            """
                            INSERT INTO farmers (
                                farmer_code, status,
                                full_name, national_id, phone_number,
                                date_of_birth, registration_consent,
                                county, sub_county, ward, location, village,
                                land_size, land_size_unit,
                                cooperative_name,
                                mpesa_number, preferred_payment_method,
                                next_of_kin_name, next_of_kin_phone,
                                registered_by_employee_id
                            ) VALUES (
                                %s, %s,
                                %s, %s, %s,
                                %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s,
                                %s,
                                %s, %s,
                                %s, %s,
                                %s
                            )
                            """,
                            (
                                "TEMP",
                                "pending_approval",
                                full_name,
                                national_id,
                                phone_number.upper(),
                                _rand_dob(rng),
                                1,
                                county,
                                sub_county,
                                ward,
                                location,
                                village,
                                land_size,
                                land_size_unit,
                                cooperative_name,
                                mpesa_number.upper(),
                                "mpesa",
                                next_of_kin_name,
                                next_of_kin_phone.upper(),
                                None,
                            ),
                        )
                        new_id = int(cur.lastrowid)
                        farmer_code = f"F{new_id:06d}"
                        membership_number = f"M{new_id:06d}"
                        cur.execute(
                            "UPDATE farmers SET farmer_code=%s, membership_number=%s, registration_date=COALESCE(registration_date, CURDATE()) WHERE id=%s",
                            (farmer_code, membership_number, new_id),
                        )

                        inserted += 1

        finally:
            conn.close()

    print(f"Inserted {inserted} farmers into DB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

