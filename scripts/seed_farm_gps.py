"""Backfill random GPS coordinates onto farmer rows.

Sample latitude/longitude are drawn from a bounding box around Meru County, Kenya
so the farm locations map renders pins in a believable location. Values are
deterministic per farmer id, so re-running the script does not move pins around.

Usage (PowerShell, from repo root):

    py -3 scripts/seed_farm_gps.py            # only fill missing GPS
    py -3 scripts/seed_farm_gps.py --all      # also overwrite existing GPS
    py -3 scripts/seed_farm_gps.py --also-fill-farm-location
                                              # additionally set blank farm_location
                                              # to "lat, lng" so the map can fall
                                              # back to it
    py -3 scripts/seed_farm_gps.py --dry-run  # preview changes only
    py -3 scripts/seed_farm_gps.py --limit 25 # restrict to first N farmers
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # noqa: E402
from app.db import get_connection  # noqa: E402


# Bounding box loosely covering Meru County (Imenti / Tigania / Igembe).
# Meru town sits around lat -0.0469, lng 37.6543.
LAT_MIN, LAT_MAX = -0.30, 0.55
LNG_MIN, LNG_MAX = 37.40, 38.10

GPS_SEED_SALT = 20260509  # bump to reshuffle pins on a future run


def _gps_for_farmer_id(farmer_id: int) -> tuple[float, float]:
    """Deterministic lat/lng for a given farmer id."""
    rng = random.Random(GPS_SEED_SALT + farmer_id * 97)
    lat = round(rng.uniform(LAT_MIN, LAT_MAX), 6)
    lng = round(rng.uniform(LNG_MIN, LNG_MAX), 6)
    return lat, lng


def _format_gps(lat: float, lng: float) -> str:
    """Match the format produced by the form's auto-detect button."""
    return f"{lat:.6f}, {lng:.6f}"


def _is_blank(value) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed random GPS coordinates onto farmer rows."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Overwrite gps_coordinates even when it is already set.",
    )
    parser.add_argument(
        "--also-fill-farm-location",
        action="store_true",
        help="When farm_location is blank, also set it to the same 'lat, lng' "
        "string so the analytics map can use it as a fallback.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only update the first N matching farmer rows (0 = no limit).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing to the database.",
    )
    args = parser.parse_args()

    app = create_app()
    updated_gps = 0
    updated_farm_loc = 0
    skipped = 0
    previewed = 0

    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, full_name, farmer_code, gps_coordinates, farm_location "
                    "FROM farmers ORDER BY id"
                )
                rows = cur.fetchall()

                for row in rows:
                    if args.limit and (updated_gps + previewed) >= args.limit:
                        break

                    fid = int(row["id"])
                    has_gps = not _is_blank(row.get("gps_coordinates"))

                    if has_gps and not args.all:
                        skipped += 1
                        continue

                    lat, lng = _gps_for_farmer_id(fid)
                    gps_text = _format_gps(lat, lng)

                    new_farm_loc = None
                    if args.also_fill_farm_location and _is_blank(row.get("farm_location")):
                        new_farm_loc = gps_text

                    if args.dry_run:
                        previewed += 1
                        label = row.get("farmer_code") or row.get("full_name") or fid
                        extra = f" + farm_location={new_farm_loc!r}" if new_farm_loc else ""
                        print(f"[dry-run] farmer #{fid} ({label}) -> gps={gps_text}{extra}")
                        continue

                    cur.execute(
                        "UPDATE farmers SET gps_coordinates=%s WHERE id=%s",
                        (gps_text, fid),
                    )
                    if cur.rowcount:
                        updated_gps += 1

                    if new_farm_loc is not None:
                        cur.execute(
                            "UPDATE farmers SET farm_location=%s "
                            "WHERE id=%s AND (farm_location IS NULL OR TRIM(farm_location)='')",
                            (new_farm_loc, fid),
                        )
                        if cur.rowcount:
                            updated_farm_loc += 1
        finally:
            conn.close()

    if args.dry_run:
        print(f"Dry run: would update GPS on {previewed} farmer row(s).")
    else:
        print(
            f"Updated GPS on {updated_gps} farmer row(s); "
            f"filled farm_location on {updated_farm_loc} row(s); "
            f"skipped {skipped} row(s) that already had GPS."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
