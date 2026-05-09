from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

# Allow running as a script from /scripts on Windows.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # noqa: E402
from app.db import get_connection  # noqa: E402


@dataclass(frozen=True)
class ProductSeed:
    product_type: str
    product_name: str
    description: str
    # shared (optional)
    brand: str | None = None
    manufacturer: str | None = None
    # Stored in DB column `package_size`: typical quantity of product (in unit_of_measure) per acre.
    amount_per_acre: str | None = None
    unit_of_measure: str | None = None
    # crop-only (optional for other types)
    scientific_name: str | None = None
    crop_category: str | None = None
    crop_variety: str | None = None
    planting_season: str | None = None
    growth_duration: str | None = None
    water_requirement: str | None = None
    average_yield_range: str | None = None
    average_yield_per_acre: str | None = None
    # agrochemicals
    active_ingredient: str | None = None
    formulation: str | None = None
    application_rate: str | None = None
    target_use: str | None = None
    safety_notes: str | None = None
    # equipment
    equipment_model: str | None = None
    power_source: str | None = None
    capacity: str | None = None
    warranty_period: str | None = None


SEEDS: list[ProductSeed] = [
    # CROPS — average_yield_per_acre & amount_per_acre use unit_of_measure (KG): demo benchmarks for Meru/smallholder Kenya.
    # amount_per_acre = seed/planting material rate per acre for warehouse × session acres (numeric).
    ProductSeed(
        product_type="CROP",
        product_name="MAIZE",
        scientific_name="ZEA MAYS",
        crop_category="CEREAL",
        crop_variety="H513",
        description="HYBRID MAIZE FOR GRAIN; COMMON LONG RAINS CROP IN CENTRAL/KENYA HIGHLANDS COOPERATIVES.",
        planting_season="LONG RAINS",
        growth_duration="110-140 DAYS",
        water_requirement="MODERATE",
        average_yield_per_acre="2200",
        brand="PANNAR",
        manufacturer="ADVENTA",
        unit_of_measure="KG",
        amount_per_acre="10",
    ),
    ProductSeed(
        product_type="CROP",
        product_name="BEANS",
        scientific_name="PHASEOLUS VULGARIS",
        crop_category="LEGUME",
        crop_variety="MWITEMANIA",
        description="BUSH BEAN FOR MARKET AND HOME USE; OFTEN INTERCROPPED WITH MAIZE.",
        planting_season="LONG RAINS",
        growth_duration="75-95 DAYS",
        water_requirement="MODERATE",
        average_yield_per_acre="700",
        brand="KALRO",
        unit_of_measure="KG",
        amount_per_acre="36",
    ),
    ProductSeed(
        product_type="CROP",
        product_name="POTATO",
        scientific_name="SOLANUM TUBEROSUM",
        crop_category="TUBER",
        crop_variety="SHANGI",
        description="TABLE POTATO; VOLCANIC LOAM AREAS AROUND MERU SUITABLE IF ROTATION AND BLIGHT MANAGED.",
        planting_season="LONG RAINS",
        growth_duration="90-110 DAYS",
        water_requirement="MODERATE",
        average_yield_per_acre="9500",
        unit_of_measure="KG",
        amount_per_acre="800",
    ),
    ProductSeed(
        product_type="CROP",
        product_name="RICE",
        scientific_name="ORYZA SATIVA",
        crop_category="CEREAL",
        crop_variety="NERICA 4",
        description="UPLAND OR PADDY RICE WHERE WATER CONTROL EXISTS; DEMO ROW FOR IRRIGATED SCHEMES.",
        planting_season="ALL YEAR",
        growth_duration="115-135 DAYS",
        water_requirement="IRRIGATED",
        average_yield_per_acre="4800",
        unit_of_measure="KG",
        amount_per_acre="72",
    ),
    ProductSeed(
        product_type="CROP",
        product_name="SORGHUM",
        scientific_name="SORGHUM BICOLOR",
        crop_category="CEREAL",
        crop_variety="GADAM EL HAMAM",
        description="DROUGHT-TOLERANT GRAIN FOR SHORT RAINS AND MARGINAL RAINFALL AREAS.",
        planting_season="SHORT RAINS",
        growth_duration="100-125 DAYS",
        water_requirement="LOW",
        average_yield_per_acre="1400",
        unit_of_measure="KG",
        amount_per_acre="10",
    ),
    ProductSeed(
        product_type="CROP",
        product_name="WHEAT",
        scientific_name="TRITICUM AESTIVUM",
        crop_category="CEREAL",
        crop_variety="KWALE",
        description="WHEAT FOR BREAD AND MARKET; COOLER HIGHLAND TIMING ALIGNED TO LONG OR SHORT RAINS WINDOWS.",
        planting_season="LONG RAINS",
        growth_duration="100-120 DAYS",
        water_requirement="MODERATE",
        average_yield_per_acre="1800",
        brand="KENYA SEED",
        unit_of_measure="KG",
        amount_per_acre="50",
    ),
    # HERBICIDES — amount_per_acre = product volume per acre (stock UOM); application_rate = spray mix / usage notes only.
    ProductSeed(
        product_type="HERBICIDE",
        product_name="ROUNDUP",
        description="GLYPHOSATE SL FOR BURNDOWN AND GENERAL WEED CONTROL WHEN LABEL ALLOWS.",
        brand="ROUNDUP",
        manufacturer="BAYER",
        amount_per_acre="1.5",
        unit_of_measure="LITER",
        active_ingredient="GLYPHOSATE (IPA SALT)",
        formulation="SL",
        application_rate="TYPICALLY 2-3 ML PRODUCT PER LITRE SPRAY WATER; FULL COVERAGE SPRAY TO RUNOFF PER LABEL",
        target_use="NON-SELECTIVE WEEDS BEFORE PLANTING OR SPOT SPRAY",
        safety_notes="PPE REQUIRED; AVOID DRIFT NEAR CROPS AND WATER; READ LABEL RATES.",
    ),
    ProductSeed(
        product_type="HERBICIDE",
        product_name="2,4-D AMINE",
        description="SELECTIVE CONTROL OF BROADLEAF WEEDS IN ESTABLISHED GRASSES AND LABELLED CEREALS.",
        brand="AGROMINE",
        manufacturer="GENERIC AGROCHEM",
        amount_per_acre="800",
        unit_of_measure="ML",
        active_ingredient="2,4-D DIMETHYLAMINE",
        formulation="SL",
        application_rate="MIX 1-2 ML PRODUCT PER LITRE WATER OR AS LABEL; MEDIUM DROPLETS; CALM WIND",
        target_use="BROADLEAF WEEDS IN MAIZE/WHEAT (LABEL STAGES ONLY)",
        safety_notes="VOLATILE; KEEP BUFFER FROM SENSITIVE BROADLEAF CROPS; NO SPRAY DRIFT.",
    ),
    ProductSeed(
        product_type="HERBICIDE",
        product_name="DUAL GOLD",
        description="S-METOLACHLOR EC FOR PRE-EMERGENCE GRASS AND SOME BROADLEAF WEEDS IN MAIZE SYSTEMS.",
        brand="DUAL GOLD",
        manufacturer="SYNGENTA",
        amount_per_acre="1.4",
        unit_of_measure="LITER",
        active_ingredient="S-METOLACHLOR",
        formulation="EC",
        application_rate="3-4 ML PRODUCT PER LITRE SPRAY WATER; INCORPORATE LIGHTLY IF LABEL REQUIRES",
        target_use="PRE-EMERGENCE IN MAIZE SOIL APPLIED",
        safety_notes="DO NOT CONTAMINATE WATERWAYS; STORE LOCKED AWAY FROM CHILDREN.",
    ),
    ProductSeed(
        product_type="HERBICIDE",
        product_name="ATRAZINE",
        description="SELECTIVE HERBICIDE USED IN MAIZE FOR BROADLEAF AND SOME GRASS WEEDS (LABEL-DEPENDENT).",
        brand="GESAPRIM",
        manufacturer="SYNGENTA",
        amount_per_acre="2",
        unit_of_measure="KG",
        active_ingredient="ATRAZINE",
        formulation="WG",
        application_rate="DISSOLVE GRANULES PER LABEL LITRES WATER; APPLY BEFORE OR AFTER EMERGENCE AS ALLOWED",
        target_use="WEED CONTROL IN MAIZE",
        safety_notes="GROUNDWATER AND BUFFER ZONES PER LABEL; ROTATE MODES OF ACTION.",
    ),
    # FERTILIZERS — amount_per_acre = KG product per acre for distribution math; application_rate = how/when to apply (not bags/acre).
    ProductSeed(
        product_type="FERTILIZER",
        product_name="DAP",
        description="DI-AMMONIUM PHOSPHATE BASAL AT PLANTING FOR ROOTING AND EARLY P UPTAKE.",
        brand="YARA",
        manufacturer="YARA EAST AFRICA",
        amount_per_acre="50",
        unit_of_measure="KG",
        active_ingredient="18-46-0",
        formulation="GRANULAR",
        application_rate="PLACEMENT IN PLANTING FURROW OR BAND; COVER WITH SOIL; DO NOT LEAVE ON SURFACE",
        target_use="BASAL AT PLANTING (MAIZE, POTATO, VEGETABLES)",
        safety_notes="STORE DRY; WASH SKIN AFTER HANDLING; USE GLOVES WHEN BULK HANDLING.",
    ),
    ProductSeed(
        product_type="FERTILIZER",
        product_name="CAN",
        description="CALCIUM AMMONIUM NITRATE TOP-DRESS FOR NITROGEN BOOST DURING VEGETATIVE GROWTH.",
        brand="MEA",
        manufacturer="MINISTRY EAST AFRICA",
        amount_per_acre="48",
        unit_of_measure="KG",
        active_ingredient="CALCIUM AMMONIUM NITRATE",
        formulation="GRANULAR",
        application_rate="SPLIT TOP-DRESS; APPLY WHEN SOIL MOIST; LIGHT RAIN OR IRRIGATION AFTER HELPS",
        target_use="TOP DRESSING MAIZE AT KNEE-HIGH TO TASSLING",
        safety_notes="HYGROSCOPIC; KEEP BAGS CLOSED; AVOID HOT STORAGE.",
    ),
    ProductSeed(
        product_type="FERTILIZER",
        product_name="NPK 17:17:17",
        description="BALANCED NPK FOR GENERAL CROP MAINTENANCE WHERE SOIL TEST SHOWS MULTI-NUTRIENT NEED.",
        brand="TWIGA",
        manufacturer="TWIGA CHEMICALS",
        amount_per_acre="50",
        unit_of_measure="KG",
        active_ingredient="17-17-17",
        formulation="GRANULAR",
        application_rate="BROADCAST AND WORK IN SHALLOW; OR BAND SIDEDRESS PER CROP ADVISORY",
        target_use="VEGETABLES AND FIELD CROPS AS ADVISOR DIRECTS",
        safety_notes="KEEP AWAY FROM CHILDREN AND LIVESTOCK FEED AREAS.",
    ),
    ProductSeed(
        product_type="FERTILIZER",
        product_name="UREA",
        description="HIGH-N SOURCE FOR TOP-DRESS OR SPLIT-N PROGRAMMES WHERE OTHER N FORMS ARE NOT USED.",
        brand="YARA",
        manufacturer="YARA EAST AFRICA",
        amount_per_acre="40",
        unit_of_measure="KG",
        active_ingredient="46% N",
        formulation="PRILLED",
        application_rate="BROADCAST EVENLY WHEN LEAF DRY; INCORPORATE LIGHTLY OR APPLY BEFORE LIGHT RAIN",
        target_use="TOP DRESSING AND SPLIT APPLICATIONS IN MAIZE",
        safety_notes="VOLATILE LOSSES POSSIBLE ON HOT SURFACE; FOLLOW BEST PRACTICE TIMING.",
    ),
    # EQUIPMENT
    ProductSeed(
        product_type="EQUIPMENT",
        product_name="KNAPSACK SPRAYER",
        description="16 L MANUAL KNAPSACK FOR SAFE APPLICATION OF HERBICIDES AND LIQUID PRODUCTS.",
        brand="SOLO",
        manufacturer="SOLO",
        unit_of_measure="UNIT",
        equipment_model="SOLO 425",
        power_source="MANUAL",
        capacity="16 L TANK",
        warranty_period="12 MONTHS",
    ),
    ProductSeed(
        product_type="EQUIPMENT",
        product_name="WATER PUMP",
        description="PETROL WATER PUMP FOR SMALLHOLDER IRRIGATION AND FARM WATER MOVEMENT.",
        brand="HONDA",
        manufacturer="HONDA",
        unit_of_measure="UNIT",
        equipment_model="WB20XT",
        power_source="PETROL",
        capacity="2 INCH OUTLET",
        warranty_period="12 MONTHS",
    ),
    ProductSeed(
        product_type="EQUIPMENT",
        product_name="HAND HOE",
        description="STEEL HAND HOE FOR WEEDING, RIDGING, AND LIGHT SOIL WORK ON SMALL PLOTS.",
        brand="JIKA",
        manufacturer="LOCAL SUPPLY",
        unit_of_measure="PIECE",
        equipment_model="STANDARD 1.2 KG HEAD",
        power_source="MANUAL",
        capacity="N/A",
        warranty_period="6 MONTHS HANDLE",
    ),
]


def sync_existing_amounts() -> int:
    """Update `package_size` (amount per acre) for rows matching seed product_type + crop_name."""
    app = create_app()
    updated = 0
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                for s in SEEDS:
                    ptype = (s.product_type or "CROP").strip().upper()
                    name = s.product_name.strip().upper()
                    amt = s.amount_per_acre.strip().upper() if s.amount_per_acre else None
                    uom = s.unit_of_measure.strip().upper() if s.unit_of_measure else None
                    cur.execute(
                        """
                        UPDATE products
                        SET package_size=%s, unit_of_measure=COALESCE(%s, unit_of_measure)
                        WHERE product_type=%s AND crop_name=%s
                        """,
                        (amt, uom, ptype, name),
                    )
                    updated += cur.rowcount
        finally:
            conn.close()
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Insert seed products or sync amount-per-acre from seeds.")
    parser.add_argument(
        "--sync-existing",
        action="store_true",
        help="Update package_size (amount per acre) on existing products that match seed names/types.",
    )
    args = parser.parse_args()

    if args.sync_existing:
        n = sync_existing_amounts()
        print(f"Updated {n} product row(s) (matched by name + type).")
        return 0

    app = create_app()
    with app.app_context():
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                inserted = 0
                for s in SEEDS:
                    ptype = (s.product_type or "CROP").strip().upper()
                    prefix = {"CROP": "CR", "HERBICIDE": "HB", "FERTILIZER": "FZ", "EQUIPMENT": "EQ"}.get(ptype, "PR")

                    # For non-crop types, fill legacy required crop fields (keeps schema compatible)
                    scientific_name = (s.scientific_name or ("N/A" if ptype != "CROP" else "")).strip().upper()
                    crop_category = (s.crop_category or (ptype if ptype != "CROP" else "")).strip().upper()
                    crop_variety = (s.crop_variety or (s.formulation or s.equipment_model or "N/A")).strip().upper()
                    planting_season = (s.planting_season or ("ALL YEAR" if ptype != "CROP" else "")).strip().upper()
                    growth_duration = (s.growth_duration or ("N/A" if ptype != "CROP" else "")).strip().upper()
                    water_requirement = (s.water_requirement or ("N/A" if ptype != "CROP" else "")).strip().upper()

                    uom_val = (s.unit_of_measure.strip().upper() if s.unit_of_measure else None)
                    ay = (s.average_yield_per_acre.strip().upper() if s.average_yield_per_acre else None)
                    ay_uom_ins = uom_val if ay else None
                    if ay and uom_val:
                        ay_range_ins = f"{ay} {uom_val}/ACRE"
                    else:
                        ay_range_ins = (s.average_yield_range.strip().upper() if s.average_yield_range else None)

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
                            ptype,
                            s.product_name.strip().upper(),
                            scientific_name,
                            crop_category,
                            crop_variety,
                            s.description.strip().upper(),
                            None,  # crop_image
                            planting_season,
                            growth_duration,
                            water_requirement,
                            ay_range_ins,
                            ay,
                            ay_uom_ins,
                            (s.brand.strip().upper() if s.brand else None),
                            (s.manufacturer.strip().upper() if s.manufacturer else None),
                            (s.unit_of_measure.strip().upper() if s.unit_of_measure else None),
                            (s.amount_per_acre.strip().upper() if s.amount_per_acre else None),
                            (s.active_ingredient.strip().upper() if s.active_ingredient else None),
                            (s.formulation.strip().upper() if s.formulation else None),
                            (s.application_rate.strip().upper() if s.application_rate else None),
                            (s.target_use.strip().upper() if s.target_use else None),
                            (s.safety_notes.strip().upper() if s.safety_notes else None),
                            (s.equipment_model.strip().upper() if s.equipment_model else None),
                            (s.power_source.strip().upper() if s.power_source else None),
                            (s.capacity.strip().upper() if s.capacity else None),
                            (s.warranty_period.strip().upper() if s.warranty_period else None),
                        ),
                    )
                    new_id = cur.lastrowid
                    code = f"{prefix}{int(new_id):06d}"
                    cur.execute("UPDATE products SET crop_code=%s WHERE id=%s", (code, int(new_id)))
                    inserted += 1
        finally:
            conn.close()

    print(f"Inserted {inserted} products into DB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

