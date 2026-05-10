import pymysql
from flask import current_app


def _connect(use_database: bool):
    cfg = current_app.config
    params = {
        "host": cfg["MYSQL_HOST"],
        "port": cfg["MYSQL_PORT"],
        "user": cfg["MYSQL_USER"],
        "password": cfg["MYSQL_PASSWORD"],
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
    }
    if use_database:
        params["database"] = cfg["MYSQL_DATABASE"]
    return pymysql.connect(**params)


def ensure_database_and_schema(app):
    """Create database if missing; create / migrate tables on startup."""
    with app.app_context():
        db_name = app.config["MYSQL_DATABASE"]
        conn = _connect(use_database=False)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            conn.close()

        conn = _connect(use_database=True)
        try:
            _ensure_employees_table(conn)
            _migrate_employees_columns(conn)
            _ensure_employee_login_sessions_table(conn)
            _migrate_employee_login_sessions_columns(conn)
            _ensure_farmers_table(conn)
            _migrate_farmers_columns(conn)
            _ensure_products_table(conn)
            _migrate_products_columns(conn)
            _ensure_inventory_tables(conn)
            _ensure_suppliers_table(conn)
            _ensure_farming_sessions_table(conn)
            _ensure_farm_activities_table(conn)
            upload_dir = app.config["UPLOAD_FOLDER"]
            upload_dir.mkdir(parents=True, exist_ok=True)
        finally:
            conn.close()


def get_connection():
    return _connect(use_database=True)


EMPLOYEES_DDL = """
CREATE TABLE IF NOT EXISTS employees (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    national_id VARCHAR(64) NOT NULL,
    phone_number VARCHAR(32) NULL,
    login_code CHAR(6) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    profile_photo VARCHAR(512) NULL,
    role ENUM(
        'administrator',
        'manager',
        'health_officer',
        'sales',
        'it_support',
        'storage',
        'employee'
    ) NOT NULL DEFAULT 'employee',
    status ENUM('pending_approval', 'active', 'suspended') NOT NULL DEFAULT 'pending_approval',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_employees_email (email),
    UNIQUE KEY uq_employees_national_id (national_id),
    UNIQUE KEY uq_employees_login_code (login_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_employees_table(conn):
    with conn.cursor() as cur:
        cur.execute(EMPLOYEES_DDL.strip())


def _existing_columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            """,
            (table,),
        )
        rows = cur.fetchall()
    return {r["COLUMN_NAME"] for r in rows}


def _add_column(conn, ddl: str):
    with conn.cursor() as cur:
        cur.execute(ddl)


def _migrate_employees_columns(conn):
    """
    Additive migrations for existing installs (CREATE TABLE handles new installs).
    """
    cols = _existing_columns(conn, "employees")
    if "profile_photo" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE employees ADD COLUMN profile_photo VARCHAR(512) NULL AFTER password_hash",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise
    if "phone_number" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE employees ADD COLUMN phone_number VARCHAR(32) NULL AFTER national_id",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise


EMPLOYEE_LOGIN_SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS employee_login_sessions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    employee_id INT UNSIGNED NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY ix_employee_login_sessions_employee (employee_id),
    KEY ix_employee_login_sessions_open (employee_id, ended_at),
    CONSTRAINT fk_employee_login_sessions_employee
      FOREIGN KEY (employee_id) REFERENCES employees(id)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_employee_login_sessions_table(conn):
    with conn.cursor() as cur:
        cur.execute(EMPLOYEE_LOGIN_SESSIONS_DDL.strip())


def _migrate_employee_login_sessions_columns(conn):
    cols = _existing_columns(conn, "employee_login_sessions")
    if not cols:
        return
    if "last_seen_at" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE employee_login_sessions ADD COLUMN last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER started_at",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise


FARMERS_DDL = """
CREATE TABLE IF NOT EXISTS farmers (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    farmer_code VARCHAR(32) NOT NULL,
    status ENUM('pending_approval', 'active', 'suspended') NOT NULL DEFAULT 'pending_approval',
    farming_session_land ENUM('none', 'partial', 'full') NOT NULL DEFAULT 'none',

    full_name VARCHAR(255) NOT NULL,
    national_id VARCHAR(64) NOT NULL,
    phone_number VARCHAR(32) NOT NULL,
    alt_phone_number VARCHAR(32) NULL,
    gender ENUM('male', 'female', 'other') NULL,
    date_of_birth DATE NULL,
    profile_photo VARCHAR(512) NULL,
    national_id_upload VARCHAR(512) NULL,
    registration_consent TINYINT(1) NOT NULL DEFAULT 0,

    county VARCHAR(120) NULL,
    sub_county VARCHAR(120) NULL,
    ward VARCHAR(120) NULL,
    location VARCHAR(120) NULL,
    village VARCHAR(120) NULL,

    farm_name VARCHAR(255) NULL,
    farm_location VARCHAR(255) NULL,
    land_size DECIMAL(10,2) NULL,
    land_size_unit ENUM('acres', 'hectares') NULL,
    ownership_type ENUM('owned', 'leased', 'family_land', 'cooperative_land') NULL,
    lease_period_value INT NULL,
    lease_period_unit ENUM('months', 'years') NULL,
    main_farming_activity VARCHAR(120) NULL,
    main_crop_livestock VARCHAR(120) NULL,

    membership_number VARCHAR(80) NULL,
    cooperative_name VARCHAR(255) NULL,
    collection_center VARCHAR(255) NULL,
    field_officer VARCHAR(255) NULL,
    registration_date DATE NULL,

    mpesa_number VARCHAR(32) NULL,
    bank_account VARCHAR(120) NULL,
    preferred_payment_method VARCHAR(64) NULL,

    next_of_kin_name VARCHAR(255) NULL,
    next_of_kin_phone VARCHAR(32) NULL,
    next_of_kin_relationship VARCHAR(120) NULL,

    gps_coordinates VARCHAR(120) NULL,
    registered_by_employee_id INT UNSIGNED NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_farmers_farmer_code (farmer_code),
    UNIQUE KEY uq_farmers_national_id (national_id),
    UNIQUE KEY uq_farmers_membership_number (membership_number),
    KEY ix_farmers_phone (phone_number),
    CONSTRAINT fk_farmers_registered_by
      FOREIGN KEY (registered_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_farmers_table(conn):
    with conn.cursor() as cur:
        cur.execute(FARMERS_DDL.strip())


def _migrate_farmers_columns(conn):
    cols = _existing_columns(conn, "farmers")
    # Additive migrations for older installs (kept minimal)
    if "profile_photo" not in cols:
        try:
            _add_column(conn, "ALTER TABLE farmers ADD COLUMN profile_photo VARCHAR(512) NULL AFTER date_of_birth")
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise
    add_cols = {
        "alt_phone_number": "ALTER TABLE farmers ADD COLUMN alt_phone_number VARCHAR(32) NULL AFTER phone_number",
        "national_id_upload": "ALTER TABLE farmers ADD COLUMN national_id_upload VARCHAR(512) NULL AFTER profile_photo",
        "registration_consent": "ALTER TABLE farmers ADD COLUMN registration_consent TINYINT(1) NOT NULL DEFAULT 0 AFTER national_id_upload",
        "collection_center": "ALTER TABLE farmers ADD COLUMN collection_center VARCHAR(255) NULL AFTER cooperative_name",
        "field_officer": "ALTER TABLE farmers ADD COLUMN field_officer VARCHAR(255) NULL AFTER collection_center",
        "bank_account": "ALTER TABLE farmers ADD COLUMN bank_account VARCHAR(120) NULL AFTER mpesa_number",
        "next_of_kin_relationship": "ALTER TABLE farmers ADD COLUMN next_of_kin_relationship VARCHAR(120) NULL AFTER next_of_kin_phone",
        "lease_period_value": "ALTER TABLE farmers ADD COLUMN lease_period_value INT NULL AFTER ownership_type",
        "lease_period_unit": "ALTER TABLE farmers ADD COLUMN lease_period_unit ENUM('months','years') NULL AFTER lease_period_value",
        "location": "ALTER TABLE farmers ADD COLUMN location VARCHAR(120) NULL AFTER ward",
        "farming_session_land": (
            "ALTER TABLE farmers ADD COLUMN farming_session_land ENUM('none','partial','full') "
            "NOT NULL DEFAULT 'none' AFTER status"
        ),
    }
    for col, ddl in add_cols.items():
        if col not in cols:
            try:
                _add_column(conn, ddl)
            except pymysql.err.OperationalError as e:
                if e.args[0] != 1060:
                    raise

    # Ensure membership numbers are unique (NULLs allowed)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE UNIQUE INDEX uq_farmers_membership_number ON farmers (membership_number)")
    except pymysql.err.OperationalError as e:
        # 1061 = duplicate key name, 1060 = duplicate column (not relevant here)
        if e.args and e.args[0] not in (1061,):
            raise


PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS products (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    crop_code VARCHAR(32) NOT NULL,
    status ENUM('active', 'suspended') NOT NULL DEFAULT 'active',

    product_type ENUM('CROP','HERBICIDE','FERTILIZER','EQUIPMENT') NOT NULL DEFAULT 'CROP',

    crop_name VARCHAR(255) NOT NULL,
    scientific_name VARCHAR(255) NOT NULL,
    crop_category VARCHAR(255) NOT NULL,
    crop_variety VARCHAR(255) NOT NULL,
    crop_description TEXT NOT NULL,
    crop_image VARCHAR(512) NULL,

    planting_season VARCHAR(64) NOT NULL,
    growth_duration VARCHAR(64) NOT NULL,
    water_requirement VARCHAR(120) NOT NULL,
    average_yield_range VARCHAR(120) NULL,
    average_yield_per_acre VARCHAR(64) NULL,
    average_yield_uom VARCHAR(64) NULL,

    brand VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    unit_of_measure VARCHAR(64) NULL,
    package_size VARCHAR(64) NULL,

    active_ingredient VARCHAR(255) NULL,
    formulation VARCHAR(120) NULL,
    application_rate VARCHAR(120) NULL,
    target_use VARCHAR(255) NULL,
    safety_notes TEXT NULL,

    equipment_model VARCHAR(255) NULL,
    power_source VARCHAR(120) NULL,
    capacity VARCHAR(120) NULL,
    warranty_period VARCHAR(120) NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_products_crop_code (crop_code),
    KEY ix_products_crop_name (crop_name),
    KEY ix_products_crop_category (crop_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_products_table(conn):
    with conn.cursor() as cur:
        cur.execute(PRODUCTS_DDL.strip())


def _migrate_products_columns(conn):
    cols = _existing_columns(conn, "products")
    # Minimal additive migrations for older installs.
    add_cols = {
        "crop_image": "ALTER TABLE products ADD COLUMN crop_image VARCHAR(512) NULL AFTER crop_description",
        "average_yield_range": "ALTER TABLE products ADD COLUMN average_yield_range VARCHAR(120) NULL AFTER water_requirement",
        "average_yield_per_acre": (
            "ALTER TABLE products ADD COLUMN average_yield_per_acre VARCHAR(64) NULL AFTER average_yield_range"
        ),
        "average_yield_uom": (
            "ALTER TABLE products ADD COLUMN average_yield_uom VARCHAR(64) NULL AFTER average_yield_per_acre"
        ),
        "status": "ALTER TABLE products ADD COLUMN status ENUM('active','suspended') NOT NULL DEFAULT 'active' AFTER crop_code",
        "product_type": "ALTER TABLE products ADD COLUMN product_type ENUM('CROP','HERBICIDE','FERTILIZER','EQUIPMENT') NOT NULL DEFAULT 'CROP' AFTER status",
        "brand": "ALTER TABLE products ADD COLUMN brand VARCHAR(255) NULL AFTER average_yield_range",
        "manufacturer": "ALTER TABLE products ADD COLUMN manufacturer VARCHAR(255) NULL AFTER brand",
        "unit_of_measure": "ALTER TABLE products ADD COLUMN unit_of_measure VARCHAR(64) NULL AFTER manufacturer",
        "package_size": "ALTER TABLE products ADD COLUMN package_size VARCHAR(64) NULL AFTER unit_of_measure",
        "active_ingredient": "ALTER TABLE products ADD COLUMN active_ingredient VARCHAR(255) NULL AFTER package_size",
        "formulation": "ALTER TABLE products ADD COLUMN formulation VARCHAR(120) NULL AFTER active_ingredient",
        "application_rate": "ALTER TABLE products ADD COLUMN application_rate VARCHAR(120) NULL AFTER formulation",
        "target_use": "ALTER TABLE products ADD COLUMN target_use VARCHAR(255) NULL AFTER application_rate",
        "safety_notes": "ALTER TABLE products ADD COLUMN safety_notes TEXT NULL AFTER target_use",
        "equipment_model": "ALTER TABLE products ADD COLUMN equipment_model VARCHAR(255) NULL AFTER safety_notes",
        "power_source": "ALTER TABLE products ADD COLUMN power_source VARCHAR(120) NULL AFTER equipment_model",
        "capacity": "ALTER TABLE products ADD COLUMN capacity VARCHAR(120) NULL AFTER power_source",
        "warranty_period": "ALTER TABLE products ADD COLUMN warranty_period VARCHAR(120) NULL AFTER capacity",
    }
    for col, ddl in add_cols.items():
        if col not in cols:
            try:
                _add_column(conn, ddl)
            except pymysql.err.OperationalError as e:
                if e.args[0] != 1060:
                    raise


INVENTORY_DDL = """
CREATE TABLE IF NOT EXISTS product_inventory (
    product_id INT UNSIGNED NOT NULL,
    quantity DECIMAL(12,2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id),
    CONSTRAINT fk_inventory_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


MOVEMENTS_DDL = """
CREATE TABLE IF NOT EXISTS product_stock_movements (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    product_id INT UNSIGNED NOT NULL,
    movement_type ENUM('IN','OUT') NOT NULL,
    quantity DECIMAL(12,2) NOT NULL,

    buying_price DECIMAL(12,2) NULL,
    supplier_name VARCHAR(255) NULL,
    supplier_contact VARCHAR(64) NULL,

    stock_out_reason ENUM('SALE','DAMAGE','EXPIRED','TRANSFER','SAMPLE','ADJUSTMENT','OTHER') NULL,
    note TEXT NULL,

    created_by_employee_id INT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY ix_movements_product (product_id),
    KEY ix_movements_created_at (created_at),
    CONSTRAINT fk_movement_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_movement_employee
      FOREIGN KEY (created_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _migrate_stock_movements_farmer_intake(conn):
    cols = _existing_columns(conn, "product_stock_movements")
    if "farmer_intake_quality" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE product_stock_movements ADD COLUMN farmer_intake_quality "
                "ENUM('high','moderate','below_average','poor') NULL AFTER note",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise
    if "farmer_payment_status" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE product_stock_movements ADD COLUMN farmer_payment_status "
                "ENUM('paid','partially_paid','not_paid') NULL AFTER farmer_intake_quality",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise


def _ensure_inventory_tables(conn):
    with conn.cursor() as cur:
        cur.execute(INVENTORY_DDL.strip())
        cur.execute(MOVEMENTS_DDL.strip())
        cur.execute(DISTRIBUTIONS_DDL.strip())
        cur.execute(DISTRIBUTION_RECIPIENTS_DDL.strip())
    _migrate_distribution_tables(conn)
    _migrate_stock_movements_farmer_intake(conn)


SUPPLIERS_DDL = """
CREATE TABLE IF NOT EXISTS suppliers (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    contact VARCHAR(64) NOT NULL,
    contact_normalized VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_suppliers_contact_norm (contact_normalized)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_suppliers_table(conn):
    with conn.cursor() as cur:
        cur.execute(SUPPLIERS_DDL.strip())


DISTRIBUTIONS_DDL = """
CREATE TABLE IF NOT EXISTS product_distributions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    product_id INT UNSIGNED NOT NULL,
    quantity_per_recipient DECIMAL(12,2) NULL,
    total_quantity DECIMAL(12,2) NULL,
    recipients_count INT UNSIGNED NOT NULL,
    note TEXT NULL,
    created_by_employee_id INT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY ix_distributions_product (product_id),
    KEY ix_distributions_created_at (created_at),
    CONSTRAINT fk_distribution_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_distribution_employee
      FOREIGN KEY (created_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


DISTRIBUTION_RECIPIENTS_DDL = """
CREATE TABLE IF NOT EXISTS product_distribution_recipients (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    distribution_id INT UNSIGNED NOT NULL,
    recipient_type ENUM('FARMER','EMPLOYEE') NOT NULL,
    recipient_id INT UNSIGNED NOT NULL,
    recipient_name VARCHAR(255) NULL,
    quantity DECIMAL(12,2) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY ix_distribution_recipients_dist (distribution_id),
    KEY ix_distribution_recipients_type_id (recipient_type, recipient_id),
    CONSTRAINT fk_distribution_recipients_dist
      FOREIGN KEY (distribution_id) REFERENCES product_distributions(id)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _migrate_distribution_tables(conn):
    # Additive/relaxing migrations for existing installs.
    dist_cols = _existing_columns(conn, "product_distributions")
    if "total_quantity" not in dist_cols:
        try:
            _add_column(conn, "ALTER TABLE product_distributions ADD COLUMN total_quantity DECIMAL(12,2) NULL AFTER quantity_per_recipient")
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise

    # Allow NULL quantity_per_recipient (for per-recipient quantities)
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE product_distributions MODIFY COLUMN quantity_per_recipient DECIMAL(12,2) NULL")
    except pymysql.err.OperationalError:
        # Ignore if already modified / unsupported on older versions.
        pass

    rec_cols = _existing_columns(conn, "product_distribution_recipients")
    if "quantity" not in rec_cols:
        try:
            _add_column(conn, "ALTER TABLE product_distribution_recipients ADD COLUMN quantity DECIMAL(12,2) NULL AFTER recipient_name")
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise


FARMING_SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS farming_sessions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    farmer_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NULL,
    season_name VARCHAR(120) NULL,
    session_started_on DATE NOT NULL,
    session_ended_on DATE NULL,
    acreage_used_acres DECIMAL(12,4) NOT NULL DEFAULT 0,
    crop_or_activity VARCHAR(255) NULL,
    land_area_notes VARCHAR(255) NULL,
    notes TEXT NULL,
    status ENUM('planned', 'active', 'closed') NOT NULL DEFAULT 'active',
    registered_by_employee_id INT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY ix_farming_sessions_farmer (farmer_id),
    KEY ix_farming_sessions_started (session_started_on),
    KEY ix_farming_sessions_product (product_id),
    CONSTRAINT fk_farming_sessions_farmer
      FOREIGN KEY (farmer_id) REFERENCES farmers(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_farming_sessions_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE SET NULL,
    CONSTRAINT fk_farming_sessions_employee
      FOREIGN KEY (registered_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_farming_sessions_table(conn):
    with conn.cursor() as cur:
        cur.execute(FARMING_SESSIONS_DDL.strip())
    _migrate_farming_sessions_columns(conn)


FARM_ACTIVITIES_DDL = """
CREATE TABLE IF NOT EXISTS farm_activities (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    product_id INT UNSIGNED NOT NULL,

    activity_type ENUM('MECHANICAL','CHEMICAL','MANUAL','IRRIGATION','HARVESTING','MONITORING') NOT NULL,
    activity_name VARCHAR(255) NOT NULL,
    activity_description VARCHAR(512) NULL,
    equipment_tools VARCHAR(512) NULL,
    estimated_cost DECIMAL(12,2) NULL,
    scheduled_day INT UNSIGNED NOT NULL,
    preferred_time ENUM('MORNING','AFTERNOON','EVENING','NIGHT') NOT NULL,

    created_by_employee_id INT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY ix_farm_activities_product (product_id),
    KEY ix_farm_activities_day (scheduled_day),
    CONSTRAINT fk_farm_activities_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_farm_activities_employee
      FOREIGN KEY (created_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_farm_activities_table(conn):
    with conn.cursor() as cur:
        cur.execute(FARM_ACTIVITIES_DDL.strip())
    _migrate_farm_activities_columns(conn)
    _ensure_farm_activity_completions_table(conn)


def _migrate_farm_activities_columns(conn):
    cols = _existing_columns(conn, "farm_activities")
    add_cols = {
        "equipment_product_id": (
            "ALTER TABLE farm_activities ADD COLUMN equipment_product_id "
            "INT UNSIGNED NULL AFTER equipment_tools"
        ),
        "equipment_unit_of_measure": (
            "ALTER TABLE farm_activities ADD COLUMN equipment_unit_of_measure "
            "VARCHAR(64) NULL AFTER equipment_product_id"
        ),
        "equipment_units_per_acre": (
            "ALTER TABLE farm_activities ADD COLUMN equipment_units_per_acre "
            "DECIMAL(12,4) NULL AFTER equipment_unit_of_measure"
        ),
        "equipment_unit_price": (
            "ALTER TABLE farm_activities ADD COLUMN equipment_unit_price "
            "DECIMAL(12,2) NULL AFTER equipment_units_per_acre"
        ),
        "equipment_cost_per_acre": (
            "ALTER TABLE farm_activities ADD COLUMN equipment_cost_per_acre "
            "DECIMAL(12,2) NULL AFTER equipment_unit_price"
        ),
        "activity_status": (
            "ALTER TABLE farm_activities ADD COLUMN activity_status "
            "ENUM('ACTIVE','SUSPENDED') NOT NULL DEFAULT 'ACTIVE' AFTER preferred_time"
        ),
        "completed_on": (
            "ALTER TABLE farm_activities ADD COLUMN completed_on "
            "DATE NULL AFTER activity_status"
        ),
        "completion_note": (
            "ALTER TABLE farm_activities ADD COLUMN completion_note "
            "VARCHAR(512) NULL AFTER completed_on"
        ),
        "completed_by_employee_id": (
            "ALTER TABLE farm_activities ADD COLUMN completed_by_employee_id "
            "INT UNSIGNED NULL AFTER completion_note"
        ),
    }
    for col, ddl in add_cols.items():
        if col not in cols:
            try:
                _add_column(conn, ddl)
            except pymysql.err.OperationalError as e:
                if e.args[0] != 1060:
                    raise
    # Best-effort FK (skip if it already exists or cannot be created)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE farm_activities
                ADD CONSTRAINT fk_farm_activities_equipment
                FOREIGN KEY (equipment_product_id) REFERENCES products(id)
                ON DELETE SET NULL
                """
            )
    except pymysql.err.OperationalError:
        pass


FARM_ACTIVITY_COMPLETIONS_DDL = """
CREATE TABLE IF NOT EXISTS farm_activity_completions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    activity_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    farmer_id INT UNSIGNED NOT NULL,

    completed_on DATE NOT NULL,
    completion_note VARCHAR(512) NOT NULL,
    completed_by_employee_id INT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_activity_completion (activity_id, farmer_id),
    KEY ix_fac_product (product_id),
    KEY ix_fac_farmer (farmer_id),
    KEY ix_fac_completed_on (completed_on),

    CONSTRAINT fk_fac_activity
      FOREIGN KEY (activity_id) REFERENCES farm_activities(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_fac_product
      FOREIGN KEY (product_id) REFERENCES products(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_fac_farmer
      FOREIGN KEY (farmer_id) REFERENCES farmers(id)
      ON DELETE CASCADE,
    CONSTRAINT fk_fac_employee
      FOREIGN KEY (completed_by_employee_id) REFERENCES employees(id)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def _ensure_farm_activity_completions_table(conn):
    with conn.cursor() as cur:
        cur.execute(FARM_ACTIVITY_COMPLETIONS_DDL.strip())


def _migrate_farming_sessions_columns(conn):
    cols = _existing_columns(conn, "farming_sessions")
    if "product_id" not in cols:
        try:
            _add_column(conn, "ALTER TABLE farming_sessions ADD COLUMN product_id INT UNSIGNED NULL AFTER farmer_id")
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE farming_sessions
                    ADD CONSTRAINT fk_farming_sessions_product
                    FOREIGN KEY (product_id) REFERENCES products(id)
                    ON DELETE SET NULL
                    """
                )
        except pymysql.err.OperationalError:
            pass
    if "acreage_used_acres" not in cols:
        try:
            _add_column(
                conn,
                "ALTER TABLE farming_sessions ADD COLUMN acreage_used_acres DECIMAL(12,4) NOT NULL DEFAULT 0 AFTER session_ended_on",
            )
        except pymysql.err.OperationalError as e:
            if e.args[0] != 1060:
                raise
