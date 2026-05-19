"""
ingestion/ingest_to_snowflake.py
---------------------------------
Ingests SAP-style CSV data into Snowflake RAW layer.
Pattern mirrors SnapLogic ETL pipelines used at Caterpillar Inc.

Steps:
  1. Read CSV (simulating SAP / SharePoint / S3 source)
  2. Add pipeline metadata columns
  3. Create Snowflake stage + RAW table if not exists
  4. COPY INTO Snowflake RAW table
  5. Log run metadata
"""

import os
import sys
import pandas as pd
import snowflake.connector
from datetime import datetime
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.snowflake_config import SNOWFLAKE_CONFIG, SCHEMAS

# ── Setup logging ─────────────────────────────────────────────────────────────
logger.add("logs/ingestion_{time}.log", rotation="1 day", retention="7 days")


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_CONFIG["account"],
            user=SNOWFLAKE_CONFIG["user"],
            password=SNOWFLAKE_CONFIG["password"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SCHEMAS["raw"],
            role=SNOWFLAKE_CONFIG["role"],
        )
        logger.info("✅ Snowflake connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to Snowflake: {e}")
        raise


def setup_raw_schema(cursor):
    """
    Create RAW schema + table if they don't exist.
    Mirrors BCD (Business Capability Domain) landing zone pattern.
    """
    logger.info("Setting up RAW schema and table...")

    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}.{SCHEMAS['raw']}",

        f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}.{SCHEMAS['raw']}.RAW_SALES_ORDERS (
            ORDER_ID            VARCHAR(50),
            ORDER_DATE          VARCHAR(20),      -- Raw string, typed in staging
            CUSTOMER_ID         VARCHAR(50),
            CUSTOMER_NAME       VARCHAR(200),
            PRODUCT_ID          VARCHAR(50),
            PRODUCT_NAME        VARCHAR(200),
            CATEGORY            VARCHAR(100),
            QUANTITY            VARCHAR(20),      -- Raw string
            UNIT_PRICE          VARCHAR(30),      -- Raw string
            TOTAL_AMOUNT        VARCHAR(30),      -- Raw string
            CURRENCY            VARCHAR(10),
            REGION              VARCHAR(50),
            PLANT_CODE          VARCHAR(20),
            SAP_DOCUMENT_TYPE   VARCHAR(20),
            CREATED_BY          VARCHAR(50),
            STATUS              VARCHAR(30),
            -- Pipeline metadata columns
            _PIPELINE_RUN_ID    VARCHAR(100),
            _INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            _SOURCE_FILE        VARCHAR(500),
            _ROW_HASH           VARCHAR(64)       -- For deduplication
        )
        """,

        # Internal stage for file uploads
        f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}.{SCHEMAS['raw']}.SALES_STAGE"
    ]

    for stmt in ddl_statements:
        cursor.execute(stmt.strip())
        logger.debug(f"Executed: {stmt.strip()[:80]}...")

    logger.info("✅ RAW schema setup complete")


def read_and_prepare_data(filepath: str, run_id: str) -> pd.DataFrame:
    """
    Read CSV and add pipeline metadata.
    Simulates reading from SAP export / SharePoint / S3.
    """
    logger.info(f"Reading source file: {filepath}")

    df = pd.read_csv(filepath, dtype=str)  # Read all as string for RAW layer
    row_count_original = len(df)

    # Add pipeline metadata columns
    df["_PIPELINE_RUN_ID"] = run_id
    df["_INGESTED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["_SOURCE_FILE"] = os.path.basename(filepath)

    # Create row hash for deduplication detection
    import hashlib
    df["_ROW_HASH"] = df.apply(
        lambda row: hashlib.md5(
            "|".join([str(v) for v in row[:16]]).encode()
        ).hexdigest(),
        axis=1
    )

    logger.info(f"✅ Loaded {row_count_original} rows from {filepath}")
    return df


def load_to_snowflake_raw(df: pd.DataFrame, cursor, conn):
    """
    Load DataFrame to Snowflake RAW table using write_pandas.
    In production this would use COPY INTO from S3/Azure Blob stage.
    """
    from snowflake.connector.pandas_tools import write_pandas

    logger.info(f"Loading {len(df)} rows to Snowflake RAW layer...")

    # Rename columns to uppercase to match Snowflake convention
    df.columns = [c.upper() for c in df.columns]

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="RAW_SALES_ORDERS",
        database=SNOWFLAKE_CONFIG["database"],
        schema=SCHEMAS["raw"],
        overwrite=False,
        auto_create_table=False,
    )

    if success:
        logger.info(f"✅ Successfully loaded {num_rows} rows in {num_chunks} chunk(s)")
    else:
        logger.error("❌ write_pandas failed")
        raise RuntimeError("Snowflake load failed")

    return num_rows


def get_row_count(cursor) -> int:
    """Verify row count after load."""
    cursor.execute(
        f"SELECT COUNT(*) FROM {SNOWFLAKE_CONFIG['database']}.{SCHEMAS['raw']}.RAW_SALES_ORDERS"
    )
    return cursor.fetchone()[0]


def run_ingestion(source_file: str = "data/sample_sales_data.csv") -> dict:
    """
    Main ingestion entry point.
    Returns run metadata dict for pipeline orchestrator.
    """
    run_id = f"RUN_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.utcnow()

    logger.info(f"{'='*60}")
    logger.info(f"  INGESTION PIPELINE STARTED — Run ID: {run_id}")
    logger.info(f"{'='*60}")

    result = {
        "run_id": run_id,
        "source_file": source_file,
        "start_time": start_time.isoformat(),
        "status": "RUNNING",
        "rows_ingested": 0,
        "error": None,
    }

    conn = None
    try:
        # Step 1: Connect
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Step 2: Setup schema/table
        setup_raw_schema(cursor)

        # Step 3: Read + prepare data
        df = read_and_prepare_data(source_file, run_id)

        # Step 4: Load to RAW
        rows_loaded = load_to_snowflake_raw(df, cursor, conn)

        # Step 5: Verify
        total_rows = get_row_count(cursor)
        logger.info(f"RAW table total row count: {total_rows}")

        result["rows_ingested"] = rows_loaded
        result["status"] = "SUCCESS"
        result["end_time"] = datetime.utcnow().isoformat()

        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"✅ INGESTION COMPLETE — {rows_loaded} rows in {duration:.2f}s")

    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = str(e)
        result["end_time"] = datetime.utcnow().isoformat()
        logger.error(f"❌ INGESTION FAILED: {e}")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

    return result


if __name__ == "__main__":
    run_ingestion()
