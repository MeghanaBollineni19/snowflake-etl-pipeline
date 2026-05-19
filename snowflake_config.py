"""
config/snowflake_config.py
--------------------------
Snowflake connection configuration.
Uses environment variables — NEVER hardcode credentials.
"""
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT", "your_account.eu-west-1"),
    "user":      os.getenv("SNOWFLAKE_USER",    "your_user"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD","your_password"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "SALES_DWH"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",   "RAW"),
    "role":      os.getenv("SNOWFLAKE_ROLE",     "DATA_ENGINEER"),
}

# Layer schemas — mirrors enterprise BCD (Business Capability Domain) design
SCHEMAS = {
    "raw":     "RAW",       # landing zone — exact copy of source
    "staging": "STAGING",   # cleaned, typed, validated
    "dwh":     "DWH",       # star schema — facts + dimensions
    "metrics": "METRICS",   # pipeline run metadata
}

# Warehouse sizes — scale up for heavy loads, down after
WAREHOUSE_SIZES = {
    "small":  "COMPUTE_WH",
    "medium": "TRANSFORM_WH_M",
    "large":  "TRANSFORM_WH_L",
}
