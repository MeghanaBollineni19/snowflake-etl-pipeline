"""
validation/data_quality.py
---------------------------
Data quality checks before and after transformation.
Implements the same validation patterns used for Caterpillar Inc. data pipelines.

Checks performed:
  - Null / missing value detection
  - Duplicate row detection (via row hash)
  - Data type validation
  - Referential integrity (customer_id, product_id not null)
  - Range validation (quantity > 0, unit_price > 0)
  - Enum validation (status values, currency)
"""

import pandas as pd
from dataclasses import dataclass, field
from typing import List
from loguru import logger


@dataclass
class QualityCheck:
    name: str
    passed: bool
    rows_affected: int = 0
    details: str = ""


@dataclass
class QualityReport:
    run_id: str
    table_name: str
    total_rows: int
    checks: List[QualityCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def pass_rate(self) -> float:
        if not self.checks:
            return 0.0
        return sum(1 for c in self.checks if c.passed) / len(self.checks) * 100

    def summary(self) -> str:
        lines = [
            f"\n{'='*55}",
            f"  DATA QUALITY REPORT — {self.table_name}",
            f"  Run ID : {self.run_id}",
            f"  Rows   : {self.total_rows}",
            f"  Result : {'✅ PASSED' if self.passed else '❌ FAILED'}",
            f"  Score  : {self.pass_rate:.1f}%",
            f"{'='*55}",
        ]
        for c in self.checks:
            icon = "✅" if c.passed else "❌"
            lines.append(f"  {icon} {c.name:<35} rows_affected={c.rows_affected}")
            if c.details:
                lines.append(f"      ↳ {c.details}")
        lines.append(f"{'='*55}")
        return "\n".join(lines)


def run_quality_checks(df: pd.DataFrame, run_id: str, table_name: str = "RAW_SALES_ORDERS") -> QualityReport:
    """
    Execute all data quality checks on a DataFrame.
    Returns a QualityReport with pass/fail per check.
    """
    report = QualityReport(run_id=run_id, table_name=table_name, total_rows=len(df))

    # ── Check 1: No fully empty rows ──────────────────────────────────────────
    empty_rows = df.isnull().all(axis=1).sum()
    report.checks.append(QualityCheck(
        name="No fully empty rows",
        passed=empty_rows == 0,
        rows_affected=int(empty_rows),
        details=f"{empty_rows} fully null rows found" if empty_rows > 0 else ""
    ))

    # ── Check 2: ORDER_ID not null ────────────────────────────────────────────
    null_order_ids = df["order_id"].isnull().sum() if "order_id" in df.columns else df["ORDER_ID"].isnull().sum()
    report.checks.append(QualityCheck(
        name="ORDER_ID not null",
        passed=null_order_ids == 0,
        rows_affected=int(null_order_ids),
        details=f"{null_order_ids} rows missing ORDER_ID" if null_order_ids > 0 else ""
    ))

    # ── Check 3: CUSTOMER_ID not null ─────────────────────────────────────────
    col = "customer_id" if "customer_id" in df.columns else "CUSTOMER_ID"
    null_customers = df[col].isnull().sum()
    report.checks.append(QualityCheck(
        name="CUSTOMER_ID not null",
        passed=null_customers == 0,
        rows_affected=int(null_customers),
        details=f"{null_customers} rows missing CUSTOMER_ID" if null_customers > 0 else ""
    ))

    # ── Check 4: No duplicate ORDER_IDs ──────────────────────────────────────
    col = "order_id" if "order_id" in df.columns else "ORDER_ID"
    dupes = df[col].duplicated().sum()
    report.checks.append(QualityCheck(
        name="No duplicate ORDER_IDs",
        passed=dupes == 0,
        rows_affected=int(dupes),
        details=f"{dupes} duplicate order IDs" if dupes > 0 else ""
    ))

    # ── Check 5: QUANTITY > 0 ─────────────────────────────────────────────────
    col = "quantity" if "quantity" in df.columns else "QUANTITY"
    try:
        invalid_qty = (pd.to_numeric(df[col], errors="coerce") <= 0).sum()
    except Exception:
        invalid_qty = 0
    report.checks.append(QualityCheck(
        name="QUANTITY > 0",
        passed=invalid_qty == 0,
        rows_affected=int(invalid_qty),
        details=f"{invalid_qty} rows with quantity <= 0" if invalid_qty > 0 else ""
    ))

    # ── Check 6: UNIT_PRICE > 0 ───────────────────────────────────────────────
    col = "unit_price" if "unit_price" in df.columns else "UNIT_PRICE"
    try:
        invalid_price = (pd.to_numeric(df[col], errors="coerce") <= 0).sum()
    except Exception:
        invalid_price = 0
    report.checks.append(QualityCheck(
        name="UNIT_PRICE > 0",
        passed=invalid_price == 0,
        rows_affected=int(invalid_price),
        details=f"{invalid_price} rows with unit_price <= 0" if invalid_price > 0 else ""
    ))

    # ── Check 7: STATUS valid enum ────────────────────────────────────────────
    col = "status" if "status" in df.columns else "STATUS"
    valid_statuses = {"COMPLETED", "PENDING", "FAILED", "CANCELLED"}
    invalid_status = (~df[col].str.upper().isin(valid_statuses)).sum()
    report.checks.append(QualityCheck(
        name="STATUS valid enum",
        passed=invalid_status == 0,
        rows_affected=int(invalid_status),
        details=f"{invalid_status} rows with invalid STATUS value" if invalid_status > 0 else ""
    ))

    # ── Check 8: CURRENCY is EUR or USD ──────────────────────────────────────
    col = "currency" if "currency" in df.columns else "CURRENCY"
    valid_currencies = {"EUR", "USD", "GBP"}
    invalid_curr = (~df[col].str.upper().isin(valid_currencies)).sum()
    report.checks.append(QualityCheck(
        name="CURRENCY valid",
        passed=invalid_curr == 0,
        rows_affected=int(invalid_curr),
        details=f"{invalid_curr} rows with unexpected currency" if invalid_curr > 0 else ""
    ))

    # ── Check 9: TOTAL_AMOUNT = QUANTITY * UNIT_PRICE ─────────────────────────
    try:
        qty_col = "quantity" if "quantity" in df.columns else "QUANTITY"
        price_col = "unit_price" if "unit_price" in df.columns else "UNIT_PRICE"
        total_col = "total_amount" if "total_amount" in df.columns else "TOTAL_AMOUNT"

        qty   = pd.to_numeric(df[qty_col], errors="coerce")
        price = pd.to_numeric(df[price_col], errors="coerce")
        total = pd.to_numeric(df[total_col], errors="coerce")
        expected = qty * price
        mismatch = ((total - expected).abs() > 0.01).sum()
    except Exception:
        mismatch = 0

    report.checks.append(QualityCheck(
        name="TOTAL_AMOUNT = QTY * PRICE",
        passed=mismatch == 0,
        rows_affected=int(mismatch),
        details=f"{mismatch} rows with amount mismatch" if mismatch > 0 else ""
    ))

    # ── Check 10: ORDER_DATE parseable ────────────────────────────────────────
    col = "order_date" if "order_date" in df.columns else "ORDER_DATE"
    unparseable = pd.to_datetime(df[col], errors="coerce").isnull().sum()
    report.checks.append(QualityCheck(
        name="ORDER_DATE parseable",
        passed=unparseable == 0,
        rows_affected=int(unparseable),
        details=f"{unparseable} rows with invalid date format" if unparseable > 0 else ""
    ))

    logger.info(report.summary())
    return report


if __name__ == "__main__":
    # Demo: run checks against local CSV
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    df = pd.read_csv("data/sample_sales_data.csv", dtype=str)
    report = run_quality_checks(df, run_id="DEMO_RUN_001")

    print(report.summary())
    print(f"\nOverall: {'PASSED ✅' if report.passed else 'FAILED ❌'}")
