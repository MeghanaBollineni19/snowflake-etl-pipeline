"""
tests/test_pipeline.py
-----------------------
Unit tests for the ETL pipeline components.
Run with: pytest tests/ -v
"""

import sys
import os
import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from validation.data_quality import run_quality_checks


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_df():
    """A clean, valid sales DataFrame."""
    return pd.DataFrame({
        "order_id":    ["ORD-001", "ORD-002", "ORD-003"],
        "order_date":  ["2024-01-15", "2024-01-16", "2024-01-17"],
        "customer_id": ["CUST-101", "CUST-102", "CUST-103"],
        "customer_name": ["Customer A", "Customer B", "Customer C"],
        "product_id":  ["PROD-500", "PROD-501", "PROD-502"],
        "product_name": ["Hydraulic Pump", "Filter Kit", "Track Assembly"],
        "category":    ["Heavy Equipment", "Maintenance Parts", "Heavy Equipment"],
        "quantity":    ["10", "50", "2"],
        "unit_price":  ["1250.00", "45.50", "8900.00"],
        "total_amount":["12500.00", "2275.00", "17800.00"],
        "currency":    ["EUR", "EUR", "EUR"],
        "region":      ["EMEA", "EMEA", "EMEA"],
        "plant_code":  ["DE01", "DE01", "DE02"],
        "sap_document_type": ["ZOR", "ZOR", "ZOR"],
        "created_by":  ["SYSTEM", "SYSTEM", "SYSTEM"],
        "status":      ["COMPLETED", "COMPLETED", "PENDING"],
    })


@pytest.fixture
def dirty_df():
    """A DataFrame with intentional quality issues."""
    return pd.DataFrame({
        "order_id":    ["ORD-001", "ORD-001", None],   # duplicate + null
        "order_date":  ["2024-01-15", "bad_date", "2024-01-17"],
        "customer_id": [None, "CUST-102", "CUST-103"],  # null customer
        "customer_name": ["Customer A", "Customer B", "Customer C"],
        "product_id":  ["PROD-500", "PROD-501", "PROD-502"],
        "product_name": ["Pump", "Filter", "Track"],
        "category":    ["HE", "MP", "HE"],
        "quantity":    ["0", "50", "2"],               # qty = 0
        "unit_price":  ["1250.00", "-10.00", "8900.00"],  # negative price
        "total_amount":["0.00", "2275.00", "17800.00"],
        "currency":    ["EUR", "XYZ", "EUR"],           # invalid currency
        "region":      ["EMEA", "EMEA", "EMEA"],
        "plant_code":  ["DE01", "DE01", "DE02"],
        "sap_document_type": ["ZOR", "ZOR", "ZOR"],
        "created_by":  ["SYSTEM", "SYSTEM", "SYSTEM"],
        "status":      ["COMPLETED", "INVALID_STATUS", "PENDING"],  # bad status
    })


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestDataQualityCleanData:

    def test_clean_data_passes_all_checks(self, clean_df):
        report = run_quality_checks(clean_df, run_id="TEST_001")
        assert report.passed, f"Expected all checks to pass. Report:\n{report.summary()}"

    def test_clean_data_pass_rate_100(self, clean_df):
        report = run_quality_checks(clean_df, run_id="TEST_002")
        assert report.pass_rate == 100.0

    def test_clean_data_row_count(self, clean_df):
        report = run_quality_checks(clean_df, run_id="TEST_003")
        assert report.total_rows == 3

    def test_no_duplicates_in_clean_data(self, clean_df):
        report = run_quality_checks(clean_df, run_id="TEST_004")
        dup_check = next(c for c in report.checks if "duplicate" in c.name.lower())
        assert dup_check.passed
        assert dup_check.rows_affected == 0


class TestDataQualityDirtyData:

    def test_dirty_data_fails_overall(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_010")
        assert not report.passed

    def test_detects_null_customer_id(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_011")
        null_check = next(c for c in report.checks if "CUSTOMER_ID" in c.name)
        assert not null_check.passed
        assert null_check.rows_affected >= 1

    def test_detects_duplicate_order_ids(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_012")
        dup_check = next(c for c in report.checks if "duplicate" in c.name.lower())
        assert not dup_check.passed

    def test_detects_invalid_quantity(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_013")
        qty_check = next(c for c in report.checks if "QUANTITY" in c.name)
        assert not qty_check.passed

    def test_detects_invalid_status(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_014")
        status_check = next(c for c in report.checks if "STATUS" in c.name)
        assert not status_check.passed

    def test_detects_invalid_currency(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_015")
        curr_check = next(c for c in report.checks if "CURRENCY" in c.name)
        assert not curr_check.passed

    def test_pass_rate_below_100_for_dirty_data(self, dirty_df):
        report = run_quality_checks(dirty_df, run_id="TEST_016")
        assert report.pass_rate < 100.0


class TestPipelineOrchestration:

    def test_pipeline_runs_in_demo_mode(self):
        """Test that pipeline runner executes end-to-end in demo mode."""
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from orchestration.pipeline_runner import run_full_pipeline

        result = run_full_pipeline("data/sample_sales_data.csv")

        assert result["status"] == "SUCCESS"
        assert result["rows_ingested"] > 0
        assert result["rows_transformed"] > 0
        assert "run_id" in result
        assert result["run_id"].startswith("RUN_")

    def test_pipeline_returns_dq_pass_rate(self):
        from orchestration.pipeline_runner import run_full_pipeline
        result = run_full_pipeline("data/sample_sales_data.csv")
        assert "dq_pass_rate" in result
        assert 0 <= result["dq_pass_rate"] <= 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
