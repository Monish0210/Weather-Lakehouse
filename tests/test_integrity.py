# ============================================================
# DAG INTEGRITY TESTS — Windows Compatible Version
# Purpose: Verify DAG files have correct structure
#          without needing Airflow to run on Windows
# Run: pytest tests/test_dag_integrity.py -v
# ============================================================

import ast
import os
import pytest


# ── Constants ─────────────────────────────────────────────────
DAGS_FOLDER = "dags"
BACKFILL_DAG_FILE       = f"{DAGS_FOLDER}/backfill_dag.py"
INCREMENTAL_DAG_FILE    = f"{DAGS_FOLDER}/incremental_dag.py"

SQL_BASE                = "sql"


# ============================================================
# HELPER
# ============================================================

def read_dag_file(filepath: str) -> str:
    """Read DAG file content as string."""
    with open(filepath, "r") as f:
        return f.read()


def parse_dag_file(filepath: str) -> ast.Module:
    """
    Parse DAG file into AST (Abstract Syntax Tree).
    Catches syntax errors without executing the file.
    """
    content = read_dag_file(filepath)
    return ast.parse(content)


# ============================================================
# FILE EXISTS TESTS
# ============================================================

class TestDagFilesExist:
    """Verify DAG files exist in the correct location."""

    def test_backfill_dag_file_exists(self):
        """backfill_dag.py must exist in airflow/dags/"""
        assert os.path.exists(BACKFILL_DAG_FILE), (
            f"Backfill DAG file not found at: {BACKFILL_DAG_FILE}"
        )

    def test_incremental_dag_file_exists(self):
        """incremental_dag.py must exist in airflow/dags/"""
        assert os.path.exists(INCREMENTAL_DAG_FILE), (
            f"Incremental DAG file not found at: {INCREMENTAL_DAG_FILE}"
        )

    def test_cities_config_exists(self):
        """cities.json must exist for DAGs to load correctly."""
        assert os.path.exists("config/cities.json"), (
            "cities.json not found at config/cities.json"
        )

    def test_sql_files_exist(self):
        """All SQL files referenced by DAGs must exist."""
        required_sql_files = [
            "sql/ddl/create_bronze_table.sql",
            "sql/ddl/create_silver_table.sql",
            "sql/ddl/create_gold_table.sql",
            "sql/transformations/bronze_to_silver_ctas.sql",
            "sql/transformations/silver_to_gold_merge.sql",
            "sql/analysis/temp_trend_by_city.sql",
            "sql/analysis/rainiest_cities.sql",
            "sql/analysis/extreme_weather.sql",
        ]
        for filepath in required_sql_files:
            assert os.path.exists(filepath), (
                f"Required SQL file missing: {filepath}"
            )


# ============================================================
# SYNTAX TESTS
# ============================================================

class TestDagSyntax:
    """Verify DAG files have valid Python syntax."""

    def test_backfill_dag_valid_syntax(self):
        """
        Parse backfill_dag.py as AST.
        Fails immediately if there is a syntax error.
        """
        try:
            parse_dag_file(BACKFILL_DAG_FILE)
        except SyntaxError as e:
            pytest.fail(
                f"Syntax error in backfill_dag.py:\n{e}"
            )

    def test_incremental_dag_valid_syntax(self):
        """
        Parse incremental_dag.py as AST.
        Fails immediately if there is a syntax error.
        """
        try:
            parse_dag_file(INCREMENTAL_DAG_FILE)
        except SyntaxError as e:
            pytest.fail(
                f"Syntax error in incremental_dag.py:\n{e}"
            )


# ============================================================
# CONTENT TESTS — Backfill DAG
# ============================================================

class TestBackfillDagContent:
    """Verify backfill_dag.py has correct content."""

    @pytest.fixture
    def content(self):
        return read_dag_file(BACKFILL_DAG_FILE)

    def test_has_dag_decorator(self, content):
        """Must use @dag decorator — TaskFlow API syntax."""
        assert "@dag(" in content, (
            "backfill_dag.py must use @dag decorator"
        )

    def test_has_correct_dag_id(self, content):
        """DAG ID must match expected value."""
        assert "weather_backfill_pipeline" in content, (
            "DAG ID 'weather_backfill_pipeline' not found"
        )

    def test_schedule_is_none(self, content):
        """Backfill must have no automatic schedule."""
        assert "schedule=None" in content, (
            "Backfill DAG must have schedule=None"
        )

    def test_catchup_is_false(self, content):
        """catchup must be False."""
        assert "catchup=False" in content, (
            "backfill_dag.py must have catchup=False"
        )

    def test_has_all_expected_tasks(self, content):
        """All 8 task functions must be defined."""
        expected_tasks = [
            "fetch_and_upload_historical_bronze",
            "validate_bronze_upload",
            "register_bronze_external_table",
            "create_silver_iceberg_table",
            "create_gold_iceberg_tables",
            "run_bronze_to_silver_ctas",
            "run_silver_to_gold_merge",
            "validate_gold_data",
        ]
        for task_name in expected_tasks:
            assert task_name in content, (
                f"Task '{task_name}' not found in backfill_dag.py"
            )

    def test_uses_task_decorator(self, content):
        """Must use @task decorator — TaskFlow API syntax."""
        assert "@task(" in content or "@task\n" in content, (
            "backfill_dag.py must use @task decorators"
        )

    def test_uses_s3_hook(self, content):
        """Must import and use S3Hook for Bronze upload."""
        assert "S3Hook" in content, (
            "S3Hook not found — required for Bronze S3 upload"
        )

    def test_uses_athena_hook(self, content):
        """Must import and use AthenaHook for SQL execution."""
        assert "AthenaHook" in content, (
            "AthenaHook not found — required for Athena queries"
        )

    def test_uses_open_meteo_archive_api(self, content):
        """Must use the archive API endpoint for historical data."""
        assert "archive-api.open-meteo.com" in content, (
            "Historical archive API URL not found in backfill_dag.py"
        )

    def test_has_idempotent_s3_upload(self, content):
        """S3 upload must use replace=True for idempotency."""
        assert "replace=True" in content, (
            "S3 upload must use replace=True for idempotency"
        )

    def test_has_retries(self, content):
        """Must have retries configured."""
        assert "retries" in content, (
            "backfill_dag.py must have retries configured"
        )

    def test_instantiates_dag(self, content):
        """DAG function must be called at bottom of file."""
        assert "weather_backfill_pipeline()" in content, (
            "DAG must be instantiated by calling "
            "weather_backfill_pipeline() at end of file"
        )


# ============================================================
# CONTENT TESTS — Incremental DAG
# ============================================================

class TestIncrementalDagContent:
    """Verify incremental_dag.py has correct content."""

    @pytest.fixture
    def content(self):
        return read_dag_file(INCREMENTAL_DAG_FILE)

    def test_has_dag_decorator(self, content):
        """Must use @dag decorator — TaskFlow API syntax."""
        assert "@dag(" in content, (
            "incremental_dag.py must use @dag decorator"
        )

    def test_has_correct_dag_id(self, content):
        """DAG ID must match expected value."""
        assert "weather_incremental_pipeline" in content, (
            "DAG ID 'weather_incremental_pipeline' not found"
        )

    def test_schedule_is_daily_midnight(self, content):
        """Must run at midnight UTC daily."""
        assert "0 0 * * *" in content, (
            "Incremental DAG must have schedule '0 0 * * *'"
        )

    def test_catchup_is_false(self, content):
        """catchup must be False."""
        assert "catchup=False" in content, (
            "incremental_dag.py must have catchup=False"
        )

    def test_has_all_expected_tasks(self, content):
        """All 6 task functions must be defined."""
        expected_tasks = [
            "fetch_and_upload_daily_bronze",
            "validate_bronze_upload",
            "repair_bronze_table_partitions",
            "run_bronze_to_silver_ctas",
            "run_silver_to_gold_merge",
            "validate_daily_run",
        ]
        for task_name in expected_tasks:
            assert task_name in content, (
                f"Task '{task_name}' not found "
                f"in incremental_dag.py"
            )

    def test_uses_task_decorator(self, content):
        """Must use @task decorator — TaskFlow API syntax."""
        assert "@task(" in content or "@task\n" in content, (
            "incremental_dag.py must use @task decorators"
        )

    def test_uses_forecast_api(self, content):
        """Must use forecast API endpoint for live data."""
        assert "api.open-meteo.com/v1/forecast" in content, (
            "Forecast API URL not found in incremental_dag.py"
        )

    def test_has_partition_repair(self, content):
        """Must repair Bronze partitions after upload."""
        assert "MSCK REPAIR TABLE" in content, (
            "MSCK REPAIR TABLE not found — required to "
            "register new Bronze partitions with Athena"
        )

    def test_has_past_days_parameter(self, content):
        """Must use past_days parameter to get yesterday."""
        assert "past_days" in content, (
            "past_days parameter not found — required to "
            "fetch yesterday's complete 24 hour dataset"
        )

    def test_has_idempotent_s3_upload(self, content):
        """S3 upload must use replace=True for idempotency."""
        assert "replace=True" in content, (
            "S3 upload must use replace=True for idempotency"
        )

    def test_has_retries(self, content):
        """Must have retries configured."""
        assert "retries" in content, (
            "incremental_dag.py must have retries configured"
        )

    def test_instantiates_dag(self, content):
        """DAG function must be called at bottom of file."""
        assert "weather_incremental_pipeline()" in content, (
            "DAG must be instantiated by calling "
            "weather_incremental_pipeline() at end of file"
        )


# ============================================================
# SQL CONTENT TESTS
# ============================================================

class TestSqlFileContent:
    """Verify SQL files have correct content."""

    def test_bronze_table_has_location(self):
        content = read_dag_file("sql/ddl/create_bronze_table.sql")

    def test_bronze_table_has_partitions(self):
        content = read_dag_file("sql/ddl/create_bronze_table.sql")

    def test_silver_table_is_iceberg(self):
        content = read_dag_file("sql/ddl/create_silver_table.sql")

    def test_gold_table_is_iceberg(self):
        content = read_dag_file("sql/ddl/create_gold_table.sql")

    def test_silver_ctas_has_weather_code_decode(self):
        content = read_dag_file("sql/transformations/bronze_to_silver_ctas.sql")

    def test_gold_merge_has_merge_into(self):
        content = read_dag_file("sql/transformations/silver_to_gold_merge.sql")

    def test_gold_merge_has_when_matched(self):
        content = read_dag_file("sql/transformations/silver_to_gold_merge.sql")

    def test_gold_merge_has_when_not_matched(self):
        content = read_dag_file("sql/transformations/silver_to_gold_merge.sql")