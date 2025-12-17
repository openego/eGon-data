"""
Dataset for generating validation reports during pipeline execution.

This module provides the ValidationReport dataset which generates comprehensive
validation reports by aggregating all validation results from individual dataset
validation tasks executed during the pipeline run.
"""

import os
import time

from egon.data import logger, db as egon_db
from egon.data.datasets import Dataset
from egon_validation import RunContext
from egon_validation.runner.aggregate import collect, build_coverage, write_outputs
from egon_validation.report.generate import generate
from egon_validation.runner.coverage_analysis import discover_total_tables
from egon_validation.config import ENV_DB_URL
import os as _os

def generate_validation_report(**kwargs):
    """
    Generate validation report aggregating all validation results.

    This function collects all validation results from individual dataset
    validation tasks that were executed during the pipeline run and generates
    a comprehensive HTML report including:
    - All validation results from individual dataset tasks
    - Coverage analysis showing which tables were validated
    - Summary statistics and pass/fail counts
    """
    # Use same run_id as other validation tasks in the pipeline
    # This ensures all tasks read/write to the same directory
    run_id = (
        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID') or
        kwargs.get('run_id') or
        (kwargs.get('ti') and hasattr(kwargs['ti'], 'dag_run') and kwargs['ti'].dag_run.run_id) or
        (kwargs.get('dag_run') and kwargs['dag_run'].run_id) or
        f"pipeline_validation_report_{int(time.time())}"
    )

    # Determine output directory at runtime (not import time)
    # Priority: EGON_VALIDATION_DIR env var > current working directory
    out_dir = os.path.join(
        os.environ.get('EGON_VALIDATION_DIR', os.getcwd()),
        "validation_runs"
    )

    try:
        ctx = RunContext(run_id=run_id, source="airflow", out_dir=out_dir)
        logger.info("Starting pipeline validation report generation", extra={
            "run_id": run_id,
            "output_dir": out_dir
        })

        # Make database connection available for table counting
        # Set the database URL from egon.data configuration
        try:
            # Get the database URL from egon.data
            db_url = str(egon_db.engine().url)
            # Temporarily set the environment variable so discover_total_tables can use it
            _os.environ[ENV_DB_URL] = db_url
            logger.info("Database connection available for table counting")
        except Exception as e:
            logger.warning(f"Could not set database URL for table counting: {e}")

        # Collect all validation results from existing validation runs
        collected = collect(ctx)
        coverage = build_coverage(ctx, collected)
        final_out_dir = write_outputs(ctx, collected, coverage)
        generate(ctx)

        report_path = os.path.join(final_out_dir, 'report.html')
        logger.info("Pipeline validation report generated successfully", extra={
            "report_path": report_path,
            "run_id": run_id,
            "total_results": len(collected.get("items", []))
        })


    except FileNotFoundError as e:
        logger.warning(
            f"No validation results found for pipeline validation report | "
            f"run_id={run_id} | out_dir={out_dir} | error={e} | "
            f"suggestion=This may be expected if no validation tasks were run during the pipeline"
        )

        # Don't raise - this is acceptable if no validations were run
    except Exception as e:
        logger.error("Pipeline validation report generation failed", extra={
            "run_id": run_id,
            "error": str(e),
            "error_type": type(e).__name__
        })
        raise


# Define the task
tasks = (generate_validation_report,)


class ValidationReport(Dataset):
    """
    Dataset for generating validation reports.

    This dataset generates a comprehensive HTML validation report by aggregating
    all validation results from individual dataset validation tasks that were
    executed during the pipeline run. It should be placed before sanity_checks
    in the DAG to ensure validation results are collected before final checks.
    """
    #:
    name: str = "ValidationReport"
    #:
    version: str = "0.0.2.dev"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
        )
