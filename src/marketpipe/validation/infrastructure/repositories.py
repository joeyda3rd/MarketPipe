# SPDX-License-Identifier: Apache-2.0
"""Validation infrastructure repositories."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
from typing import List, Optional
import logging

from ..domain.value_objects import ValidationResult


class CsvReportRepository:
    """Repository for saving validation reports as CSV files."""

    def __init__(self, root: Path = Path("data/validation_reports")):
        self.root = root
        self.log = logging.getLogger(self.__class__.__name__)

    def save(self, job_id: str, result: ValidationResult) -> Path:
        """Save validation result to CSV file.

        Args:
            job_id: Unique identifier for the ingestion job
            result: ValidationResult containing symbol and error details

        Returns:
            Path to the saved CSV file
        """
        # Create directory structure: root/job_id/
        job_dir = self.root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        # Filename pattern: <job_id>_<symbol>.csv
        path = job_dir / f"{job_id}_{result.symbol}.csv"

        if result.errors:
            # Convert errors to DataFrame with columns: symbol, ts_ns, reason
            error_data = []
            for error in result.errors:
                error_data.append(
                    {
                        "symbol": result.symbol,
                        "ts_ns": error.ts_ns,
                        "reason": error.reason,
                    }
                )
            df = pd.DataFrame(error_data)
        else:
            # Create empty DataFrame with expected columns for valid results
            df = pd.DataFrame(columns=["symbol", "ts_ns", "reason"])

        # Save to CSV
        df.to_csv(path, index=False)
        self.log.info(f"Validation report written: {path}")

        return path

    def list_reports(self, job_id: Optional[str] = None) -> List[Path]:
        """List all validation report files.

        Args:
            job_id: Optional job ID to filter reports. If None, returns all reports.

        Returns:
            List of paths to CSV report files
        """
        reports = []

        if job_id is not None:
            # List reports for specific job
            job_dir = self.root / job_id
            if job_dir.exists():
                reports.extend(job_dir.glob("*.csv"))
        else:
            # List all reports across all jobs
            if self.root.exists():
                reports.extend(self.root.glob("**/*.csv"))

        # Sort by modification time (newest first)
        reports.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return reports

    def load_report(self, path: Path) -> pd.DataFrame:
        """Load validation report from CSV file.

        Args:
            path: Path to the CSV file

        Returns:
            DataFrame with validation report data

        Raises:
            FileNotFoundError: If the report file doesn't exist
            pd.errors.EmptyDataError: If the CSV file is empty
        """
        if not path.exists():
            raise FileNotFoundError(f"Validation report not found: {path}")

        try:
            df = pd.read_csv(path)
            return df
        except pd.errors.EmptyDataError:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=["symbol", "ts_ns", "reason"])

    def get_report_summary(self, path: Path) -> dict:
        """Get summary statistics for a validation report.

        Args:
            path: Path to the CSV file

        Returns:
            Dictionary with summary statistics
        """
        df = self.load_report(path)

        if df.empty:
            return {
                "total_bars": 0,
                "total_errors": 0,
                "error_rate": 0.0,
                "symbols": [],
                "most_common_errors": [],
            }

        summary = {
            "total_bars": len(df["symbol"].unique()) if "symbol" in df.columns else 0,
            "total_errors": len(df),
            "error_rate": 0.0,  # Would need total bar count from job to calculate
            "symbols": df["symbol"].unique().tolist() if "symbol" in df.columns else [],
            "most_common_errors": [],
        }

        # Get most common error types
        if "reason" in df.columns and not df.empty:
            error_counts = df["reason"].value_counts().head(5)
            summary["most_common_errors"] = [
                {"reason": reason, "count": count}
                for reason, count in error_counts.items()
            ]

        return summary
