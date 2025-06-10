# SPDX-License-Identifier: Apache-2.0
"""Unit tests for CsvReportRepository."""

from pathlib import Path
import pytest
import pandas as pd

from marketpipe.validation.infrastructure.repositories import CsvReportRepository
from marketpipe.validation.domain.value_objects import ValidationResult, BarError


@pytest.fixture
def tmp_repo(tmp_path):
    """Create a CsvReportRepository with a temporary directory."""
    return CsvReportRepository(tmp_path)


@pytest.fixture
def validation_result_with_errors():
    """Create a ValidationResult with some errors."""
    errors = [
        BarError(ts_ns=1640995800000000000, reason="OHLC inconsistency at index 0"),
        BarError(ts_ns=1640995860000000000, reason="negative volume at index 1"),
    ]
    return ValidationResult(symbol="AAPL", total=10, errors=errors)


@pytest.fixture
def validation_result_no_errors():
    """Create a ValidationResult with no errors."""
    return ValidationResult(symbol="GOOGL", total=5, errors=[])


def test_save_and_load_with_errors(tmp_repo, validation_result_with_errors):
    """Test saving and loading a validation result with errors."""
    job_id = "test-job-123"
    
    # Save the result
    path = tmp_repo.save(job_id, validation_result_with_errors)
    
    # Check that file was created
    assert path.exists()
    assert path.name == f"{job_id}_AAPL.csv"
    assert path.parent.name == job_id
    
    # Load and verify content
    df = tmp_repo.load_report(path)
    assert len(df) == 2
    assert list(df.columns) == ['symbol', 'ts_ns', 'reason']
    
    # Check first error
    assert df.iloc[0]['symbol'] == 'AAPL'
    assert df.iloc[0]['ts_ns'] == 1640995800000000000
    assert df.iloc[0]['reason'] == "OHLC inconsistency at index 0"
    
    # Check second error
    assert df.iloc[1]['symbol'] == 'AAPL'
    assert df.iloc[1]['ts_ns'] == 1640995860000000000
    assert df.iloc[1]['reason'] == "negative volume at index 1"


def test_save_and_load_no_errors(tmp_repo, validation_result_no_errors):
    """Test saving and loading a validation result with no errors."""
    job_id = "test-job-456"
    
    # Save the result
    path = tmp_repo.save(job_id, validation_result_no_errors)
    
    # Check that file was created
    assert path.exists()
    assert path.name == f"{job_id}_GOOGL.csv"
    
    # Load and verify content
    df = tmp_repo.load_report(path)
    assert len(df) == 0
    assert list(df.columns) == ['symbol', 'ts_ns', 'reason']


def test_list_reports_empty_directory(tmp_repo):
    """Test listing reports when no reports exist."""
    reports = tmp_repo.list_reports()
    assert reports == []


def test_list_reports_with_job_id_filter(tmp_repo, validation_result_with_errors, validation_result_no_errors):
    """Test listing reports filtered by job ID."""
    job_id_1 = "job-123"
    job_id_2 = "job-456"
    
    # Save reports for different jobs
    path1 = tmp_repo.save(job_id_1, validation_result_with_errors)
    path2 = tmp_repo.save(job_id_2, validation_result_no_errors)
    
    # List all reports
    all_reports = tmp_repo.list_reports()
    assert len(all_reports) == 2
    assert path1 in all_reports
    assert path2 in all_reports
    
    # List reports for specific job
    job1_reports = tmp_repo.list_reports(job_id_1)
    assert len(job1_reports) == 1
    assert path1 in job1_reports
    assert path2 not in job1_reports
    
    job2_reports = tmp_repo.list_reports(job_id_2)
    assert len(job2_reports) == 1
    assert path2 in job2_reports
    assert path1 not in job2_reports


def test_list_reports_nonexistent_job(tmp_repo):
    """Test listing reports for a job that doesn't exist."""
    reports = tmp_repo.list_reports("nonexistent-job")
    assert reports == []


def test_load_report_nonexistent_file(tmp_repo):
    """Test loading a report file that doesn't exist."""
    nonexistent_path = tmp_repo.root / "nonexistent.csv"
    
    with pytest.raises(FileNotFoundError):
        tmp_repo.load_report(nonexistent_path)


def test_get_report_summary_with_errors(tmp_repo, validation_result_with_errors):
    """Test getting summary statistics for a report with errors."""
    job_id = "test-job-789"
    path = tmp_repo.save(job_id, validation_result_with_errors)
    
    summary = tmp_repo.get_report_summary(path)
    
    assert summary['total_bars'] == 1  # One unique symbol
    assert summary['total_errors'] == 2
    assert summary['symbols'] == ['AAPL']
    assert len(summary['most_common_errors']) == 2
    
    # Check most common errors
    error_reasons = [err['reason'] for err in summary['most_common_errors']]
    assert "OHLC inconsistency at index 0" in error_reasons
    assert "negative volume at index 1" in error_reasons


def test_get_report_summary_no_errors(tmp_repo, validation_result_no_errors):
    """Test getting summary statistics for a report with no errors."""
    job_id = "test-job-clean"
    path = tmp_repo.save(job_id, validation_result_no_errors)
    
    summary = tmp_repo.get_report_summary(path)
    
    assert summary['total_bars'] == 0
    assert summary['total_errors'] == 0
    assert summary['symbols'] == []
    assert summary['most_common_errors'] == []


def test_multiple_symbols_same_job(tmp_repo):
    """Test saving multiple symbols for the same job."""
    job_id = "multi-symbol-job"
    
    # Create results for different symbols
    errors_aapl = [BarError(ts_ns=1640995800000000000, reason="test error 1")]
    result_aapl = ValidationResult(symbol="AAPL", total=5, errors=errors_aapl)
    
    errors_googl = [BarError(ts_ns=1640995900000000000, reason="test error 2")]
    result_googl = ValidationResult(symbol="GOOGL", total=3, errors=errors_googl)
    
    # Save both results
    path_aapl = tmp_repo.save(job_id, result_aapl)
    path_googl = tmp_repo.save(job_id, result_googl)
    
    # Check files were created correctly
    assert path_aapl.name == f"{job_id}_AAPL.csv"
    assert path_googl.name == f"{job_id}_GOOGL.csv"
    assert path_aapl.parent == path_googl.parent  # Same job directory
    
    # List reports for the job
    reports = tmp_repo.list_reports(job_id)
    assert len(reports) == 2
    assert path_aapl in reports
    assert path_googl in reports


def test_file_sorting_by_modification_time(tmp_repo, validation_result_with_errors):
    """Test that list_reports returns files sorted by modification time (newest first)."""
    import time
    
    # Save first report
    path1 = tmp_repo.save("job-1", validation_result_with_errors)
    
    # Wait a bit to ensure different modification times
    time.sleep(0.1)
    
    # Save second report
    path2 = tmp_repo.save("job-2", validation_result_with_errors)
    
    # List all reports
    reports = tmp_repo.list_reports()
    
    # Should be sorted with newest first
    assert len(reports) == 2
    assert reports[0] == path2  # Newer file first
    assert reports[1] == path1  # Older file second


def test_empty_csv_handling(tmp_repo):
    """Test handling of empty CSV files."""
    # Create an empty CSV file manually
    job_dir = tmp_repo.root / "empty-job"
    job_dir.mkdir(parents=True)
    empty_file = job_dir / "empty-job_TSLA.csv"
    empty_file.touch()
    
    # Should handle empty file gracefully
    df = tmp_repo.load_report(empty_file)
    assert len(df) == 0
    assert list(df.columns) == ['symbol', 'ts_ns', 'reason']
    
    # Summary should handle empty data
    summary = tmp_repo.get_report_summary(empty_file)
    assert summary['total_errors'] == 0 