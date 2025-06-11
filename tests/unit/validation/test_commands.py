"""Tests for validation application commands."""

from marketpipe.validation.application.commands import ValidateJobCommand


def test_validate_job_command_creation():
    """Test ValidateJobCommand can be created with required fields."""
    job_id = "test-job-123"

    command = ValidateJobCommand(job_id=job_id)

    assert command.job_id == job_id


def test_validate_job_command_equality():
    """Test ValidateJobCommand equality comparison."""
    job_id = "test-job-123"

    command1 = ValidateJobCommand(job_id=job_id)
    command2 = ValidateJobCommand(job_id=job_id)

    # Should be equal since they're frozen dataclasses with same data
    assert command1 == command2


def test_validate_job_command_different_values():
    """Test ValidateJobCommand with different values are not equal."""
    command1 = ValidateJobCommand(job_id="job-1")
    command2 = ValidateJobCommand(job_id="job-2")

    assert command1 != command2
