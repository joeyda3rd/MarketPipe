from __future__ import annotations

from marketpipe.events import DomainEvent


class AggregationCompleted(DomainEvent):
    """Event raised when aggregation is completed for a job."""
    
    def __init__(self, job_id: str, frames_processed: int):
        self.job_id = job_id
        self.frames_processed = frames_processed


class AggregationFailed(DomainEvent):
    """Event raised when aggregation fails for a job."""
    
    def __init__(self, job_id: str, error_message: str):
        self.job_id = job_id
        self.error_message = error_message 