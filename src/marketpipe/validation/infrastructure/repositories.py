"""Validation infrastructure repositories."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
from datetime import date

from ..domain.value_objects import ValidationResult


class CsvReportRepository:
    """Repository for saving validation reports as CSV files."""
    
    def __init__(self, root: Path = Path("data/validation_reports")):
        self.root = root
    
    def save(self, result: ValidationResult) -> Path:
        """Save validation result to CSV file."""
        day_dir = self.root / date.today().isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"{result.symbol}.csv"
        
        if result.errors:
            df = pd.DataFrame([e.__dict__ for e in result.errors])
        else:
            # Create empty DataFrame with expected columns for valid results
            df = pd.DataFrame(columns=['ts_ns', 'reason'])
        
        df.to_csv(path, index=False)
        return path 