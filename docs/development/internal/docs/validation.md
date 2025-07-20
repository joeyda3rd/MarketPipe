# Validation Module

## Purpose

The validation module provides comprehensive data quality validation for ingested OHLCV market data. It implements event-driven validation triggered by ingestion completion, applies business rules for financial data integrity, and generates detailed CSV audit reports for quality monitoring.

## Key Public Interfaces

### Application Services
```python
from marketpipe.validation import ValidationRunnerService

# Build and register validation service
service = ValidationRunnerService.build_default()
ValidationRunnerService.register()  # Auto-subscribes to ingestion events

# Manual validation handling
service.handle_ingestion_completed(ingestion_event)
```

### Domain Services
```python
from marketpipe.validation.domain.services import ValidationDomainService
from marketpipe.validation.domain.value_objects import ValidationResult, BarError

# Create validation service
validator = ValidationDomainService()

# Validate OHLCV bars
result = validator.validate_bars("AAPL", bars)

# Check results
if result.is_valid:
    print(f"All {result.total} bars passed validation")
else:
    print(f"Found {len(result.errors)} validation errors")
    for error in result.errors:
        print(f"Error at {error.ts_ns}: {error.reason}")
```

### Validation Results
```python
from marketpipe.validation.domain.value_objects import ValidationResult, BarError

# Validation result structure
result = ValidationResult(
    symbol="AAPL",
    total=390,  # Total bars validated
    errors=[
        BarError(ts_ns=1640995800000000000, reason="OHLC inconsistency at index 45"),
        BarError(ts_ns=1640995860000000000, reason="negative volume at index 46")
    ]
)

# Check validation status
is_clean = result.is_valid  # True if no errors
error_count = len(result.errors)
```

### Report Repository
```python
from marketpipe.validation.infrastructure.repositories import CsvReportRepository

# Create report repository
reporter = CsvReportRepository(root=Path("data/validation_reports"))

# Save validation report
report_path = reporter.save(job_id="AAPL_2024-01-01", result=validation_result)

# List and load reports
reports = reporter.list_reports(job_id="AAPL_2024-01-01")
df = reporter.load_report(report_path)
summary = reporter.get_report_summary(report_path)
```

## Brief Call Graph

```
Ingestion Completion Event
    ↓
ValidationRunnerService.handle_ingestion_completed()
    ↓
ParquetStorageEngine.load_job_bars()
    ↓
ValidationDomainService.validate_bars()
    ↓
Business Rule Validation
    ↓
CsvReportRepository.save()
    ↓
Metrics Recording
```

### Validation Flow

1. **Event Trigger**: `IngestionJobCompleted` event → validation service
2. **Data Loading**: Load ingested bars from Parquet storage
3. **Domain Validation**: Apply business rules to OHLCV data
4. **Report Generation**: Save validation results to CSV files
5. **Metrics Recording**: Track validation metrics by provider/feed

## Examples

### Event-Driven Validation
```python
@Code(src/marketpipe/validation/application/services.py:30-55)
# Handle ingestion completion automatically
def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
    # Extract provider/feed for metrics
    provider, feed = self._extract_provider_feed_info(event)
    
    # Load data and validate
    symbol_dataframes = self._storage_engine.load_job_bars(event.job_id)
    
    for symbol_name, df in symbol_dataframes.items():
        bars = self._convert_dataframe_to_bars(df, symbol_name)
        result = self._validator.validate_bars(symbol_name, bars)
        
        # Save report and record metrics
        report_path = self._reporter.save(event.job_id, result)
```

### Domain Validation Rules
```python
@Code(src/marketpipe/validation/domain/services.py:15-45)
# Comprehensive OHLCV validation
def validate_bars(self, symbol: str, bars: list[OHLCVBar]) -> ValidationResult:
    errors = []
    
    for i, bar in enumerate(bars):
        # Monotonic timestamp validation
        if bar.timestamp_ns <= prev_ts:
            errors.append(BarError(bar.timestamp_ns, f"non-monotonic timestamp at index {i}"))
        
        # Positive price validation
        if any(price.value <= 0 for price in [bar.open_price, bar.high_price, bar.low_price, bar.close_price]):
            errors.append(BarError(bar.timestamp_ns, f"non-positive price at index {i}"))
        
        # OHLC consistency validation
        if not self._validate_ohlc_consistency(bar):
            errors.append(BarError(bar.timestamp_ns, f"OHLC inconsistency at index {i}"))
```

### OHLC Consistency Rules
```python
@Code(src/marketpipe/validation/domain/services.py:70-80)
# Business rule: OHLC price relationships
def _validate_ohlc_consistency(self, bar: OHLCVBar) -> bool:
    return (
        bar.high_price.value >= bar.open_price.value
        and bar.high_price.value >= bar.close_price.value
        and bar.high_price.value >= bar.low_price.value
        and bar.low_price.value <= bar.open_price.value
        and bar.low_price.value <= bar.close_price.value
    )
```

### Timestamp Alignment Validation
```python
@Code(src/marketpipe/validation/domain/services.py:82-85)
# Validate minute-bar timestamp alignment
def _validate_timestamp_alignment(self, bar: OHLCVBar) -> bool:
    # 1-minute bars should align to minute boundaries
    return bar.timestamp_ns % 60_000_000_000 == 0
```

### Price Movement Validation
```python
@Code(src/marketpipe/validation/domain/services.py:87-105)
# Detect extreme price movements between bars
def _validate_price_movements(self, current_bar: OHLCVBar, previous_bar: OHLCVBar, index: int) -> list[BarError]:
    errors = []
    prev_close = previous_bar.close_price.value
    curr_open = current_bar.open_price.value
    
    if prev_close > 0:
        price_change_pct = abs(float(curr_open - prev_close)) / float(prev_close)
        if price_change_pct > 0.5:  # 50% change threshold
            errors.append(BarError(
                current_bar.timestamp_ns,
                f"extreme price movement at index {index}: {price_change_pct*100:.1f}% change"
            ))
    return errors
```

### CSV Report Generation
```python
@Code(src/marketpipe/validation/infrastructure/repositories.py:20-45)
# Save validation results to CSV
def save(self, job_id: str, result: ValidationResult) -> Path:
    job_dir = self.root / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    path = job_dir / f"{job_id}_{result.symbol}.csv"
    
    if result.errors:
        error_data = []
        for error in result.errors:
            error_data.append({
                "symbol": result.symbol,
                "ts_ns": error.ts_ns,
                "reason": error.reason,
            })
        df = pd.DataFrame(error_data)
    else:
        # Empty DataFrame for clean validation
        df = pd.DataFrame(columns=["symbol", "ts_ns", "reason"])
    
    df.to_csv(path, index=False)
```

### Metrics Integration
```python
@Code(src/marketpipe/validation/application/services.py:55-75)
# Record validation metrics by provider/feed
record_metric("validation_bars_processed", len(bars), provider=provider, feed=feed)

if error_count > 0:
    record_metric("validation_errors_found", error_count, provider=provider, feed=feed)
    record_metric(f"validation_errors_{symbol_name}", error_count, provider=provider, feed=feed)
else:
    record_metric("validation_success", 1, provider=provider, feed=feed)
    record_metric(f"validation_success_{symbol_name}", 1, provider=provider, feed=feed)
```

### Report Analysis
```python
@Code(src/marketpipe/validation/infrastructure/repositories.py:100-125)
# Analyze validation report statistics
def get_report_summary(self, path: Path) -> dict:
    df = self.load_report(path)
    
    summary = {
        "total_bars": len(df["symbol"].unique()),
        "total_errors": len(df),
        "symbols": df["symbol"].unique().tolist(),
        "most_common_errors": []
    }
    
    # Get most common error types
    if "reason" in df.columns and not df.empty:
        error_counts = df["reason"].value_counts().head(5)
        summary["most_common_errors"] = [
            {"reason": reason, "count": count}
            for reason, count in error_counts.items()
        ]
```

### Service Registration
```python
@Code(src/marketpipe/validation/application/services.py:150-159)
# Auto-register validation service for events
@classmethod
def register(cls):
    svc = cls.build_default()
    get_event_bus().subscribe(IngestionJobCompleted, svc.handle_ingestion_completed)

# Usage in bootstrap
ValidationRunnerService.register()  # Automatic event subscription
```

## Validation Rules

### Core Business Rules
- **OHLC Consistency**: High ≥ Open, Close, Low; Low ≤ Open, Close
- **Positive Prices**: All OHLC prices must be > 0
- **Non-negative Volume**: Volume ≥ 0
- **Monotonic Timestamps**: Bars must be in chronological order
- **Timestamp Alignment**: 1-minute bars align to minute boundaries

### Quality Checks
- **Extreme Price Movements**: >50% price change between consecutive bars
- **Zero Volume with Price Movement**: Price change without trading volume
- **Unreasonable Values**: Prices >$100k or <$0.01, volume >1B shares
- **Trading Hours**: Timestamps within reasonable market hours

### Error Categories
- **Structural Errors**: Timestamp, ordering, alignment issues
- **Price Errors**: OHLC consistency, extreme movements, unreasonable values
- **Volume Errors**: Negative volume, volume/price inconsistencies
- **Business Rule Violations**: Domain-specific financial data rules

## Architecture Benefits

- **Event-Driven**: Automatic validation on ingestion completion
- **Comprehensive Rules**: Financial domain-specific validation logic
- **Detailed Reporting**: CSV audit trails for quality monitoring
- **Metrics Integration**: Provider/feed-specific quality metrics
- **Extensible**: Easy to add new validation rules and checks
- **Separation of Concerns**: Domain validation isolated from infrastructure 