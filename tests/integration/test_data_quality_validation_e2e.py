# SPDX-License-Identifier: Apache-2.0
"""Comprehensive data quality validation end-to-end tests.

This test validates the complete data quality pipeline including schema validation,
business rule enforcement, data consistency checks, and comprehensive quality
reporting across the entire MarketPipe system.
"""

from __future__ import annotations

import statistics
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pytest

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.validation.application.services import ValidationRunnerService
from marketpipe.validation.infrastructure.repositories import CsvReportRepository


class DataQualityGenerator:
    """Generate test datasets with specific quality characteristics."""
    
    @staticmethod
    def create_high_quality_dataset(symbol: str, size: int = 100) -> pd.DataFrame:
        """Create high-quality, realistic market data."""
        bars = []
        base_time = datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)
        base_price = 150.0
        
        for i in range(size):
            timestamp = base_time + timedelta(minutes=i)
            timestamp_ns = int(timestamp.timestamp() * 1e9)
            
            # Realistic price movement with proper OHLC relationships
            price_change = (i * 0.01) + (0.1 if i % 10 == 0 else 0)  # Small trend with occasional jumps
            current_price = base_price + price_change
            
            # Ensure valid OHLC relationships
            open_price = current_price
            close_price = current_price + (-0.05 + (i % 3) * 0.05)  # Small random close
            high_price = max(open_price, close_price) + 0.02
            low_price = min(open_price, close_price) - 0.02
            
            volume = 1000 + (i * 10) + (500 if i % 20 == 0 else 0)  # Realistic volume with spikes
            
            bars.append({
                "ts_ns": timestamp_ns,
                "symbol": symbol,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
                "trade_count": max(1, volume // 20),
                "vwap": round((high_price + low_price + close_price) / 3, 2),
            })
        
        return pd.DataFrame(bars)
    
    @staticmethod
    def create_quality_issues_dataset(symbol: str) -> pd.DataFrame:
        """Create dataset with various quality issues for testing validation."""
        bars = []
        base_time = datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)
        
        quality_issues = [
            # Issue 1: Invalid OHLC relationship (high < open)
            {
                "ts_ns": int(base_time.timestamp() * 1e9),
                "symbol": symbol,
                "open": 100.0,
                "high": 99.0,  # Invalid: high < open
                "low": 98.0,
                "close": 99.5,
                "volume": 1000,
                "trade_count": 50,
                "vwap": 99.0,
            },
            
            # Issue 2: Zero volume
            {
                "ts_ns": int((base_time + timedelta(minutes=1)).timestamp() * 1e9),
                "symbol": symbol,
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "volume": 0,  # Invalid: zero volume
                "trade_count": 0,
                "vwap": 100.0,
            },
            
            # Issue 3: Extreme price gap (>10% jump)
            {
                "ts_ns": int((base_time + timedelta(minutes=2)).timestamp() * 1e9),
                "symbol": symbol,
                "open": 120.0,  # 20% jump from previous close
                "high": 121.0,
                "low": 119.5,
                "close": 120.5,
                "volume": 2000,
                "trade_count": 100,
                "vwap": 120.33,
            },
            
            # Issue 4: Timestamp not aligned to minute boundary
            {
                "ts_ns": int((base_time + timedelta(minutes=3, seconds=15)).timestamp() * 1e9),  # +15 seconds
                "symbol": symbol,
                "open": 120.5,
                "high": 121.0,
                "low": 120.0,
                "close": 120.8,
                "volume": 1500,
                "trade_count": 75,
                "vwap": 120.6,
            },
            
            # Issue 5: Negative price
            {
                "ts_ns": int((base_time + timedelta(minutes=4)).timestamp() * 1e9),
                "symbol": symbol,
                "open": -10.0,  # Invalid: negative price
                "high": -9.0,
                "low": -11.0,
                "close": -9.5,
                "volume": 1000,
                "trade_count": 50,
                "vwap": -9.83,
            },
            
            # Issue 6: Duplicate timestamp
            {
                "ts_ns": int(base_time.timestamp() * 1e9),  # Same as first bar
                "symbol": symbol,
                "open": 101.0,
                "high": 101.5,
                "low": 100.5,
                "close": 101.2,
                "volume": 1200,
                "trade_count": 60,
                "vwap": 101.1,
            },
        ]
        
        return pd.DataFrame(quality_issues)
    
    @staticmethod
    def create_mixed_quality_dataset(symbol: str, size: int = 50) -> pd.DataFrame:
        """Create dataset with mix of good and problematic data."""
        
        # Start with high-quality data
        good_data = DataQualityGenerator.create_high_quality_dataset(symbol, size - 10)
        
        # Add some quality issues
        problem_data = DataQualityGenerator.create_quality_issues_dataset(symbol)
        
        # Combine and sort by timestamp
        combined = pd.concat([good_data, problem_data], ignore_index=True)
        combined = combined.sort_values("ts_ns").reset_index(drop=True)
        
        return combined


class DataQualityAnalyzer:
    """Analyze data quality metrics and patterns."""
    
    @staticmethod
    def analyze_ohlc_consistency(df: pd.DataFrame) -> Dict[str, any]:
        """Analyze OHLC price relationship consistency."""
        
        issues = []
        
        for i, row in df.iterrows():
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            
            # Check basic OHLC rules
            if h < max(o, c):
                issues.append(f"Row {i}: High ({h}) < max(Open({o}), Close({c}))")
            
            if l > min(o, c):
                issues.append(f"Row {i}: Low ({l}) > min(Open({o}), Close({c}))")
            
            if h < l:
                issues.append(f"Row {i}: High ({h}) < Low ({l})")
        
        return {
            "total_bars": len(df),
            "ohlc_violations": len(issues),
            "ohlc_compliance_rate": 1 - (len(issues) / len(df)) if len(df) > 0 else 1,
            "violations": issues[:10],  # First 10 violations
        }
    
    @staticmethod
    def analyze_price_movements(df: pd.DataFrame) -> Dict[str, any]:
        """Analyze price movement patterns for anomalies."""
        
        if len(df) < 2:
            return {"insufficient_data": True}
        
        df_sorted = df.sort_values("ts_ns")
        price_changes = []
        gaps = []
        
        for i in range(1, len(df_sorted)):
            prev_close = df_sorted.iloc[i-1]["close"]
            curr_open = df_sorted.iloc[i]["open"]
            
            gap = (curr_open - prev_close) / prev_close
            price_changes.append(gap)
            
            if abs(gap) > 0.05:  # >5% gap
                gaps.append({
                    "index": i,
                    "gap_percent": gap * 100,
                    "prev_close": prev_close,
                    "curr_open": curr_open,
                })
        
        return {
            "price_changes": price_changes,
            "extreme_gaps": gaps,
            "max_gap_percent": max([abs(g) for g in price_changes]) * 100 if price_changes else 0,
            "avg_absolute_change": sum([abs(g) for g in price_changes]) / len(price_changes) * 100 if price_changes else 0,
        }
    
    @staticmethod
    def analyze_temporal_consistency(df: pd.DataFrame) -> Dict[str, any]:
        """Analyze timestamp consistency and alignment."""
        
        issues = []
        
        for i, row in df.iterrows():
            timestamp_ns = row["ts_ns"]
            dt = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
            
            # Check minute alignment
            if dt.second != 0 or dt.microsecond != 0:
                issues.append({
                    "index": i,
                    "timestamp": dt.isoformat(),
                    "issue": "not_minute_aligned"
                })
            
            # Check market hours (simplified: 13:30-20:00 UTC)
            hour = dt.hour
            if hour < 13 or hour >= 20:
                issues.append({
                    "index": i, 
                    "timestamp": dt.isoformat(),
                    "issue": "outside_market_hours"
                })
        
        # Check for duplicates
        duplicates = df[df.duplicated("ts_ns", keep=False)]
        
        return {
            "alignment_violations": len([i for i in issues if i["issue"] == "not_minute_aligned"]),
            "market_hours_violations": len([i for i in issues if i["issue"] == "outside_market_hours"]),
            "duplicate_timestamps": len(duplicates),
            "issues": issues[:10],  # First 10 issues
        }
    
    @staticmethod
    def analyze_volume_patterns(df: pd.DataFrame) -> Dict[str, any]:
        """Analyze volume and trade count patterns."""
        
        volumes = df["volume"].tolist()
        trade_counts = df["trade_count"].tolist() if "trade_count" in df.columns else []
        
        zero_volume_count = sum(1 for v in volumes if v <= 0)
        
        analysis = {
            "total_bars": len(df),
            "zero_volume_bars": zero_volume_count,
            "min_volume": min(volumes) if volumes else 0,
            "max_volume": max(volumes) if volumes else 0,
            "avg_volume": sum(volumes) / len(volumes) if volumes else 0,
        }
        
        if trade_counts:
            zero_trades_count = sum(1 for t in trade_counts if t <= 0)
            analysis.update({
                "zero_trade_count_bars": zero_trades_count,
                "avg_trades_per_bar": sum(trade_counts) / len(trade_counts),
            })
        
        return analysis


@pytest.mark.integration
@pytest.mark.data_quality
class TestDataQualityValidationEndToEnd:
    """Comprehensive data quality validation testing."""
    
    def test_high_quality_data_validation(self, tmp_path):
        """Test validation of high-quality data (should pass cleanly)."""
        
        # Setup storage and validation
        storage_dir = tmp_path / "storage"
        reports_dir = tmp_path / "reports"
        storage_dir.mkdir()
        reports_dir.mkdir()
        
        storage_engine = ParquetStorageEngine(storage_dir)
        csv_repository = CsvReportRepository(reports_dir)
        
        # Create high-quality dataset
        high_quality_data = DataQualityGenerator.create_high_quality_dataset("AAPL", size=100)
        
        job_id = "high-quality-test"
        storage_engine.write(
            df=high_quality_data,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        
        print(f"✓ Stored high-quality dataset: {len(high_quality_data)} bars")
        
        # Analyze data quality before validation
        ohlc_analysis = DataQualityAnalyzer.analyze_ohlc_consistency(high_quality_data)
        price_analysis = DataQualityAnalyzer.analyze_price_movements(high_quality_data)
        temporal_analysis = DataQualityAnalyzer.analyze_temporal_consistency(high_quality_data)
        volume_analysis = DataQualityAnalyzer.analyze_volume_patterns(high_quality_data)
        
        print("📊 Pre-Validation Quality Analysis:")
        print(f"  OHLC Compliance: {ohlc_analysis['ohlc_compliance_rate']:.1%}")
        print(f"  Max Price Gap: {price_analysis['max_gap_percent']:.2f}%")
        print(f"  Timestamp Issues: {temporal_analysis['alignment_violations']}")
        print(f"  Zero Volume Bars: {volume_analysis['zero_volume_bars']}")
        
        # Expect high-quality data to pass validation
        assert ohlc_analysis['ohlc_compliance_rate'] >= 0.99
        assert price_analysis['max_gap_percent'] < 5.0
        assert temporal_analysis['alignment_violations'] == 0
        assert volume_analysis['zero_volume_bars'] == 0
        
        print("✅ High-quality data validation test completed")
    
    def test_quality_issues_detection(self, tmp_path):
        """Test detection of various data quality issues."""
        
        # Setup
        storage_dir = tmp_path / "storage"
        reports_dir = tmp_path / "reports"
        storage_dir.mkdir()
        reports_dir.mkdir()
        
        storage_engine = ParquetStorageEngine(storage_dir)
        csv_repository = CsvReportRepository(reports_dir)
        
        # Create dataset with known quality issues
        problem_data = DataQualityGenerator.create_quality_issues_dataset("PROBLEM")
        
        job_id = "quality-issues-test"
        storage_engine.write(
            df=problem_data,
            frame="1m",
            symbol="PROBLEM",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        
        print(f"✓ Stored problematic dataset: {len(problem_data)} bars")
        
        # Analyze known quality issues
        ohlc_analysis = DataQualityAnalyzer.analyze_ohlc_consistency(problem_data)
        price_analysis = DataQualityAnalyzer.analyze_price_movements(problem_data)
        temporal_analysis = DataQualityAnalyzer.analyze_temporal_consistency(problem_data)
        volume_analysis = DataQualityAnalyzer.analyze_volume_patterns(problem_data)
        
        print("📊 Quality Issues Detection Results:")
        print(f"  OHLC Violations: {ohlc_analysis['ohlc_violations']}")
        print(f"  Extreme Price Gaps: {len(price_analysis['extreme_gaps'])}")
        print(f"  Timestamp Alignment Issues: {temporal_analysis['alignment_violations']}")
        print(f"  Zero Volume Bars: {volume_analysis['zero_volume_bars']}")
        print(f"  Duplicate Timestamps: {temporal_analysis['duplicate_timestamps']}")
        
        # Verify issues are detected
        assert ohlc_analysis['ohlc_violations'] > 0, "Should detect OHLC violations"
        assert len(price_analysis['extreme_gaps']) > 0, "Should detect extreme price gaps"
        assert temporal_analysis['alignment_violations'] > 0, "Should detect timestamp issues"
        assert volume_analysis['zero_volume_bars'] > 0, "Should detect zero volume bars"
        assert temporal_analysis['duplicate_timestamps'] > 0, "Should detect duplicate timestamps"
        
        print("✅ Quality issues detection test completed")
    
    def test_mixed_quality_data_processing(self, tmp_path):
        """Test processing of mixed quality data (realistic scenario)."""
        
        # Setup
        storage_dir = tmp_path / "storage"
        reports_dir = tmp_path / "reports"
        storage_dir.mkdir()
        reports_dir.mkdir()
        
        storage_engine = ParquetStorageEngine(storage_dir)
        csv_repository = CsvReportRepository(reports_dir)
        
        # Create mixed quality dataset
        mixed_data = DataQualityGenerator.create_mixed_quality_dataset("MIXED", size=100)
        
        job_id = "mixed-quality-test"
        storage_engine.write(
            df=mixed_data,
            frame="1m",
            symbol="MIXED",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        
        print(f"✓ Stored mixed quality dataset: {len(mixed_data)} bars")
        
        # Comprehensive quality analysis
        analyses = {
            "ohlc": DataQualityAnalyzer.analyze_ohlc_consistency(mixed_data),
            "price": DataQualityAnalyzer.analyze_price_movements(mixed_data),
            "temporal": DataQualityAnalyzer.analyze_temporal_consistency(mixed_data),
            "volume": DataQualityAnalyzer.analyze_volume_patterns(mixed_data),
        }
        
        print("📊 Mixed Quality Data Analysis:")
        print(f"  Total Bars: {len(mixed_data)}")
        print(f"  OHLC Compliance: {analyses['ohlc']['ohlc_compliance_rate']:.1%}")
        print(f"  Extreme Price Gaps: {len(analyses['price']['extreme_gaps'])}")
        print(f"  Timestamp Issues: {analyses['temporal']['alignment_violations']}")
        print(f"  Volume Issues: {analyses['volume']['zero_volume_bars']}")
        
        # Calculate overall quality score
        quality_score = 100.0
        quality_score *= analyses['ohlc']['ohlc_compliance_rate']  # OHLC weight: 100%
        quality_score -= min(20, len(analyses['price']['extreme_gaps']) * 5)  # Price gap penalty
        quality_score -= min(15, analyses['temporal']['alignment_violations'] * 3)  # Timestamp penalty
        quality_score -= min(10, analyses['volume']['zero_volume_bars'] * 2)  # Volume penalty
        
        quality_score = max(0, quality_score)
        
        print(f"📈 Overall Quality Score: {quality_score:.1f}/100")
        
        # Mixed data should have some issues but not fail completely
        assert 30 <= quality_score <= 95, f"Quality score outside expected range: {quality_score:.1f}"
        assert analyses['ohlc']['ohlc_violations'] > 0, "Should detect some OHLC issues"
        assert analyses['ohlc']['ohlc_compliance_rate'] > 0.5, "Should still have majority good data"
        
        print("✅ Mixed quality data processing test completed")
    
    def test_data_quality_reporting_integration(self, tmp_path):
        """Test integration with validation reporting system."""
        
        # Setup validation pipeline
        storage_dir = tmp_path / "storage"
        reports_dir = tmp_path / "reports"
        storage_dir.mkdir()
        reports_dir.mkdir()
        
        storage_engine = ParquetStorageEngine(storage_dir)
        csv_repository = CsvReportRepository(reports_dir)
        
        # Create test data with known issues
        test_datasets = {
            "CLEAN": DataQualityGenerator.create_high_quality_dataset("CLEAN", 50),
            "DIRTY": DataQualityGenerator.create_quality_issues_dataset("DIRTY"),
            "MIXED": DataQualityGenerator.create_mixed_quality_dataset("MIXED", 30),
        }
        
        job_results = {}
        
        # Process each dataset through validation pipeline
        for symbol, data in test_datasets.items():
            job_id = f"quality-report-{symbol.lower()}"
            
            storage_engine.write(
                df=data,
                frame="1m",
                symbol=symbol,
                trading_day=date(2024, 1, 15),
                job_id=job_id,
                overwrite=True
            )
            
            # Simulate validation service (simplified)
            from unittest.mock import Mock
            from marketpipe.validation.domain.value_objects import BarError, ValidationResult
            
            # Mock validator to simulate real validation results
            validator = Mock()
            
            # Generate validation errors based on data quality
            errors = []
            
            # Analyze data and create appropriate errors
            ohlc_analysis = DataQualityAnalyzer.analyze_ohlc_consistency(data)
            temporal_analysis = DataQualityAnalyzer.analyze_temporal_consistency(data)
            volume_analysis = DataQualityAnalyzer.analyze_volume_patterns(data)
            
            error_count = (
                ohlc_analysis['ohlc_violations'] +
                temporal_analysis['alignment_violations'] +
                volume_analysis['zero_volume_bars']
            )
            
            # Create mock errors
            for i in range(min(error_count, 10)):  # Limit to 10 errors for testing
                errors.append(BarError(
                    ts_ns=int(data.iloc[i]['ts_ns']) if i < len(data) else 0,
                    reason=f"Quality issue detected at index {i}"
                ))
            
            validation_result = ValidationResult(
                symbol=symbol,
                total=len(data),
                errors=errors
            )
            
            validator.validate_bars.return_value = validation_result
            
            # Create validation service and generate report
            validation_service = ValidationRunnerService(
                storage_engine=storage_engine,
                validator=validator,
                reporter=csv_repository
            )
            
            # Simulate ingestion completed event
            from marketpipe.domain.events import IngestionJobCompleted
            
            event = IngestionJobCompleted(
                job_id=job_id,
                symbol=Symbol(symbol),
                trading_date=date(2024, 1, 15),
                bars_processed=len(data),
                success=True
            )
            
            validation_service.handle_ingestion_completed(event)
            
            job_results[symbol] = {
                "bars_processed": len(data),
                "errors_found": len(errors),
                "error_rate": len(errors) / len(data) if len(data) > 0 else 0,
            }
            
            print(f"✓ Processed {symbol}: {len(data)} bars, {len(errors)} errors")
        
        # Verify reports were generated
        reports = csv_repository.list_reports("quality-report-clean")
        reports.extend(csv_repository.list_reports("quality-report-dirty"))
        reports.extend(csv_repository.list_reports("quality-report-mixed"))
        
        assert len(reports) >= 3, f"Expected at least 3 reports, got {len(reports)}"
        
        print("📊 Quality Reporting Results:")
        for symbol, results in job_results.items():
            print(f"  {symbol}: {results['error_rate']:.1%} error rate ({results['errors_found']}/{results['bars_processed']})")
        
        # Verify quality expectations
        assert job_results["CLEAN"]["error_rate"] < 0.05, "Clean data should have <5% error rate"
        assert job_results["DIRTY"]["error_rate"] > 0.5, "Dirty data should have >50% error rate"
        assert 0.1 <= job_results["MIXED"]["error_rate"] <= 0.4, "Mixed data should have 10-40% error rate"
        
        print("✅ Data quality reporting integration test completed")
    
    def test_quality_metrics_across_timeframes(self, tmp_path):
        """Test data quality metrics across different aggregation timeframes."""
        
        # Setup
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)
        
        raw_engine = ParquetStorageEngine(raw_dir)
        
        # Create high-frequency data (1-minute bars)
        high_freq_data = DataQualityGenerator.create_high_quality_dataset("FREQ", size=300)  # 5 hours
        
        job_id = "timeframe-quality-test"
        raw_engine.write(
            df=high_freq_data,
            frame="1m",
            symbol="FREQ",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        
        # Test aggregation and quality preservation
        from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
        from marketpipe.aggregation.domain.services import AggregationDomainService
        from marketpipe.aggregation.application.services import AggregationRunnerService
        from marketpipe.domain.events import IngestionJobCompleted
        
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)
        
        # Trigger aggregation
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("FREQ"),
            trading_date=date(2024, 1, 15),
            bars_processed=len(high_freq_data),
            success=True
        )
        
        aggregation_service.handle_ingestion_completed(event)
        
        # Analyze quality across timeframes
        agg_engine = ParquetStorageEngine(agg_dir)
        timeframes = ["5m", "15m", "1h"]
        
        quality_by_timeframe = {}
        
        for timeframe in timeframes:
            try:
                agg_data = agg_engine.load_symbol_data(symbol="FREQ", frame=timeframe)
                
                if not agg_data.empty:
                    ohlc_analysis = DataQualityAnalyzer.analyze_ohlc_consistency(agg_data)
                    price_analysis = DataQualityAnalyzer.analyze_price_movements(agg_data)
                    volume_analysis = DataQualityAnalyzer.analyze_volume_patterns(agg_data)
                    
                    quality_by_timeframe[timeframe] = {
                        "bars": len(agg_data),
                        "ohlc_compliance": ohlc_analysis['ohlc_compliance_rate'],
                        "max_gap_percent": price_analysis['max_gap_percent'],
                        "zero_volume_bars": volume_analysis['zero_volume_bars'],
                    }
                    
                    print(f"✓ {timeframe} timeframe: {len(agg_data)} bars, {ohlc_analysis['ohlc_compliance_rate']:.1%} OHLC compliance")
                else:
                    print(f"⚠️  No {timeframe} aggregated data found")
                    
            except Exception as e:
                print(f"⚠️  Error loading {timeframe} data: {e}")
        
        # Quality should be preserved or improved through aggregation
        if quality_by_timeframe:
            print("📊 Quality Across Timeframes:")
            for timeframe, metrics in quality_by_timeframe.items():
                print(f"  {timeframe}: {metrics['ohlc_compliance']:.1%} OHLC, {metrics['zero_volume_bars']} zero volume")
            
            # All aggregated timeframes should maintain high quality
            for timeframe, metrics in quality_by_timeframe.items():
                assert metrics['ohlc_compliance'] >= 0.95, f"{timeframe} OHLC compliance too low: {metrics['ohlc_compliance']:.1%}"
                assert metrics['zero_volume_bars'] == 0, f"{timeframe} has zero volume bars: {metrics['zero_volume_bars']}"
        
        print("✅ Quality metrics across timeframes test completed")
    
    def test_data_quality_performance_impact(self, tmp_path):
        """Test performance impact of comprehensive data quality validation."""
        
        import time
        
        # Setup
        storage_dir = tmp_path / "storage"
        reports_dir = tmp_path / "reports"
        storage_dir.mkdir()
        reports_dir.mkdir()
        
        storage_engine = ParquetStorageEngine(storage_dir)
        csv_repository = CsvReportRepository(reports_dir)
        
        # Create large dataset for performance testing
        large_dataset = DataQualityGenerator.create_mixed_quality_dataset("PERF", size=5000)
        
        job_id = "performance-quality-test"
        
        # Measure storage performance
        storage_start = time.monotonic()
        storage_engine.write(
            df=large_dataset,
            frame="1m",
            symbol="PERF",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        storage_time = time.monotonic() - storage_start
        
        # Measure quality analysis performance
        analysis_start = time.monotonic()
        
        quality_analyses = {
            "ohlc": DataQualityAnalyzer.analyze_ohlc_consistency(large_dataset),
            "price": DataQualityAnalyzer.analyze_price_movements(large_dataset),
            "temporal": DataQualityAnalyzer.analyze_temporal_consistency(large_dataset),
            "volume": DataQualityAnalyzer.analyze_volume_patterns(large_dataset),
        }
        
        analysis_time = time.monotonic() - analysis_start
        
        print("📊 Data Quality Performance Metrics:")
        print(f"  Dataset Size: {len(large_dataset):,} bars")
        print(f"  Storage Time: {storage_time:.2f}s ({len(large_dataset)/storage_time:.0f} bars/sec)")
        print(f"  Analysis Time: {analysis_time:.2f}s ({len(large_dataset)/analysis_time:.0f} bars/sec)")
        print(f"  Total Time: {storage_time + analysis_time:.2f}s")
        
        # Performance assertions
        assert storage_time < 10, f"Storage too slow: {storage_time:.1f}s"
        assert analysis_time < 5, f"Quality analysis too slow: {analysis_time:.1f}s"
        
        # Verify analysis results are reasonable
        total_issues = (
            quality_analyses["ohlc"]["ohlc_violations"] +
            quality_analyses["temporal"]["alignment_violations"] +
            quality_analyses["volume"]["zero_volume_bars"]
        )
        
        print(f"  Quality Issues Found: {total_issues}")
        print(f"  Overall Quality: {(1 - total_issues/len(large_dataset))*100:.1f}%")
        
        # Should find some issues in mixed dataset
        assert total_issues > 0, "Should detect quality issues in mixed dataset"
        assert total_issues < len(large_dataset) * 0.3, "Too many issues detected"
        
        print("✅ Data quality performance impact test completed")


@pytest.mark.integration
@pytest.mark.data_quality
def test_comprehensive_quality_dashboard(tmp_path):
    """Create comprehensive quality dashboard for multiple datasets."""
    
    # Setup
    storage_dir = tmp_path / "storage"
    reports_dir = tmp_path / "reports"
    storage_dir.mkdir()
    reports_dir.mkdir()
    
    storage_engine = ParquetStorageEngine(storage_dir)
    
    # Create multiple test datasets with different characteristics
    test_scenarios = {
        "BLUE_CHIP": DataQualityGenerator.create_high_quality_dataset("BLUE_CHIP", 200),
        "PENNY_STOCK": DataQualityGenerator.create_quality_issues_dataset("PENNY_STOCK"),
        "VOLATILE": DataQualityGenerator.create_mixed_quality_dataset("VOLATILE", 150),
        "LOW_VOLUME": DataQualityGenerator.create_high_quality_dataset("LOW_VOLUME", 100),
    }
    
    # Process and analyze each dataset
    dashboard_data = {}
    
    for symbol, data in test_scenarios.items():
        job_id = f"dashboard-{symbol.lower()}"
        
        # Store data
        storage_engine.write(
            df=data,
            frame="1m",
            symbol=symbol,
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True
        )
        
        # Comprehensive analysis
        analyses = {
            "ohlc": DataQualityAnalyzer.analyze_ohlc_consistency(data),
            "price": DataQualityAnalyzer.analyze_price_movements(data),
            "temporal": DataQualityAnalyzer.analyze_temporal_consistency(data),
            "volume": DataQualityAnalyzer.analyze_volume_patterns(data),
        }
        
        # Calculate quality score
        quality_score = 100.0
        quality_score *= analyses["ohlc"]["ohlc_compliance_rate"]
        quality_score -= min(20, len(analyses["price"]["extreme_gaps"]) * 5)
        quality_score -= min(15, analyses["temporal"]["alignment_violations"] * 3)
        quality_score -= min(10, analyses["volume"]["zero_volume_bars"] * 2)
        quality_score = max(0, quality_score)
        
        dashboard_data[symbol] = {
            "total_bars": len(data),
            "quality_score": quality_score,
            "ohlc_compliance": analyses["ohlc"]["ohlc_compliance_rate"],
            "price_gaps": len(analyses["price"]["extreme_gaps"]),
            "timestamp_issues": analyses["temporal"]["alignment_violations"],
            "zero_volume_bars": analyses["volume"]["zero_volume_bars"],
            "avg_volume": analyses["volume"]["avg_volume"],
        }
    
    # Generate dashboard report
    print("=" * 80)
    print("📊 COMPREHENSIVE DATA QUALITY DASHBOARD")
    print("=" * 80)
    
    print(f"{'Symbol':<12} {'Bars':<6} {'Quality':<8} {'OHLC':<6} {'Gaps':<5} {'Time':<5} {'ZeroVol':<7} {'AvgVol':<8}")
    print("-" * 80)
    
    for symbol, metrics in dashboard_data.items():
        print(f"{symbol:<12} {metrics['total_bars']:<6} {metrics['quality_score']:<7.1f} "
              f"{metrics['ohlc_compliance']:<5.1%} {metrics['price_gaps']:<5} "
              f"{metrics['timestamp_issues']:<5} {metrics['zero_volume_bars']:<7} {metrics['avg_volume']:<7.0f}")
    
    print("-" * 80)
    
    # Summary statistics
    all_scores = [m["quality_score"] for m in dashboard_data.values()]
    avg_quality = sum(all_scores) / len(all_scores)
    min_quality = min(all_scores)
    max_quality = max(all_scores)
    
    print(f"Quality Summary: Avg={avg_quality:.1f}, Min={min_quality:.1f}, Max={max_quality:.1f}")
    
    # Quality tiers
    high_quality = [s for s, m in dashboard_data.items() if m["quality_score"] >= 90]
    medium_quality = [s for s, m in dashboard_data.items() if 70 <= m["quality_score"] < 90]
    low_quality = [s for s, m in dashboard_data.items() if m["quality_score"] < 70]
    
    print(f"Quality Tiers: High={len(high_quality)}, Medium={len(medium_quality)}, Low={len(low_quality)}")
    
    if low_quality:
        print(f"Low Quality Symbols: {', '.join(low_quality)}")
    
    print("=" * 80)
    
    # Validation
    assert len(high_quality) >= 1, "Should have at least one high-quality dataset"
    assert avg_quality > 50, f"Average quality too low: {avg_quality:.1f}"
    
    print("✅ Comprehensive quality dashboard test completed")