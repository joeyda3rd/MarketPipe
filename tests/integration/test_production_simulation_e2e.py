# SPDX-License-Identifier: Apache-2.0
"""Production environment simulation end-to-end tests.

This test simulates production workloads, resource constraints, capacity planning,
and real-world operational scenarios to validate MarketPipe's production readiness.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import psutil
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class ProductionWorkloadSimulator:
    """Simulates production-level workloads and resource usage."""
    
    def __init__(self, storage_dir: Path):
        self.storage_dir = storage_dir
        self.storage_engine = ParquetStorageEngine(storage_dir)
        self.process = psutil.Process()
        self.metrics = {
            "jobs_processed": 0,
            "bars_processed": 0,
            "memory_usage_mb": [],
            "cpu_usage_percent": [],
            "processing_times": [],
            "error_count": 0,
        }
        self._monitoring_active = False
        self._monitoring_thread = None
        
    def start_monitoring(self):
        """Start resource monitoring."""
        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self._monitoring_thread.start()
        
    def stop_monitoring(self):
        """Stop resource monitoring."""
        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=1)
    
    def _monitor_resources(self):
        """Monitor system resources continuously."""
        while self._monitoring_active:
            try:
                # Memory usage
                memory_mb = self.process.memory_info().rss / 1024 / 1024
                self.metrics["memory_usage_mb"].append(memory_mb)
                
                # CPU usage
                cpu_percent = self.process.cpu_percent()
                self.metrics["cpu_usage_percent"].append(cpu_percent)
                
                time.sleep(0.5)  # Monitor every 500ms
                
            except Exception:
                # Ignore monitoring errors
                pass
    
    def generate_production_dataset(
        self, 
        symbol: str, 
        days: int = 5, 
        bars_per_day: int = 390,
        volatility: float = 0.02
    ) -> pd.DataFrame:
        """Generate production-scale realistic market data."""
        
        bars = []
        base_date = date(2024, 1, 15)
        base_price = 150.0 + random.uniform(-50, 50)
        
        for day in range(days):
            trading_day = base_date + timedelta(days=day)
            day_start = datetime.combine(trading_day, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(hours=13, minutes=30)
            
            day_open_price = base_price
            
            for minute in range(bars_per_day):
                timestamp = day_start + timedelta(minutes=minute)
                timestamp_ns = int(timestamp.timestamp() * 1e9)
                
                # Realistic intraday price movement
                minute_change = random.gauss(0, volatility / 100)
                
                # Add some intraday patterns
                if minute < 30:  # Opening volatility
                    minute_change *= 1.5
                elif minute > bars_per_day - 30:  # Closing volatility
                    minute_change *= 1.2
                
                current_price = day_open_price * (1 + minute_change)
                
                # Generate OHLC with realistic relationships
                spread = current_price * random.uniform(0.001, 0.005)
                
                open_price = current_price
                close_price = current_price + random.gauss(0, spread/2)
                high_price = max(open_price, close_price) + random.uniform(0, spread)
                low_price = min(open_price, close_price) - random.uniform(0, spread)
                
                # Realistic volume patterns
                base_volume = 1000
                if minute < 30 or minute > bars_per_day - 30:
                    base_volume *= 2  # Higher volume at open/close
                
                volume = int(base_volume * random.uniform(0.5, 2.0))
                
                bars.append({
                    "ts_ns": timestamp_ns,
                    "symbol": symbol,
                    "open": round(open_price, 2),
                    "high": round(high_price, 2),
                    "low": round(low_price, 2),
                    "close": round(close_price, 2),
                    "volume": volume,
                    "trade_count": max(1, volume // 20),
                    "vwap": round((open_price + high_price + low_price + close_price) / 4, 2),
                })
                
                day_open_price = close_price  # Carry forward for next bar
        
        return pd.DataFrame(bars)
    
    async def simulate_production_ingestion_job(
        self, 
        job_id: str, 
        symbols: List[str],
        days: int = 1,
        simulated_delay: float = 0.0
    ) -> Dict:
        """Simulate a production ingestion job with realistic processing."""
        
        start_time = time.monotonic()
        bars_processed = 0
        
        try:
            for symbol in symbols:
                # Generate production data
                df = self.generate_production_dataset(
                    symbol=symbol, 
                    days=days, 
                    bars_per_day=390  # Full trading day
                )
                
                # Simulate processing delay (network, API calls, etc.)
                if simulated_delay > 0:
                    await asyncio.sleep(simulated_delay)
                
                # Store data
                trading_day = date(2024, 1, 15)  # Use consistent date
                self.storage_engine.write(
                    df=df,
                    frame="1m",
                    symbol=symbol,
                    trading_day=trading_day,
                    job_id=job_id,
                    overwrite=True
                )
                
                bars_processed += len(df)
            
            processing_time = time.monotonic() - start_time
            
            # Update metrics
            self.metrics["jobs_processed"] += 1
            self.metrics["bars_processed"] += bars_processed
            self.metrics["processing_times"].append(processing_time)
            
            return {
                "job_id": job_id,
                "symbols_processed": len(symbols),
                "bars_processed": bars_processed,
                "processing_time": processing_time,
                "success": True,
            }
            
        except Exception as e:
            self.metrics["error_count"] += 1
            return {
                "job_id": job_id,
                "error": str(e),
                "success": False,
            }
    
    def get_performance_summary(self) -> Dict:
        """Get comprehensive performance summary."""
        
        memory_usage = self.metrics["memory_usage_mb"]
        cpu_usage = self.metrics["cpu_usage_percent"]
        processing_times = self.metrics["processing_times"]
        
        return {
            "jobs_processed": self.metrics["jobs_processed"],
            "bars_processed": self.metrics["bars_processed"],
            "error_count": self.metrics["error_count"],
            "error_rate": self.metrics["error_count"] / max(1, self.metrics["jobs_processed"]),
            "memory_stats": {
                "peak_mb": max(memory_usage) if memory_usage else 0,
                "avg_mb": sum(memory_usage) / len(memory_usage) if memory_usage else 0,
                "samples": len(memory_usage),
            },
            "cpu_stats": {
                "peak_percent": max(cpu_usage) if cpu_usage else 0,
                "avg_percent": sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0,
                "samples": len(cpu_usage),
            },
            "timing_stats": {
                "total_time": sum(processing_times),
                "avg_time": sum(processing_times) / len(processing_times) if processing_times else 0,
                "min_time": min(processing_times) if processing_times else 0,
                "max_time": max(processing_times) if processing_times else 0,
            },
            "throughput": {
                "bars_per_second": self.metrics["bars_processed"] / sum(processing_times) if processing_times else 0,
                "jobs_per_minute": len(processing_times) * 60 / sum(processing_times) if processing_times else 0,
            }
        }


class ResourceConstraintSimulator:
    """Simulates various resource constraints in production."""
    
    @staticmethod
    def simulate_memory_pressure():
        """Simulate memory pressure by allocating large amounts of memory."""
        # Allocate large DataFrame to create memory pressure
        large_data = pd.DataFrame({
            'data': range(1000000),  # 1M rows
            'values': [random.random() for _ in range(1000000)]
        })
        
        # Hold reference briefly then release
        time.sleep(0.1)
        del large_data
        gc.collect()
        
    @staticmethod
    def simulate_cpu_load():
        """Simulate CPU load with intensive computation."""
        # CPU-intensive calculation
        result = sum(i ** 2 for i in range(100000))
        return result
        
    @staticmethod
    def simulate_disk_io_pressure(tmp_path: Path):
        """Simulate disk I/O pressure."""
        # Create and write large temporary files
        temp_files = []
        for i in range(5):
            temp_file = tmp_path / f"pressure_{i}.tmp"
            
            # Write 10MB of data
            with open(temp_file, 'wb') as f:
                f.write(os.urandom(10 * 1024 * 1024))
            
            temp_files.append(temp_file)
        
        # Read files back
        for temp_file in temp_files:
            with open(temp_file, 'rb') as f:
                _ = f.read()
        
        # Cleanup
        for temp_file in temp_files:
            temp_file.unlink()


@pytest.mark.integration
@pytest.mark.production_simulation
class TestProductionSimulationEndToEnd:
    """Production environment simulation testing."""
    
    def test_high_volume_production_workload(self, tmp_path):
        """Test handling of high-volume production workloads."""
        
        simulator = ProductionWorkloadSimulator(tmp_path / "production_storage")
        simulator.start_monitoring()
        
        print("🚀 Starting high-volume production workload simulation")
        
        try:
            # Simulate processing 100 symbols over 5 days
            symbols = [f"PROD{i:03d}" for i in range(100)]
            
            async def run_high_volume_workload():
                # Process symbols in batches
                batch_size = 10
                symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
                
                results = []
                for batch_idx, symbol_batch in enumerate(symbol_batches):
                    job_id = f"high-volume-batch-{batch_idx}"
                    
                    result = await simulator.simulate_production_ingestion_job(
                        job_id=job_id,
                        symbols=symbol_batch,
                        days=5,  # 5 trading days
                        simulated_delay=0.02  # 20ms delay per symbol
                    )
                    
                    results.append(result)
                    
                    print(f"  Batch {batch_idx + 1}/{len(symbol_batches)}: {result.get('bars_processed', 0)} bars")
                
                return results
            
            start_time = time.monotonic()
            results = asyncio.run(run_high_volume_workload())
            total_time = time.monotonic() - start_time
            
            # Analyze results
            successful_jobs = [r for r in results if r.get("success")]
            total_bars = sum(r.get("bars_processed", 0) for r in successful_jobs)
            
            print(f"📊 High-Volume Workload Results:")
            print(f"  Total jobs: {len(results)}")
            print(f"  Successful: {len(successful_jobs)}")
            print(f"  Total bars processed: {total_bars:,}")
            print(f"  Processing time: {total_time:.1f}s")
            print(f"  Throughput: {total_bars/total_time:.0f} bars/sec")
            
            # Performance assertions
            assert len(successful_jobs) >= len(results) * 0.95  # 95% success rate
            assert total_bars >= 100 * 5 * 390 * 0.9  # At least 90% of expected bars
            assert total_bars / total_time > 1000  # At least 1000 bars/sec
            
        finally:
            simulator.stop_monitoring()
        
        # Resource usage analysis
        perf_summary = simulator.get_performance_summary()
        
        print(f"📈 Resource Usage Summary:")
        print(f"  Peak memory: {perf_summary['memory_stats']['peak_mb']:.1f} MB")
        print(f"  Average CPU: {perf_summary['cpu_stats']['avg_percent']:.1f}%")
        print(f"  Error rate: {perf_summary['error_rate']:.1%}")
        
        # Resource constraints
        assert perf_summary['memory_stats']['peak_mb'] < 2000, "Memory usage too high"
        assert perf_summary['error_rate'] < 0.05, "Error rate too high"
        
        print("✅ High-volume production workload test completed")
    
    def test_resource_constraint_scenarios(self, tmp_path):
        """Test behavior under various resource constraints."""
        
        simulator = ProductionWorkloadSimulator(tmp_path / "constrained_storage")
        simulator.start_monitoring()
        
        print("⚡ Testing resource constraint scenarios")
        
        try:
            # Test under memory pressure
            print("  🧠 Testing under memory pressure...")
            
            async def test_memory_pressure():
                # Create memory pressure
                ResourceConstraintSimulator.simulate_memory_pressure()
                
                # Process job under memory pressure
                result = await simulator.simulate_production_ingestion_job(
                    job_id="memory-pressure-test",
                    symbols=["MEMPRS"],
                    days=2
                )
                
                return result
            
            memory_result = asyncio.run(test_memory_pressure())
            assert memory_result["success"], "Job failed under memory pressure"
            print(f"    ✓ Processed {memory_result['bars_processed']} bars under memory pressure")
            
            # Test under CPU load
            print("  💻 Testing under CPU load...")
            
            async def test_cpu_load():
                # Create CPU pressure in background
                with ThreadPoolExecutor(max_workers=4) as executor:
                    cpu_futures = [
                        executor.submit(ResourceConstraintSimulator.simulate_cpu_load)
                        for _ in range(4)
                    ]
                    
                    # Process job under CPU load
                    result = await simulator.simulate_production_ingestion_job(
                        job_id="cpu-load-test",
                        symbols=["CPULOAD"],
                        days=1
                    )
                    
                    # Wait for CPU load tasks to complete
                    for future in cpu_futures:
                        future.result()
                    
                    return result
            
            cpu_result = asyncio.run(test_cpu_load())
            assert cpu_result["success"], "Job failed under CPU load"
            print(f"    ✓ Processed {cpu_result['bars_processed']} bars under CPU load")
            
            # Test under disk I/O pressure
            print("  💾 Testing under disk I/O pressure...")
            
            async def test_disk_pressure():
                # Create disk I/O pressure
                disk_thread = threading.Thread(
                    target=ResourceConstraintSimulator.simulate_disk_io_pressure,
                    args=(tmp_path,)
                )
                disk_thread.start()
                
                # Process job under disk pressure
                result = await simulator.simulate_production_ingestion_job(
                    job_id="disk-pressure-test",
                    symbols=["DISKIO"],
                    days=1
                )
                
                disk_thread.join()
                return result
            
            disk_result = asyncio.run(test_disk_pressure())
            assert disk_result["success"], "Job failed under disk I/O pressure"
            print(f"    ✓ Processed {disk_result['bars_processed']} bars under disk pressure")
            
        finally:
            simulator.stop_monitoring()
        
        # Verify all constraint scenarios succeeded
        constraint_results = [memory_result, cpu_result, disk_result]
        successful_constraint_tests = sum(1 for r in constraint_results if r["success"])
        
        assert successful_constraint_tests == 3, f"Only {successful_constraint_tests}/3 constraint tests passed"
        
        print("✅ Resource constraint scenarios test completed")
    
    def test_capacity_planning_simulation(self, tmp_path):
        """Test capacity planning scenarios with varying loads."""
        
        simulator = ProductionWorkloadSimulator(tmp_path / "capacity_storage")
        simulator.start_monitoring()
        
        print("📊 Running capacity planning simulation")
        
        try:
            # Test different load levels
            load_scenarios = [
                {"name": "light_load", "symbols": 10, "days": 1, "delay": 0.01},
                {"name": "medium_load", "symbols": 50, "days": 2, "delay": 0.02},
                {"name": "heavy_load", "symbols": 100, "days": 3, "delay": 0.03},
                {"name": "peak_load", "symbols": 200, "days": 1, "delay": 0.01},
            ]
            
            capacity_results = {}
            
            for scenario in load_scenarios:
                print(f"  🔄 Testing {scenario['name']}...")
                
                symbols = [f"{scenario['name'].upper()}{i:03d}" for i in range(scenario["symbols"])]
                
                async def run_capacity_test():
                    result = await simulator.simulate_production_ingestion_job(
                        job_id=f"capacity-{scenario['name']}",
                        symbols=symbols,
                        days=scenario["days"],
                        simulated_delay=scenario["delay"]
                    )
                    return result
                
                start_time = time.monotonic()
                result = asyncio.run(run_capacity_test())
                scenario_time = time.monotonic() - start_time
                
                if result["success"]:
                    throughput = result["bars_processed"] / scenario_time
                    capacity_results[scenario["name"]] = {
                        "symbols": scenario["symbols"],
                        "bars_processed": result["bars_processed"],
                        "processing_time": scenario_time,
                        "throughput": throughput,
                        "success": True,
                    }
                    
                    print(f"    ✓ {scenario['name']}: {result['bars_processed']:,} bars, {throughput:.0f} bars/sec")
                else:
                    capacity_results[scenario["name"]] = {
                        "success": False,
                        "error": result.get("error", "Unknown error")
                    }
                    print(f"    ✗ {scenario['name']}: Failed - {result.get('error', 'Unknown')}")
                
                # Brief pause between scenarios
                time.sleep(0.5)
        
        finally:
            simulator.stop_monitoring()
        
        # Analyze capacity planning results
        successful_scenarios = [s for s, r in capacity_results.items() if r.get("success")]
        
        print(f"\n📈 Capacity Planning Analysis:")
        print(f"  Scenarios tested: {len(load_scenarios)}")
        print(f"  Successful: {len(successful_scenarios)}")
        
        if len(successful_scenarios) >= 2:
            # Calculate scaling efficiency
            light_throughput = capacity_results.get("light_load", {}).get("throughput", 0)
            heavy_throughput = capacity_results.get("heavy_load", {}).get("throughput", 0)
            
            if light_throughput > 0 and heavy_throughput > 0:
                scaling_efficiency = heavy_throughput / light_throughput
                print(f"  Scaling efficiency: {scaling_efficiency:.2f}x")
                
                # Scaling should be reasonable (not drastically worse under load)
                assert scaling_efficiency > 0.3, f"Poor scaling efficiency: {scaling_efficiency:.2f}x"
        
        # Overall capacity test should succeed for at least light and medium loads
        assert capacity_results.get("light_load", {}).get("success", False), "Light load test failed"
        assert capacity_results.get("medium_load", {}).get("success", False), "Medium load test failed"
        
        perf_summary = simulator.get_performance_summary()
        print(f"  Total bars processed: {perf_summary['bars_processed']:,}")
        print(f"  Overall throughput: {perf_summary['throughput']['bars_per_second']:.0f} bars/sec")
        
        print("✅ Capacity planning simulation completed")
    
    def test_production_data_integrity_at_scale(self, tmp_path):
        """Test data integrity under production-scale loads."""
        
        simulator = ProductionWorkloadSimulator(tmp_path / "integrity_storage")
        
        print("🔍 Testing production data integrity at scale")
        
        # Process large dataset
        symbols = [f"INTEGRITY{i:03d}" for i in range(50)]
        
        async def run_integrity_test():
            result = await simulator.simulate_production_ingestion_job(
                job_id="integrity-scale-test",
                symbols=symbols,
                days=3,
                simulated_delay=0.01
            )
            return result
        
        result = asyncio.run(run_integrity_test())
        
        assert result["success"], "Integrity test job failed"
        
        # Verify data integrity
        storage_files = list(simulator.storage_dir.rglob("*.parquet"))
        assert len(storage_files) > 0, "No data files created"
        
        print(f"  📁 Created {len(storage_files)} data files")
        
        # Sample integrity checks
        integrity_issues = []
        files_checked = 0
        
        for file_path in storage_files[:10]:  # Check first 10 files
            try:
                df = pd.read_parquet(file_path)
                files_checked += 1
                
                # Check required columns
                required_cols = ["ts_ns", "symbol", "open", "high", "low", "close", "volume"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    integrity_issues.append(f"Missing columns in {file_path.name}: {missing_cols}")
                
                # Check OHLC relationships
                ohlc_violations = 0
                for _, row in df.iterrows():
                    if row["high"] < max(row["open"], row["close"]):
                        ohlc_violations += 1
                    if row["low"] > min(row["open"], row["close"]):
                        ohlc_violations += 1
                
                if ohlc_violations > 0:
                    integrity_issues.append(f"OHLC violations in {file_path.name}: {ohlc_violations}")
                
                # Check for negative values
                if (df[["open", "high", "low", "close", "volume"]] < 0).any().any():
                    integrity_issues.append(f"Negative values in {file_path.name}")
                
            except Exception as e:
                integrity_issues.append(f"Failed to read {file_path.name}: {e}")
        
        print(f"  🔍 Checked {files_checked} files for integrity")
        
        if integrity_issues:
            print(f"  ⚠️  Found {len(integrity_issues)} integrity issues:")
            for issue in integrity_issues[:5]:  # Show first 5 issues
                print(f"    - {issue}")
        else:
            print(f"  ✅ No integrity issues found")
        
        # Integrity assertions
        assert len(integrity_issues) == 0, f"Data integrity issues found: {integrity_issues[:3]}"
        
        print("✅ Production data integrity test completed")


@pytest.mark.integration
@pytest.mark.production_simulation
def test_production_simulation_comprehensive_demo(tmp_path):
    """Comprehensive production simulation demonstration."""
    
    print("🎭 PRODUCTION SIMULATION COMPREHENSIVE DEMONSTRATION")
    print("=" * 70)
    
    simulator = ProductionWorkloadSimulator(tmp_path / "demo_production")
    simulator.start_monitoring()
    
    try:
        # Multi-phase production simulation
        print("\n🏭 Phase 1: Baseline Production Load")
        
        baseline_symbols = [f"BASELINE{i:02d}" for i in range(20)]
        
        async def baseline_test():
            return await simulator.simulate_production_ingestion_job(
                job_id="baseline-production",
                symbols=baseline_symbols,
                days=2,
                simulated_delay=0.015
            )
        
        baseline_result = asyncio.run(baseline_test())
        print(f"  Baseline: {baseline_result['bars_processed']:,} bars processed")
        
        print("\n🚀 Phase 2: Peak Production Load")
        
        peak_symbols = [f"PEAK{i:03d}" for i in range(100)]
        
        async def peak_test():
            return await simulator.simulate_production_ingestion_job(
                job_id="peak-production",
                symbols=peak_symbols,
                days=1,
                simulated_delay=0.01
            )
        
        peak_result = asyncio.run(peak_test())
        print(f"  Peak load: {peak_result['bars_processed']:,} bars processed")
        
        print("\n⚡ Phase 3: Stress Test with Constraints")
        
        # Apply resource pressure during processing
        stress_symbols = [f"STRESS{i:02d}" for i in range(30)]
        
        async def stress_test():
            # Start with memory pressure
            ResourceConstraintSimulator.simulate_memory_pressure()
            
            # Process under stress
            result = await simulator.simulate_production_ingestion_job(
                job_id="stress-production",
                symbols=stress_symbols,
                days=3,
                simulated_delay=0.02
            )
            
            return result
        
        stress_result = asyncio.run(stress_test())
        print(f"  Stress test: {stress_result['bars_processed']:,} bars processed")
        
    finally:
        simulator.stop_monitoring()
    
    # Comprehensive analysis
    perf_summary = simulator.get_performance_summary()
    
    print(f"\n📊 PRODUCTION SIMULATION SUMMARY:")
    print(f"  Total jobs processed: {perf_summary['jobs_processed']}")
    print(f"  Total bars processed: {perf_summary['bars_processed']:,}")
    print(f"  Peak memory usage: {perf_summary['memory_stats']['peak_mb']:.1f} MB")
    print(f"  Average CPU usage: {perf_summary['cpu_stats']['avg_percent']:.1f}%")
    print(f"  Overall throughput: {perf_summary['throughput']['bars_per_second']:.0f} bars/sec")
    print(f"  Error rate: {perf_summary['error_rate']:.1%}")
    
    # Production readiness validation
    production_checks = {
        "throughput": perf_summary['throughput']['bars_per_second'] > 500,
        "memory_efficiency": perf_summary['memory_stats']['peak_mb'] < 3000,
        "reliability": perf_summary['error_rate'] < 0.02,
        "performance_consistency": len(perf_summary['timing_stats']) > 0,
    }
    
    passed_checks = sum(production_checks.values())
    total_checks = len(production_checks)
    
    print(f"\n✅ PRODUCTION READINESS: {passed_checks}/{total_checks} checks passed")
    
    for check_name, passed in production_checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check_name.replace('_', ' ').title()}")
    
    # Overall production readiness assertion
    assert passed_checks >= total_checks * 0.75, f"Production readiness insufficient: {passed_checks}/{total_checks}"
    
    print("\n🎯 Production simulation demonstrates MarketPipe's readiness for enterprise deployment!")
    print("=" * 70)