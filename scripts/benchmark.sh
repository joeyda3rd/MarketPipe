#!/usr/bin/env python3
"""Performance benchmarking for MarketPipe.

Benchmarks key operations to establish performance baselines
and detect regressions in alpha releases.
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List


class MarketPipeBenchmark:
    """Performance benchmarking suite for MarketPipe."""

    def __init__(self, output_file: str = None, verbose: bool = False):
        self.output_file = output_file
        self.verbose = verbose
        self.project_root = Path(__file__).parent.parent
        self.results = {}

    def log(self, message: str):
        """Log a message if verbose."""
        if self.verbose:
            print(f"🔍 {message}")

    def run_command(self, cmd: List[str], timeout: int = 120) -> tuple:
        """Run command and return (success, duration, stdout, stderr)."""
        start_time = time.perf_counter()

        try:
            result = subprocess.run(
                cmd, cwd=self.project_root, capture_output=True, text=True, timeout=timeout
            )
            duration = time.perf_counter() - start_time
            return True, duration, result.stdout, result.stderr

        except subprocess.TimeoutExpired:
            duration = time.perf_counter() - start_time
            return False, duration, "", f"Timeout after {timeout}s"
        except Exception as e:
            duration = time.perf_counter() - start_time
            return False, duration, "", str(e)

    def benchmark_fake_provider_small(self) -> Dict[str, Any]:
        """Benchmark small dataset ingestion with fake provider."""
        self.log("Benchmarking fake provider - small dataset (1 symbol, 1 day)")

        with tempfile.TemporaryDirectory() as temp_dir:
            cmd = [
                sys.executable,
                "-m",
                "marketpipe.cli",
                "ingest-ohlcv",
                "--provider",
                "fake",
                "--symbols",
                "AAPL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-01",
                "--output",
                temp_dir,
                "--feed-type",
                "iex",
            ]

            success, duration, stdout, stderr = self.run_command(cmd)

            # Count output files
            output_files = list(Path(temp_dir).glob("**/*.parquet"))
            file_count = len(output_files)

            # Calculate total file size
            total_size = sum(f.stat().st_size for f in output_files)

            return {
                "name": "fake_provider_small",
                "description": "1 symbol, 1 day with fake provider",
                "success": success,
                "duration_seconds": duration,
                "files_created": file_count,
                "total_size_bytes": total_size,
                "throughput_files_per_second": file_count / duration if duration > 0 else 0,
                "error": stderr if not success else None,
            }

    def benchmark_fake_provider_medium(self) -> Dict[str, Any]:
        """Benchmark medium dataset ingestion with fake provider."""
        self.log("Benchmarking fake provider - medium dataset (5 symbols, 2 days)")

        with tempfile.TemporaryDirectory() as temp_dir:
            cmd = [
                sys.executable,
                "-m",
                "marketpipe.cli",
                "ingest-ohlcv",
                "--provider",
                "fake",
                "--symbols",
                "AAPL,GOOGL,MSFT,TSLA,NVDA",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--output",
                temp_dir,
                "--feed-type",
                "iex",
            ]

            success, duration, stdout, stderr = self.run_command(cmd)

            output_files = list(Path(temp_dir).glob("**/*.parquet"))
            file_count = len(output_files)
            total_size = sum(f.stat().st_size for f in output_files)

            return {
                "name": "fake_provider_medium",
                "description": "5 symbols, 2 days with fake provider",
                "success": success,
                "duration_seconds": duration,
                "files_created": file_count,
                "total_size_bytes": total_size,
                "throughput_files_per_second": file_count / duration if duration > 0 else 0,
                "error": stderr if not success else None,
            }

    def benchmark_cli_startup(self) -> Dict[str, Any]:
        """Benchmark CLI startup time."""
        self.log("Benchmarking CLI startup time")

        # Test help command (minimal overhead)
        cmd = [sys.executable, "-m", "marketpipe.cli", "--help"]

        # Run multiple times for average
        durations = []
        for _ in range(5):
            success, duration, stdout, stderr = self.run_command(cmd, timeout=30)
            if success:
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else 0

        return {
            "name": "cli_startup",
            "description": "CLI startup time (--help command)",
            "success": len(durations) >= 3,  # At least 3 successful runs
            "duration_seconds": avg_duration,
            "runs": len(durations),
            "min_duration": min(durations) if durations else 0,
            "max_duration": max(durations) if durations else 0,
            "error": "Too many failures" if len(durations) < 3 else None,
        }

    def benchmark_health_check(self) -> Dict[str, Any]:
        """Benchmark health check command."""
        self.log("Benchmarking health check command")

        cmd = [sys.executable, "-m", "marketpipe.cli", "health-check", "--verbose"]
        success, duration, stdout, stderr = self.run_command(cmd, timeout=60)

        return {
            "name": "health_check",
            "description": "Health check command performance",
            "success": success,
            "duration_seconds": duration,
            "error": stderr if not success else None,
        }

    def benchmark_test_suite(self) -> Dict[str, Any]:
        """Benchmark fast test suite execution."""
        self.log("Benchmarking fast test suite")

        cmd = [sys.executable, "-m", "pytest", "-m", "fast", "--tb=no", "-q"]
        success, duration, stdout, stderr = self.run_command(cmd, timeout=180)

        # Parse test count from output
        test_count = 0
        if success and stdout:
            lines = stdout.split("\n")
            for line in lines:
                if "passed" in line and "selected" in line:
                    # Extract numbers from pytest summary
                    import re

                    numbers = re.findall(r"\d+", line)
                    if numbers:
                        test_count = int(numbers[0])
                    break

        return {
            "name": "fast_tests",
            "description": "Fast test suite execution time",
            "success": success,
            "duration_seconds": duration,
            "tests_run": test_count,
            "tests_per_second": test_count / duration if duration > 0 and test_count > 0 else 0,
            "error": stderr if not success else None,
        }

    def get_system_info(self) -> Dict[str, Any]:
        """Collect system information for context."""
        import platform

        import psutil

        try:
            return {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "cpu_count": psutil.cpu_count(),
                "memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
                "disk_free_gb": round(psutil.disk_usage(".").free / (1024**3), 2),
            }
        except Exception as e:
            return {"error": f"Could not collect system info: {e}"}

    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Run all benchmarks and return results."""
        print("🚀 MarketPipe Performance Benchmarks")
        print("=" * 50)

        # Collect system info
        system_info = self.get_system_info()

        # Run benchmarks
        benchmarks = [
            self.benchmark_cli_startup,
            self.benchmark_health_check,
            self.benchmark_fake_provider_small,
            self.benchmark_fake_provider_medium,
            self.benchmark_test_suite,
        ]

        results = []
        total_time = 0

        for benchmark_func in benchmarks:
            print(f"\n🔍 Running: {benchmark_func.__name__}")
            start = time.perf_counter()

            try:
                result = benchmark_func()
                duration = time.perf_counter() - start
                total_time += duration

                if result["success"]:
                    print(f"✅ {result['name']}: {result['duration_seconds']:.2f}s")
                else:
                    print(f"❌ {result['name']}: FAILED - {result.get('error', 'Unknown error')}")

                results.append(result)

            except Exception as e:
                print(f"❌ {benchmark_func.__name__}: ERROR - {e}")
                results.append(
                    {
                        "name": benchmark_func.__name__,
                        "success": False,
                        "error": str(e),
                        "duration_seconds": time.perf_counter() - start,
                    }
                )

        # Compile final results
        final_results = {
            "timestamp": time.time(),
            "iso_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "system_info": system_info,
            "benchmarks": results,
            "summary": {
                "total_benchmarks": len(results),
                "successful_benchmarks": sum(1 for r in results if r["success"]),
                "failed_benchmarks": sum(1 for r in results if not r["success"]),
                "total_time_seconds": total_time,
            },
        }

        # Store results
        self.results = final_results

        # Print summary
        print("\n📊 Benchmark Summary")
        print("=" * 30)
        print(f"✅ Successful: {final_results['summary']['successful_benchmarks']}")
        print(f"❌ Failed: {final_results['summary']['failed_benchmarks']}")
        print(f"⏱️  Total time: {final_results['summary']['total_time_seconds']:.2f}s")

        # Save to file if requested
        if self.output_file:
            output_path = Path(self.output_file)
            output_path.write_text(json.dumps(final_results, indent=2))
            print(f"📁 Results saved to: {output_path}")

        return final_results

    def compare_with_baseline(self, baseline_file: str):
        """Compare current results with a baseline file."""
        baseline_path = Path(baseline_file)
        if not baseline_path.exists():
            print(f"⚠️  Baseline file not found: {baseline_file}")
            return

        try:
            baseline_data = json.loads(baseline_path.read_text())
            current_data = self.results

            print("\n📊 Performance Comparison with Baseline")
            print("=" * 50)
            print(f"Baseline: {baseline_data.get('iso_timestamp', 'Unknown time')}")
            print(f"Current:  {current_data.get('iso_timestamp', 'Unknown time')}")
            print()

            # Compare individual benchmarks
            baseline_benchmarks = {b["name"]: b for b in baseline_data.get("benchmarks", [])}
            current_benchmarks = {b["name"]: b for b in current_data.get("benchmarks", [])}

            for name in current_benchmarks:
                if name in baseline_benchmarks:
                    baseline_duration = baseline_benchmarks[name].get("duration_seconds", 0)
                    current_duration = current_benchmarks[name].get("duration_seconds", 0)

                    if baseline_duration > 0 and current_duration > 0:
                        change = ((current_duration - baseline_duration) / baseline_duration) * 100

                        if abs(change) < 5:
                            symbol = "≈"  # Similar performance
                        elif change < -10:
                            symbol = "🚀"  # Much faster
                        elif change < 0:
                            symbol = "⬆️"  # Faster
                        elif change > 20:
                            symbol = "🐌"  # Much slower
                        else:
                            symbol = "⬇️"  # Slower

                        print(
                            f"{symbol} {name:20} {current_duration:6.2f}s vs {baseline_duration:6.2f}s ({change:+5.1f}%)"
                        )
                    else:
                        print(f"❓ {name:20} No comparison available")
                else:
                    print(f"🆕 {name:20} New benchmark")

        except Exception as e:
            print(f"❌ Error comparing with baseline: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="MarketPipe performance benchmarks")
    parser.add_argument("--output", "-o", help="Output JSON file for results")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--compare", help="Compare with baseline results file")
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Save results as baseline (saves to benchmark-baseline.json)",
    )

    args = parser.parse_args()

    # Determine output file
    output_file = args.output
    if args.baseline:
        output_file = "benchmark-baseline.json"
    elif not output_file:
        # Default output with timestamp
        import datetime

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"benchmark-{timestamp}.json"

    # Run benchmarks
    benchmark = MarketPipeBenchmark(output_file=output_file, verbose=args.verbose)
    results = benchmark.run_all_benchmarks()

    # Compare with baseline if requested
    if args.compare:
        benchmark.compare_with_baseline(args.compare)

    # Exit with appropriate code
    failed_count = results["summary"]["failed_benchmarks"]
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} benchmark(s) failed")
        sys.exit(1)
    else:
        print("\n🎉 All benchmarks completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
