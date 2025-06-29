#!/usr/bin/env python3
"""
Comprehensive CLI Command Testing Suite

Runs all CLI testing frameworks to validate complete command matrix coverage:
1. Basic command discovery and help validation
2. Option combinations and edge cases  
3. Backward compatibility testing
4. Enhanced edge case and error scenarios
5. Performance benchmarking

Usage:
    python scripts/test_all_cli_commands.py --quick     # Fast validation
    python scripts/test_all_cli_commands.py --full      # Complete testing
    python scripts/test_all_cli_commands.py --report    # Generate detailed report
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    # Import testing frameworks
    from tests.integration.test_cli_command_matrix import (
        CLICommandDiscovery,
        CLICommandValidator, 
        CLIMatrixTestReporter
    )
    from tests.integration.test_cli_enhanced_matrix import EnhancedCLITester
    TESTING_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Testing framework import failed: {e}")
    TESTING_AVAILABLE = False


class ComprehensiveCLITester:
    """Orchestrates all CLI testing frameworks."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.results = {}
        
    def run_basic_command_validation(self) -> Tuple[bool, str]:
        """Run basic command discovery and validation."""
        print("🔍 Running basic command matrix validation...")
        
        if not TESTING_AVAILABLE:
            return False, "Testing framework not available"
        
        try:
            discovery = CLICommandDiscovery()
            validator = CLICommandValidator(use_subprocess=True)
            reporter = CLIMatrixTestReporter()
            
            # Discover all commands
            commands = discovery.discover_all_commands()
            print(f"   Discovered {len(commands)} commands")
            
            # Validate each command
            results = []
            for i, command in enumerate(commands, 1):
                print(f"   Testing {i}/{len(commands)}: {' '.join(command.path)}", end="", flush=True)
                result = validator.validate_command(command)
                results.append(result)
                
                status = "✅" if result.help_works and result.side_effects_clean else "❌"
                print(f" {status}")
            
            reporter.add_results(results)
            report = reporter.generate_coverage_report()
            
            # Check for failures
            failed_commands = reporter.get_failed_commands()
            success = len(failed_commands) == 0
            
            self.results['basic_validation'] = {
                'success': success,
                'total_commands': len(commands),
                'failed_commands': len(failed_commands),
                'report': report
            }
            
            return success, report
            
        except Exception as e:
            error_msg = f"Basic validation failed: {e}"
            self.results['basic_validation'] = {'success': False, 'error': error_msg}
            return False, error_msg
    
    def run_edge_case_testing(self) -> Tuple[bool, str]:
        """Run enhanced edge case testing."""
        print("🧪 Running edge case and error scenario testing...")
        
        if not TESTING_AVAILABLE:
            return False, "Testing framework not available"
        
        try:
            tester = EnhancedCLITester()
            results = tester.run_all_edge_case_tests()
            
            report = tester.generate_edge_case_report(results)
            
            failed_tests = [r for r in results if not r.success]
            success = len(failed_tests) == 0
            
            print(f"   Completed {len(results)} edge case tests")
            print(f"   Failed: {len(failed_tests)}")
            
            self.results['edge_case_testing'] = {
                'success': success,
                'total_tests': len(results),
                'failed_tests': len(failed_tests),
                'report': report
            }
            
            return success, report
            
        except Exception as e:
            error_msg = f"Edge case testing failed: {e}"
            self.results['edge_case_testing'] = {'success': False, 'error': error_msg}
            return False, error_msg
    
    def run_performance_benchmarks(self) -> Tuple[bool, str]:
        """Run command performance benchmarks."""
        print("⚡ Running performance benchmarks...")
        
        performance_tests = [
            (["--help"], "Main help", 1.0),
            (["providers"], "List providers", 2.0),
            (["health-check", "--help"], "Health check help", 2.0),
            (["query", "--help"], "Query help", 1.5),
            (["jobs", "list", "--help"], "Jobs list help", 1.5),
            (["factory-reset", "--help"], "Factory reset help", 1.0)
        ]
        
        results = []
        slow_commands = []
        
        for cmd_path, description, max_time_sec in performance_tests:
            print(f"   Testing {description}...", end="", flush=True)
            
            start_time = time.time()
            try:
                result = subprocess.run(
                    ["python", "-m", "marketpipe"] + cmd_path,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    cwd=self.base_dir
                )
                execution_time = time.time() - start_time
                
                success = result.returncode == 0
                if execution_time > max_time_sec:
                    slow_commands.append(f"{' '.join(cmd_path)}: {execution_time:.2f}s (max: {max_time_sec}s)")
                    success = False
                
                results.append({
                    'command': ' '.join(cmd_path),
                    'description': description,
                    'time_sec': execution_time,
                    'max_time_sec': max_time_sec,
                    'success': success
                })
                
                status = "✅" if success else "❌"
                print(f" {status} ({execution_time:.2f}s)")
                
            except subprocess.TimeoutExpired:
                print(" ❌ (timeout)")
                results.append({
                    'command': ' '.join(cmd_path),
                    'description': description,
                    'time_sec': 10.0,
                    'max_time_sec': max_time_sec,
                    'success': False
                })
                slow_commands.append(f"{' '.join(cmd_path)}: timeout (>10s)")
        
        success = len(slow_commands) == 0
        
        report = f"Performance Benchmark Results:\n"
        report += f"Total commands tested: {len(results)}\n"
        report += f"Commands within performance limits: {sum(1 for r in results if r['success'])}\n"
        
        if slow_commands:
            report += f"\nSlow commands:\n"
            for slow in slow_commands:
                report += f"  - {slow}\n"
        
        self.results['performance'] = {
            'success': success,
            'total_tests': len(results),
            'slow_commands': slow_commands,
            'results': results,
            'report': report
        }
        
        return success, report
    
    def run_pytest_integration(self) -> Tuple[bool, str]:
        """Run the existing pytest CLI tests."""
        print("🧬 Running pytest CLI integration tests...")
        
        try:
            # Run specific CLI test files
            test_files = [
                "tests/integration/test_cli_command_matrix.py",
                "tests/integration/test_cli_option_validation.py", 
                "tests/integration/test_cli_backward_compatibility.py",
                "tests/integration/test_cli_enhanced_matrix.py"
            ]
            
            existing_files = [f for f in test_files if (self.base_dir / f).exists()]
            
            if not existing_files:
                return False, "No pytest CLI test files found"
            
            cmd = ["python", "-m", "pytest"] + existing_files + ["-v", "--tb=short"]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                cwd=self.base_dir
            )
            
            success = result.returncode == 0
            
            # Parse output for summary
            output_lines = result.stdout.split('\n')
            summary_line = next((line for line in output_lines if 'passed' in line and '=' in line), "")
            
            report = f"Pytest CLI Tests Results:\n"
            report += f"Exit code: {result.returncode}\n"
            report += f"Summary: {summary_line}\n"
            
            if not success:
                report += f"\nErrors:\n{result.stderr[:1000]}\n"
                report += f"\nOutput:\n{result.stdout[-1000:]}\n"
            
            self.results['pytest_integration'] = {
                'success': success,
                'exit_code': result.returncode,
                'summary': summary_line,
                'report': report
            }
            
            return success, report
            
        except subprocess.TimeoutExpired:
            error_msg = "Pytest tests timed out after 5 minutes"
            self.results['pytest_integration'] = {'success': False, 'error': error_msg}
            return False, error_msg
        except Exception as e:
            error_msg = f"Pytest execution failed: {e}"
            self.results['pytest_integration'] = {'success': False, 'error': error_msg}
            return False, error_msg
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive testing report."""
        report = []
        report.append("# MarketPipe CLI Comprehensive Testing Report")
        report.append("=" * 70)
        report.append("")
        
        # Overall summary
        total_phases = len(self.results)
        successful_phases = sum(1 for r in self.results.values() if r.get('success', False))
        
        report.append("## Overall Summary")
        report.append(f"- Test Phases Completed: {total_phases}")
        report.append(f"- Successful Phases: {successful_phases}/{total_phases}")
        report.append(f"- Overall Success Rate: {successful_phases/total_phases*100:.1f}%" if total_phases > 0 else "- No tests completed")
        report.append("")
        
        # Phase-by-phase results
        report.append("## Phase Results")
        
        phase_names = {
            'basic_validation': '1. Basic Command Matrix Validation',
            'edge_case_testing': '2. Edge Case and Error Scenario Testing', 
            'performance': '3. Performance Benchmarking',
            'pytest_integration': '4. Pytest Integration Testing'
        }
        
        for phase_key, phase_name in phase_names.items():
            if phase_key in self.results:
                result = self.results[phase_key]
                status = "✅ PASSED" if result.get('success', False) else "❌ FAILED"
                report.append(f"### {phase_name}: {status}")
                
                if 'total_commands' in result:
                    report.append(f"- Commands tested: {result['total_commands']}")
                if 'failed_commands' in result:
                    report.append(f"- Failed commands: {result['failed_commands']}")
                if 'total_tests' in result:
                    report.append(f"- Tests executed: {result['total_tests']}")
                if 'failed_tests' in result:
                    report.append(f"- Failed tests: {result['failed_tests']}")
                if 'slow_commands' in result:
                    report.append(f"- Slow commands: {len(result['slow_commands'])}")
                if 'error' in result:
                    report.append(f"- Error: {result['error']}")
                
                report.append("")
        
        # Detailed reports
        report.append("## Detailed Reports")
        for phase_key, result in self.results.items():
            if 'report' in result and result['report']:
                phase_name = phase_names.get(phase_key, phase_key)
                report.append(f"### {phase_name}")
                report.append(result['report'])
                report.append("")
        
        return "\n".join(report)
    
    def run_comprehensive_testing(self, include_pytest: bool = True, quick_mode: bool = False) -> bool:
        """Run all testing phases."""
        print("🚀 Starting comprehensive CLI testing suite...")
        print("=" * 60)
        
        overall_success = True
        
        # Phase 1: Basic validation
        success, _ = self.run_basic_command_validation()
        if not success:
            overall_success = False
        
        if not quick_mode:
            # Phase 2: Edge case testing
            success, _ = self.run_edge_case_testing()
            if not success:
                overall_success = False
        
        # Phase 3: Performance benchmarks
        success, _ = self.run_performance_benchmarks()
        if not success:
            overall_success = False
        
        if include_pytest and not quick_mode:
            # Phase 4: Pytest integration
            success, _ = self.run_pytest_integration()
            if not success:
                overall_success = False
        
        return overall_success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive CLI command testing suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--quick", 
        action="store_true",
        help="Run quick validation only (basic commands + performance)"
    )
    parser.add_argument(
        "--full",
        action="store_true", 
        help="Run complete testing suite including edge cases and pytest"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate detailed report file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("cli_testing_report.md"),
        help="Output file for detailed report"
    )
    
    args = parser.parse_args()
    
    # Default to full testing if no mode specified
    if not args.quick and not args.full:
        args.full = True
    
    tester = ComprehensiveCLITester()
    
    try:
        # Run testing based on mode
        if args.quick:
            print("🏃 Running quick CLI validation...")
            success = tester.run_comprehensive_testing(include_pytest=False, quick_mode=True)
        else:
            print("🔬 Running full CLI testing suite...")
            success = tester.run_comprehensive_testing(include_pytest=True, quick_mode=False)
        
        # Generate and display report
        report = tester.generate_comprehensive_report()
        print("\n" + "=" * 60)
        print(report)
        
        # Save report if requested
        if args.report:
            args.output.write_text(report)
            print(f"\n📄 Detailed report saved to: {args.output}")
        
        # Exit with appropriate code
        if success:
            print("\n🎉 All CLI testing phases completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Some CLI testing phases failed - check report for details")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n🛑 CLI testing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 CLI testing failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()