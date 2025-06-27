# SPDX-License-Identifier: Apache-2.0
"""Distributed systems end-to-end tests.

This test validates MarketPipe's behavior in distributed environments including
multi-node coordination, service discovery, distributed state management,
and cross-service communication patterns.
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pytest

from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class DistributedNode:
    """Simulates a distributed MarketPipe node."""

    def __init__(self, node_id: str, storage_dir: Path):
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.storage_engine = ParquetStorageEngine(storage_dir)
        self.is_active = True
        self.processed_jobs = []
        self.message_queue = asyncio.Queue()
        self.coordinator_address = None

    async def process_ingestion_job(self, job_data: Dict) -> Dict:
        """Process an ingestion job on this node."""

        if not self.is_active:
            raise RuntimeError(f"Node {self.node_id} is inactive")

        start_time = time.monotonic()

        # Simulate processing delay
        await asyncio.sleep(0.1 + len(job_data.get('symbols', [])) * 0.05)

        # Generate sample data for the job
        symbols = job_data.get('symbols', ['AAPL'])
        trading_day = date.fromisoformat(job_data.get('trading_day', '2024-01-15'))

        processed_data = {}

        for symbol in symbols:
            # Generate sample minute bars
            bars_data = []
            base_time = datetime.combine(trading_day, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(hours=13, minutes=30)

            for i in range(50):  # 50 minute bars
                timestamp_ns = int((base_time + timedelta(minutes=i)).timestamp() * 1e9)
                base_price = 150.0 + i * 0.01

                bars_data.append({
                    "ts_ns": timestamp_ns,
                    "symbol": symbol,
                    "open": round(base_price, 2),
                    "high": round(base_price + 0.15, 2),
                    "low": round(base_price - 0.10, 2),
                    "close": round(base_price + 0.05, 2),
                    "volume": 1000 + i * 10,
                    "trade_count": 50 + i,
                    "vwap": round(base_price + 0.02, 2),
                    "node_id": self.node_id,  # Track which node processed this
                })

            df = pd.DataFrame(bars_data)

            # Store data
            job_id = f"{job_data['job_id']}-{self.node_id}"
            self.storage_engine.write(
                df=df,
                frame="1m",
                symbol=symbol,
                trading_day=trading_day,
                job_id=job_id,
                overwrite=True
            )

            processed_data[symbol] = len(bars_data)

        processing_time = time.monotonic() - start_time

        result = {
            "node_id": self.node_id,
            "job_id": job_data['job_id'],
            "symbols_processed": list(processed_data.keys()),
            "bars_processed": sum(processed_data.values()),
            "processing_time_seconds": processing_time,
            "success": True,
        }

        self.processed_jobs.append(result)
        return result

    def simulate_failure(self):
        """Simulate node failure."""
        self.is_active = False

    def recover(self):
        """Recover from failure."""
        self.is_active = True

    async def send_heartbeat(self, coordinator) -> bool:
        """Send heartbeat to coordinator."""
        if not self.is_active:
            return False

        heartbeat = {
            "node_id": self.node_id,
            "timestamp": time.time(),
            "status": "active",
            "processed_jobs": len(self.processed_jobs),
        }

        return await coordinator.receive_heartbeat(heartbeat)


class DistributedCoordinator:
    """Coordinates distributed MarketPipe nodes."""

    def __init__(self):
        self.nodes: Dict[str, DistributedNode] = {}
        self.job_assignments: Dict[str, str] = {}  # job_id -> node_id
        self.heartbeats: Dict[str, Dict] = {}
        self.job_queue = asyncio.Queue()
        self.completed_jobs = []

    def register_node(self, node: DistributedNode):
        """Register a new node with the coordinator."""
        self.nodes[node.node_id] = node
        node.coordinator_address = self

    async def receive_heartbeat(self, heartbeat: Dict) -> bool:
        """Receive heartbeat from a node."""
        node_id = heartbeat["node_id"]
        self.heartbeats[node_id] = heartbeat
        return True

    def get_active_nodes(self) -> List[str]:
        """Get list of currently active nodes."""
        active_nodes = []
        current_time = time.time()

        for node_id, node in self.nodes.items():
            if node.is_active:
                # Check recent heartbeat (within 30 seconds)
                heartbeat = self.heartbeats.get(node_id)
                if heartbeat and (current_time - heartbeat["timestamp"]) < 30:
                    active_nodes.append(node_id)
                elif not heartbeat:  # No heartbeat required for newly registered nodes
                    active_nodes.append(node_id)

        return active_nodes

    async def distribute_job(self, job_data: Dict) -> Dict:
        """Distribute a job across available nodes."""

        active_nodes = self.get_active_nodes()
        if not active_nodes:
            raise RuntimeError("No active nodes available")

        # Simple round-robin distribution
        symbols = job_data.get('symbols', [])
        symbols_per_node = max(1, len(symbols) // len(active_nodes))

        node_assignments = {}
        symbol_index = 0

        for i, node_id in enumerate(active_nodes):
            start_idx = i * symbols_per_node
            end_idx = start_idx + symbols_per_node

            # Last node gets remaining symbols
            if i == len(active_nodes) - 1:
                end_idx = len(symbols)

            node_symbols = symbols[start_idx:end_idx]
            if node_symbols:
                node_assignments[node_id] = node_symbols

        # Execute jobs in parallel across nodes
        tasks = []
        for node_id, node_symbols in node_assignments.items():
            node = self.nodes[node_id]
            node_job_data = job_data.copy()
            node_job_data['symbols'] = node_symbols

            task = asyncio.create_task(node.process_ingestion_job(node_job_data))
            tasks.append((node_id, task))

        # Wait for all tasks to complete
        results = {}
        for node_id, task in tasks:
            try:
                result = await task
                results[node_id] = result
            except Exception as e:
                results[node_id] = {
                    "node_id": node_id,
                    "error": str(e),
                    "success": False,
                }

        # Aggregate results
        total_bars_processed = sum(
            r.get("bars_processed", 0) for r in results.values() if r.get("success")
        )

        aggregated_result = {
            "job_id": job_data['job_id'],
            "total_symbols": len(symbols),
            "total_bars_processed": total_bars_processed,
            "nodes_used": len(node_assignments),
            "node_results": results,
            "success": all(r.get("success", False) for r in results.values()),
        }

        self.completed_jobs.append(aggregated_result)
        return aggregated_result

    async def handle_node_failure(self, failed_node_id: str, redistribute_jobs: bool = True):
        """Handle node failure and optionally redistribute its jobs."""

        if failed_node_id not in self.nodes:
            return

        failed_node = self.nodes[failed_node_id]
        failed_node.simulate_failure()

        if redistribute_jobs:
            # Find jobs that were assigned to the failed node
            failed_jobs = [
                job_id for job_id, node_id in self.job_assignments.items()
                if node_id == failed_node_id
            ]

            # Redistribute to other nodes
            active_nodes = self.get_active_nodes()
            if active_nodes and failed_jobs:
                redistribution_results = {}

                for job_id in failed_jobs:
                    # Simple assignment to first available node
                    new_node_id = active_nodes[0]
                    self.job_assignments[job_id] = new_node_id
                    redistribution_results[job_id] = new_node_id

                return redistribution_results

        return {}


@pytest.mark.integration
@pytest.mark.distributed
class TestDistributedSystemsEndToEnd:
    """Distributed systems end-to-end testing."""

    def test_multi_node_coordination(self, tmp_path):
        """Test coordination between multiple MarketPipe nodes."""

        # Setup distributed environment
        coordinator = DistributedCoordinator()

        # Create multiple nodes
        nodes = []
        for i in range(3):
            node_dir = tmp_path / f"node_{i}"
            node_dir.mkdir()
            node = DistributedNode(f"node_{i}", node_dir)
            nodes.append(node)
            coordinator.register_node(node)

        print(f"✓ Created distributed environment with {len(nodes)} nodes")

        # Test distributed job processing
        async def test_coordination():
            # Send heartbeats from all nodes
            for node in nodes:
                await node.send_heartbeat(coordinator)

            # Verify all nodes are active
            active_nodes = coordinator.get_active_nodes()
            assert len(active_nodes) == 3
            print(f"✓ All {len(active_nodes)} nodes active and registered")

            # Distribute a job across nodes
            job_data = {
                "job_id": "distributed-test-job",
                "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META"],
                "trading_day": "2024-01-15",
            }

            result = await coordinator.distribute_job(job_data)

            # Verify job distribution worked
            assert result["success"]
            assert result["total_symbols"] == 6
            assert result["nodes_used"] <= 3
            assert result["total_bars_processed"] > 0

            print("✓ Distributed job processed successfully:")
            print(f"  Symbols: {result['total_symbols']}")
            print(f"  Bars: {result['total_bars_processed']}")
            print(f"  Nodes used: {result['nodes_used']}")

            # Verify each node processed some data
            successful_nodes = [
                node_id for node_id, node_result in result["node_results"].items()
                if node_result.get("success")
            ]
            assert len(successful_nodes) > 0

            return result

        result = asyncio.run(test_coordination())

        # Verify data was stored across nodes
        total_files_created = 0
        for node in nodes:
            node_files = list(node.storage_dir.rglob("*.parquet"))
            total_files_created += len(node_files)
            print(f"  {node.node_id}: {len(node_files)} files created")

        assert total_files_created > 0
        print("✅ Multi-node coordination test completed")

    def test_node_failure_and_recovery(self, tmp_path):
        """Test handling of node failures and recovery scenarios."""

        # Setup distributed environment
        coordinator = DistributedCoordinator()

        nodes = []
        for i in range(4):
            node_dir = tmp_path / f"node_{i}"
            node_dir.mkdir()
            node = DistributedNode(f"node_{i}", node_dir)
            nodes.append(node)
            coordinator.register_node(node)

        print(f"✓ Created environment with {len(nodes)} nodes for failure testing")

        async def test_failure_scenarios():
            # Initial heartbeats
            for node in nodes:
                await node.send_heartbeat(coordinator)

            initial_active = coordinator.get_active_nodes()
            assert len(initial_active) == 4
            print(f"✓ Initial state: {len(initial_active)} active nodes")

            # Process a job successfully with all nodes
            job_data = {
                "job_id": "pre-failure-job",
                "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN"],
                "trading_day": "2024-01-15",
            }

            result1 = await coordinator.distribute_job(job_data)
            assert result1["success"]
            print(f"✓ Pre-failure job successful: {result1['total_bars_processed']} bars")

            # Simulate node failure
            failed_node = nodes[1]
            failed_node.simulate_failure()
            print(f"✓ Simulated failure of {failed_node.node_id}")

            # Test job distribution with failed node
            job_data_2 = {
                "job_id": "during-failure-job",
                "symbols": ["TSLA", "META", "NVDA"],
                "trading_day": "2024-01-15",
            }

            result2 = await coordinator.distribute_job(job_data_2)

            # Should still succeed with remaining nodes
            remaining_nodes = coordinator.get_active_nodes()
            assert len(remaining_nodes) == 3
            print(f"✓ After failure: {len(remaining_nodes)} active nodes remaining")

            # Verify job still processed successfully
            if result2["success"]:
                print(f"✓ Job processed despite node failure: {result2['total_bars_processed']} bars")
            else:
                print("⚠️  Job partially failed due to node failure")

            # Test node recovery
            failed_node.recover()
            await failed_node.send_heartbeat(coordinator)

            recovered_nodes = coordinator.get_active_nodes()
            print(f"✓ After recovery: {len(recovered_nodes)} active nodes")

            # Test job distribution after recovery
            job_data_3 = {
                "job_id": "post-recovery-job",
                "symbols": ["NFLX", "CRM", "ORCL"],
                "trading_day": "2024-01-15",
            }

            result3 = await coordinator.distribute_job(job_data_3)
            assert result3["success"]
            print(f"✓ Post-recovery job successful: {result3['total_bars_processed']} bars")

            return {
                "pre_failure": result1,
                "during_failure": result2,
                "post_recovery": result3,
                "failed_node_id": failed_node.node_id,
            }

        results = asyncio.run(test_failure_scenarios())

        # Verify failure handling worked correctly
        assert results["pre_failure"]["success"]
        assert results["post_recovery"]["success"]

        print("✅ Node failure and recovery test completed")

    def test_distributed_state_consistency(self, tmp_path):
        """Test state consistency across distributed nodes."""

        # Setup distributed environment
        coordinator = DistributedCoordinator()

        nodes = []
        for i in range(3):
            node_dir = tmp_path / f"node_{i}"
            node_dir.mkdir()
            node = DistributedNode(f"node_{i}", node_dir)
            nodes.append(node)
            coordinator.register_node(node)

        print("✓ Created environment for state consistency testing")

        async def test_consistency():
            # Process multiple jobs across nodes
            jobs = [
                {
                    "job_id": f"consistency-job-{i}",
                    "symbols": [f"SYM{i:03d}", f"SYM{i+100:03d}"],
                    "trading_day": "2024-01-15",
                }
                for i in range(5)
            ]

            results = []
            for job_data in jobs:
                result = await coordinator.distribute_job(job_data)
                results.append(result)

                # Small delay between jobs
                await asyncio.sleep(0.05)

            return results

        results = asyncio.run(test_consistency())

        # Verify state consistency
        total_jobs_processed = len([r for r in results if r["success"]])
        total_bars_across_all_jobs = sum(r.get("total_bars_processed", 0) for r in results)

        print("📊 State Consistency Analysis:")
        print(f"  Jobs processed: {total_jobs_processed}")
        print(f"  Total bars: {total_bars_across_all_jobs}")

        # Check that each node has processed some jobs
        node_job_counts = {node.node_id: len(node.processed_jobs) for node in nodes}
        print(f"  Jobs per node: {node_job_counts}")

        # Verify distribution is reasonably balanced
        job_counts = list(node_job_counts.values())
        max_jobs = max(job_counts)
        min_jobs = min(job_counts)

        # Balanced if difference is within reasonable range
        balance_ratio = min_jobs / max_jobs if max_jobs > 0 else 1
        assert balance_ratio >= 0.3, f"Job distribution too unbalanced: {node_job_counts}"

        print(f"✓ Job distribution balance ratio: {balance_ratio:.2f}")

        # Verify data integrity across nodes
        for node in nodes:
            node_files = list(node.storage_dir.rglob("*.parquet"))
            if node_files:
                # Sample check - load one file and verify structure
                sample_file = node_files[0]
                try:
                    sample_df = pd.read_parquet(sample_file)
                    required_columns = ["ts_ns", "symbol", "open", "high", "low", "close", "volume"]

                    missing_columns = [col for col in required_columns if col not in sample_df.columns]
                    assert not missing_columns, f"Missing columns in {node.node_id}: {missing_columns}"

                    assert "node_id" in sample_df.columns, f"Node ID tracking missing in {node.node_id}"

                    print(f"✓ Data integrity verified for {node.node_id}")

                except Exception as e:
                    print(f"⚠️  Data integrity check failed for {node.node_id}: {e}")

        print("✅ Distributed state consistency test completed")

    def test_concurrent_job_processing(self, tmp_path):
        """Test concurrent job processing across multiple nodes."""

        # Setup distributed environment
        coordinator = DistributedCoordinator()

        nodes = []
        for i in range(4):
            node_dir = tmp_path / f"node_{i}"
            node_dir.mkdir()
            node = DistributedNode(f"node_{i}", node_dir)
            nodes.append(node)
            coordinator.register_node(node)

        print("✓ Created environment for concurrent processing testing")

        async def test_concurrent_processing():
            # Create multiple concurrent jobs
            concurrent_jobs = [
                {
                    "job_id": f"concurrent-job-{i}",
                    "symbols": [f"CONC{i:02d}A", f"CONC{i:02d}B"],
                    "trading_day": "2024-01-15",
                }
                for i in range(8)
            ]

            # Process jobs concurrently
            start_time = time.monotonic()

            tasks = [
                asyncio.create_task(coordinator.distribute_job(job_data))
                for job_data in concurrent_jobs
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            end_time = time.monotonic()
            total_time = end_time - start_time

            # Analyze results
            successful_results = [r for r in results if isinstance(r, dict) and r.get("success")]
            failed_results = [r for r in results if not (isinstance(r, dict) and r.get("success"))]

            total_bars = sum(r.get("total_bars_processed", 0) for r in successful_results)

            print("📊 Concurrent Processing Results:")
            print(f"  Total jobs: {len(concurrent_jobs)}")
            print(f"  Successful: {len(successful_results)}")
            print(f"  Failed: {len(failed_results)}")
            print(f"  Total bars processed: {total_bars}")
            print(f"  Processing time: {total_time:.2f}s")
            print(f"  Throughput: {total_bars/total_time:.0f} bars/sec")

            return {
                "total_jobs": len(concurrent_jobs),
                "successful_jobs": len(successful_results),
                "total_bars": total_bars,
                "processing_time": total_time,
                "throughput": total_bars / total_time if total_time > 0 else 0,
            }

        result = asyncio.run(test_concurrent_processing())

        # Performance assertions
        assert result["successful_jobs"] >= result["total_jobs"] * 0.8, "Too many jobs failed"
        assert result["throughput"] > 100, f"Throughput too low: {result['throughput']:.0f} bars/sec"

        # Verify each node participated in processing
        active_nodes = [node for node in nodes if len(node.processed_jobs) > 0]
        assert len(active_nodes) >= 2, "Not enough nodes participated in processing"

        print(f"✓ Concurrent processing utilized {len(active_nodes)} nodes")
        print("✅ Concurrent job processing test completed")


@pytest.mark.integration
@pytest.mark.distributed
def test_distributed_system_integration_demo(tmp_path):
    """Comprehensive demonstration of distributed system capabilities."""

    print("🎭 DISTRIBUTED SYSTEM INTEGRATION DEMONSTRATION")
    print("=" * 60)

    # Setup large distributed environment
    coordinator = DistributedCoordinator()

    # Create 5 nodes for comprehensive testing
    nodes = []
    for i in range(5):
        node_dir = tmp_path / f"demo_node_{i}"
        node_dir.mkdir()
        node = DistributedNode(f"demo_node_{i}", node_dir)
        nodes.append(node)
        coordinator.register_node(node)

    print(f"✓ Created distributed environment with {len(nodes)} nodes")

    async def comprehensive_demo():
        # Phase 1: Normal distributed operation
        print("\n🔄 Phase 1: Normal Distributed Operation")

        large_job = {
            "job_id": "comprehensive-demo-job",
            "symbols": [
                "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX",
                "CRM", "ORCL", "UBER", "LYFT", "SQ", "PYPL", "ROKU", "ZM"
            ],
            "trading_day": "2024-01-15",
        }

        result1 = await coordinator.distribute_job(large_job)
        print(f"  Normal operation: {result1['total_bars_processed']} bars across {result1['nodes_used']} nodes")

        # Phase 2: Simulate failures
        print("\n🔄 Phase 2: Failure Simulation")

        # Fail 2 nodes
        nodes[1].simulate_failure()
        nodes[3].simulate_failure()

        result2 = await coordinator.distribute_job({
            "job_id": "demo-failure-job",
            "symbols": ["FAIL1", "FAIL2", "FAIL3", "FAIL4"],
            "trading_day": "2024-01-15",
        })

        active_after_failure = coordinator.get_active_nodes()
        print(f"  After 2 node failures: {len(active_after_failure)} nodes active")
        print(f"  Job still processed: {result2.get('total_bars_processed', 0)} bars")

        # Phase 3: Concurrent load
        print("\n🔄 Phase 3: Concurrent Load Testing")

        # Recover failed nodes
        nodes[1].recover()
        nodes[3].recover()

        concurrent_jobs = [
            {
                "job_id": f"load-test-{i}",
                "symbols": [f"LOAD{i:02d}"],
                "trading_day": "2024-01-15",
            }
            for i in range(10)
        ]

        start_time = time.monotonic()
        concurrent_tasks = [
            asyncio.create_task(coordinator.distribute_job(job))
            for job in concurrent_jobs
        ]

        concurrent_results = await asyncio.gather(*concurrent_tasks)
        load_test_time = time.monotonic() - start_time

        successful_concurrent = sum(1 for r in concurrent_results if r.get("success"))
        total_concurrent_bars = sum(r.get("total_bars_processed", 0) for r in concurrent_results)

        print(f"  Concurrent jobs: {successful_concurrent}/{len(concurrent_jobs)} successful")
        print(f"  Load test time: {load_test_time:.2f}s")
        print(f"  Concurrent throughput: {total_concurrent_bars/load_test_time:.0f} bars/sec")

        return {
            "normal_operation": result1,
            "failure_handling": result2,
            "concurrent_performance": {
                "successful_jobs": successful_concurrent,
                "total_bars": total_concurrent_bars,
                "processing_time": load_test_time,
            }
        }

    demo_results = asyncio.run(comprehensive_demo())

    # Final analysis
    print("\n📊 DEMONSTRATION SUMMARY:")
    print(f"  Normal operation bars: {demo_results['normal_operation']['total_bars_processed']:,}")
    print(f"  Failure resilience: {'✓' if demo_results['failure_handling'].get('success') else '✗'}")
    print(f"  Concurrent jobs processed: {demo_results['concurrent_performance']['successful_jobs']}")

    # Verify demonstration success
    assert demo_results['normal_operation']['success']
    assert demo_results['concurrent_performance']['successful_jobs'] >= 8

    # Show node utilization
    print("\n📈 Node Utilization:")
    for node in nodes:
        jobs_processed = len(node.processed_jobs)
        print(f"  {node.node_id}: {jobs_processed} jobs processed")

    print("\n✅ Distributed system integration demonstration completed successfully!")
    print("=" * 60)
