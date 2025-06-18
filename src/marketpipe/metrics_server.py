# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from http.server import HTTPServer
from threading import Thread
from typing import Optional
from pathlib import Path
import tempfile

from prometheus_client import (
    start_http_server,
    generate_latest,
    CollectorRegistry,
    CONTENT_TYPE_LATEST,
    Gauge,
)
from prometheus_client.multiprocess import MultiProcessCollector
from wsgiref.simple_server import make_server


# Event loop lag gauge for monitoring blocking operations
EVENT_LOOP_LAG = Gauge(
    'marketpipe_event_loop_lag_seconds',
    'Time difference between expected and actual event loop execution'
)

# Connection and request limits for production use
MAX_CONNECTIONS = int(os.getenv("METRICS_MAX_CONNECTIONS", "100"))
MAX_HEADER_SIZE = int(os.getenv("METRICS_MAX_HEADER_SIZE", "16384"))  # 16 KiB

logger = logging.getLogger(__name__)


class AsyncMetricsServer:
    """Asynchronous Prometheus metrics server using asyncio.start_server."""
    
    def __init__(self, port: int = 8000, host: str = "0.0.0.0", max_connections: int = MAX_CONNECTIONS):
        self.port = port
        self.host = host
        self.max_connections = max_connections
        self.server: Optional[asyncio.Server] = None
        self._lag_monitor_task: Optional[asyncio.Task] = None
        self._registry = CollectorRegistry()
        
        # Setup multiprocess collector if available
        multiproc_dir = setup_multiprocess_metrics_dir()
        if multiproc_dir:
            MultiProcessCollector(self._registry)
            logger.info(f"Using multiprocess metrics directory: {multiproc_dir}")
        else:
            # For single process, use the default registry
            from prometheus_client import REGISTRY
            self._registry = REGISTRY
    
    async def start(self) -> None:
        """Start the async metrics server."""
        if self.server is not None:
            raise RuntimeError("Server is already running")
        
        # Start the HTTP server with connection limit
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=self.max_connections
        )
        
        # Start event loop lag monitoring
        self._lag_monitor_task = asyncio.create_task(self._monitor_event_loop_lag())
        
        logger.info(f"Async metrics server started on http://{self.host}:{self.port}/metrics (max_connections={self.max_connections})")
        print(f"📊 Async metrics server started on http://{self.host}:{self.port}/metrics")
    
    async def stop(self) -> None:
        """Stop the async metrics server gracefully."""
        if self.server is None:
            return
        
        # Stop lag monitoring
        if self._lag_monitor_task:
            self._lag_monitor_task.cancel()
            try:
                await self._lag_monitor_task
            except asyncio.CancelledError:
                pass
            self._lag_monitor_task = None
        
        # Close the server
        self.server.close()
        await self.server.wait_closed()
        self.server = None
        logger.info("Async metrics server stopped")
        print("📊 Async metrics server stopped")
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle incoming HTTP requests with improved parsing and error handling."""
        try:
            # Read HTTP headers with size limit
            request_data = b""
            while b"\r\n\r\n" not in request_data:
                chunk = await reader.read(1024)
                if not chunk:
                    await self._send_response(writer, 400, "Bad Request", b"Incomplete request")
                    return
                
                request_data += chunk
                if len(request_data) > MAX_HEADER_SIZE:
                    await self._send_response(writer, 400, "Bad Request", b"Request headers too large")
                    return
            
            # Parse HTTP request line
            try:
                request_str = request_data.decode('utf-8', errors='ignore')
                request_line = request_str.split('\r\n', 1)[0]
                parts = request_line.split()
                
                if len(parts) < 2:
                    await self._send_response(writer, 400, "Bad Request", b"Invalid request line")
                    return
                
                method, path = parts[0], parts[1]
                
            except (UnicodeDecodeError, IndexError):
                await self._send_response(writer, 400, "Bad Request", b"Invalid request format")
                return
            
            # Check HTTP method
            if method not in ("GET", "HEAD"):
                await self._send_response(writer, 405, "Method Not Allowed", b"Method not allowed")
                return
            
            # Check exact path match
            if path != "/metrics":
                await self._send_response(writer, 404, "Not Found", b"Not found - try /metrics")
                return
            
            # Generate metrics data
            try:
                metrics_data = generate_latest(self._registry)
            except Exception as e:
                logger.error(f"Failed to generate metrics: {e}")
                await self._send_response(writer, 500, "Internal Server Error", b"Failed to generate metrics")
                return
            
            # Send successful response
            await self._send_response(
                writer, 
                200, 
                "OK", 
                metrics_data,
                content_type=CONTENT_TYPE_LATEST
            )
            
        except Exception as e:
            # Log detailed error but return generic message
            logger.error(f"Unexpected error handling request: {e}", exc_info=True)
            try:
                await self._send_response(writer, 500, "Internal Server Error", b"Internal server error")
            except Exception:
                # If we can't even send error response, just log and continue
                logger.error("Failed to send error response", exc_info=True)
        
        finally:
            try:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing connection: {e}")
    
    async def _send_response(
        self, 
        writer: asyncio.StreamWriter, 
        status_code: int, 
        status_text: str, 
        body: bytes,
        content_type: str = "text/plain"
    ) -> None:
        """Send HTTP response."""
        response = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"Server: MarketPipe-AsyncMetrics/1.0\r\n"
            "\r\n"
        ).encode('utf-8') + body
        
        try:
            writer.write(response)
            await writer.drain()
        except Exception as e:
            logger.debug(f"Error sending response: {e}")
    
    async def _monitor_event_loop_lag(self) -> None:
        """Monitor event loop scheduling delays."""
        while True:
            try:
                # Schedule a callback and measure how long it takes to execute
                future = asyncio.Future()
                expected_time = time.monotonic()
                
                def callback():
                    actual_time = time.monotonic()
                    schedule_lag = actual_time - expected_time
                    EVENT_LOOP_LAG.set(schedule_lag)
                    future.set_result(schedule_lag)
                
                asyncio.get_event_loop().call_soon(callback)
                await future
                
                # Wait 1 second before next measurement
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Event loop lag monitoring error: {e}")
                await asyncio.sleep(1.0)

    @asynccontextmanager
    async def run_context(self):
        """Context manager for running the server."""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()


# Global server instance for CLI integration
_async_server_instance: Optional[AsyncMetricsServer] = None


async def start_async_server(port: int = 8000, host: str = "0.0.0.0") -> AsyncMetricsServer:
    """Start the global async metrics server."""
    global _async_server_instance
    
    if _async_server_instance is not None:
        raise RuntimeError("Async metrics server is already running")
    
    # Use METRICS_PORT env var if available
    port = int(os.getenv("METRICS_PORT", port))
    
    _async_server_instance = AsyncMetricsServer(port=port, host=host)
    await _async_server_instance.start()
    return _async_server_instance


async def stop_async_server() -> None:
    """Stop the global async metrics server."""
    global _async_server_instance
    
    if _async_server_instance is not None:
        await _async_server_instance.stop()
        _async_server_instance = None


def metrics_app(environ, start_response):
    """WSGI app for multiprocess metrics (legacy compatibility)."""
    registry = CollectorRegistry()
    MultiProcessCollector(registry)
    data = generate_latest(registry)
    status = "200 OK"
    response_headers = [
        ("Content-type", CONTENT_TYPE_LATEST),
        ("Content-Length", str(len(data))),
    ]
    start_response(status, response_headers)
    return [data]


def run(port: int = 8000, legacy: bool = False) -> None:
    """Run the metrics server.
    
    Args:
        port: Port to bind to
        legacy: Use legacy WSGI mode instead of async mode
    """
    # Set up multiprocess directory
    multiproc_dir = setup_multiprocess_metrics_dir()
    logger.info(f"Using multiprocess metrics directory: {multiproc_dir}")
    
    if legacy:
        # Legacy multiprocess mode
        logger.info(f"Prometheus metrics server (legacy mode) serving on port {port}")
        print(f"Prometheus metrics server (legacy mode) serving on port {port}")
        
        from wsgiref.simple_server import make_server
        server = make_server("", port, metrics_app)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Prometheus metrics server stopped")
            print("Prometheus metrics server stopped")
    else:
        # Modern async mode
        logger.info(f"Prometheus metrics server (async mode) serving on port {port}")
        print(f"Prometheus metrics server (async mode) serving on port {port}")
        
        async def run_async():
            server = AsyncMetricsServer(port=port)
            async with server.run_context():
                try:
                    # Keep the server running
                    while True:
                        await asyncio.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Prometheus metrics server stopping...")
                    print("Prometheus metrics server stopping...")
        
        try:
            asyncio.run(run_async())
        except KeyboardInterrupt:
            logger.info("Prometheus metrics server stopped")
            print("Prometheus metrics server stopped")


def setup_multiprocess_metrics_dir() -> str:
    """Set up Prometheus multiprocess directory with proper default location.
    
    Returns:
        str: Path to the multiprocess directory
    """
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        # Use existing environment variable
        multiproc_dir = os.environ["PROMETHEUS_MULTIPROC_DIR"]
    else:
        # Set up default location in data/metrics/multiprocess
        multiproc_dir = Path("data/metrics/multiprocess")
        multiproc_dir.mkdir(parents=True, exist_ok=True)
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(multiproc_dir)
        
    return str(multiproc_dir)
