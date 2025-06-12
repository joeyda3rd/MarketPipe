from unittest.mock import patch, AsyncMock
from typer.testing import CliRunner
from marketpipe.cli import app

runner = CliRunner()

with patch.dict('os.environ', {'ALPACA_KEY': 'test-key', 'ALPACA_SECRET': 'test-secret'}):
    with patch('marketpipe.cli.ohlcv_ingest._build_ingestion_services') as mock_build:
        mock_job_service = AsyncMock()
        mock_coordinator_service = AsyncMock()
        
        # Mock create_job to return a job ID
        mock_job_service.create_job.return_value = 'job-123'
        
        # Mock execute_job to return a result dict
        mock_coordinator_service.execute_job.return_value = {
            'symbols_processed': 1,
            'total_bars': 100,
            'symbols_failed': 0,
            'processing_time_seconds': 5.0,
        }
        
        mock_build.return_value = (mock_job_service, mock_coordinator_service)
        
        result = runner.invoke(app, ['ingest-ohlcv', '--symbols', 'AAPL', '--start', '2025-01-01', '--end', '2025-01-02'])
        print('Exit code:', result.exit_code)
        print('Stdout:', repr(result.stdout))
        if result.exception:
            print('Exception:', result.exception)
            import traceback
            traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__) 