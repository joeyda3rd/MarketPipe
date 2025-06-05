import subprocess


def test_help():
    result = subprocess.run([
        'python', '-m', 'marketpipe.cli', '--help'
    ], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'MarketPipe ETL commands' in result.stdout
