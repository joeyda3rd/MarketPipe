from typer.testing import CliRunner
from marketpipe.cli import symbols
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as tmp:
    db_path = Path(tmp) / 'wh.db'
    data_dir = Path(tmp) / 'symbols_master'
    
    runner = CliRunner()
    try:
        res = runner.invoke(
            symbols.app,
            [
                'update',
                '-p', 'dummy',
                '--db', str(db_path),
                '--data-dir', str(data_dir),
                '--execute'
            ],
        )
        
        print('Exit code:', res.exit_code)
        print('STDOUT:', res.stdout)
        print('STDERR:', res.stderr)
        if res.exception:
            print('Exception:')
            import traceback
            traceback.print_exception(type(res.exception), res.exception, res.exception.__traceback__)
    except Exception as e:
        print('Error calling CLI:', e)
        import traceback
        traceback.print_exc() 