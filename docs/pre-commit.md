# Pre-commit Framework

MarketPipe uses [pre-commit](https://pre-commit.com/) to maintain code quality and run automated checks before each commit.

## Installation

```bash
# Install pre-commit
pip install pre-commit

# Install the git hook scripts
pre-commit install

# Optional: Install pre-push hooks too
pre-commit install --hook-type pre-push
```

## What Gets Checked

The pre-commit hooks run the following checks:

### Code Quality
- **Black** - Code formatting
- **isort** - Import sorting
- **Ruff** - Fast linting and code style
- **MyPy** - Type checking (src/ only)

### Security & Safety
- **Bandit** - Security vulnerability scanning
- **Check AST** - Python syntax validation
- **Debug statements** - Prevent debug statements in commits

### File Quality
- **Trailing whitespace** - Remove trailing spaces
- **End of file fixer** - Ensure files end with newline
- **Large files** - Prevent accidentally committing large files
- **Merge conflicts** - Detect unresolved merge conflicts

### Project-Specific
- **Fast tests** - Run ultra-fast test suite (~2s)
- **Config validation** - Validate YAML/JSON configuration files
- **No placeholders** - Ensure no placeholder values in configs

## Running Pre-commit

### Automatic (Recommended)
Pre-commit runs automatically on `git commit`:
```bash
git add .
git commit -m "Your commit message"
# Pre-commit hooks run automatically
```

### Manual Execution
```bash
# Run all hooks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run black

# Run only on staged files
pre-commit run

# Skip hooks for emergency commits
git commit --no-verify -m "Emergency fix"
```

## Performance

The pre-commit suite is optimized for speed:
- **Total runtime**: ~10-15 seconds on full codebase
- **Incremental**: ~2-5 seconds on changed files only
- **Fast tests**: ~2 seconds (only tests marked as 'fast')
- **Parallel execution**: Multiple hooks run in parallel

## Skipping Hooks

### Temporarily Skip All Hooks
```bash
git commit --no-verify -m "Skip hooks for this commit"
```

### Skip Specific Hooks
```bash
SKIP=mypy,bandit git commit -m "Skip mypy and bandit"
```

### Skip Certain Files
Add to `.pre-commit-config.yaml`:
```yaml
- id: mypy
  exclude: ^(tests/legacy/|scripts/old/)
```

## Configuration

The configuration is in `.pre-commit-config.yaml`. Key sections:

### Hook Configuration
```yaml
- repo: https://github.com/psf/black
  rev: 24.2.0
  hooks:
    - id: black
      args: [--line-length=100]
```

### Global Settings
```yaml
default_stages: [commit]  # When to run hooks
exclude: |                # Files to always skip
  (?x)^(
    \.git/.*|
    __pycache__/.*|
    \.pytest_cache/.*
  )$
```

## Troubleshooting

### Hook Installation Issues
```bash
# Reinstall hooks
pre-commit uninstall
pre-commit install

# Update hook repositories
pre-commit autoupdate
```

### Performance Issues
```bash
# Clean hook cache
pre-commit clean

# Run with timing information
pre-commit run --all-files --verbose
```

### Hook Failures
```bash
# See what failed
git status

# Fix formatting issues
pre-commit run black --all-files
pre-commit run isort --all-files

# Run tests to see specific failures
scripts/test-fast
```

## Integration with IDEs

### VS Code
Add to `.vscode/settings.json`:
```json
{
    "python.formatting.provider": "black",
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "editor.formatOnSave": true
}
```

### PyCharm
1. Install Black plugin
2. Configure Black as formatter
3. Enable "Format on save"

## CI/CD Integration

Pre-commit hooks are also run in GitHub Actions:
```yaml
- name: Run pre-commit
  uses: pre-commit/action@v3.0.0
```

This ensures the same checks run locally and in CI.

## Best Practices

1. **Install early**: Set up pre-commit right after cloning
2. **Run locally**: Don't rely only on CI - catch issues early
3. **Keep updated**: Run `pre-commit autoupdate` periodically
4. **Consistent team**: Ensure all team members use pre-commit
5. **Fast feedback**: Use `scripts/test-fast` during development

## Customization

### Adding New Hooks
Edit `.pre-commit-config.yaml`:
```yaml
- repo: https://github.com/your-repo/your-hook
  rev: v1.0.0
  hooks:
    - id: your-hook-id
      args: [--your-arg]
```

### Project-Specific Checks
Add local hooks:
```yaml
- repo: local
  hooks:
    - id: custom-check
      name: Custom Check
      entry: python scripts/custom-check.py
      language: system
```

For more information, see the [official pre-commit documentation](https://pre-commit.com/).
