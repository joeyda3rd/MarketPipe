# Personal Workspace

This directory contains optional development tools for MarketPipe. **None of this is required for contributing.**

## Quick Setup

```bash
python .workspace/setup.py
```

This sets up:
- Smart test runner for fast feedback
- Optional pre-commit hooks
- Convenience aliases

## Tools

### Smart Test Runner
```bash
.workspace/test                    # Auto-detect relevant tests
.workspace/test --dry-run          # See what would run  
.workspace/test --all              # Run full test suite
```

### Development Scripts
```bash
.workspace/dev-tools/              # Advanced development tools (moved from scripts/)
```

## Philosophy

The main repo is kept clean and simple for contributors. This workspace is for personal productivity tools that you might want but others don't need.

**Contributors should use standard commands:**
- `pytest` - run tests
- `make test` - run tests with optimizations
- `make fmt` - format code

**You can use workspace tools for faster development cycles.**

## Git Hooks

The setup installs an optional pre-commit hook that runs fast tests. 

**If it's annoying:**
- Skip once: `git commit --no-verify`
- Disable: `rm .git/hooks/pre-commit`

## Testing the Testing System

```bash
python .workspace/test-runner/test_smart_test.py
```

This runs meta-tests to make sure the smart test runner works correctly. 