# How to Never Miss Tests - MarketPipe

## 🎯 The Problem

**How do we ensure that when we make code changes, the appropriate tests aren't missed accidentally?**

This is a critical question for maintaining code quality while preserving development speed.

## ✅ The Solution: Multi-Layered Test Protection

MarketPipe implements a **4-layer defense system** to automatically ensure the right tests are run:

### Layer 1: Smart Test Detection 🧠
**Automatically identifies relevant tests based on changed files**

```bash
# Run tests for your recent changes
make test-smart

# See what tests it would run (dry run)
make test-smart-all

# Get the exact command to run
make test-smart-cmd
```

### Layer 2: Pre-Commit Hooks 🚫
**Prevents commits with failing tests**

```bash
# Normal commit - tests run automatically
git commit -m "fix: update validation logic"

# For urgent fixes, you can skip
SKIP_TESTS=1 git commit -m "hotfix: critical production issue"
```

### Layer 3: Coverage Verification 📊
**Ensures changed code is properly tested**

- Tracks test coverage for modified areas
- Warns if new code lacks tests
- Suggests additional tests for complex changes

### Layer 4: Safety Nets 🛟
**Multiple fallbacks to catch missed tests**

- Unmatched changes → Run unit tests as safety net
- Critical files → Trigger full test suite
- Config changes → Run integration tests
- Many changes → Force complete test run

## 🚀 Quick Setup (One Time)

```bash
# Install the complete test automation system
make setup-test-automation

# Verify it's working
make test-smart-all
```

## 💡 Daily Development Workflow

### The New Workflow (Automated)
```bash
# 1. Make your changes
vim src/marketpipe/ingestion/coordinator.py

# 2. Stage changes
git add .

# 3. Commit (tests run automatically!)
git commit -m "feat: improve error handling"

# 4. Before pushing, run full suite once
make test-all
```

### Manual Alternative (If hooks disabled)
```bash
# After making changes, run smart tests
make test-smart

# For complex changes, check what it would run
make test-smart-all

# Before committing, ensure tests pass
make test-all
```

## 📋 Command Cheat Sheet

### Smart Commands (New!)
| Command | What It Does | When to Use |
|---------|-------------|-------------|
| `make test-smart` | Run tests for changed files | After making changes |
| `make test-smart-all` | Show what tests would run | Check test coverage |
| `make test-smart-cmd` | Get command to copy/paste | IDE integration |

### Existing Fast Commands
| Command | What It Does | When to Use |
|---------|-------------|-------------|
| `make test` | Fast unit tests | During development |
| `make test-fast` | No cache, fail fast | Clean slate testing |
| `make test-unit` | Unit tests only | Quick validation |

### Comprehensive Commands
| Command | What It Does | When to Use |
|---------|-------------|-------------|
| `make test-all` | Complete test suite | Before pushing |
| `make test-integration` | Integration tests | Infrastructure changes |
| `make ci-check` | Full CI simulation | Major changes |

## 🎯 How It Maps Changes to Tests

The system uses intelligent pattern matching:

| You Change | It Runs |
|-----------|---------|
| `src/marketpipe/cli.py` | CLI unit tests + CLI integration tests |
| `src/marketpipe/domain/` | Domain tests + DDD guard rails |
| `src/marketpipe/infrastructure/` | Infrastructure + integration tests |
| `src/marketpipe/*_adapter.py` | Provider-specific tests |
| `alembic/versions/` | Migration tests + database tests |
| `pytest.ini` | **Full test suite** (critical file) |
| Test files | **The changed test files** |

**Safety Net:** If your changes don't match any pattern, it runs unit tests to be safe.

## ⚠️ Override Options

### Skip Tests Temporarily
```bash
# Skip pre-commit tests for this commit
SKIP_TESTS=1 git commit -m "wip: work in progress"

# Skip all git hooks
git commit --no-verify -m "urgent hotfix"
```

### Debug What Would Run
```bash
# See detailed mapping
make test-smart-all

# Check git status
git status
git diff --name-only

# Force specific tests
pytest tests/specific_test.py
```

### Emergency Recovery
```bash
# If something's broken, force full suite
make test-all

# Disable hooks temporarily
git config core.hooksPath /dev/null

# Re-enable hooks
git config core.hooksPath .githooks
```

## 🔧 Customizing the System

### Add New File Patterns
Edit `scripts/smart_test_runner.py`:

```python
self.test_mappings = {
    # Add your pattern
    r"src/marketpipe/my_new_module/": ["tests/unit/my_new_module/"],
    
    # Existing patterns...
}
```

### Modify Safety Nets
```python
# Add critical files that trigger full suite
self.critical_files = {
    "src/marketpipe/__init__.py",
    "src/marketpipe/cli.py",
    "src/marketpipe/my_critical_file.py",  # Add yours here
}
```

## 📊 Success Metrics

**You'll know it's working when:**
- ✅ Tests run automatically when you commit
- ✅ The right tests run for your changes (not too many, not too few)
- ✅ Development feels faster, not slower
- ✅ Fewer bugs escape to production
- ✅ You stop thinking about which tests to run

## 🐛 Troubleshooting

### "No tests detected for my changes"
```bash
# Check what files changed
git diff --name-only

# See pattern mapping
make test-smart-all

# Fallback to unit tests
make test-unit
```

### "Wrong tests are running"
```bash
# Debug the pattern matching
python3 scripts/smart_test_runner.py --show-mapping

# Run specific tests manually
pytest tests/specific/test_file.py

# Update patterns in scripts/smart_test_runner.py
```

### "Tests are too slow"
```bash
# Use faster alternatives during development
make test          # Fastest for active development
make test-unit     # Unit tests only
make test-fast     # No cache, fail fast

# Check test performance
make test-timing
```

## 🎓 Best Practices

### For Individual Developers
1. **Trust the automation** - let it run the tests it suggests
2. **Use `make test-smart`** after making changes
3. **Always run `make test-all`** before pushing to main
4. **Don't bypass tests** unless absolutely necessary
5. **Add new patterns** when creating new modules

### For Teams
1. **Monitor automation effectiveness** - are the right tests being run?
2. **Update file patterns** as the codebase evolves
3. **Review bypass usage** - should be rare
4. **Train team members** on the new workflow
5. **Celebrate fewer missed regressions** 🎉

## 💪 Advanced Usage

### IDE Integration
Configure your IDE to use the smart test runner:

```bash
# Get command for IDE
make test-smart-cmd

# Use in VS Code/PyCharm run configurations
python3 scripts/smart_test_runner.py --format command
```

### CI/CD Integration
```yaml
# In your CI pipeline
- name: Run Smart Tests
  run: make test-smart

- name: Full Suite for Critical Changes
  run: |
    if python3 scripts/smart_test_runner.py --format json | grep -q "FULL_SUITE"; then
      make test-all
    fi
```

---

## 🎯 Bottom Line

**The goal is simple:** Make it impossible to accidentally miss running the right tests.

With this system:
- ✅ Tests run automatically when you commit
- ✅ Only relevant tests run (fast feedback)
- ✅ Safety nets catch edge cases
- ✅ You can still override when needed
- ✅ Development stays fast while being safe

**Setup once, never think about it again.** 🚀

---

*Want more details? See [docs/TEST_AUTOMATION_STRATEGY.md](TEST_AUTOMATION_STRATEGY.md) for the complete technical documentation.* 