# 🤖 Roadmap Automation System

The MarketPipe project uses an automated system to keep `TODO.md` synchronized with the actual codebase state. This ensures the roadmap is always accurate and actionable.

## 🎯 How It Works

The automation system analyzes the codebase and automatically:

✅ **Updates task completion status** based on code analysis  
✅ **Refreshes test coverage statistics** from actual test runs  
✅ **Discovers new TODO/FIXME items** as they're added to code  
✅ **Identifies production blockers** like NotImplementedError  
✅ **Tracks dependency completion** across bounded contexts  

## 🔧 Components

### 1. Core Analysis Script
**`scripts/update_roadmap.py`** - The main analysis engine

```bash
# Manual updates
python scripts/update_roadmap.py --verbose

# Preview changes without updating
python scripts/update_roadmap.py --dry-run
```

**What it analyzes:**
- Repository interface implementations (pass statements)
- Domain event publishers/subscribers
- Test coverage per bounded context
- CLI command wiring completion
- Infrastructure component status
- TODO/FIXME comments in code

### 2. GitHub Actions Workflow
**`.github/workflows/update-roadmap.yml`** - Automatic updates on changes

**Triggers:**
- Push to `main` branch with `src/` or `tests/` changes
- Pull requests affecting code
- Manual workflow dispatch

**Actions:**
- Runs roadmap analysis
- Auto-commits updated `TODO.md` on main branch
- Comments on PRs with roadmap impact summary
- Uploads roadmap artifacts

### 3. Pre-commit Hook
**`scripts/pre-commit-roadmap-check.sh`** - Local development guard

**Install:** `make install-hooks`

**Functions:**
- Warns before commits that might affect roadmap
- Detects new TODO/FIXME items
- Prompts to update roadmap if needed
- Prevents commits of stale roadmap status

### 4. Make Targets
**`Makefile`** - Convenient commands

```bash
make update-roadmap    # Update TODO.md 
make check-roadmap     # Preview roadmap status
make install-hooks     # Setup pre-commit hook
make ci-check          # Run all checks including roadmap
```

## 📊 Task Detection Logic

### ✅ Completion Criteria

| Task | Detection Method |
|------|------------------|
| **Delete duplicate events.py** | File existence check: `events.py` at root |
| **Implement repositories** | Count of `pass` statements in `domain/repositories.py` |
| **Complete aggregates** | Presence of business methods like `calculate_daily_summary` |
| **Wire CLI commands** | Function definitions with `@app.command()` decorator |
| **Test coverage ≥70%** | Actual pytest coverage report parsing |
| **Remove placeholders** | Count of `NotImplementedError` in codebase |

### 📈 Coverage Tracking

The system tracks test coverage per bounded context:

```python
# Example coverage calculation
def _calculate_context_coverage(coverage_data, context):
    context_files = [f for f in files if f"/{context}/" in f]
    total_statements = sum(files[f]["summary"]["num_statements"] 
                          for f in context_files)
    covered_statements = sum(files[f]["summary"]["covered_lines"] 
                            for f in context_files)
    return (covered_statements / total_statements * 100)
```

## 🚀 Development Workflow

### For Contributors

1. **Work normally** - Write code, tests, fix issues
2. **Pre-commit check** - Hook warns if roadmap might be affected
3. **Manual update** (optional) - `make update-roadmap` to see progress
4. **Push changes** - GitHub Action automatically updates roadmap
5. **Review impact** - PR comments show roadmap changes

### For Maintainers  

1. **Monitor progress** - Roadmap automatically tracks completion
2. **Review blockers** - Critical tasks with 🔴 priority are highlighted
3. **Update priorities** - Manually adjust task priorities in `TODO.md`
4. **Add new tasks** - System detects new TODO/FIXME and suggests tasks

## 🎛️ Configuration

### Customizing Task Detection

Edit `scripts/update_roadmap.py` to modify detection logic:

```python
class RoadmapUpdater:
    def _analyze_core_domain(self):
        # Add custom completion criteria
        tasks["my_custom_task"] = TaskStatus(
            task_id="my_custom_task",
            description="My custom implementation",
            completed=self._check_custom_completion()
        )
```

### Adding New Contexts

To track a new bounded context:

1. Add analysis method: `_analyze_my_context()`
2. Update coverage tracking: Add context to `CoverageStats`
3. Update `TODO.md` with new section
4. Add context to GitHub Action workflow

### Coverage Thresholds

Modify coverage targets in the script:

```python
# Current thresholds
COVERAGE_EXCELLENT = 70.0  # ✅ Green
COVERAGE_GOOD = 50.0       # ⚠️ Yellow  
COVERAGE_POOR = 30.0       # ❌ Red
```

## 🔍 Troubleshooting

### Roadmap Not Updating

1. **Check script dependencies:**
   ```bash
   pip install pytest pytest-cov
   ```

2. **Test script manually:**
   ```bash
   python scripts/update_roadmap.py --verbose
   ```

3. **Check GitHub Action logs** in repository Actions tab

### False Positive Detections

1. **Task marked complete incorrectly** - Update detection logic in `_analyze_*` methods
2. **Missing new TODOs** - Check file permissions and gitignore patterns
3. **Coverage calculation wrong** - Verify pytest-cov configuration

### Hook Not Working

1. **Reinstall hook:**
   ```bash
   make install-hooks
   ```

2. **Check permissions:**
   ```bash
   chmod +x scripts/pre-commit-roadmap-check.sh
   ```

## 🎉 Benefits

### For Development Teams
- ✅ **Always current roadmap** - No more stale TODO lists
- ✅ **Automatic progress tracking** - See completion in real-time  
- ✅ **Early issue detection** - New TODOs surface immediately
- ✅ **Coverage accountability** - Context-specific coverage targets

### For Project Management
- ✅ **Accurate status reporting** - Roadmap reflects actual code state
- ✅ **Dependency tracking** - See what blocks what automatically
- ✅ **Quality metrics** - Built-in coverage and blocker detection
- ✅ **Historical tracking** - Git history shows progress over time

---

**💡 The roadmap automation system ensures `TODO.md` is a living document that evolves with your codebase, not a static wishlist that becomes outdated.** 