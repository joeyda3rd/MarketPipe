#!/usr/bin/env python3
"""
Personal workspace setup for MarketPipe development.

This sets up optional development tools that aren't required for contributing.
"""

import os
import subprocess
import sys
from pathlib import Path

def setup_git_hooks():
    """Set up optional git hooks."""
    print("🔗 Setting up optional git hooks...")
    
    # Create hooks directory if it doesn't exist
    hooks_dir = Path(".git/hooks")
    hooks_dir.mkdir(exist_ok=True)
    
    # Pre-commit hook: fast tests only
    pre_commit_hook = hooks_dir / "pre-commit"
    pre_commit_content = '''#!/bin/bash
# Optional pre-commit hook - runs fast tests only
# If this is too annoying, just delete this file

echo "🧪 Running fast tests before commit..."

# Run smart test runner
python .workspace/test-runner/smart_test.py

exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo ""
    echo "❌ Tests failed. Commit blocked."
    echo "💡 To skip: git commit --no-verify"
    echo "💡 To disable: rm .git/hooks/pre-commit"
    exit 1
fi

echo "✅ Tests passed. Proceeding with commit."
'''
    
    with open(pre_commit_hook, 'w') as f:
        f.write(pre_commit_content)
    
    os.chmod(pre_commit_hook, 0o755)
    
    print("   ✅ Pre-commit hook installed (runs fast tests)")
    print("   💡 To disable: rm .git/hooks/pre-commit")
    print("   💡 To skip once: git commit --no-verify")

def create_aliases():
    """Create convenience aliases/scripts."""
    print("📝 Creating convenience aliases...")
    
    # Create a simple test alias
    test_script = Path(".workspace/test")
    test_script_content = '''#!/bin/bash
# Quick test alias for personal use
python .workspace/test-runner/smart_test.py "$@"
'''
    
    with open(test_script, 'w') as f:
        f.write(test_script_content)
    
    os.chmod(test_script, 0o755)
    
    print("   ✅ Created .workspace/test (quick test alias)")
    print("   💡 Usage: .workspace/test [--dry-run] [--all]")

def print_usage():
    """Print usage instructions."""
    print("""
🎉 Personal workspace setup complete!

## Quick Commands:
  .workspace/test                    # Smart test runner
  .workspace/test --dry-run          # See what would run
  .workspace/test --all              # Run full test suite
  
## Development Tools:
  .workspace/dev-tools/              # Advanced development scripts
  .workspace/test-runner/            # Smart test system
  
## Git Hooks:
  - Pre-commit hook runs fast tests automatically
  - Skip with: git commit --no-verify  
  - Disable with: rm .git/hooks/pre-commit

## For Contributors:
  Standard commands still work for everyone:
  - make test        # Standard pytest with optimizations
  - make test-all    # Full test suite
  - pytest          # Direct pytest
  
The workspace tools are just for your personal productivity!
""")

def main():
    """Set up the personal workspace."""
    print("🚀 Setting up personal MarketPipe workspace...")
    print()
    
    # Change to repo root
    repo_root = Path(__file__).parent.parent
    os.chdir(repo_root)
    
    try:
        setup_git_hooks()
        print()
        create_aliases() 
        print()
        print_usage()
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 