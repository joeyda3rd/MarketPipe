#!/bin/bash
"""
Pre-commit hook to check if roadmap should be updated

This script runs before commits and:
1. Detects if code changes might affect TODO.md
2. Runs a quick roadmap analysis 
3. Warns about potential roadmap updates needed

Install with: ln -s ../../scripts/pre-commit-roadmap-check.sh .git/hooks/pre-commit
"""

set -e

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 Checking if roadmap needs updates...${NC}"

# Get staged files
STAGED_FILES=$(git diff --cached --name-only)

# Check if any important files are being modified
SIGNIFICANT_CHANGES=false

if echo "$STAGED_FILES" | grep -qE "(src/.*\.py|tests/.*\.py|pyproject\.toml)"; then
    SIGNIFICANT_CHANGES=true
fi

if [ "$SIGNIFICANT_CHANGES" = false ]; then
    echo -e "${GREEN}✅ No significant changes detected${NC}"
    exit 0
fi

# Run roadmap analysis (dry-run)
if python scripts/update_roadmap.py --dry-run > /tmp/roadmap_check.txt 2>&1; then
    
    # Check if any tasks would be marked complete
    if grep -q "Would update TODO.md" /tmp/roadmap_check.txt; then
        echo -e "${YELLOW}⚠️  Your changes may affect the project roadmap!${NC}"
        echo ""
        cat /tmp/roadmap_check.txt
        echo ""
        echo -e "${BLUE}💡 Consider running: python scripts/update_roadmap.py${NC}"
        echo -e "${BLUE}   to update TODO.md with your progress${NC}"
        echo ""
        
        # Ask if user wants to continue
        read -p "Continue with commit? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${RED}❌ Commit aborted. Update roadmap first if needed.${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✅ Roadmap appears up-to-date${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Could not run roadmap analysis (missing dependencies?)${NC}"
    echo -e "${BLUE}💡 You may want to run: python scripts/update_roadmap.py${NC}"
fi

# Check for new TODO/FIXME items
NEW_TODOS=$(git diff --cached | grep -E "^\+.*TODO|^\+.*FIXME" | wc -l)
if [ "$NEW_TODOS" -gt 0 ]; then
    echo -e "${YELLOW}📝 Detected $NEW_TODOS new TODO/FIXME items in this commit${NC}"
    echo -e "${BLUE}💡 These will be picked up in the next roadmap update${NC}"
fi

echo -e "${GREEN}✅ Pre-commit roadmap check complete${NC}"
exit 0 