#!/bin/bash
# Bounded Context Isolation Check for MarketPipe DDD Architecture

set -e

echo "🏗️ Checking bounded context isolation..."

# Define source directory
src_dir="src/marketpipe"

# Exit codes
violations_found=0

# Function to check for infrastructure dependencies in domain layer
check_domain_purity() {
    echo "Checking domain layer purity..."
    
    # List of forbidden infrastructure imports in domain layer
    forbidden_imports=(
        "import sqlite3"
        "import psycopg2"
        "import requests"
        "import httpx"
        "import sqlalchemy"
        "from sqlite3"
        "from psycopg2"
        "from requests"
        "from httpx" 
        "from sqlalchemy"
    )
    
    # Check if domain directory exists
    if [ ! -d "$src_dir/domain" ]; then
        echo "ℹ️ Domain directory not found, skipping domain purity checks"
        return 0
    fi
    
    for import_stmt in "${forbidden_imports[@]}"; do
        if grep -r --include="*.py" -q "$import_stmt" "$src_dir/domain/"; then
            echo "❌ Found forbidden infrastructure import in domain layer: $import_stmt"
            grep -r --include="*.py" -n "$import_stmt" "$src_dir/domain/"
            violations_found=$((violations_found + 1))
        fi
    done
    
    if [ $violations_found -eq 0 ]; then
        echo "✅ Domain layer purity check passed"
    fi
}

# Function to check cross-context domain imports
check_cross_context_imports() {
    echo "Checking cross-context domain imports..."
    
    # Find all context directories (excluding domain and special directories)
    contexts=()
    if [ -d "$src_dir" ]; then
        for dir in "$src_dir"/*; do
            if [ -d "$dir" ]; then
                context_name=$(basename "$dir")
                if [[ "$context_name" != "domain" && "$context_name" != "__pycache__" && ! "$context_name" =~ ^__.+__$ ]]; then
                    contexts+=("$context_name")
                fi
            fi
        done
    fi
    
    if [ ${#contexts[@]} -eq 0 ]; then
        echo "ℹ️ No bounded contexts found, skipping cross-context checks"
        return 0
    fi
    
    echo "Found contexts: ${contexts[*]}"
    
    # Check each context for imports from other contexts' domain models
    for context in "${contexts[@]}"; do
        context_dir="$src_dir/$context"
        
        if [ ! -d "$context_dir" ]; then
            continue
        fi
        
        echo "Checking context: $context"
        
        # Look for imports from other contexts' domain layers
        for other_context in "${contexts[@]}"; do
            if [ "$context" != "$other_context" ]; then
                # Pattern to match imports from other context domain layers
                forbidden_pattern="from marketpipe\.$other_context\.domain"
                
                if grep -r --include="*.py" -q "$forbidden_pattern" "$context_dir/"; then
                    echo "❌ Context '$context' imports domain models from '$other_context' context"
                    grep -r --include="*.py" -n "$forbidden_pattern" "$context_dir/"
                    violations_found=$((violations_found + 1))
                fi
            fi
        done
    done
    
    if [ $violations_found -eq 0 ]; then
        echo "✅ Cross-context isolation check passed"
    fi
}

# Function to check for proper interface usage between contexts
check_interface_usage() {
    echo "Checking interface usage between contexts..."
    
    # Look for direct concrete class imports across contexts
    direct_import_patterns=(
        "from marketpipe\..*\..*Service import"
        "from marketpipe\..*\..*Repository import" 
        "from marketpipe\..*\..*Client import"
    )
    
    violation_found_in_interfaces=false
    
    for pattern in "${direct_import_patterns[@]}"; do
        if grep -r --include="*.py" -E "$pattern" "$src_dir/"; then
            echo "⚠️ Found potential direct concrete class import (consider using interfaces):"
            grep -r --include="*.py" -nE "$pattern" "$src_dir/"
            violation_found_in_interfaces=true
        fi
    done
    
    if [ "$violation_found_in_interfaces" = false ]; then
        echo "✅ Interface usage check passed"
    else
        echo "💡 Suggestion: Use abstract interfaces (I* prefix) for cross-context dependencies"
    fi
}

# Function to check for shared kernel violations
check_shared_kernel() {
    echo "Checking shared kernel boundaries..."
    
    # Shared kernel should only contain generic domain concepts
    shared_kernel_dir="$src_dir/domain"
    
    if [ ! -d "$shared_kernel_dir" ]; then
        echo "ℹ️ Shared kernel (domain) directory not found"
        return 0
    fi
    
    # Look for context-specific logic in shared kernel
    context_specific_patterns=(
        "alpaca"
        "ingestion_job"
        "validation_rule"
        "storage_partition"
    )
    
    shared_kernel_violations=false
    
    for pattern in "${context_specific_patterns[@]}"; do
        if grep -r --include="*.py" -i "$pattern" "$shared_kernel_dir/"; then
            echo "⚠️ Found context-specific concept in shared kernel: $pattern"
            grep -r --include="*.py" -in "$pattern" "$shared_kernel_dir/"
            shared_kernel_violations=true
        fi
    done
    
    if [ "$shared_kernel_violations" = false ]; then
        echo "✅ Shared kernel boundaries check passed"
    else
        echo "💡 Suggestion: Move context-specific concepts to appropriate bounded contexts"
    fi
}

# Function to check for proper event handling isolation
check_event_isolation() {
    echo "Checking domain event isolation..."
    
    # Events should be defined in the context where they originate
    event_pattern="Event\|DomainEvent"
    
    event_isolation_violations=false
    
    # Check that contexts don't subscribe to events from other contexts directly
    for context_dir in "$src_dir"/*; do
        if [ -d "$context_dir" ]; then
            context_name=$(basename "$context_dir")
            
            if [[ "$context_name" != "domain" && "$context_name" != "__pycache__" ]]; then
                # Look for event imports from other contexts
                other_context_event_pattern="from marketpipe\..*\..*Event import"
                
                if grep -r --include="*.py" -E "$other_context_event_pattern" "$context_dir/"; then
                    echo "⚠️ Context '$context_name' directly imports events from other contexts:"
                    grep -r --include="*.py" -nE "$other_context_event_pattern" "$context_dir/"
                    event_isolation_violations=true
                fi
            fi
        fi
    done
    
    if [ "$event_isolation_violations" = false ]; then
        echo "✅ Domain event isolation check passed"
    else
        echo "💡 Suggestion: Use event buses or message queues for cross-context communication"
    fi
}

# Run all checks
echo "===================="
echo "BOUNDED CONTEXT ISOLATION CHECKS"
echo "===================="

check_domain_purity
echo ""

check_cross_context_imports
echo ""

check_interface_usage
echo ""

check_shared_kernel
echo ""

check_event_isolation
echo ""

# Summary
echo "===================="
echo "ISOLATION COMPLIANCE SUMMARY"
echo "===================="

if [ $violations_found -eq 0 ]; then
    echo "✅ All bounded context isolation checks passed!"
    echo "Your DDD architecture maintains proper context boundaries."
else
    echo "❌ Found $violations_found isolation violations"
    echo ""
    echo "Violation fixes required:"
    echo "1. Remove infrastructure imports from domain layer"
    echo "2. Avoid direct imports between context domain models"
    echo "3. Use interfaces for cross-context dependencies"
    echo "4. Keep shared kernel generic and context-agnostic"
    echo "5. Use proper event patterns for cross-context communication"
    echo ""
    echo "For guidance, see:"
    echo "- CLAUDE.md (Domain-Driven Design section)"
    echo "- .cursor/rules/ddd/bounded_contexts.mdc"
    
    exit 1
fi

echo ""
echo "Context isolation guidelines:"
echo "- Domain layer: Pure business logic, no infrastructure"
echo "- Context boundaries: No direct domain model sharing"
echo "- Integration: Use interfaces and events for communication"
echo "- Shared kernel: Only generic domain concepts"