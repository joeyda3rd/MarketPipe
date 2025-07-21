#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
CI validation script to ensure all environment variables defined in settings classes
exist as placeholders in .env.example file.

This script prevents configuration drift by ensuring that:
1. All required env vars in provider settings are documented in .env.example
2. No orphaned variables exist in .env.example that aren't used by any settings
3. Environment variable naming follows the MP_{PROVIDER}_{CREDENTIAL} convention

Usage:
    python scripts/check_env_placeholders.py

Exit codes:
    0: All checks passed
    1: Validation errors found
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Add src to path to import settings
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from marketpipe.settings.providers import PROVIDER_SETTINGS
except ImportError as e:
    print(f"❌ Failed to import provider settings: {e}")
    print("Make sure you're running from the project root and settings are properly installed")
    sys.exit(1)


def extract_env_vars_from_settings() -> dict[str, list[str]]:
    """Extract environment variables from all provider settings classes.

    Returns:
        Dictionary mapping provider names to their required environment variables
    """
    env_vars_by_provider = {}

    for provider_name, settings_class in PROVIDER_SETTINGS.items():
        env_vars = []

        # Extract environment variable names from Pydantic fields
        # Use model_fields for Pydantic v2, fall back to __fields__ for v1
        if hasattr(settings_class, "model_fields"):
            fields = settings_class.model_fields
        else:
            fields = getattr(settings_class, "__fields__", {})
        for field_name, field_info in fields.items():
            env_name = None

            # Check for alias (pydantic-settings style)
            if hasattr(field_info, "alias") and field_info.alias:
                env_name = field_info.alias

            # Pydantic v2 style - check json_schema_extra
            elif hasattr(field_info, "json_schema_extra") and field_info.json_schema_extra:
                env_name = field_info.json_schema_extra.get(
                    "env"
                ) or field_info.json_schema_extra.get("alias")

            # Pydantic v1 style fallback
            elif hasattr(field_info, "field_info"):
                env_name = getattr(field_info.field_info, "env", None)

            # Another fallback: check if Field has env parameter
            elif hasattr(field_info, "default") and hasattr(field_info.default, "__dict__"):
                field_dict = field_info.default.__dict__
                env_name = field_dict.get("env")

            if env_name:
                env_vars.append(env_name)

        # Handle legacy field extraction for different Pydantic versions
        if not env_vars:
            # Try alternative approach for Field definitions
            try:
                # Create a temporary instance to extract field info
                temp_instance = settings_class(_env_file=None)
                if hasattr(settings_class, "model_fields"):
                    fields = settings_class.model_fields
                else:
                    fields = getattr(temp_instance, "__fields__", {})
                for field_name in fields:
                    field = fields[field_name]
                    if hasattr(field, "field_info") and hasattr(field.field_info, "extra"):
                        env_name = field.field_info.extra.get("env")
                        if env_name:
                            env_vars.append(env_name)
            except Exception:
                # If we can't instantiate, try to parse the source
                pass

        env_vars_by_provider[provider_name] = sorted(env_vars)

    return env_vars_by_provider


def extract_env_vars_from_example_file(file_path: Path) -> set[str]:
    """Extract environment variable names from .env.example file.

    Args:
        file_path: Path to .env.example file

    Returns:
        Set of environment variable names found in the file
    """
    if not file_path.exists():
        return set()

    env_vars = set()
    content = file_path.read_text()

    # Match lines like "VAR_NAME=" (ignoring comments and empty lines)
    pattern = r"^([A-Z_][A-Z0-9_]*)="

    for line in content.splitlines():
        line = line.strip()
        # Skip comments and empty lines
        if line.startswith("#") or not line:
            continue

        match = re.match(pattern, line)
        if match:
            env_vars.add(match.group(1))

    return env_vars


def validate_naming_convention(env_var: str, provider_name: str) -> list[str]:
    """Validate that environment variable follows naming convention.

    Args:
        env_var: Environment variable name
        provider_name: Provider name it belongs to

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Special cases for legacy compatibility
    legacy_vars = {"ALPACA_KEY", "ALPACA_SECRET", "IEX_TOKEN"}
    if env_var in legacy_vars:
        return errors  # Allow legacy naming

    # Standard naming convention: MP_{PROVIDER}_{CREDENTIAL}
    expected_prefix = f"MP_{provider_name.upper()}_"

    if not env_var.startswith("MP_"):
        errors.append("Should start with 'MP_' prefix")
    elif not env_var.startswith(expected_prefix):
        errors.append(f"Should start with '{expected_prefix}' for provider '{provider_name}'")

    # Check for valid characters (uppercase letters, numbers, underscores)
    if not re.match(r"^[A-Z0-9_]+$", env_var):
        errors.append("Should only contain uppercase letters, numbers, and underscores")

    return errors


def main() -> int:
    """Main validation function.

    Returns:
        Exit code (0 for success, 1 for errors)
    """
    project_root = Path(__file__).parent.parent
    env_example_path = project_root / ".env.example"

    print("🔍 Validating environment variable configuration...")
    print(f"Project root: {project_root}")
    print(f"Checking: {env_example_path}")
    print()

    # Extract environment variables from settings
    print("📋 Extracting environment variables from provider settings...")
    try:
        settings_env_vars = extract_env_vars_from_settings()
    except Exception as e:
        print(f"❌ Failed to extract settings: {e}")
        return 1

    # Extract environment variables from .env.example
    print("📄 Reading .env.example file...")
    example_env_vars = extract_env_vars_from_example_file(env_example_path)

    # Collect all required environment variables
    all_required_vars = set()
    for provider_vars in settings_env_vars.values():
        all_required_vars.update(provider_vars)

    print(
        f"Found {len(all_required_vars)} required variables across {len(settings_env_vars)} providers"
    )
    print(f"Found {len(example_env_vars)} variables in .env.example")
    print()

    # Track validation results
    errors = []
    warnings = []

    # Check 1: All required variables are in .env.example
    print("✅ Checking required variables are documented...")
    missing_vars = all_required_vars - example_env_vars
    if missing_vars:
        errors.append("Missing environment variables in .env.example:")
        for var in sorted(missing_vars):
            errors.append(f"  - {var}")

    # Check 2: No orphaned variables in .env.example
    print("🔍 Checking for orphaned variables...")

    # System/framework variables that are allowed in .env.example
    allowed_system_vars = {
        "DATABASE_URL",
        "MP_DB",
        "METRICS_DB_PATH",
        "MP_DATA_DIR",
        "PROMETHEUS_PORT",
        "METRICS_ENABLED",
        "LOG_LEVEL",
        "MASK_SECRETS",
        "TESTING_MODE",
    }

    orphaned_vars = example_env_vars - all_required_vars - allowed_system_vars
    if orphaned_vars:
        warnings.append("Orphaned variables in .env.example (not used by any settings):")
        for var in sorted(orphaned_vars):
            warnings.append(f"  - {var}")

    # Check 3: Naming convention compliance
    print("📏 Validating naming conventions...")
    for provider_name, provider_vars in settings_env_vars.items():
        for env_var in provider_vars:
            naming_errors = validate_naming_convention(env_var, provider_name)
            if naming_errors:
                errors.append(
                    f"Naming convention violation for {env_var} (provider: {provider_name}):"
                )
                for naming_error in naming_errors:
                    errors.append(f"  - {naming_error}")

    # Check 4: Alphabetical ordering within provider sections
    print("🔤 Checking alphabetical ordering...")
    if env_example_path.exists():
        content = env_example_path.read_text()
        lines = content.splitlines()

        current_section = None
        section_vars = []

        for line in lines:
            line = line.strip()
            if line.startswith("#") and ("PROVIDER" in line.upper() or "TARGET" in line.upper()):
                # Process previous section
                if current_section and section_vars:
                    sorted_vars = sorted(section_vars)
                    if section_vars != sorted_vars:
                        warnings.append(
                            f"Variables not alphabetically sorted in section '{current_section}':"
                        )
                        warnings.append(f"  Current: {section_vars}")
                        warnings.append(f"  Expected: {sorted_vars}")

                current_section = line
                section_vars = []
            elif "=" in line and not line.startswith("#"):
                var_name = line.split("=")[0]
                section_vars.append(var_name)

    # Print results
    print("\n" + "=" * 60)
    print("VALIDATION RESULTS")
    print("=" * 60)

    if not errors and not warnings:
        print("✅ All checks passed! Environment variable configuration is valid.")
        return 0

    if warnings:
        print("\n⚠️  WARNINGS:")
        for warning in warnings:
            print(f"   {warning}")

    if errors:
        print("\n❌ ERRORS:")
        for error in errors:
            print(f"   {error}")

        print("\n💡 To fix these issues:")
        print("   1. Add missing variables to .env.example")
        print("   2. Follow naming convention: MP_{PROVIDER}_{CREDENTIAL}")
        print("   3. Keep variables alphabetically sorted within sections")
        print("   4. Remove any unused variables")

        return 1

    print("\n✅ Validation completed with warnings only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
