# Configuration Versioning

MarketPipe uses explicit configuration versioning to ensure that breaking changes to the configuration schema are handled gracefully and that users receive clear error messages when their configuration is incompatible.

## Overview

Every MarketPipe configuration file must include a `config_version` field that specifies which version of the configuration schema it uses. This allows the system to:

- Reject configurations that are too old to be supported
- Warn when configurations are newer than the current binary understands
- Prevent silent failures due to unknown configuration keys
- Provide clear migration paths when the schema changes

## Configuration Version Field

All YAML configuration files must include a `config_version` field:

```yaml
config_version: "1"
symbols:
  - AAPL
  - MSFT
start: "2024-01-01"
end: "2024-01-02"
provider: alpaca
feed_type: iex
output_path: "./data"
workers: 3
batch_size: 1000
```

## Version Compatibility

### Current Version
- **Current Config Version**: `1`
- **Minimum Supported Version**: `1`

### Version Behavior

| Scenario | Behavior | Example |
|----------|----------|---------|
| **Missing version** | ❌ Error | `ConfigVersionError: config_version missing. Add config_version: "1" to your YAML.` |
| **Too old** | ❌ Error | `ConfigVersionError: Config version 0 is too old. Minimum supported is 1.` |
| **Current version** | ✅ Load normally | Configuration loads successfully |
| **Future version** | ⚠️ Warning + Load | Warning issued but parsing continues with best effort |

### Unknown Keys

With versioned configuration, unknown keys are **rejected** to prevent silent configuration errors:

```yaml
config_version: "1"
symbols: ["AAPL"]
start: "2024-01-01"
end: "2024-01-02"
unknown_field: "this will cause an error"  # ❌ Error
```

This helps catch:
- Typos in configuration keys
- Deprecated fields that are no longer supported
- Invalid configuration that might silently fail

## Usage

### Loading Configuration

Use the centralized loader for version validation:

```python
from marketpipe.config import load_config, ConfigVersionError

try:
    config = load_config("path/to/config.yaml")
except ConfigVersionError as e:
    print(f"Configuration version error: {e}")
    sys.exit(1)
except FileNotFoundError:
    print("Configuration file not found")
    sys.exit(1)
```

### CLI Usage

The CLI automatically handles version validation:

```bash
# This will fail with clear error if version is missing/incompatible
mp ingest-ohlcv --config config.yaml

# Example error output:
# ❌ Configuration version error: config_version missing. Add `config_version: "1"` to your YAML.
```

### Backward Compatibility

The old `IngestionJobConfig.from_yaml()` method still works but now uses the new versioned loader internally:

```python
# Still works, but issues deprecation warning
config = IngestionJobConfig.from_yaml("config.yaml")

# Preferred approach
from marketpipe.config import load_config
config = load_config("config.yaml")
```

## Schema Evolution

### Version History

| Version | Changes | Migration Required |
|---------|---------|-------------------|
| `1` | Initial versioned schema | Add `config_version: "1"` to existing configs |

### Adding New Versions

When making breaking changes to the configuration schema:

1. **Increment version constants** in `src/marketpipe/config/ingestion.py`:
   ```python
   CURRENT_CONFIG_VERSION = "2"  # Was "1"
   MIN_SUPPORTED_VERSION = "1"   # Still support older configs
   ```

2. **Update the Pydantic model** field constraint:
   ```python
   config_version: Literal["1", "2"] = Field(
       default=CURRENT_CONFIG_VERSION,
       description="Configuration schema version"
   )
   ```

3. **Add migration logic** in the loader if needed for automatic upgrades

4. **Update documentation** with the new version and any breaking changes

### Migration Strategy

For backward compatibility, consider:

- **Additive changes**: New optional fields don't require version bumps
- **Deprecation warnings**: Warn about deprecated fields before removing them
- **Automatic migration**: Convert old formats to new formats transparently
- **Clear error messages**: Provide specific instructions for manual migration

## Testing

The configuration versioning system includes comprehensive tests:

```bash
# Run versioning tests
pytest tests/config/test_versioning.py -v

# Test scenarios covered:
# - Valid version loading
# - Missing version error
# - Too old version error
# - Future version warning
# - Unknown keys rejection
# - Kebab-case normalization
# - Environment variable expansion
# - Backward compatibility
```

## CI Integration

The CI pipeline automatically validates configuration version consistency:

- Checks that version constants are properly defined
- Ensures `MIN_SUPPORTED_VERSION <= CURRENT_CONFIG_VERSION`
- Prevents accidental schema changes without version updates

## Migration Guide

### From Unversioned to Versioned Configuration

If you have existing configuration files without `config_version`:

1. **Add version field** to all YAML files:
   ```yaml
   config_version: "1"
   # ... rest of your configuration
   ```

2. **Test your configuration**:
   ```bash
   mp ingest-ohlcv --config your-config.yaml --dry-run
   ```

3. **Fix any unknown keys** that are now rejected:
   - Check for typos in field names
   - Remove deprecated or invalid fields
   - Refer to current schema documentation

### Example Migration

**Before** (unversioned):
```yaml
symbols: ["AAPL", "MSFT"]
start: "2024-01-01"
end: "2024-01-02"
some-typo-field: "ignored silently"  # This was ignored
```

**After** (versioned):
```yaml
config_version: "1"
symbols: ["AAPL", "MSFT"]
start: "2024-01-01"
end: "2024-01-02"
# Removed typo field - would now cause error
```

## Best Practices

1. **Always include version**: Never create configuration files without `config_version`

2. **Use latest version**: Use the current version for new configurations

3. **Test after updates**: Validate configurations after MarketPipe updates

4. **Monitor warnings**: Pay attention to warnings about future versions

5. **Keep configs updated**: Migrate to newer versions when available

## Error Handling

Common errors and solutions:

### Missing Version
```
❌ ConfigVersionError: config_version missing. Add `config_version: "1"` to your YAML.
```
**Solution**: Add `config_version: "1"` to your configuration file.

### Too Old Version
```
❌ ConfigVersionError: Config version 0 is too old. Minimum supported is 1.
```
**Solution**: Update your configuration to use version "1" and check for any required field changes.

### Unknown Fields
```
❌ ValueError: Extra inputs are not permitted
```
**Solution**: Remove unknown fields or check for typos in field names.

### Future Version Warning
```
⚠️ UserWarning: This binary understands config_version 1, but file is 2. Attempting best-effort parse.
```
**Solution**: Update MarketPipe to a version that supports configuration version 2, or downgrade your configuration to version 1.

## Related Documentation

- [Configuration Reference](configuration.md) - Complete field documentation
- [CLI Usage](cli.md) - Command-line interface guide
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
