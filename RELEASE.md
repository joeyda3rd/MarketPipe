# Release Guide

This document outlines the release process for MarketPipe.

## Alpha Release Process

### Prerequisites

1. All tests must be passing on the main branch
2. Version numbers must be consistent across `pyproject.toml` and `src/marketpipe/__init__.py`
3. `CHANGELOG.md` must be updated with the new version
4. GitHub repository must have required secrets configured (for PyPI publishing)

### Creating an Alpha Release

#### Method 1: Tag-based Release (Recommended)

1. **Prepare the version**:
   ```bash
   # Update version in pyproject.toml and src/marketpipe/__init__.py
   # Update CHANGELOG.md with release notes
   git add .
   git commit -m "Prepare v0.1.0-alpha.1 release"
   git push origin main
   ```

2. **Create and push tag**:
   ```bash
   git tag v0.1.0-alpha.1
   git push origin v0.1.0-alpha.1
   ```

3. **Monitor the release workflow**:
   - Go to GitHub Actions and monitor the "Release" workflow
   - The workflow will:
     - Validate version consistency
     - Run full test suite
     - Build and test the package
     - Create GitHub release with changelog
     - Publish to Test PyPI (for alpha releases)

#### Method 2: Manual Release Trigger

1. **Use GitHub Actions workflow dispatch**:
   - Go to GitHub Actions → Release workflow
   - Click "Run workflow"
   - Enter version (e.g., `0.1.0-alpha.1`)
   - Choose dry run if you want to test first

### Release Workflow Features

The release workflow includes:

- **Validation**: Version consistency, full test suite, package building
- **GitHub Release**: Automatic release creation with changelog extraction
- **Test PyPI Publishing**: Alpha releases go to Test PyPI for validation
- **Asset Upload**: Release includes built distributions
- **Prerelease Marking**: Alpha/beta releases are marked as prereleases

### Alpha Release Limitations

- **Test PyPI Only**: Alpha releases are published to Test PyPI, not production PyPI
- **Manual Production**: Real PyPI publishing is commented out and requires manual enable
- **Environment Protection**: PyPI environment protection is disabled for alpha
- **Notification**: Release summary is provided in workflow output

### Installation from Alpha Release

Users can install alpha releases from Test PyPI:

```bash
# Install from Test PyPI
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ marketpipe

# Verify installation
python -c "import marketpipe; print(marketpipe.__version__)"
```

### Post-Release Checklist

After a successful alpha release:

1. ✅ Verify GitHub release was created
2. ✅ Check Test PyPI package is available
3. ✅ Test installation from Test PyPI
4. ✅ Update documentation if needed
5. ✅ Notify team/users of new alpha release

### Troubleshooting

**Common Issues:**

1. **Version Mismatch**: Ensure `pyproject.toml` and `__init__.py` have identical versions
2. **Test Failures**: All tests must pass before release
3. **PyPI Secrets**: Ensure `TEST_PYPI_TOKEN` secret is configured
4. **Changelog Format**: Ensure changelog follows the expected format for extraction

**Manual Debugging:**

```bash
# Test version consistency locally
python -c "
import toml
from pathlib import Path
pyproject = toml.load('pyproject.toml')
init_file = Path('src/marketpipe/__init__.py').read_text()
import re
version_match = re.search(r'__version__\s*=\s*[\"\'](.*?)[\"\']', init_file)
print(f'pyproject.toml: {pyproject[\"project\"][\"version\"]}')
print(f'__init__.py: {version_match.group(1)}')
"

# Test package building
python -m build
python -m twine check dist/*
```

## Future: Production Release Process

For production releases (v1.0.0+):

1. Remove `-alpha` suffix from version
2. Update changelog to stable release notes
3. Enable PyPI publishing in release workflow
4. Configure PyPI environment protection
5. Create proper release announcement

## Security Considerations

- **Secrets Management**: PyPI tokens are stored as GitHub repository secrets
- **Test PyPI**: Alpha releases use Test PyPI to avoid pollution of production index
- **Environment Protection**: Production releases should use environment protection
- **Two-Factor**: Maintainers should use 2FA on PyPI accounts

---

**Next Steps for Team:**
1. Set up Test PyPI account and configure `TEST_PYPI_TOKEN` secret
2. Test the alpha release process with v0.1.0-alpha.1
3. Document any issues or improvements needed
4. Plan for beta and stable release processes
