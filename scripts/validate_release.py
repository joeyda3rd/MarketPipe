"""Validate release input and source versions without creating public artifacts."""

import ast
import os
from pathlib import Path

from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def main():
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    version = project["version"]
    module = ast.parse(Path("src/marketpipe/__init__.py").read_text())
    source_version = next(
        ast.literal_eval(node.value)
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        )
    )
    expected = os.environ.get("EXPECTED_RELEASE_VERSION") or os.environ.get("GITHUB_REF_NAME", "")
    expected = expected.removeprefix("v")
    try:
        valid = bool(expected) and Version(expected) == Version(version) == Version(source_version)
    except InvalidVersion:
        valid = False
    if not valid:
        raise SystemExit("Release input, project metadata, and source version must agree")
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(
                f"version={version}\nprerelease={str(Version(version).is_prerelease).lower()}\n"
            )
    print(f"Validated release {version}")


if __name__ == "__main__":
    main()
