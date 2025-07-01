#!/usr/bin/env python3
"""
generate_api_reference.py

Usage:
    python generate_api_reference.py /path/to/project
Produces:
    api_reference.json
    test_api_reference.json
"""

import ast
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

SKIP_DIRS = {".git", ".venv", "venv", "__pycache__"}  # extend as needed


def is_test_file(filepath: Path) -> bool:
    return (
        "tests" in filepath.parts
        or filepath.name.startswith("test_")
        or filepath.name.endswith("_test.py")
    )


def collect_defs(code: str) -> List[Dict]:
    """
    Return a list of {"type": "class|function", "name": str,
                      "signature": str, "returns": str|None}
    """
    tree = ast.parse(code)
    collected = []

    class Visitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node):
            collected.append(_info(node, "function"))
            self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node):
            collected.append(_info(node, "function"))
            self.generic_visit(node)

        def visit_ClassDef(self, node):
            collected.append(_info(node, "class"))
            self.generic_visit(node)

    def _info(node, def_type):
        args = []
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for arg in node.args.args:
                args.append(arg.arg)
            if node.args.vararg:
                args.append(f"*{node.args.vararg.arg}")
            if node.args.kwarg:
                args.append(f"**{node.args.kwarg.arg}")
            signature = f"({', '.join(args)})"
            returns = (
                ast.unparse(node.returns) if getattr(node, "returns", None) else None
            )
        else:  # class
            signature = "()"
            returns = None
        return {
            "type": def_type,
            "name": node.name,
            "signature": signature,
            "returns": returns,
        }

    Visitor().visit(tree)
    return collected


def scan(root: Path):
    api: Dict[str, List[Dict]] = {}
    tests: Dict[str, List[Dict]] = {}

    # Scan src/ directory for main API reference
    src_dir = root / "src"
    if src_dir.exists():
        for dirpath, dirnames, filenames in os.walk(src_dir):
            # prune unwanted dirs
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for fname in filenames:
                if not fname.endswith(".py"):
                    continue
                path = Path(dirpath, fname)
                rel_path = str(path.relative_to(root))
                with path.open("r", encoding="utf-8") as f:
                    try:
                        code = f.read()
                    except Exception:
                        continue  # skip unreadable files
                defs = collect_defs(code)
                if not defs:
                    continue
                api[rel_path] = defs

    # Scan tests/ directory for test API reference
    tests_dir = root / "tests"
    if tests_dir.exists():
        for dirpath, dirnames, filenames in os.walk(tests_dir):
            # prune unwanted dirs
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for fname in filenames:
                if not fname.endswith(".py"):
                    continue
                path = Path(dirpath, fname)
                rel_path = str(path.relative_to(root))
                with path.open("r", encoding="utf-8") as f:
                    try:
                        code = f.read()
                    except Exception:
                        continue  # skip unreadable files
                defs = collect_defs(code)
                if not defs:
                    continue
                tests[rel_path] = defs

    # Create output directory
    output_dir = root / "dev" / "reference"
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "api_reference.json").write_text(
        json.dumps(api, indent=2, ensure_ascii=False)
    )
    (output_dir / "test_api_reference.json").write_text(
        json.dumps(tests, indent=2, ensure_ascii=False)
    )
    print("Wrote dev/reference/api_reference.json and dev/reference/test_api_reference.json")


if __name__ == "__main__":
    project_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    scan(project_root)
