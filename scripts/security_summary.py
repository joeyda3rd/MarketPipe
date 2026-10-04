"""Render scanner artifacts; missing or malformed output is a failed scan."""

import json
import os
from pathlib import Path


def main():
    audit = json.loads(Path("audit-results.json").read_text())
    bandit = json.loads(Path("bandit-results.json").read_text())
    vulnerabilities = sum(len(package["vulns"]) for package in audit["dependencies"])
    findings = len(bandit["results"])
    errors = len(bandit["errors"])
    summary = f"Dependency vulnerabilities: {vulnerabilities}\nStatic findings: {findings}\nScanner errors: {errors}\n"
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
            output.write(summary)
    if vulnerabilities or findings or errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
