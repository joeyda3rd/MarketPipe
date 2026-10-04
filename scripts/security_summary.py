"""Render scanner artifacts; missing or malformed output is a failed scan."""

import json
import os
from pathlib import Path


def main():
    audit = json.loads(Path("audit-results.json").read_text())
    bandit = json.loads(Path("bandit-results.json").read_text())
    vulnerabilities = 0
    unpublished = []
    for package in audit["dependencies"]:
        if "skip_reason" in package:
            if package["name"] != "marketpipe":
                raise ValueError(f"Dependency could not be audited: {package['name']}")
            unpublished.append(package["name"])
        else:
            vulnerabilities += len(package["vulns"])
    findings = len(bandit["results"])
    errors = len(bandit["errors"])
    summary = f"Dependency vulnerabilities: {vulnerabilities}\nStatic findings: {findings}\nScanner errors: {errors}\n"
    if unpublished:
        summary += "Local unpublished project excluded from advisory lookup: marketpipe\n"
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
            output.write(summary)
    if vulnerabilities or findings or errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
