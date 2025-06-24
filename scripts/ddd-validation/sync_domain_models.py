# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
Domain Model Synchronization Script for MarketPipe

This script synchronizes domain models with code changes, ensuring
that DDD patterns remain consistent as the codebase evolves.

Usage:
    python scripts/ddd-validation/sync_domain_models.py
    python scripts/ddd-validation/sync_domain_models.py --check-only
    python scripts/ddd-validation/sync_domain_models.py --update-docs
"""

import argparse
import ast
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class DomainModelInfo:
    """Information about a domain model."""
    name: str
    type: str  # entity, value_object, aggregate, repository, service
    file_path: str
    line_number: int
    methods: List[str]
    properties: List[str]
    base_classes: List[str]
    dependencies: List[str]
    last_modified: str


@dataclass
class BoundedContextInfo:
    """Information about a bounded context."""
    name: str
    path: str
    entities: List[str]
    value_objects: List[str]
    aggregates: List[str]
    repositories: List[str]
    services: List[str]
    dependencies: List[str]


class DomainModelSynchronizer:
    """Synchronizes domain models with code changes."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_path = project_root / "src" / "marketpipe"
        self.domain_path = self.src_path / "domain"
        self.docs_path = project_root / "docs"
        self.cache_file = project_root / ".domain_model_cache.json"

        # Load previous state
        self.previous_state = self._load_cache()

    def sync_all(self, check_only: bool = False, update_docs: bool = False) -> bool:
        """Synchronize all domain models and detect changes."""
        print("🔄 Analyzing domain model changes...")

        # Discover current domain models
        current_models = self._discover_domain_models()
        current_contexts = self._discover_bounded_contexts()

        # Compare with previous state
        changes = self._detect_changes(current_models, current_contexts)

        if not changes:
            print("✅ No changes detected in domain models")
            return True

        # Report changes
        self._report_changes(changes)

        if check_only:
            return len(changes.get('breaking_changes', [])) == 0

        # Update documentation if requested
        if update_docs:
            self._update_documentation(current_models, current_contexts, changes)

        # Update cache
        self._save_cache(current_models, current_contexts)

        # Validate consistency
        validation_errors = self._validate_consistency(current_models, current_contexts)
        if validation_errors:
            print("\n⚠️  Consistency validation errors:")
            for error in validation_errors:
                print(f"  • {error}")
            return False

        print("\n✅ Domain model synchronization complete")
        return True

    def _discover_domain_models(self) -> Dict[str, DomainModelInfo]:
        """Discover all domain models in the codebase."""
        models = {}

        # Check domain directory
        if self.domain_path.exists():
            for py_file in self.domain_path.glob("*.py"):
                if py_file.name.startswith("__"):
                    continue

                file_models = self._analyze_domain_file(py_file)
                models.update(file_models)

        # Check context-specific domain directories
        for context_dir in self.src_path.iterdir():
            if context_dir.is_dir() and context_dir.name not in ["domain", "__pycache__"]:
                context_domain_dir = context_dir / "domain"
                if context_domain_dir.exists():
                    for py_file in context_domain_dir.glob("*.py"):
                        if py_file.name.startswith("__"):
                            continue

                        file_models = self._analyze_domain_file(py_file)
                        models.update(file_models)

        return models

    def _discover_bounded_contexts(self) -> Dict[str, BoundedContextInfo]:
        """Discover bounded contexts from directory structure."""
        contexts = {}

        # Core domain context
        if self.domain_path.exists():
            core_context = self._analyze_context_directory("core", self.domain_path)
            if core_context:
                contexts["core"] = core_context

        # Other bounded contexts
        for context_dir in self.src_path.iterdir():
            if (context_dir.is_dir() and
                context_dir.name not in ["domain", "__pycache__"] and
                not context_dir.name.startswith("__")):

                context_info = self._analyze_context_directory(context_dir.name, context_dir)
                if context_info:
                    contexts[context_dir.name] = context_info

        return contexts

    def _analyze_domain_file(self, file_path: Path) -> Dict[str, DomainModelInfo]:
        """Analyze a single domain file for models."""
        models = {}

        try:
            with open(file_path, encoding='utf-8') as f:
                content = f.read()

            tree = ast.parse(content)

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    model_info = self._extract_class_info(node, file_path, content)
                    if model_info:
                        models[model_info.name] = model_info

        except Exception as e:
            print(f"Warning: Could not analyze {file_path}: {e}")

        return models

    def _extract_class_info(self, node: ast.ClassDef, file_path: Path, content: str) -> Optional[DomainModelInfo]:
        """Extract information about a class."""
        # Determine model type
        model_type = self._determine_model_type(node, file_path)
        if not model_type:
            return None

        # Extract methods
        methods = []
        properties = []
        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                if any(isinstance(d, ast.Name) and d.id == "property" for d in item.decorator_list):
                    properties.append(item.name)
                else:
                    methods.append(item.name)

        # Extract base classes
        base_classes = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                base_classes.append(base.id)
            elif isinstance(base, ast.Attribute):
                base_classes.append(f"{base.value.id}.{base.attr}")

        # Extract dependencies (simplified)
        dependencies = self._extract_dependencies(content)

        # Get file modification time
        last_modified = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

        return DomainModelInfo(
            name=node.name,
            type=model_type,
            file_path=str(file_path.relative_to(self.project_root)),
            line_number=node.lineno,
            methods=methods,
            properties=properties,
            base_classes=base_classes,
            dependencies=dependencies,
            last_modified=last_modified
        )

    def _determine_model_type(self, node: ast.ClassDef, file_path: Path) -> Optional[str]:
        """Determine the type of domain model."""
        file_name = file_path.name
        class_name = node.name

        # Check by file name
        if "entities.py" in file_name:
            return "entity"
        elif "value_objects.py" in file_name:
            return "value_object"
        elif "aggregates.py" in file_name:
            return "aggregate"
        elif "repositories.py" in file_name:
            return "repository"
        elif "services.py" in file_name:
            return "service"

        # Check by class name patterns
        if class_name.endswith("Entity") or any(base.id == "Entity" for base in node.bases if isinstance(base, ast.Name)):
            return "entity"
        elif class_name.endswith("Aggregate"):
            return "aggregate"
        elif class_name.startswith("I") and class_name.endswith("Repository"):
            return "repository"
        elif class_name.endswith("Service"):
            return "service"

        # Check by decorators (for value objects)
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
                if decorator.func.id == "dataclass":
                    return "value_object"

        return None

    def _extract_dependencies(self, content: str) -> List[str]:
        """Extract dependencies from file content."""
        dependencies = []

        # Look for imports
        import_pattern = r'from\s+([\w.]+)\s+import|import\s+([\w.]+)'
        matches = re.findall(import_pattern, content)

        for match in matches:
            dep = match[0] or match[1]
            if dep and not dep.startswith('__future__') and dep not in ['abc', 'typing', 'dataclasses']:
                dependencies.append(dep)

        return list(set(dependencies))

    def _analyze_context_directory(self, context_name: str, context_path: Path) -> Optional[BoundedContextInfo]:
        """Analyze a bounded context directory."""
        if not context_path.exists():
            return None

        entities = []
        value_objects = []
        aggregates = []
        repositories = []
        services = []
        dependencies = []

        # Look for domain models in context
        domain_dir = context_path / "domain" if context_name != "core" else context_path

        if domain_dir.exists():
            for py_file in domain_dir.glob("*.py"):
                if py_file.name.startswith("__"):
                    continue

                models = self._analyze_domain_file(py_file)
                for model in models.values():
                    if model.type == "entity":
                        entities.append(model.name)
                    elif model.type == "value_object":
                        value_objects.append(model.name)
                    elif model.type == "aggregate":
                        aggregates.append(model.name)
                    elif model.type == "repository":
                        repositories.append(model.name)
                    elif model.type == "service":
                        services.append(model.name)

                    dependencies.extend(model.dependencies)

        # Only return if context has domain models
        if any([entities, value_objects, aggregates, repositories, services]):
            return BoundedContextInfo(
                name=context_name,
                path=str(context_path.relative_to(self.project_root)),
                entities=entities,
                value_objects=value_objects,
                aggregates=aggregates,
                repositories=repositories,
                services=services,
                dependencies=list(set(dependencies))
            )

        return None

    def _detect_changes(self, current_models: Dict[str, DomainModelInfo],
                       current_contexts: Dict[str, BoundedContextInfo]) -> Dict[str, Any]:
        """Detect changes between current and previous state."""
        changes = {
            'new_models': [],
            'deleted_models': [],
            'modified_models': [],
            'new_contexts': [],
            'deleted_contexts': [],
            'modified_contexts': [],
            'breaking_changes': [],
            'summary': {}
        }

        if not self.previous_state:
            # First run, everything is new
            changes['new_models'] = list(current_models.keys())
            changes['new_contexts'] = list(current_contexts.keys())
            return changes

        previous_models = self.previous_state.get('models', {})
        previous_contexts = self.previous_state.get('contexts', {})

        # Detect model changes
        current_model_names = set(current_models.keys())
        previous_model_names = set(previous_models.keys())

        changes['new_models'] = list(current_model_names - previous_model_names)
        changes['deleted_models'] = list(previous_model_names - current_model_names)

        # Check for modified models
        for model_name in current_model_names & previous_model_names:
            current_model = current_models[model_name]
            previous_model = DomainModelInfo(**previous_models[model_name])

            if current_model.last_modified != previous_model.last_modified:
                # Detailed change analysis
                model_changes = self._analyze_model_changes(current_model, previous_model)
                if model_changes:
                    changes['modified_models'].append({
                        'name': model_name,
                        'changes': model_changes
                    })

                    # Check for breaking changes
                    if any(change['type'] in ['method_removed', 'property_removed', 'base_class_changed']
                           for change in model_changes):
                        changes['breaking_changes'].append({
                            'model': model_name,
                            'type': 'api_change',
                            'details': model_changes
                        })

        # Detect context changes
        current_context_names = set(current_contexts.keys())
        previous_context_names = set(previous_contexts.keys())

        changes['new_contexts'] = list(current_context_names - previous_context_names)
        changes['deleted_contexts'] = list(previous_context_names - current_context_names)

        # Summary
        changes['summary'] = {
            'total_models': len(current_models),
            'total_contexts': len(current_contexts),
            'models_changed': len(changes['new_models']) + len(changes['deleted_models']) + len(changes['modified_models']),
            'contexts_changed': len(changes['new_contexts']) + len(changes['deleted_contexts']) + len(changes['modified_contexts'])
        }

        return changes

    def _analyze_model_changes(self, current: DomainModelInfo, previous: DomainModelInfo) -> List[Dict[str, Any]]:
        """Analyze detailed changes between model versions."""
        changes = []

        # Method changes
        current_methods = set(current.methods)
        previous_methods = set(previous.methods)

        for method in current_methods - previous_methods:
            changes.append({'type': 'method_added', 'name': method})

        for method in previous_methods - current_methods:
            changes.append({'type': 'method_removed', 'name': method})

        # Property changes
        current_properties = set(current.properties)
        previous_properties = set(previous.properties)

        for prop in current_properties - previous_properties:
            changes.append({'type': 'property_added', 'name': prop})

        for prop in previous_properties - current_properties:
            changes.append({'type': 'property_removed', 'name': prop})

        # Base class changes
        if current.base_classes != previous.base_classes:
            changes.append({
                'type': 'base_class_changed',
                'from': previous.base_classes,
                'to': current.base_classes
            })

        # Dependency changes
        current_deps = set(current.dependencies)
        previous_deps = set(previous.dependencies)

        for dep in current_deps - previous_deps:
            changes.append({'type': 'dependency_added', 'name': dep})

        for dep in previous_deps - current_deps:
            changes.append({'type': 'dependency_removed', 'name': dep})

        return changes

    def _validate_consistency(self, current_models: Dict[str, DomainModelInfo],
                            current_contexts: Dict[str, BoundedContextInfo]) -> List[str]:
        """Validate consistency of domain models."""
        errors = []

        # Check that entities have proper base classes
        for model in current_models.values():
            if model.type == "entity":
                if "Entity" not in model.base_classes and model.name != "Entity":
                    errors.append(f"Entity {model.name} should inherit from Entity base class")

        # Check that value objects are immutable
        for model in current_models.values():
            if model.type == "value_object":
                # This would need more sophisticated analysis
                pass

        # Check that aggregates manage events
        for model in current_models.values():
            if model.type == "aggregate":
                has_event_methods = any("event" in method.lower() for method in model.methods)
                if not has_event_methods:
                    errors.append(f"Aggregate {model.name} should manage domain events")

        # Check repository interfaces
        for model in current_models.values():
            if model.type == "repository" and model.name.startswith("I"):
                has_abstract_methods = any("abstractmethod" in str(model.dependencies))
                if not has_abstract_methods:
                    errors.append(f"Repository interface {model.name} should have abstract methods")

        return errors

    def _report_changes(self, changes: Dict[str, Any]) -> None:
        """Report detected changes."""
        print("\n📊 Domain Model Changes Detected:")
        print("=" * 50)

        summary = changes.get('summary', {})
        print(f"Total Models: {summary.get('total_models', 0)}")
        print(f"Total Contexts: {summary.get('total_contexts', 0)}")
        print(f"Models Changed: {summary.get('models_changed', 0)}")
        print(f"Contexts Changed: {summary.get('contexts_changed', 0)}")

        if changes.get('new_models'):
            print(f"\n✅ New Models ({len(changes['new_models'])}):")
            for model in changes['new_models']:
                print(f"  + {model}")

        if changes.get('deleted_models'):
            print(f"\n❌ Deleted Models ({len(changes['deleted_models'])}):")
            for model in changes['deleted_models']:
                print(f"  - {model}")

        if changes.get('modified_models'):
            print(f"\n🔄 Modified Models ({len(changes['modified_models'])}):")
            for change in changes['modified_models']:
                print(f"  ~ {change['name']}")
                for detail in change['changes']:
                    print(f"    • {detail['type']}: {detail.get('name', detail)}")

        if changes.get('breaking_changes'):
            print(f"\n⚠️  Breaking Changes ({len(changes['breaking_changes'])}):")
            for change in changes['breaking_changes']:
                print(f"  ! {change['model']}: {change['type']}")
                for detail in change['details']:
                    print(f"    • {detail['type']}: {detail.get('name', detail)}")

        if changes.get('new_contexts'):
            print(f"\n🎯 New Contexts ({len(changes['new_contexts'])}):")
            for context in changes['new_contexts']:
                print(f"  + {context}")

    def _update_documentation(self, current_models: Dict[str, DomainModelInfo],
                            current_contexts: Dict[str, BoundedContextInfo],
                            changes: Dict[str, Any]) -> None:
        """Update documentation based on changes."""
        print("\n📝 Updating documentation...")

        # Update domain model documentation
        self._update_domain_model_docs(current_models, current_contexts)

        # Update context documentation
        self._update_context_docs(current_contexts)

        # Update change log
        self._update_change_log(changes)

    def _update_domain_model_docs(self, current_models: Dict[str, DomainModelInfo],
                                 current_contexts: Dict[str, BoundedContextInfo]) -> None:
        """Update domain model documentation."""
        docs_file = self.docs_path / "domain_model_reference.md"

        content = "# Domain Model Reference\n\n"
        content += f"Generated on: {datetime.now().isoformat()}\n\n"

        # Group by type
        by_type = {}
        for model in current_models.values():
            if model.type not in by_type:
                by_type[model.type] = []
            by_type[model.type].append(model)

        for model_type, models in by_type.items():
            content += f"## {model_type.replace('_', ' ').title()}s\n\n"
            for model in sorted(models, key=lambda m: m.name):
                content += f"### {model.name}\n\n"
                content += f"- **File**: `{model.file_path}`\n"
                content += f"- **Base Classes**: {', '.join(model.base_classes) if model.base_classes else 'None'}\n"
                if model.methods:
                    content += f"- **Methods**: {', '.join(sorted(model.methods))}\n"
                if model.properties:
                    content += f"- **Properties**: {', '.join(sorted(model.properties))}\n"
                content += "\n"

        # Write documentation
        self.docs_path.mkdir(exist_ok=True)
        with open(docs_file, 'w') as f:
            f.write(content)

        print(f"  ✅ Updated: {docs_file}")

    def _update_context_docs(self, current_contexts: Dict[str, BoundedContextInfo]) -> None:
        """Update bounded context documentation."""
        docs_file = self.docs_path / "bounded_contexts_reference.md"

        content = "# Bounded Contexts Reference\n\n"
        content += f"Generated on: {datetime.now().isoformat()}\n\n"

        for context_name, context in current_contexts.items():
            content += f"## {context_name.replace('_', ' ').title()} Context\n\n"
            content += f"- **Path**: `{context.path}`\n\n"

            if context.entities:
                content += f"**Entities**: {', '.join(sorted(context.entities))}\n\n"
            if context.value_objects:
                content += f"**Value Objects**: {', '.join(sorted(context.value_objects))}\n\n"
            if context.aggregates:
                content += f"**Aggregates**: {', '.join(sorted(context.aggregates))}\n\n"
            if context.repositories:
                content += f"**Repositories**: {', '.join(sorted(context.repositories))}\n\n"
            if context.services:
                content += f"**Services**: {', '.join(sorted(context.services))}\n\n"

        with open(docs_file, 'w') as f:
            f.write(content)

        print(f"  ✅ Updated: {docs_file}")

    def _update_change_log(self, changes: Dict[str, Any]) -> None:
        """Update domain model change log."""
        changelog_file = self.docs_path / "domain_model_changelog.md"

        # Read existing changelog
        existing_content = ""
        if changelog_file.exists():
            with open(changelog_file) as f:
                existing_content = f.read()

        # Prepare new entry
        timestamp = datetime.now().isoformat()
        new_entry = f"\n## {timestamp}\n\n"

        summary = changes.get('summary', {})
        new_entry += f"**Summary**: {summary.get('models_changed', 0)} models changed, "
        new_entry += f"{summary.get('contexts_changed', 0)} contexts changed\n\n"

        if changes.get('new_models'):
            new_entry += f"**New Models**: {', '.join(changes['new_models'])}\n\n"

        if changes.get('deleted_models'):
            new_entry += f"**Deleted Models**: {', '.join(changes['deleted_models'])}\n\n"

        if changes.get('modified_models'):
            new_entry += "**Modified Models**:\n"
            for change in changes['modified_models']:
                new_entry += f"- {change['name']}: "
                change_types = [c['type'] for c in change['changes']]
                new_entry += f"{', '.join(set(change_types))}\n"
            new_entry += "\n"

        if changes.get('breaking_changes'):
            new_entry += "**⚠️ Breaking Changes**:\n"
            for change in changes['breaking_changes']:
                new_entry += f"- {change['model']}: {change['type']}\n"
            new_entry += "\n"

        # Write updated changelog
        content = "# Domain Model Change Log\n"
        content += new_entry
        content += existing_content.replace("# Domain Model Change Log\n", "")

        with open(changelog_file, 'w') as f:
            f.write(content)

        print(f"  ✅ Updated: {changelog_file}")

    def _load_cache(self) -> Optional[Dict[str, Any]]:
        """Load previous state from cache."""
        if not self.cache_file.exists():
            return None

        try:
            with open(self.cache_file) as f:
                return json.load(f)
        except Exception:
            return None

    def _save_cache(self, current_models: Dict[str, DomainModelInfo],
                   current_contexts: Dict[str, BoundedContextInfo]) -> None:
        """Save current state to cache."""
        state = {
            'models': {name: asdict(model) for name, model in current_models.items()},
            'contexts': {name: asdict(context) for name, context in current_contexts.items()},
            'timestamp': datetime.now().isoformat()
        }

        with open(self.cache_file, 'w') as f:
            json.dump(state, f, indent=2)


def main():
    """Main entry point for domain model synchronization."""
    parser = argparse.ArgumentParser(description="Synchronize domain models with code changes")
    parser.add_argument("--check-only", action="store_true", help="Only check for changes, don't update")
    parser.add_argument("--update-docs", action="store_true", help="Update documentation")

    args = parser.parse_args()

    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    print(f"🔄 Synchronizing domain models in: {project_root}")

    # Run synchronization
    synchronizer = DomainModelSynchronizer(project_root)
    success = synchronizer.sync_all(
        check_only=args.check_only,
        update_docs=args.update_docs
    )

    if not success:
        print("❌ Domain model synchronization failed")
        exit(1)
    else:
        print("✅ Domain model synchronization completed successfully")


if __name__ == "__main__":
    main()
