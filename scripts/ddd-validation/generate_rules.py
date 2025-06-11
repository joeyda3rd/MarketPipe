# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
Cursor Rules Generator for MarketPipe DDD Architecture

This script automatically generates and updates Cursor .mdc rule files
based on the current codebase structure and DDD patterns.

Usage:
    python scripts/ddd-validation/generate_rules.py
    python scripts/ddd-validation/generate_rules.py --update-existing
    python scripts/ddd-validation/generate_rules.py --context ingestion
"""

import os
import re
import ast
from pathlib import Path
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
import argparse
import yaml


@dataclass
class RuleTemplate:
    """Template for generating rule files."""
    name: str
    description: str
    globs: List[str]
    priority: str
    content: str


class CursorRulesGenerator:
    """Generates Cursor .mdc rule files for DDD architecture."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_path = project_root / "src" / "marketpipe"
        self.cursor_rules_path = project_root / ".cursor" / "rules"
        
        # Analyze codebase
        self.domain_entities = self._discover_domain_entities()
        self.value_objects = self._discover_value_objects()
        self.aggregates = self._discover_aggregates()
        self.repositories = self._discover_repositories()
        self.bounded_contexts = self._discover_bounded_contexts()
    
    def generate_all_rules(self, update_existing: bool = False) -> None:
        """Generate all DDD rule files."""
        print("🔄 Analyzing codebase structure...")
        
        rules_to_generate = [
            self._generate_domain_entities_rule(),
            self._generate_value_objects_rule(),
            self._generate_aggregates_rule(),
            self._generate_repositories_rule(),
            self._generate_bounded_contexts_rule(),
            self._generate_ubiquitous_language_rule(),
            self._generate_layered_architecture_rule(),
            self._generate_anti_corruption_rule(),
        ]
        
        # Create rules directory if not exists
        self.cursor_rules_path.mkdir(parents=True, exist_ok=True)
        ddd_rules_path = self.cursor_rules_path / "ddd"
        ddd_rules_path.mkdir(exist_ok=True)
        
        for rule in rules_to_generate:
            rule_file = ddd_rules_path / f"{rule.name}.mdc"
            
            if rule_file.exists() and not update_existing:
                print(f"⏭️  Skipping existing rule: {rule.name}")
                continue
            
            with open(rule_file, 'w') as f:
                f.write(self._format_rule_content(rule))
            
            print(f"✅ Generated rule: {rule.name}")
        
        # Generate context-specific rules
        for context in self.bounded_contexts:
            context_rule = self._generate_context_specific_rule(context)
            if context_rule:
                context_file = ddd_rules_path / f"{context}_context.mdc"
                
                if context_file.exists() and not update_existing:
                    continue
                
                with open(context_file, 'w') as f:
                    f.write(self._format_rule_content(context_rule))
                
                print(f"✅ Generated context rule: {context}_context")
        
        print(f"\n🎉 Rule generation complete! Files saved to: {ddd_rules_path}")
    
    def _discover_domain_entities(self) -> List[str]:
        """Discover domain entities in the codebase."""
        entities = []
        domain_path = self.src_path / "domain"
        
        if not domain_path.exists():
            return entities
        
        entities_file = domain_path / "entities.py"
        if entities_file.exists():
            entities.extend(self._extract_class_names(entities_file))
        
        return entities
    
    def _discover_value_objects(self) -> List[str]:
        """Discover value objects in the codebase."""
        value_objects = []
        domain_path = self.src_path / "domain"
        
        if not domain_path.exists():
            return value_objects
        
        vo_file = domain_path / "value_objects.py"
        if vo_file.exists():
            value_objects.extend(self._extract_class_names(vo_file))
        
        return value_objects
    
    def _discover_aggregates(self) -> List[str]:
        """Discover aggregates in the codebase."""
        aggregates = []
        domain_path = self.src_path / "domain"
        
        if not domain_path.exists():
            return aggregates
        
        agg_file = domain_path / "aggregates.py"
        if agg_file.exists():
            aggregates.extend(self._extract_class_names(agg_file))
        
        return aggregates
    
    def _discover_repositories(self) -> List[str]:
        """Discover repository interfaces in the codebase."""
        repositories = []
        domain_path = self.src_path / "domain"
        
        if not domain_path.exists():
            return repositories
        
        repo_file = domain_path / "repositories.py"
        if repo_file.exists():
            repositories.extend(self._extract_class_names(repo_file))
        
        return repositories
    
    def _discover_bounded_contexts(self) -> List[str]:
        """Discover bounded contexts from directory structure."""
        contexts = []
        
        # Look for main context directories
        for item in self.src_path.iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                if item.name not in ['domain', '__pycache__']:
                    contexts.append(item.name)
        
        return contexts
    
    def _extract_class_names(self, file_path: Path) -> List[str]:
        """Extract class names from a Python file."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            tree = ast.parse(content)
            classes = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    classes.append(node.name)
            
            return classes
        except Exception:
            return []
    
    def _generate_domain_entities_rule(self) -> RuleTemplate:
        """Generate rule for domain entities."""
        entities_list = "\\n".join([f"- `{entity}`" for entity in self.domain_entities])
        
        content = f"""---
description: Domain entity patterns and validation rules for MarketPipe
globs:
  - 'src/marketpipe/domain/entities.py'
  - 'src/marketpipe/**/domain/**/*.py'
alwaysApply: true
priority: high
---

# Domain Entities

## Objective
Ensure all domain entities follow proper DDD patterns and maintain business invariants.

## Context
MarketPipe domain entities discovered in codebase:
{entities_list}

## Rules

### Entity Identity
Every entity must have a unique identifier:

✅ Good:
```python
class OHLCVBar(Entity):
    def __init__(self, id: EntityId, symbol: Symbol, ...):
        super().__init__(id)
        # Entity initialization
```

### Entity Behavior
Entities should encapsulate business behavior, not just data:

✅ Good:
```python
class OHLCVBar(Entity):
    def calculate_price_range(self) -> Price:
        \"\"\"Calculate business-relevant price range.\"\"\"
        return Price(self._high_price.value - self._low_price.value)
    
    def is_during_market_hours(self) -> bool:
        \"\"\"Business rule for market hours validation.\"\"\"
        return self._timestamp.is_market_hours()
```

❌ Avoid:
```python
class OHLCVBar(Entity):
    # Just getters/setters without business logic
    def get_high(self): return self._high
    def set_high(self, value): self._high = value
```

### Invariant Protection
Entities must protect business invariants:

✅ Good:
```python
def _validate_ohlc_consistency(self) -> None:
    \"\"\"Validate OHLC price relationships.\"\"\"
    if not (self._high_price >= self._open_price and 
            self._high_price >= self._close_price):
        raise ValueError("OHLC prices are inconsistent")
```
"""
        
        return RuleTemplate(
            name="domain_entities",
            description="Domain entity patterns and validation rules",
            globs=['src/marketpipe/domain/entities.py', 'src/marketpipe/**/domain/**/*.py'],
            priority="high",
            content=content
        )
    
    def _generate_value_objects_rule(self) -> RuleTemplate:
        """Generate rule for value objects."""
        vo_list = "\\n".join([f"- `{vo}`" for vo in self.value_objects])
        
        content = f"""---
description: Value object patterns and immutability rules for MarketPipe
globs:
  - 'src/marketpipe/domain/value_objects.py'
  - 'src/marketpipe/**/domain/**/*.py'
alwaysApply: true
priority: high
---

# Value Objects

## Objective
Ensure value objects are immutable and properly encapsulate domain concepts.

## Context
MarketPipe value objects discovered in codebase:
{vo_list}

## Rules

### Immutability
Value objects must be immutable using @dataclass(frozen=True):

✅ Good:
```python
@dataclass(frozen=True)
class Price:
    value: Decimal
    
    def __post_init__(self):
        if self.value < 0:
            raise ValueError("Price cannot be negative")
```

### Value Semantics
Value objects are compared by value, not identity:

✅ Good:
```python
@dataclass(frozen=True)
class Symbol:
    value: str
    
    def __post_init__(self):
        normalized = self.value.upper().strip()
        object.__setattr__(self, 'value', normalized)
```

### Business Validation
Value objects should validate their own constraints:

✅ Good:
```python
@dataclass(frozen=True)
class Timestamp:
    value: datetime
    
    def __post_init__(self):
        if self.value.tzinfo is None:
            utc_dt = self.value.replace(tzinfo=timezone.utc)
            object.__setattr__(self, 'value', utc_dt)
```
"""
        
        return RuleTemplate(
            name="value_objects",
            description="Value object patterns and immutability rules",
            globs=['src/marketpipe/domain/value_objects.py'],
            priority="high",
            content=content
        )
    
    def _generate_aggregates_rule(self) -> RuleTemplate:
        """Generate rule for aggregates."""
        agg_list = "\\n".join([f"- `{agg}`" for agg in self.aggregates])
        
        content = f"""---
description: Aggregate root patterns and consistency boundary rules for MarketPipe
globs:
  - 'src/marketpipe/domain/aggregates.py'
  - 'src/marketpipe/**/domain/**/*.py'
alwaysApply: true
priority: high
---

# Aggregates

## Objective
Ensure aggregates maintain consistency boundaries and manage domain events properly.

## Context
MarketPipe aggregates discovered in codebase:
{agg_list}

## Rules

### Consistency Boundaries
Aggregates enforce business invariants across related entities:

✅ Good:
```python
class SymbolBarsAggregate:
    def add_bar(self, bar: OHLCVBar) -> None:
        # Validate aggregate invariants
        if bar.symbol != self._symbol:
            raise ValueError("Bar symbol doesn't match aggregate")
        
        if bar.timestamp in self._bars:
            raise ValueError("Duplicate timestamp not allowed")
        
        self._bars[bar.timestamp] = bar
        self._version += 1
```

### Domain Events
Aggregates should raise domain events for significant business occurrences:

✅ Good:
```python
def complete_collection(self) -> None:
    self._is_complete = True
    event = BarCollectionCompleted(
        symbol=self._symbol,
        trading_date=self._trading_date,
        bar_count=self.bar_count
    )
    self._events.append(event)
```

### Version Control
Implement optimistic concurrency control:

✅ Good:
```python
@property
def version(self) -> int:
    return self._version

def _increment_version(self) -> None:
    self._version += 1
```
"""
        
        return RuleTemplate(
            name="aggregates",
            description="Aggregate root patterns and consistency boundaries",
            globs=['src/marketpipe/domain/aggregates.py'],
            priority="high",
            content=content
        )
    
    def _generate_repositories_rule(self) -> RuleTemplate:
        """Generate rule for repositories."""
        repo_list = "\\n".join([f"- `{repo}`" for repo in self.repositories])
        
        content = f"""---
description: Repository interface patterns for MarketPipe domain access
globs:
  - 'src/marketpipe/domain/repositories.py'
  - 'src/marketpipe/**/repositories/**/*.py'
alwaysApply: true
priority: medium
---

# Repository Interfaces

## Objective
Define clean repository interfaces that abstract data access concerns from domain logic.

## Context
MarketPipe repository interfaces discovered in codebase:
{repo_list}

## Rules

### Interface Definition
Repositories are defined as abstract interfaces in the domain layer:

✅ Good:
```python
class ISymbolBarsRepository(ABC):
    @abstractmethod
    async def get_by_symbol_and_date(
        self, symbol: Symbol, trading_date: date
    ) -> Optional[SymbolBarsAggregate]:
        pass
    
    @abstractmethod
    async def save(self, aggregate: SymbolBarsAggregate) -> None:
        pass
```

### Domain-Focused Methods
Repository methods should use domain language and concepts:

✅ Good:
```python
async def find_symbols_with_data(
    self, start_date: date, end_date: date
) -> List[Symbol]:
    \"\"\"Find symbols that have data in date range.\"\"\"
```

❌ Avoid:
```python
async def select_records_by_date_range(
    self, table: str, start: str, end: str
) -> List[Dict]:  # Too database-focused
```

### Exception Handling
Use domain-specific exceptions:

✅ Good:
```python
class ConcurrencyError(RepositoryError):
    \"\"\"Raised when optimistic concurrency control fails.\"\"\"

class NotFoundError(RepositoryError):
    \"\"\"Raised when requested data is not found.\"\"\"
```
"""
        
        return RuleTemplate(
            name="repositories",
            description="Repository interface patterns for domain access",
            globs=['src/marketpipe/domain/repositories.py'],
            priority="medium",
            content=content
        )
    
    def _generate_bounded_contexts_rule(self) -> RuleTemplate:
        """Generate rule for bounded contexts."""
        context_list = "\\n".join([f"- `{ctx}` Context" for ctx in self.bounded_contexts])
        
        content = f"""---
description: Bounded context separation and integration patterns for MarketPipe
globs:
  - 'src/marketpipe/**/*.py'
alwaysApply: true
priority: high
---

# Bounded Contexts

## Objective
Maintain clear boundaries between different business domains and prevent tight coupling.

## Context
MarketPipe bounded contexts discovered in codebase:
{context_list}

## Rules

### Context Boundaries
Each context should have its own domain model and avoid dependencies on other contexts' internals:

✅ Good:
```python
# ingestion/domain/ingestion_job.py
class IngestionJob(Entity):
    \"\"\"Ingestion context's view of a processing job\"\"\"

# storage/domain/data_partition.py  
class DataPartition(Entity):
    \"\"\"Storage context's view of data organization\"\"\"
```

❌ Avoid:
```python
# DON'T import domain models across contexts
from marketpipe.storage.domain.data_partition import DataPartition
from marketpipe.ingestion.domain.ingestion_job import IngestionJob

class ValidationService:
    def validate(self, job: IngestionJob, partition: DataPartition):
        pass  # Tight coupling across contexts
```

### Anti-Corruption Layers
Use adapters to protect domain models from external systems:

✅ Good:
```python
class AlpacaMarketDataAdapter:
    \"\"\"Anti-corruption layer for Alpaca API integration.\"\"\"
    
    def __init__(self, alpaca_client: AlpacaApiClient):
        self._alpaca_client = alpaca_client
    
    async def fetch_bars(self, symbol: Symbol, time_range: TimeRange) -> List[OHLCVBar]:
        # Translate external API to domain model
        alpaca_response = await self._alpaca_client.get_bars(...)
        return [self._translate_to_domain(bar) for bar in alpaca_response]
```

### Context Integration
Communicate between contexts via well-defined interfaces:

✅ Good:
```python
class IngestionOrchestrator:
    def __init__(
        self,
        market_data_integration: IMarketDataIntegration,
        data_validation: IDataValidation,
        data_storage: IDataStorage
    ):
        # Depend on interfaces, not concrete implementations
```
"""
        
        return RuleTemplate(
            name="bounded_contexts",
            description="Bounded context separation and integration patterns",
            globs=['src/marketpipe/**/*.py'],
            priority="high",
            content=content
        )
    
    def _generate_ubiquitous_language_rule(self) -> RuleTemplate:
        """Generate rule for ubiquitous language."""
        content = """---
description: Ubiquitous language consistency for MarketPipe financial domain
globs:
  - 'src/**/*.py'
  - 'tests/**/*.py'
  - 'docs/**/*.md'
alwaysApply: true
priority: high
---

# Ubiquitous Language

## Objective
Enforce consistent use of domain terminology across all code, documentation, and communication.

## Context
Financial market data processing domain with vendor-agnostic abstractions.

## Rules

### Approved Domain Terms
Use these exact terms consistently:

- **Symbol**: Stock identifier (not "ticker", "security", "instrument")
- **OHLCV Bar**: Price/volume data (not "candle", "quote", "price data")  
- **Trading Date**: Market calendar date (not "business date", "market date")
- **Ingestion**: Data collection process (not "import", "fetch", "load")
- **Market Data Provider**: External data source (not "vendor", "feed", "source")
- **Validation**: Business rule checking (not "verification", "check")

### Banned Terms
Avoid these terms in domain code:

❌ `ticker`, `security`, `instrument` → Use `symbol`
❌ `candle`, `quote`, `price_data` → Use `ohlcv_bar`  
❌ `business_date`, `market_date` → Use `trading_date`
❌ `import`, `fetch`, `load` → Use `ingestion`
❌ `vendor`, `feed`, `source` → Use `market_data_provider`
❌ `verification`, `check` → Use `validation`

### Implementation Examples

✅ Good:
```python
class Symbol:
    \"\"\"Stock symbol (e.g., AAPL, GOOGL)\"\"\"

def ingest_symbol_data(symbol: Symbol) -> None:
    \"\"\"Ingest OHLCV bars for the specified symbol.\"\"\"

class MarketDataProvider:
    \"\"\"External source of market data.\"\"\"
```

❌ Avoid:
```python
class Ticker:  # Use Symbol
class SecurityData:  # Use OHLCVBar
def fetch_data():  # Use ingest_data
```
"""
        
        return RuleTemplate(
            name="ubiquitous_language",
            description="Ubiquitous language consistency",
            globs=['src/**/*.py', 'tests/**/*.py', 'docs/**/*.md'],
            priority="high",
            content=content
        )
    
    def _generate_layered_architecture_rule(self) -> RuleTemplate:
        """Generate rule for layered architecture."""
        content = """---
description: Layered architecture dependency rules for MarketPipe DDD implementation
globs:
  - 'src/marketpipe/**/*.py'
alwaysApply: true
priority: high
---

# Layered Architecture

## Objective
Enforce proper dependency direction in DDD layered architecture.

## Context
MarketPipe follows DDD layered architecture with clear separation of concerns.

## Rules

### Dependency Direction
Dependencies should flow inward toward the domain:

```
Infrastructure → Application → Domain
        ↓             ↓         ↓
    External      Use Cases   Core Business
     Systems                    Logic
```

### Domain Layer Purity
Domain layer must not depend on infrastructure:

✅ Good:
```python
# domain/entities.py
from abc import ABC, abstractmethod
from typing import List, Optional
from .value_objects import Symbol, Price

class OHLCVBar(Entity):
    # Pure domain logic, no infrastructure dependencies
```

❌ Avoid:
```python  
# domain/entities.py
import requests  # Infrastructure dependency
import sqlalchemy  # Infrastructure dependency

class OHLCVBar(Entity):
    def save_to_database(self):  # Infrastructure concern in domain
        pass
```

### Application Layer Coordination
Application services coordinate between domain and infrastructure:

✅ Good:
```python
# application/ingestion_service.py
class IngestionApplicationService:
    def __init__(
        self,
        symbol_repo: ISymbolBarsRepository,  # Domain interface
        market_data_client: IMarketDataProvider,  # Domain interface
        event_publisher: IEventPublisher  # Domain interface
    ):
        # Depends on domain interfaces
```

### Infrastructure Layer Implementation
Infrastructure implements domain interfaces:

✅ Good:
```python
# infrastructure/repositories/symbol_bars_repository.py
class SqliteSymbolBarsRepository(ISymbolBarsRepository):
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection
    
    async def get_by_symbol_and_date(self, symbol: Symbol, date: date) -> Optional[SymbolBarsAggregate]:
        # Infrastructure implementation of domain interface
```
"""
        
        return RuleTemplate(
            name="layered_architecture",
            description="Layered architecture dependency rules",
            globs=['src/marketpipe/**/*.py'],
            priority="high",
            content=content
        )
    
    def _generate_anti_corruption_rule(self) -> RuleTemplate:
        """Generate rule for anti-corruption layers."""
        content = """---
description: Anti-corruption layer patterns for external system integration
globs:
  - 'src/marketpipe/integration/**/*.py'
  - 'src/marketpipe/**/adapters/**/*.py'
alwaysApply: true
priority: medium
---

# Anti-Corruption Layers

## Objective
Protect domain models from external system formats and prevent corruption of domain language.

## Context
MarketPipe integrates with multiple external market data providers that have different APIs and data formats.

## Rules

### Adapter Pattern
Create adapters that translate between external formats and domain models:

✅ Good:
```python
class AlpacaMarketDataAdapter:
    \"\"\"Anti-corruption layer for Alpaca API integration.\"\"\"
    
    def __init__(self, alpaca_client: AlpacaApiClient):
        self._alpaca_client = alpaca_client
    
    async def fetch_bars(self, symbol: Symbol, time_range: TimeRange) -> List[OHLCVBar]:
        \"\"\"Fetch bars and translate to domain model.\"\"\"
        # Get raw external data
        alpaca_response = await self._alpaca_client.get_bars(
            symbol=symbol.value,
            start=time_range.start.isoformat(),
            end=time_range.end.isoformat()
        )
        
        # Translate to domain model
        domain_bars = []
        for alpaca_bar in alpaca_response["bars"]:
            domain_bar = self._translate_alpaca_bar_to_domain(alpaca_bar)
            domain_bars.append(domain_bar)
        
        return domain_bars
    
    def _translate_alpaca_bar_to_domain(self, alpaca_bar: Dict[str, Any]) -> OHLCVBar:
        \"\"\"Translate Alpaca format to domain model.\"\"\"
        return OHLCVBar(
            id=EntityId.generate(),
            symbol=Symbol(alpaca_bar["S"]),
            timestamp=Timestamp.from_iso(alpaca_bar["t"]),
            open_price=Price.from_float(alpaca_bar["o"]),
            high_price=Price.from_float(alpaca_bar["h"]),
            low_price=Price.from_float(alpaca_bar["l"]),
            close_price=Price.from_float(alpaca_bar["c"]),
            volume=Volume(alpaca_bar["v"])
        )
```

### External Model Isolation
Never expose external data models directly to domain layer:

❌ Avoid:
```python
# Don't pass external models directly
def process_market_data(alpaca_response: AlpacaBarResponse):
    # Domain logic should not depend on external formats
    pass
```

✅ Good:
```python
# Translate first, then use domain models
def process_market_data(bars: List[OHLCVBar]):
    # Domain logic works with domain models
    pass
```

### Configuration Translation
Translate external configuration to domain concepts:

✅ Good:
```python
class AlpacaConfigAdapter:
    \"\"\"Translates Alpaca-specific config to domain concepts.\"\"\"
    
    def to_domain_config(self, alpaca_config: Dict[str, Any]) -> MarketDataProviderConfig:
        return MarketDataProviderConfig(
            provider_id="alpaca",
            rate_limit=RateLimit(
                requests_per_minute=alpaca_config["rate_limit_per_min"],
                window_seconds=60
            ),
            supported_feeds=[
                DataFeed.IEX if alpaca_config["feed"] == "iex" else DataFeed.SIP
            ]
        )
```
"""
        
        return RuleTemplate(
            name="anti_corruption",
            description="Anti-corruption layer patterns for external integration",
            globs=['src/marketpipe/integration/**/*.py', 'src/marketpipe/**/adapters/**/*.py'],
            priority="medium",
            content=content
        )
    
    def _generate_context_specific_rule(self, context: str) -> Optional[RuleTemplate]:
        """Generate context-specific rules."""
        if context == "ingestion":
            return self._generate_ingestion_context_rule()
        elif context == "validation":
            return self._generate_validation_context_rule()
        elif context == "storage":
            return self._generate_storage_context_rule()
        return None
    
    def _generate_ingestion_context_rule(self) -> RuleTemplate:
        """Generate ingestion context specific rules."""
        content = """---
description: Ingestion context domain rules and patterns for MarketPipe
globs:
  - 'src/marketpipe/ingestion/**/*.py'
alwaysApply: true
priority: medium
---

# Ingestion Context Rules

## Objective
Ensure ingestion context follows DDD patterns for coordinating market data collection.

## Context
Core domain context responsible for orchestrating parallel data collection from external sources.

## Rules

### Ingestion Job Entity
Model ingestion work as domain entities with lifecycle:

✅ Good:
```python
class IngestionJob(Entity):
    def __init__(self, job_id: IngestionJobId, symbols: List[Symbol], trading_date: date):
        super().__init__(EntityId.generate())
        self._job_id = job_id
        self._symbols = symbols
        self._trading_date = trading_date
        self._status = JobStatus.PENDING
    
    def start(self) -> None:
        if self._status != JobStatus.PENDING:
            raise ValueError(f"Cannot start job in status {self._status}")
        self._status = JobStatus.IN_PROGRESS
        self._started_at = datetime.now(timezone.utc)
    
    def complete(self) -> None:
        if self._status != JobStatus.IN_PROGRESS:
            raise ValueError(f"Cannot complete job in status {self._status}")
        self._status = JobStatus.COMPLETED
        self._completed_at = datetime.now(timezone.utc)
```

### Coordinator Pattern
Use application services to coordinate ingestion workflow:

✅ Good:
```python
class IngestionCoordinatorService:
    def __init__(
        self,
        market_data_provider: IMarketDataProvider,
        data_storage: IDataStorage,
        data_validator: IDataValidator,
        job_repository: IIngestionJobRepository
    ):
        # Coordinate across multiple contexts via interfaces
```

### Checkpoint Management
Implement resumable operations with domain events:

✅ Good:
```python
def save_checkpoint(self, symbol: Symbol, checkpoint: str | int) -> None:
    if self.state:
        self.state.set(symbol, checkpoint)
        # Raise domain event for checkpoint saved
        event = CheckpointSaved(symbol=symbol, checkpoint=checkpoint)
        self._events.append(event)
```
"""
        
        return RuleTemplate(
            name="ingestion_context",
            description="Ingestion context domain rules and patterns",
            globs=['src/marketpipe/ingestion/**/*.py'],
            priority="medium",
            content=content
        )
    
    def _generate_validation_context_rule(self) -> RuleTemplate:
        """Generate validation context specific rules."""
        content = """---
description: Validation context domain rules for MarketPipe data quality
globs:
  - 'src/marketpipe/validation/**/*.py'
alwaysApply: true
priority: medium
---

# Validation Context Rules

## Objective
Ensure validation context properly encapsulates data quality business rules.

## Rules

### Validation Rules as Domain Objects
Model validation rules as first-class domain objects:

✅ Good:
```python
class ValidationRule(Entity):
    def __init__(self, rule_id: str, name: str, severity: ValidationSeverity):
        super().__init__(EntityId.generate())
        self._rule_id = rule_id
        self._name = name
        self._severity = severity
    
    @abstractmethod
    def validate(self, bar: OHLCVBar) -> ValidationResult:
        pass

class OHLCConsistencyRule(ValidationRule):
    def validate(self, bar: OHLCVBar) -> ValidationResult:
        errors = []
        if not (bar.high_price >= bar.open_price and bar.high_price >= bar.close_price):
            errors.append("High price must be >= open and close prices")
        return ValidationResult(rule_id=self._rule_id, passed=len(errors) == 0, errors=errors)
```

### Business Rule Domain Events
Raise domain events when validation fails:

✅ Good:
```python
def validate_bars(self, bars: List[OHLCVBar]) -> ValidationSummary:
    for bar in bars:
        for rule in self._validation_rules:
            result = rule.validate(bar)
            if not result.passed:
                event = ValidationFailed(
                    symbol=bar.symbol,
                    timestamp=bar.timestamp,
                    error_message="; ".join(result.errors),
                    rule_id=rule.rule_id
                )
                self._events.append(event)
```
"""
        
        return RuleTemplate(
            name="validation_context",
            description="Validation context domain rules for data quality",
            globs=['src/marketpipe/validation/**/*.py'],
            priority="medium",
            content=content
        )
    
    def _generate_storage_context_rule(self) -> RuleTemplate:
        """Generate storage context specific rules."""
        content = """---
description: Storage context domain rules for MarketPipe data persistence
globs:
  - 'src/marketpipe/storage/**/*.py'
alwaysApply: true
priority: medium
---

# Storage Context Rules

## Objective
Ensure storage context encapsulates data persistence concerns with domain focus.

## Rules

### Partition as Domain Concept
Model data partitions as domain entities:

✅ Good:
```python
class DataPartition(Entity):
    def __init__(self, partition_key: PartitionKey, storage_format: StorageFormat):
        super().__init__(EntityId.generate())
        self._partition_key = partition_key
        self._storage_format = storage_format
        self._created_at = datetime.now(timezone.utc)
    
    def update_statistics(self, size_bytes: int, record_count: int) -> None:
        self._size_bytes = size_bytes
        self._record_count = record_count
        self._increment_version()
```

### Storage Events
Raise domain events for storage operations:

✅ Good:
```python
def store_bars(self, bars: List[OHLCVBar]) -> List[DataPartition]:
    partitions = self._partition_strategy.partition_bars(bars)
    created_partitions = []
    
    for partition_key, partition_bars in partitions.items():
        partition = DataPartition(partition_key, self._storage_format)
        await self._storage_engine.write_partition(partition, partition_bars)
        
        # Raise domain event
        event = DataStored(
            symbol=partition_key.symbol,
            trading_date=partition_key.trading_date,
            partition_path=partition_key.to_path(),
            record_count=len(partition_bars),
            file_size_bytes=await self._storage_engine.get_partition_size(partition)
        )
        self._events.append(event)
        
        created_partitions.append(partition)
    
    return created_partitions
```
"""
        
        return RuleTemplate(
            name="storage_context",
            description="Storage context domain rules for data persistence",
            globs=['src/marketpipe/storage/**/*.py'],
            priority="medium",
            content=content
        )
    
    def _format_rule_content(self, rule: RuleTemplate) -> str:
        """Format rule content as .mdc file."""
        return rule.content


def main():
    """Main entry point for rule generation."""
    parser = argparse.ArgumentParser(description="Generate Cursor DDD rules for MarketPipe")
    parser.add_argument("--update-existing", action="store_true", help="Update existing rule files")
    parser.add_argument("--context", help="Generate rules for specific context only")
    
    args = parser.parse_args()
    
    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    
    print(f"🚀 Generating Cursor DDD rules for: {project_root}")
    
    # Generate rules
    generator = CursorRulesGenerator(project_root)
    
    if args.context:
        print(f"🎯 Generating rules for context: {args.context}")
        # Generate context-specific rules only
        context_rule = generator._generate_context_specific_rule(args.context)
        if context_rule:
            rules_path = project_root / ".cursor" / "rules" / "ddd"
            rules_path.mkdir(parents=True, exist_ok=True)
            
            rule_file = rules_path / f"{args.context}_context.mdc"
            with open(rule_file, 'w') as f:
                f.write(generator._format_rule_content(context_rule))
            
            print(f"✅ Generated: {args.context}_context.mdc")
        else:
            print(f"❌ Unknown context: {args.context}")
    else:
        generator.generate_all_rules(update_existing=args.update_existing)
    
    print("\n🎉 Rule generation complete!")
    print("📝 Review the generated rules and commit when ready.")


if __name__ == "__main__":
    main()