"""Legacy ParquetDataStorage - re-exported from new storage engine."""

# Re-export the production storage engine to maintain backward compatibility
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine as ParquetDataStorage

__all__ = ["ParquetDataStorage"]
