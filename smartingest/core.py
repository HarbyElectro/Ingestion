from __future__ import annotations

import csv
import json
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DataSource:
    name: str
    path: str
    modality: Optional[str] = None
    ingestion_mode: Optional[str] = None
    target_table: Optional[str] = None
    worker: Optional[str] = None
    options: Optional[Dict[str, Any]] = None


@dataclass
class SchemaInfo:
    source_name: str
    modality: str
    fields: Dict[str, str]
    version: int = 1


@dataclass
class ProfileStats:
    source_name: str
    modality: str
    ingestion_mode: str
    record_count: int
    invalid_count: int
    valid_count: int
    processing_time_seconds: float
    ingestion_rate_records_per_second: float


class SchemaRegistry:
    """Local JSON schema registry with schema evolution detection."""

    def __init__(self, registry_path: str):
        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry: Dict[str, List[Dict[str, Any]]] = self._load()

    def _load(self) -> Dict[str, List[Dict[str, Any]]]:
        if not self.registry_path.exists():
            return {}
        with self.registry_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self) -> None:
        with self.registry_path.open("w", encoding="utf-8") as f:
            json.dump(self.registry, f, indent=2)

    def latest(self, source_name: str) -> Optional[Dict[str, Any]]:
        versions = self.registry.get(source_name, [])
        return versions[-1] if versions else None

    def has_schema_evolution(self, schema: SchemaInfo) -> bool:
        latest = self.latest(schema.source_name)
        if latest is None:
            return True
        return latest.get("fields", {}) != schema.fields or latest.get("modality") != schema.modality

    def register(self, schema: SchemaInfo) -> SchemaInfo:
        versions = self.registry.setdefault(schema.source_name, [])
        schema.version = len(versions) + 1
        versions.append(asdict(schema))
        self._save()
        return schema


class MetadataCatalog:
    """Local metadata catalog storing Dataset, File/TextData/Log-like entities, profiles, and lineage."""

    def __init__(self, catalog_path: str):
        self.catalog_path = Path(catalog_path)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self.catalog = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.catalog_path.exists():
            with self.catalog_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        return {
            "datasets": {},
            "files": [],
            "text_data": [],
            "logs": [],
            "lineage": [],
            "profiles": [],
            "application_layer": {},
        }

    def _save(self) -> None:
        with self.catalog_path.open("w", encoding="utf-8") as f:
            json.dump(self.catalog, f, indent=2)

    def upsert_dataset(
        self,
        source: DataSource,
        schema: SchemaInfo,
        metadata: Dict[str, Any],
        staging_path: str,
        lakehouse_path: Optional[str],
    ) -> None:
        self.catalog["datasets"][source.name] = {
            "entity_type": "Dataset",
            "source": asdict(source),
            "schema": asdict(schema),
            "metadata": metadata,
            "staging_path": staging_path,
            "lakehouse_path": lakehouse_path,
            "updated_at_epoch": time.time(),
        }
        self._save()

    def add_file_entities(self, source_name: str, root_path: str, limit: int = 5000) -> None:
        root = Path(root_path)
        if not root.exists():
            return

        count = 0
        for file_path in root.rglob("*") if root.is_dir() else [root]:
            if not file_path.is_file():
                continue
            self.catalog["files"].append({
                "entity_type": "File",
                "source_name": source_name,
                "path": str(file_path),
                "name": file_path.name,
                "extension": file_path.suffix.lower(),
                "size_bytes": file_path.stat().st_size,
            })
            count += 1
            if count >= limit:
                break
        self._save()

    def record_lineage(self, source_name: str, from_path: str, to_path: str, operation: str) -> None:
        self.catalog["lineage"].append({
            "lineage_id": str(uuid.uuid4()),
            "source_name": source_name,
            "from_path": from_path,
            "to_path": to_path,
            "operation": operation,
            "timestamp_epoch": time.time(),
        })
        self._save()

    def record_profile(self, profile: ProfileStats) -> None:
        self.catalog["profiles"].append(asdict(profile))
        self._save()

    def expose_dataset(self, source_name: str, lakehouse_path: str) -> None:
        self.catalog["application_layer"][source_name] = {
            "status": "available",
            "access_mode": "lakehouse_table_path",
            "lakehouse_path": lakehouse_path,
        }
        self._save()


class SmartAgent:
    """Control-plane coordinator for SmartIngest."""

    STRUCTURED_EXTENSIONS = {".csv", ".tsv", ".parquet", ".delta"}
    SEMI_STRUCTURED_EXTENSIONS = {".json", ".xml", ".yaml", ".yml"}
    TEXT_EXTENSIONS = {".txt", ".md", ".log"}
    IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
    VIDEO_EXTENSIONS = {".mp4", ".avi", ".mov", ".mkv", ".webm"}
    STREAMING_HINTS = {"kafka", "flink", "stream", "topic", "socket"}

    def __init__(
        self,
        registry: SchemaRegistry,
        catalog: MetadataCatalog,
        staging_dir: str,
        lakehouse_dir: str,
        quarantine_dir: str,
    ):
        self.registry = registry
        self.catalog = catalog
        self.staging_dir = Path(staging_dir)
        self.lakehouse_dir = Path(lakehouse_dir)
        self.quarantine_dir = Path(quarantine_dir)

        for directory in [self.staging_dir, self.lakehouse_dir, self.quarantine_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def identify_modality(self, source: DataSource) -> str:
        if source.modality:
            return source.modality

        lower_path = source.path.lower()
        if any(hint in lower_path for hint in self.STREAMING_HINTS):
            return "streaming"

        path = Path(source.path)
        if path.is_file():
            return self._modality_from_extension(path.suffix.lower())

        if path.is_dir():
            extensions = {p.suffix.lower() for p in path.rglob("*") if p.is_file()}
            if extensions & self.STRUCTURED_EXTENSIONS:
                return "structured"
            if extensions & self.SEMI_STRUCTURED_EXTENSIONS:
                return "semi_structured"
            if extensions & self.IMAGE_EXTENSIONS:
                return "unstructured"
            if extensions & self.VIDEO_EXTENSIONS:
                return "unstructured"
            if extensions & self.TEXT_EXTENSIONS:
                return "unstructured"

        return "unstructured"

    def _modality_from_extension(self, extension: str) -> str:
        if extension in self.STRUCTURED_EXTENSIONS:
            return "structured"
        if extension in self.SEMI_STRUCTURED_EXTENSIONS:
            return "semi_structured"
        return "unstructured"

    def select_ingestion_mode(self, source: DataSource, modality: str) -> str:
        if source.ingestion_mode:
            return source.ingestion_mode
        return "stream" if modality == "streaming" else "batch"

    def discover_schema_and_metadata(self, source: DataSource, modality: str) -> Tuple[SchemaInfo, Dict[str, Any]]:
        path = Path(source.path)
        metadata = {
            "source_path": source.path,
            "exists": path.exists(),
            "modality": modality,
            "worker": source.worker,
            "options": source.options or {},
        }

        if path.exists():
            metadata["size_bytes"] = self._size_bytes(path)
            metadata["file_count"] = self._file_count(path)
            metadata["extensions"] = sorted({p.suffix.lower() for p in path.rglob("*") if p.is_file()} if path.is_dir() else {path.suffix.lower()})

        if modality == "structured":
            fields = self._discover_structured_schema(path)
        elif modality == "semi_structured":
            fields = self._discover_semi_structured_schema(path)
        elif modality == "streaming":
            fields = {
                "key": "string",
                "value": "string",
                "timestamp": "timestamp",
                "topic": "string",
                "partition": "integer",
                "offset": "long",
            }
        else:
            fields = {
                "file_path": "string",
                "file_name": "string",
                "extension": "string",
                "size_bytes": "long",
                "content_type": "string",
            }

        return SchemaInfo(source.name, modality, fields), metadata

    def _discover_structured_schema(self, path: Path) -> Dict[str, str]:
        target = self._first_file(path, [".csv", ".tsv"])
        if target is None:
            return {"unknown": "string"}

        delimiter = "\t" if target.suffix.lower() == ".tsv" else ","
        try:
            with target.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader)
                return {col.strip() or f"column_{i}": "string" for i, col in enumerate(header)}
        except Exception:
            return {"unknown": "string"}

    def _discover_semi_structured_schema(self, path: Path) -> Dict[str, str]:
        target = self._first_file(path, [".json"])
        if target is None:
            return {"unknown": "string"}

        try:
            with target.open("r", encoding="utf-8") as f:
                sample = json.load(f)
            if isinstance(sample, list) and sample:
                sample = sample[0]
            if isinstance(sample, dict):
                return {str(k): type(v).__name__ for k, v in sample.items()}
        except Exception:
            pass

        return {"unknown": "string"}

    def _first_file(self, path: Path, suffixes: List[str]) -> Optional[Path]:
        if path.is_file() and path.suffix.lower() in suffixes:
            return path
        if path.is_dir():
            for suffix in suffixes:
                matches = list(path.rglob(f"*{suffix}"))
                if matches:
                    return matches[0]
        return None

    def _size_bytes(self, path: Path) -> int:
        if path.is_file():
            return path.stat().st_size
        return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())

    def _file_count(self, path: Path) -> int:
        if path.is_file():
            return 1
        return sum(1 for p in path.rglob("*") if p.is_file())

    def route_to_staging(self, source: DataSource) -> Path:
        staging_path = self.staging_dir / source.name
        if staging_path.exists():
            shutil.rmtree(staging_path)

        source_path = Path(source.path)
        if source_path.exists() and source_path.is_file():
            staging_path.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, staging_path / source_path.name)
        elif source_path.exists() and source_path.is_dir():
            shutil.copytree(source_path, staging_path)
        else:
            staging_path.mkdir(parents=True, exist_ok=True)

        self.catalog.record_lineage(source.name, source.path, str(staging_path), "route_to_temporary_staging")
        self.catalog.add_file_entities(source.name, str(staging_path))
        return staging_path

    def persist_to_lakehouse(self, source: DataSource, staging_path: Path) -> Path:
        table_name = source.target_table or source.name
        target = self.lakehouse_dir / table_name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(staging_path, target)
        self.catalog.record_lineage(source.name, str(staging_path), str(target), "store_validated_data_in_lakehouse")
        return target

    def quarantine(self, source: DataSource, staging_path: Path, reason: str) -> Path:
        target = self.quarantine_dir / source.name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(staging_path, target)
        self.catalog.record_lineage(source.name, str(staging_path), str(target), f"quarantine:{reason}")
        return target
