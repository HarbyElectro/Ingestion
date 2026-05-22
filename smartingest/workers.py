from __future__ import annotations

import csv
import importlib
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .core import DataSource, ProfileStats, SchemaInfo


class WorkerResult:
    def __init__(
        self,
        success: bool,
        profile: ProfileStats,
        output_path: Optional[str] = None,
        error: Optional[str] = None,
    ):
        self.success = success
        self.profile = profile
        self.output_path = output_path
        self.error = error


class BaseWorker:
    name = "base"

    def process(
        self,
        source: DataSource,
        staging_path: Path,
        schema: SchemaInfo,
        ingestion_mode: str,
    ) -> WorkerResult:
        start = time.time()
        record_count = self.count_records(staging_path)
        invalid_count = self.count_invalid_records(staging_path, schema)
        valid_count = max(record_count - invalid_count, 0)
        elapsed = time.time() - start
        profile = ProfileStats(
            source_name=source.name,
            modality=schema.modality,
            ingestion_mode=ingestion_mode,
            record_count=record_count,
            invalid_count=invalid_count,
            valid_count=valid_count,
            processing_time_seconds=elapsed,
            ingestion_rate_records_per_second=(valid_count / elapsed if elapsed > 0 else 0.0),
        )
        return WorkerResult(success=invalid_count == 0, profile=profile)

    def count_records(self, staging_path: Path) -> int:
        if staging_path.is_file():
            return 1
        return sum(1 for p in staging_path.rglob("*") if p.is_file())

    def count_invalid_records(self, staging_path: Path, schema: SchemaInfo) -> int:
        return 0


class DirectWorker(BaseWorker):
    """Worker for structured and semi-structured files."""

    name = "direct"

    def count_records(self, staging_path: Path) -> int:
        csv_files = list(staging_path.rglob("*.csv")) + list(staging_path.rglob("*.tsv"))
        if not csv_files:
            return super().count_records(staging_path)

        total = 0
        for file_path in csv_files:
            delimiter = "\t" if file_path.suffix.lower() == ".tsv" else ","
            try:
                with file_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    next(reader, None)
                    total += sum(1 for _ in reader)
            except Exception:
                total += 1
        return total

    def count_invalid_records(self, staging_path: Path, schema: SchemaInfo) -> int:
        expected = list(schema.fields.keys())
        if not expected or expected == ["unknown"]:
            return 0

        invalid = 0
        for file_path in list(staging_path.rglob("*.csv")) + list(staging_path.rglob("*.tsv")):
            delimiter = "\t" if file_path.suffix.lower() == ".tsv" else ","
            try:
                with file_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    header = next(reader, [])
                    if len(header) != len(expected):
                        invalid += 1
            except Exception:
                invalid += 1
        return invalid


class TextWorker(BaseWorker):
    name = "text"

    def count_invalid_records(self, staging_path: Path, schema: SchemaInfo) -> int:
        invalid = 0
        for file_path in staging_path.rglob("*"):
            if not file_path.is_file():
                continue
            try:
                file_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                invalid += 1
        return invalid


class ImageWorker(BaseWorker):
    name = "image"
    valid_extensions = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}

    def count_invalid_records(self, staging_path: Path, schema: SchemaInfo) -> int:
        return sum(
            1 for p in staging_path.rglob("*")
            if p.is_file() and p.suffix.lower() not in self.valid_extensions and p.suffix.lower() != ".csv"
        )


class VideoWorker(BaseWorker):
    name = "video"
    valid_extensions = {".mp4", ".avi", ".mov", ".mkv", ".webm"}

    def count_invalid_records(self, staging_path: Path, schema: SchemaInfo) -> int:
        return sum(
            1 for p in staging_path.rglob("*")
            if p.is_file() and p.suffix.lower() not in self.valid_extensions
        )


class StreamingWorker(BaseWorker):
    name = "streaming"

    def count_records(self, staging_path: Path) -> int:
        return 0


class ExistingScriptWorker(BaseWorker):
    """
    Adapter for the current repository scripts.

    It attempts to call one of the existing functions:
    - MobiAct_Ingestion.MobiAct()
    - IMDb_Ingestion.IMDB()
    - CelebA_Ingestion.CelebA()
    - Flink_Ingestion.Ingest_MobiAct(...)

    Note: the current scripts contain hard-coded paths, so use this adapter only after
    parameterizing those scripts or when running on the original local environment.
    """

    name = "existing_script"

    def __init__(self, module_name: str, function_name: str, kwargs: Optional[Dict[str, Any]] = None):
        self.module_name = module_name
        self.function_name = function_name
        self.kwargs = kwargs or {}

    def process(
        self,
        source: DataSource,
        staging_path: Path,
        schema: SchemaInfo,
        ingestion_mode: str,
    ) -> WorkerResult:
        start = time.time()
        error = None

        try:
            module = importlib.import_module(self.module_name)
            func = getattr(module, self.function_name)
            func(**self.kwargs)
            success = True
        except TypeError:
            try:
                # Existing repo functions usually accept no arguments.
                module = importlib.import_module(self.module_name)
                func = getattr(module, self.function_name)
                func()
                success = True
            except Exception as exc:
                success = False
                error = str(exc)
        except Exception as exc:
            success = False
            error = str(exc)

        elapsed = time.time() - start
        record_count = self.count_records(staging_path)
        profile = ProfileStats(
            source_name=source.name,
            modality=schema.modality,
            ingestion_mode=ingestion_mode,
            record_count=record_count,
            invalid_count=0 if success else record_count,
            valid_count=record_count if success else 0,
            processing_time_seconds=elapsed,
            ingestion_rate_records_per_second=(record_count / elapsed if elapsed > 0 and success else 0.0),
        )
        return WorkerResult(success=success, profile=profile, error=error)


def build_worker(source: DataSource, modality: str):
    options = source.options or {}

    # Use explicit repo script adapters when requested in config.
    if source.worker == "mobiact_script":
        return ExistingScriptWorker("MobiAct_Ingestion", "MobiAct", options.get("kwargs"))
    if source.worker == "imdb_script":
        return ExistingScriptWorker("IMDb_Ingestion", "IMDB", options.get("kwargs"))
    if source.worker == "celeba_script":
        return ExistingScriptWorker("CelebA_Ingestion", "CelebA", options.get("kwargs"))
    if source.worker == "flink_script":
        return ExistingScriptWorker("Flink_Ingestion", "Ingest_MobiAct", options.get("kwargs"))
    if source.worker == "kafka_script":
        # Kafka_streaming.py is script-style and runs on import, so keep this opt-in.
        return ExistingScriptWorker("Kafka_streaming", "__loader__", options.get("kwargs"))

    if source.worker == "image":
        return ImageWorker()
    if source.worker == "video":
        return VideoWorker()
    if source.worker == "text":
        return TextWorker()
    if source.worker == "streaming":
        return StreamingWorker()

    if modality in {"structured", "semi_structured"}:
        return DirectWorker()
    if modality == "streaming":
        return StreamingWorker()
    return TextWorker()
