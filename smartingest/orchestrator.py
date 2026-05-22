from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from .core import DataSource, MetadataCatalog, SchemaRegistry, SmartAgent
from .workers import build_worker


class SmartIngestOrchestrator:
    """
    Implements Algorithm: SmartIngest Ingestion Workflow.

    The orchestrator:
    1. Initializes Smart Agent.
    2. Iterates over heterogeneous sources.
    3. Detects modality and batch/stream mode.
    4. Discovers schema and metadata.
    5. Detects schema evolution and registers new versions.
    6. Updates catalog and lineage.
    7. Routes to temporary staging.
    8. Allocates modality-aware workers.
    9. Validates, cleans, profiles.
    10. Sends invalid data to quarantine/DLQ.
    11. Stores valid data in Lakehouse tables.
    12. Exposes datasets to the application layer.
    """

    def __init__(self, agent: SmartAgent):
        self.agent = agent

    def run(self, sources: List[DataSource]) -> List[Dict[str, Any]]:
        results = []

        for source in sources:
            print(f"\n[SmartIngest] Source: {source.name}")

            modality = self.agent.identify_modality(source)
            ingestion_mode = self.agent.select_ingestion_mode(source, modality)
            print(f"  modality={modality}, ingestion_mode={ingestion_mode}")

            schema, metadata = self.agent.discover_schema_and_metadata(source, modality)

            if self.agent.registry.has_schema_evolution(schema):
                schema = self.agent.registry.register(schema)
                print(f"  schema=evolved_or_new, version={schema.version}")
            else:
                latest = self.agent.registry.latest(source.name)
                schema.version = latest.get("version", schema.version) if latest else schema.version
                print(f"  schema=reused, version={schema.version}")

            staging_path = self.agent.route_to_staging(source)
            self.agent.catalog.upsert_dataset(source, schema, metadata, str(staging_path), lakehouse_path=None)

            worker = build_worker(source, modality)
            print(f"  worker={worker.name}")

            worker_result = worker.process(source, staging_path, schema, ingestion_mode)
            self.agent.catalog.record_profile(worker_result.profile)

            if not worker_result.success:
                quarantine_path = self.agent.quarantine(
                    source,
                    staging_path,
                    reason=worker_result.error or "schema_or_quality_violation",
                )
                result = {
                    "source": source.name,
                    "status": "quarantined",
                    "modality": modality,
                    "ingestion_mode": ingestion_mode,
                    "schema_version": schema.version,
                    "profile": asdict(worker_result.profile),
                    "quarantine_path": str(quarantine_path),
                    "error": worker_result.error,
                }
                print("  status=quarantined")
            else:
                lakehouse_path = self.agent.persist_to_lakehouse(source, staging_path)
                self.agent.catalog.upsert_dataset(source, schema, metadata, str(staging_path), str(lakehouse_path))
                self.agent.catalog.expose_dataset(source.name, str(lakehouse_path))

                result = {
                    "source": source.name,
                    "status": "stored",
                    "modality": modality,
                    "ingestion_mode": ingestion_mode,
                    "schema_version": schema.version,
                    "profile": asdict(worker_result.profile),
                    "lakehouse_path": str(lakehouse_path),
                }
                print("  status=stored")

            results.append(result)

        return results


def load_sources(config_path: str) -> List[DataSource]:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    return [
        DataSource(
            name=item["name"],
            path=item["path"],
            modality=item.get("modality"),
            ingestion_mode=item.get("ingestion_mode"),
            target_table=item.get("target_table"),
            worker=item.get("worker"),
            options=item.get("options", {}),
        )
        for item in raw.get("sources", [])
    ]


def build_orchestrator(args) -> SmartIngestOrchestrator:
    registry = SchemaRegistry(args.registry)
    catalog = MetadataCatalog(args.catalog)
    agent = SmartAgent(
        registry=registry,
        catalog=catalog,
        staging_dir=args.staging,
        lakehouse_dir=args.lakehouse,
        quarantine_dir=args.quarantine,
    )
    return SmartIngestOrchestrator(agent)
