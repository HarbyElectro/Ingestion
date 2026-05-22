# SmartIngest Orchestrator

This patch adds a control-plane orchestrator that follows the SmartIngest ingestion workflow from the paper.

## Architecture mapping

| Paper layer | Code |
|---|---|
| Smart Data Processing Layer | `smartingest/orchestrator.py`, `SmartIngestOrchestrator`, `SmartAgent` |
| Data Source Layer | `configs/smartingest_sources.json`, `DataSource` |
| Temporary Data Staging Layer | `lakehouse/staging/` |
| Data Storage Layer | `lakehouse/tables/` |
| Quarantine / DLQ | `lakehouse/quarantine/` |
| Metadata Catalog | `metadata/catalog.json` |
| Schema Registry | `metadata/schema_registry.json` |
| Application Layer | `catalog["application_layer"]` entries |

## Run

```bash
python run_smartingest.py --config configs/smartingest_sources.json
```

## Outputs

```text
metadata/schema_registry.json
metadata/catalog.json
metadata/latest_run.json
lakehouse/staging/
lakehouse/tables/
lakehouse/quarantine/
```

## Using the current GitHub scripts

The repository currently contains dataset scripts:

```text
MobiAct_Ingestion.py
IMDb_Ingestion.py
CelebA_Ingestion.py
UCF101.py
Kafka_streaming.py
Flink_Ingestion.py
```

The new orchestrator can either:

1. Use its built-in workers: `direct`, `image`, `video`, `text`, `streaming`.
2. Use adapters for existing scripts:
   - `mobiact_script`
   - `imdb_script`
   - `celeba_script`
   - `flink_script`

The current scripts contain hard-coded paths, so the recommended first step is to run the built-in workers with configurable paths from `configs/smartingest_sources.json`.
