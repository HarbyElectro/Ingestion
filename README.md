# SmartIngest: Intelligent Data Ingestion Framework

SmartIngest is a scalable data ingestion framework for processing heterogeneous data sources in a Lakehouse environment. It supports batch and streaming ingestion for structured, semi-structured, unstructured, and real-time data sources. The framework uses a Smart Agent to coordinate schema discovery, metadata extraction, schema evolution, temporary staging, validation, profiling, quarantine handling, and Lakehouse storage.

This repository contains ingestion pipelines for multiple datasets and streaming tools, including MobiAct, IMDb, CelebA, UCF101, Kafka, and Apache Flink.

---

## Repository Contents

```text
Ingestion/
├── MobiAct_Ingestion.py          # Batch ingestion for MobiAct sensor data
├── IMDb_Ingestion.py             # Batch ingestion for IMDb TSV/CSV data
├── CelebA_Ingestion.py           # Batch ingestion for image data
├── UCF101.py                     # Batch ingestion for video data
├── Kafka_streaming.py            # Kafka-based streaming ingestion
├── Flink_Ingestion.py            # Apache Flink ingestion workflow
├── run_smartingest.py            # SmartIngest orchestrator entry point
├── configs/
│   └── smartingest_sources.json  # Data source configuration
├── smartingest/
│   ├── __init__.py
│   ├── core.py                   # Smart Agent, schema registry, metadata catalog
│   ├── workers.py                # Direct, Text, Image, Video, and Streaming workers
│   └── orchestrator.py           # End-to-end SmartIngest workflow
├── metadata/                     # Generated schema registry, catalog, run summaries
└── lakehouse/                    # Generated staging, quarantine, and table outputs
```

---

## SmartIngest Overview

SmartIngest organizes ingestion into four main layers:

1. **Data Source Layer**  
   Captures heterogeneous sources such as relational files, sensor data, text, images, videos, and streaming events.

2. **Smart Data Processing Layer**  
   Uses a Smart Agent to detect modality, select batch or streaming ingestion, discover schemas, extract metadata, route workloads, and coordinate modality-aware workers.

3. **Temporary Data Staging Layer**  
   Stores incoming data temporarily for schema validation, lightweight cleaning, data profiling, and quality checks. Invalid records are redirected to quarantine or a dead-letter queue.

4. **Data Storage and Application Layer**  
   Stores validated data in curated Lakehouse tables and exposes analysis-ready datasets, metadata, lineage, and profiling statistics to downstream analytics and machine learning applications.

---

## Supported Data Modalities

| Modality | Example Sources | Worker |
|---|---|---|
| Structured | CSV, TSV, Parquet, Delta | Direct Worker |
| Semi-structured | JSON, XML, YAML | Direct Worker |
| Unstructured Text | TXT, LOG, MD | Text Worker |
| Images | JPG, PNG, JPEG, WEBP | Image Worker |
| Videos | MP4, AVI, MOV, MKV | Video Worker |
| Streaming | Kafka, Flink, socket streams | Streaming Worker |

> Note: Audio ingestion is intentionally not included in the current implementation.

---

## Datasets

### MobiAct
MobiAct is used for mobile sensor-based activity data. It includes accelerometer and gyroscope readings collected from mobile devices during activities such as walking, jogging, sitting, standing, and stair movement.

### IMDb
IMDb is used as a large structured dataset containing movie-related information such as titles, actors, directors, ratings, and production metadata.

### CelebA
CelebA is used as an image ingestion workload. It contains large-scale celebrity face images with attribute labels, making it suitable for testing image-oriented ingestion and metadata extraction.

### UCF101
UCF101 is used as a video ingestion workload. It contains realistic action videos from multiple categories and is useful for evaluating video ingestion and unstructured data handling.

### Kafka and Flink
Kafka and Apache Flink are used to represent real-time ingestion scenarios and low-latency streaming workflows.

---

## Requirements

Recommended environment:

```text
Python >= 3.9
Apache Spark / PySpark >= 3.3
Delta Lake / delta-spark
Confluent Kafka Python client
Apache Flink / PyFlink
pandas
psutil
sparkmeasure
```

Install Python dependencies:

```bash
pip install -r requirements_smartingest.txt
```

If you are only testing the orchestrator without running Spark, Kafka, or Flink jobs, you can start with:

```bash
pip install pandas psutil
```

---

## Configuration

Data sources are defined in:

```text
configs/smartingest_sources.json
```

Example:

```json
{
  "sources": [
    {
      "name": "MobiAct",
      "path": "data/MobiAct",
      "modality": "structured",
      "ingestion_mode": "batch",
      "target_table": "mobiact_sensor_data",
      "worker": "direct"
    },
    {
      "name": "IMDb",
      "path": "data/IMDb",
      "modality": "structured",
      "ingestion_mode": "batch",
      "target_table": "imdb_movies",
      "worker": "direct"
    },
    {
      "name": "CelebA",
      "path": "data/CelebA",
      "modality": "unstructured",
      "ingestion_mode": "batch",
      "target_table": "celeba_images",
      "worker": "image"
    },
    {
      "name": "UCF101",
      "path": "data/UCF101",
      "modality": "unstructured",
      "ingestion_mode": "batch",
      "target_table": "ucf101_videos",
      "worker": "video"
    },
    {
      "name": "KafkaStream",
      "path": "kafka://localhost:9092/csv_topic",
      "modality": "streaming",
      "ingestion_mode": "stream",
      "target_table": "kafka_events",
      "worker": "streaming"
    }
  ]
}
```

Update the `path` values to match your local dataset locations before running the pipeline.

---

## Running SmartIngest

Run the full SmartIngest orchestration workflow:

```bash
python run_smartingest.py --config configs/smartingest_sources.json
```

Optional arguments:

```bash
python run_smartingest.py \
  --config configs/smartingest_sources.json \
  --registry metadata/schema_registry.json \
  --catalog metadata/catalog.json \
  --staging lakehouse/staging \
  --lakehouse lakehouse/tables \
  --quarantine lakehouse/quarantine \
  --output metadata/latest_run.json
```

---

## Generated Outputs

After execution, SmartIngest generates:

```text
metadata/schema_registry.json     # Registered schema versions
metadata/catalog.json             # Dataset metadata, lineage, profiles, and application-layer entries
metadata/latest_run.json          # Summary of the latest ingestion run
lakehouse/staging/                # Temporary staged data
lakehouse/tables/                 # Validated Lakehouse-ready data
lakehouse/quarantine/             # Invalid or rejected data
```

---

## SmartIngest Workflow

The orchestrator follows this workflow:

1. Initialize the Smart Agent as the control-plane coordinator.
2. Inspect each source and identify its modality.
3. Select batch or streaming ingestion.
4. Discover schema and extract metadata.
5. Compare discovered schema with the schema registry.
6. Register a new schema version if schema evolution is detected.
7. Record metadata and lineage in the catalog.
8. Route data to the temporary staging layer.
9. Allocate the correct worker: Direct, Text, Image, Video, or Streaming.
10. Validate records, apply lightweight cleaning, and compute profiling statistics.
11. Redirect invalid data to quarantine or DLQ.
12. Store validated data in Lakehouse tables.
13. Commit metadata, lineage, schema version, and profiling results.
14. Expose validated datasets to the application layer.

---

## Running Existing Dataset Scripts

The repository also contains standalone scripts for each dataset:

```bash
python MobiAct_Ingestion.py
python IMDb_Ingestion.py
python CelebA_Ingestion.py
python UCF101.py
python Kafka_streaming.py
python Flink_Ingestion.py
```

Some existing scripts may contain local hard-coded paths. Before running them, update dataset paths inside the scripts or move path configuration into `configs/smartingest_sources.json`.

---

## Metadata Catalog

The metadata catalog stores:

- dataset entities
- file entities
- schema versions
- source provenance
- staging paths
- Lakehouse output paths
- lineage records
- profiling statistics
- application-layer access entries

Example catalog output:

```json
{
  "datasets": {},
  "files": [],
  "lineage": [],
  "profiles": [],
  "application_layer": {}
}
```

---

## Schema Registry

The schema registry stores schema versions for each source. If a source changes, SmartIngest registers a new schema version.

Example:

```json
{
  "MobiAct": [
    {
      "source_name": "MobiAct",
      "modality": "structured",
      "fields": {
        "timestamp": "string",
        "acc_x": "string",
        "acc_y": "string",
        "acc_z": "string"
      },
      "version": 1
    }
  ]
}
```

---

## Lakehouse Organization

Validated data is stored under:

```text
lakehouse/tables/<target_table_name>/
```

Rejected data is stored under:

```text
lakehouse/quarantine/<source_name>/
```

This separation helps preserve reliability, traceability, and reproducibility.

---

## Research Context

SmartIngest is designed as an intelligent ingestion framework for Lakehouse systems. It addresses common ingestion challenges such as:

- heterogeneous data formats
- schema discovery
- schema evolution
- metadata consistency
- workload variability
- batch and streaming coordination
- invalid record isolation
- governance and lineage tracking
- analysis-ready data preparation


---

## License

This project is licensed under the Creative Commons Attribution 4.0 International License.

See:

```text
CC BY 4.0
```

---


## Citation

If you use this repository in academic work, cite it as:

```bibtex
@misc{smartingest2026,
  title        = {SmartIngest: Intelligent Data Ingestion Framework for Lakehouse Systems},
  author       = {Ahmed Harby},
  year         = {2026},
  howpublished = {\url{https://github.com/HarbyElectro/Ingestion}},
  note         = {GitHub repository}
}
```
