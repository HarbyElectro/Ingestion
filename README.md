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

## Notes

This repository is research-oriented and intended for experimentation with heterogeneous ingestion workflows. For production deployment, additional integration may be required with services such as Apache Atlas, Unity Catalog, AWS Glue Data Catalog, Confluent Schema Registry, or enterprise monitoring systems.


# Ingestion

# Requirements
  * Python 3.5
  * Apache Spark (Pyspark Python) version 3.3.2
  * Delta LH (delta python) library, version 0.4.2
  * Confluent Kafka 2.3.0 Python library
  * Pyflink 1.9 Python library
  
  
# DataSets
* MobiAct: We use the MobiAct dataset, which is a publicly available dataset of accelerometer and gyroscope sensor readings collected from mobile devices during various physical activities. The dataset contains sensor data for activities such as walking, jogging, sitting, standing, and ascending and descending stairs, among others. To collect the data, we will download the MobiAct dataset from the source website and extract the relevant sensor data.

* IMDb: We opted to use the IMDb database‎ instead of a synthetic data set, as it contains extensive information on movies, actors, directors, and production companies. The dataset at hand is rather intricate, occupying a considerable amount of storage, amounting to 5.34 GB in TSV format. The complexity of the dataset may pose a challenge to its processing and analysis.

* CelebA: CelebA is a large-scale dataset with over 200,000 celebrity images and 40 attribute labels describing facial characteristics and features. The dataset is diverse, with people from different backgrounds, genders, and age groups. CelebA has been used for various tasks, including facial attribute prediction, face detection and recognition, and generative model training.

* UCF101: The UCF101 dataset contains over 13320 realistic action videos from 101 categories, making it the most diverse action recognition dataset available. Unlike other datasets, UCF101 features realistic representations of actions, offering additional context through 25 distinct groups. The dataset is challenging due to variations in camera motion, object appearance and pose, object scale, viewpoint, cluttered background, and illumination conditions. UCF101 is a unique and invaluable resource for advancing the field of action recognition. The action categories can be divided into five types: a) Human-Object Interaction, b) Body-Motion Only 3) Human-Human Interaction 4) Playing Musical Instruments 5) Sports. However, ingesting video data is a complex process that varies depending on the application and use case. By ingesting the UCF101 dataset, researchers and developers can rely on complex video datasets to create, train, and test algorithms, models, and systems in challenging real-world scenarios.

# Data Pipelines

![image](https://github.com/HarbyElectro/Ingestion/assets/152432979/5b5f793f-d2d9-4f84-9429-e6e0862cdfbc)

<p xmlns:cc="http://creativecommons.org/ns#" xmlns:dct="http://purl.org/dc/terms/"><a property="dct:title" rel="cc:attributionURL" href="https://github.com/HarbyElectro/Ingestion.git">SmartIngest</a> by <span property="cc:attributionName">Ahmed Harby</span> is licensed under <a href="https://creativecommons.org/licenses/by/4.0/?ref=chooser-v1" target="_blank" rel="license noopener noreferrer" style="display:inline-block;">CC BY 4.0<img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/cc.svg?ref=chooser-v1"><img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/by.svg?ref=chooser-v1"></a></p>



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
