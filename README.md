# Siesta Framework

Siesta is a Spark-based process mining and querying framework for event logs.
It can run as:
- an API server (FastAPI), or
- module-oriented CLI jobs (indexer, miner, executor).

Core data assumptions:
- `trace_id` is a string; it may be derived at indexing time from a combination of source
  columns instead of a single one (see `field_mappings` in the indexer config)
- `activity` is a string
- `position` inside a trace is 0-indexed integer (derived from timestamp)

## Demo video

[Watch the demo](https://canva.link/85edzr7tp3yuxfg)

## What each module does

- `indexer`: ingests batch/stream events and builds storage indexes/tables.
- `miner`: discovers constraints from stored traces.
- `executor`: executes statistics/detection/exploration queries over indexed logs.
- `analyser`: process-mining analytics (directly-follows, durations, loop/deviation detection, DFG/BPMN/Petri net models, bottlenecks).
- `manager`: log/metadata management endpoints.

## Architecture
![Siesta Framework Architecture](siesta-architecture.svg)

## Project layout

- `main.py`: top-level entrypoint.
- `siesta/core`: framework bootstrapping (config, Spark, storage factory, interfaces).
- `siesta/model`: shared schemas and typed config/data models (system, storage, mining/event structures).
- `siesta/modules`: feature modules (`indexer`, `miner`, `executor`, `analyser`, `manager`).
- `siesta/storage`: storage implementations (currently S3/MinIO-based).
- `config`: sample runtime and module config JSON files.
- `docker-compose.yml` + `siesta/dockerbase`: API + Spark + MinIO + Kafka stack.
- `tests`: integration/unit tests and test utilities.
- `ui`: standalone Streamlit frontend for the API (see `ui/README.md`).
- `demo-eval`: benchmark/evaluation scripts (see [Demo-eval benchmarks](#demo-eval-benchmarks) below).

## Dependencies

Requirements are split per submodule (`**/requirements.txt`).

Install all framework dependencies:

```bash
python3 siesta/install_dependencies.py
```

Install test dependencies:

```bash
python3 tests/install_dependencies.py
```

## Running Siesta

### 1. API mode

Default config (from siesta/model/SystemModel/DEFAULT_SYSTEM_CONFIG):

```bash
python3 main.py
```

Specific system config:

```bash
python3 main.py --config config/siesta.config.json
```

API routes are auto-registered from modules and exposed as:
- `POST /indexing/run`
- `POST /mining/run`
- `POST /querying/statistics`, `POST /querying/detection`, `POST /querying/exploration`

### 2. CLI module mode

General pattern:

```bash
python3 main.py --config <system_config.json> <module> <module_args>
```

`<module>` is each module's CLI name (`indexer`, `miner`, `executor`), not its API route prefix.

Examples:

```bash
# Index (batch/stream setup)
python3 main.py --config config/siesta.config.json indexer --index_config config/index.config.json

# Mining
python3 main.py --config config/siesta.config.json miner --mining_config config/mining.config.json

# Querying
python3 main.py --config config/siesta.config.json executor --query_config config/query.config.json
```

## Configuration files

- `config/siesta.config.json`: local host-oriented system config.
- `config/siesta.docker.config.json`: container-network hostnames (`minio`, `kafka`, `spark-master`).
- `config/index.config.json`: ingestion and field mappings.
- `config/mining.config.json`: mining categories, thresholds, output path.
- `config/query.config.json`: method and query payload.
- `config/analyser.config.json`: analyser method, target log, output path.

## Docker (recommended for full stack)

Start API and dependencies:

```bash
docker compose up --build siesta-api
```

Use Docker-aware config explicitly (service-to-service hostnames):

```bash
SIESTA_CONFIG=/workspace/config/siesta.docker.config.json docker compose up --build siesta-api
```

This starts:
- `siesta-api`
- `spark-master`, `spark-worker`, `spark-worker2`
- `minio`
- `kafka`, `zookeeper`

`siesta/dockerbase` contains image definitions and entrypoints used by compose:
- `API/`: API container image and startup script.
- `Spark/`: Spark image and logging config.
- `Kafka/`: Kafka image and startup script.

## Extending Siesta (developer hints)

### Add a new module

1. Create `siesta/modules/<YourModule>/main.py`.
2. Implement a class that extends `SiestaModule`:
	- set `name` and `version`
	- implement `startup()`
	- implement `cli_run(args, **kwargs)`
	- optionally implement `register_routes()` for API endpoints
3. Add module-specific dependencies in `siesta/modules/<YourModule>/requirements.txt`.

Module discovery is automatic from `siesta.modules.*.main`.

### Add a new storage backend

1. Implement a new class extending `StorageManager` (see `siesta/core/interfaces.py`).
2. Add it under `siesta/storage/<Backend>/`.
3. Register it in `StorageManagerFactory._registry` (in `siesta/core/storageFactory.py`).
4. Set `storage_type` in system/module config to your backend key.

## Practical run order

1. Run indexer on a log.
2. Run miner/executor on the same `log_name` and `storage_namespace`.
3. Check outputs under `output/` (and persisted data in configured storage).

## Demo-eval benchmarks

`demo-eval/` holds standalone benchmark/evaluation scripts that drive a running Siesta stack (and, for the ELK comparison, a separate Elasticsearch instance). They are not part of the framework's module system — run them directly with Python. Each script also accepts `--help` for its full flag list.

### SIESTA vs. ELK query-latency benchmark (`exp_predicates.py`)

Compares SIESTA query latency against Elasticsearch as a function of the number of event-attribute predicates, at a fixed pattern length.

```bash
# 1. Start Elasticsearch
docker compose -f demo-eval/docker-compose-elk.yml up -d

# 2. Start the Siesta API (see "Docker" above), then index the target log into both stores
python3 demo-eval/exp_predicates.py --log bpic2017 --ingest-elk --ingest-siesta

# 3. Run the benchmark (assumes both stores are already indexed)
python3 demo-eval/exp_predicates.py --log bpic2017

# Dry run: print the generated queries without hitting either server
python3 demo-eval/exp_predicates.py --log bpic2017 --dry-run
```

Results are written as `demo-eval/exp_pred_<log>_len<N>_summary.csv`, `..._queries.jsonl`, and a latency plot PNG.

### Time-to-Queryable (TTQ) streaming sweep (`ttq_run.py`)

Measures how long streamed events take to become queryable, sweeping the Kafka injection rate. Requires the full Docker stack (`docker compose up --build siesta-api`) with Kafka reachable.

```bash
# Smoke test (short sweep, quick sanity check)
python3 demo-eval/ttq_run.py --smoke

# Full sweep (defaults to lambdas 250..3000 events/s, driving the siesta-api container)
python3 demo-eval/ttq_run.py

# Optional: commit-visibility sanity check against a running API
python3 demo-eval/ttq_sanity.py --endpoint http://localhost:8000
```

`ttq_run.py` orchestrates `ttq_producer.py` (Kafka replay) per lambda, collects results into `demo-eval/ttq_results.csv`, and calls `ttq_analyze.py` to produce `ttq_summary.csv` and `ttq_plot.png`. It uses `demo-eval/ttq_index.config.json` as the base streaming index config.
