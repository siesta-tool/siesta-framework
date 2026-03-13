# Siesta Framework

A distributed event log processing and declarative constraint mining framework built with Apache Spark.

## Features

- 🔄 **Event Log Preprocessing**: Support for XES, CSV, and JSON formats
- ⛏️ **Constraint Mining**: Discover declarative constraints (existential, positional, ordered, unordered, negations)
- 💾 **Distributed Storage**: S3-compatible storage with Delta Lake
- 🌊 **Streaming Support**: Real-time event processing via Kafka
- 🎨 **Modern Web UI**: Beautiful, minimal interface for all operations
- 📡 **REST API**: FastAPI-based API for programmatic access

## Quick Start

### Prerequisites

- Python 3.10+
- Apache Spark
- S3-compatible storage (MinIO or AWS S3)
- Kafka (optional, for streaming)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd siesta-framework
   ```

2. Create and activate a virtual environment (Recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   You can choose which dependencies to install using the provided scripts:

   - **All dependencies** (core, UI, and testing):
     ```bash
     python install_dependencies.py
     ```
   - **Core Siesta framework only**:
     ```bash
     cd siesta
     python install_dependencies.py
     ```
   - **Web UI only**:
     ```bash
     cd ui
     python install_dependencies.py
     ```
   - **Testing only**:
     ```bash
     cd tests
     python install_dependencies.py
     ```

### Running the Framework

Since the components run independently, use the two separate main files to start the UI and API.

#### 1. API Server

Start the core API server (default config):
```bash
cd siesta
python main.py
```

Run with specific config:
```bash
python main.py --config ../config/siesta.config.json
```
- **API Docs**: http://localhost:8000/docs

#### 2. Web UI

In a new terminal window (with the virtual environment activated), run the UI:
```bash
cd ui
streamlit run main.py
```
- **Web UI**: http://localhost:8501

#### CLI Mode

Run a specific module directly (bypassing the API server):
```bash
cd siesta
python main.py --config ../config/preprocess.config.json preprocess
```

## Architecture

```
siesta-framework/
├── config/                 # Configuration files
│   ├── siesta.config.json
│   ├── preprocess.config.json
│   └── mining.config.json
├── siesta/               # Core framework
│   ├── core/              # Core components
│   ├── modules/           # Processing modules
│   │   ├── Preprocess/   # Event log preprocessing
│   │   └── Mining/       # Constraint mining
│   ├── storage/          # Storage implementations
│   ├── api/              # REST API
│   └── main.py           # Entry point
├── ui/                    # Web interface
│   ├── app.py            # Streamlit app
│   ├── pages/            # UI pages
│   └── utils/            # UI utilities
├── datasets/             # Sample datasets
└── output/              # Mining results
```

## Usage Examples

### Preprocessing an Event Log

#### Via Web UI
1. Navigate to the **Preprocess** page
2. Upload your event log file
3. Configure field mappings
4. Click **Run Preprocess**

#### Via API
```bash
curl -X POST "http://localhost:8000/preprocess/run" \
  -F "preprocess_config={\"log_name\":\"my_log\",\"storage_namespace\":\"siesta\",\"overwrite_data\":false}" \
  -F "log_file=@datasets/example.csv"
```

#### Via CLI
```bash
cd siesta
python main.py --config ../config/preprocess.config.json preprocess
```

### Mining Constraints

#### Via Web UI
1. Navigate to the **Mining** page
2. Select a preprocessed log
3. Choose constraint types and thresholds
4. Click **Run Mining**
5. View and download results

#### Via API
```bash
curl -X POST "http://localhost:8000/mining/run" \
  -F "mining_config={\"log_name\":\"my_log\",\"storage_namespace\":\"siesta\",\"force_recompute\":true}"
```

#### Via CLI
```bash
cd siesta
python main.py --config ../config/mining.config.json mining
```

## Configuration

### System Configuration (siesta.config.json)

```json
{
  "storage_type": "s3",
  "s3_endpoint": "http://localhost:9000",
  "s3_access_key": "minioadmin",
  "s3_secret_key": "minioadmin",
  "spark_master": "spark://localhost:7077",
  "api": {
    "host": "0.0.0.0",
    "port": 8000
  }
}
```

### Preprocessing Configuration

```json
{
  "log_name": "my_event_log",
  "storage_namespace": "siesta",
  "field_mappings": {
    "csv": {
      "activity": "activity",
      "trace_id": "case_id",
      "start_timestamp": "timestamp"
    }
  }
}
```

### Mining Configuration

```json
{
  "log_name": "my_event_log",
  "storage_namespace": "siesta",
  "output_path": "output/constraints.csv",
  "constraint_types": ["existential", "ordered"],
  "support_threshold": 0.8,
  "confidence_threshold": 0.9
}
```

## Module Overview

### Preprocess Module

Transforms raw event logs into structured, indexed tables ready for constraint mining.

**Supported formats**: XES, CSV, JSON  
**Output tables**: events, sequence, activity_index, metadata

### Mining Module

Discovers declarative constraints from preprocessed logs.

**Constraint types**:
- Existential (Existence, Absence, Exactly)
- Positional (Init, End)
- Ordered (Response, Precedence, ChainResponse, ChainPrecedence)
- Unordered (CoExistence, NotCoExistence)
- Negations (NotResponse, NotPrecedence)

## Storage

Siesta uses S3-compatible storage with Delta Lake format for efficient distributed data processing.

**Tables created per log**:
- `events`: Raw event data
- `sequence`: Ordered event sequences
- `activity_index`: Activity lookup table
- `pairs_index`: Activity pair relationships
- `count`: Statistics and counts
- `metadata`: Log metadata

## Development

### Adding a New Module

1. Create module directory in `siesta/modules/`
2. Implement `SiestaModule` interface
3. Add `register_routes()` for API endpoints
4. Implement `cli_run()` for CLI mode
5. Framework auto-discovers the module

### Running Tests

```bash
pytest tests/
```

## Technical Notes

- Trace IDs: strings
- Activities: strings  
- Position in trace: 1-indexed integers
- All timestamps are stored in ISO format
- Delta Lake provides ACID transactions and versioning

## License

[Add your license here]

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing patterns
- Tests pass
- Documentation is updated
- Commit messages are clear

## Support

For issues, questions, or contributions, please visit the project repository.
