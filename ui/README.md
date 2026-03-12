# Siesta Framework - Web UI

A beautiful, modern, and minimal web interface for the Siesta Framework event log processing and constraint mining system.

![Siesta Framework UI](.github/ui-preview.png)

## Features

- 🔄 **Preprocess Module**: Upload and process event logs (XES, CSV, JSON)
- ⛏️ **Mining Module**: Discover declarative constraints with configurable parameters
- 💾 **Storage Browser**: Browse S3 buckets and inspect processed logs
- 📊 **Real-time Progress**: Track API request progress and status
- ⚙️ **Settings**: Configure API and S3 connections
- 🎨 **Modern Design**: Clean, minimal interface with dark theme

## Installation

### Prerequisites

- Python 3.10 or higher
- Siesta Framework API running (see main project README)
- S3-compatible storage (MinIO or AWS S3)

### Setup

1. Navigate to the UI directory:
   ```bash
   cd ui
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables (optional):
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

## Running the UI

1. Make sure the Siesta Framework API is running:
   ```bash
   cd ../siesta_framework
   python main.py
   ```

2. In a new terminal, start the Streamlit UI:
   ```bash
   cd ui
   streamlit run app.py
   ```

3. Open your browser and navigate to:
   ```
   http://localhost:8501
   ```

## Quick Start Guide

### 1. Preprocess an Event Log

1. Go to the **Preprocess** page
2. Upload your event log file (XES, CSV, or JSON)
3. Configure field mappings to match your data structure
4. Click **Run Preprocess** to process the log
5. Wait for the processing to complete

### 2. Mine Constraints

1. Navigate to the **Mining** page
2. Select a preprocessed log from the dropdown
3. Choose constraint types to discover
4. Adjust support and confidence thresholds
5. Click **Run Mining** to start constraint discovery
6. View and download the results

### 3. Browse Storage

1. Visit the **Storage Browser** page
2. Select a storage namespace (S3 bucket)
3. Browse available event logs
4. Inspect tables and metadata
5. View storage statistics

## Configuration

### Environment Variables

Create a `.env` file in the `ui/` directory:

```bash
API_BASE_URL=http://localhost:8000
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_REGION=us-east-1
DEFAULT_NAMESPACE=siesta
```

### Runtime Configuration

You can also configure settings through the **Settings** page in the UI:

- **API Connection**: Set the base URL for the Siesta Framework API
- **S3 Storage**: Configure S3 endpoint, credentials, and default namespace
- **Test Connections**: Verify API and S3 connectivity

## Architecture

```
ui/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── .env.example          # Example environment variables
├── pages/                # Page modules
│   ├── preprocess.py     # Preprocess module interface
│   ├── mining.py         # Mining module interface
│   ├── storage_browser.py # S3 storage browser
│   └── settings.py       # Settings and configuration
└── utils/                # Utility modules
    ├── api_client.py     # API client for Siesta Framework
    └── s3_browser.py     # S3 browser utilities
```

## Features in Detail

### Preprocess Module

- **Batch Mode**: Upload and process event log files
- **Streaming Mode**: Initialize real-time event collectors from Kafka
- **Field Mappings**: Configure how your data maps to Siesta's schema
- **Configuration Presets**: Quick-start templates for common log formats
- **Progress Tracking**: Real-time feedback during processing

### Mining Module

- **Constraint Types**:
  - Existential (Existence, Absence, Exactly)
  - Positional (Init, End)
  - Ordered (Response, Precedence, Chain variants)
  - Unordered (Co-existence, Not Co-existence)
  - Negations (Not Response, Not Precedence)
- **Configurable Thresholds**: Adjust support and confidence levels
- **Interactive Results**: Filter, sort, and download constraint results
- **Visualizations**: Metrics and statistics for discovered constraints

### Storage Browser

- **Bucket Navigation**: Browse S3 namespaces
- **Log Inspection**: View metadata and statistics
- **Table Explorer**: Inspect individual tables and their contents
- **Storage Metrics**: Monitor size and file counts

## Customization

### Styling

The UI uses custom CSS for a modern, minimal design. You can customize the appearance by modifying the CSS in [app.py](app.py#L15-L69).

### Adding New Pages

1. Create a new Python file in `pages/`
2. Implement a `render()` function
3. Import and call it from `app.py`

### Extending API Client

Add new API endpoints in [utils/api_client.py](utils/api_client.py) by creating new methods in the `SiestaAPIClient` class.

## Troubleshooting

### API Connection Issues

- Verify the API is running: `http://localhost:8000/docs`
- Check the API URL in Settings
- Use the "Test Connection" button in Settings

### S3 Connection Issues

- Ensure MinIO or S3 is running
- Verify S3 endpoint and credentials in Settings
- Use the "Test S3 Connection" button in Settings

### Missing Dependencies

```bash
pip install -r requirements.txt --upgrade
```

## Development

### Running in Development Mode

```bash
streamlit run app.py --server.runOnSave true
```

This enables auto-reload when you modify files.

### Debug Mode

Add `--logger.level=debug` to see detailed logs:

```bash
streamlit run app.py --logger.level=debug
```

## License

This project is part of the Siesta Framework and follows the same license.

## Contributing

Contributions are welcome! Please ensure your changes:

- Follow the existing code style
- Include appropriate error handling
- Update documentation as needed
- Test against the latest Siesta Framework API

## Support

For issues and questions:

- Check the main Siesta Framework documentation
- Review API documentation at `/docs` endpoint
- Examine browser console for client-side errors
- Check terminal output for server-side errors
