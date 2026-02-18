# siesta-framework

### notes
trace ids are strings, activities are strings, positioning inside trace is 1-indexed (int)

Example usages:
- Run API server (default config) 

`python3 main.py`

- Run API server with specific config

`python3 main.py --config config/siesta.config.json`

- Run a specific module (CLI mode) 

`python3 main.py --config config/preprocess.config.json preprocess`
