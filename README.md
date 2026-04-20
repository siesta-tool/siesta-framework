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

Docker Compose API service:

`docker compose up --build siesta-api`

For container-to-container connectivity, prefer the Docker-specific config:

`SIESTA_CONFIG=/workspace/config/siesta.docker.config.json docker compose up --build siesta-api`
