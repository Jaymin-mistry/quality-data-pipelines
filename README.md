# Quality data pipelines

This is the repo for the EARL 2024 conference workshop "Building data pipelines in Python" https://earl-conference.com/  This repo contains the skeleton code to start the project,

## Getting started on Linux/Mac

```bash
# install virtualenv if not present
pip install virtualenv

# create and activate virtual environment
python3-m venv dagster_env
source dagster_env/bin/activate

# install dependencies and repo
pip install -r requirements.txt
pip install -e .
```

## Getting started on Windows

```powershell
# Install virtualenv if not present
pip install virtualenv

# Create and activate virtual environment
python -m venv dagster_env
./dagster_env/Scripts/activate

# Install dependencies and repo
pip install -r requirements.txt
pip install -e .
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `quality_data_pipelines/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Unit testing

Tests are in the `quality_data_pipelines_tests` directory and you can run tests using `pytest`:

```bash
pytest quality_data_pipelines_tests
```
