# Quality data pipelines

This is the repo for the EARL 2024 conference workshop "Building data pipelines in Python" https://earl-conference.com/  This repo contains the skeleton code to start the project, 

## Getting started

If you have a bash compatible system, you can setup the project by executing the  `setup.sh` script

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in [&#34;editable mode&#34;](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
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
