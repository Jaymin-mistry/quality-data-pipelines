#!/bin/bash

# install virtualenv if not present
pip install virtualenv

# create and activate virtual environment
python3 -m venv dagster_env
source dagster_env/bin/activate

# install dependencies and repo
pip install -r requirements.txt

pip install -e .
