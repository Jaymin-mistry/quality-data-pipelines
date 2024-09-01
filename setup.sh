#!/bin/bash

# install virtualenv if not present 
pip install virtualenv

# create and activate virtual environment
python virtualenv dagster_env
source dagster_env/bin/activate

# install dependencies and repo
pip install -r requirements.txt

pip install -e .