#!/bin/bash
pip install virtualenv

python virtualenv dagster_env
source dagster_env/bin/activate

pip install -r requirements.txt