#!/usr/bin/env bash

set -ex
ROOT_DIR=$(pwd)

python3 -m venv venv
. venv/bin/activate

pip install --upgrade pip==18.1

python3 setup.py install

cd ${ROOT_DIR}/integration_tests
pip install --upgrade --process-dependency-links --requirement requirements.txt

cd ${ROOT_DIR}/scripts/manifest_loader
pip install --upgrade --process-dependency-links --requirement requirements.txt

cd ${ROOT_DIR}/scripts/osdu_summary
pip install --upgrade --process-dependency-links --requirement requirements.txt

cd ${ROOT_DIR}/tests
pip install --upgrade --process-dependency-links --requirement requirements.txt
