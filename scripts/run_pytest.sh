#!/usr/bin/env bash
set -euo pipefail

. venv/bin/activate


usage() {
    echo "Usage: $(basename "$0") tests_directory -- run Python tests in pytest"
}

if [[ "$#" != 1 ]]; then
    usage
    exit 0
fi

AWS_REGION="us-east-1" \
AWS_ACCESS_KEY="ak" \
AWS_SECRET_ACCESS_KEY="sak" \
COGNITO_USER_POOL_ID="dummy_cognito_pool_id" \
COGNITO_APP_CLIENT_ID="dummy_cognito_app_client_id" \
COGNITO_APP_CLIENT_SECRET="dummy_cognito_app_client_secret" \
    pytest -vv -s \
        --junitxml=tests_results.xml \
        --cov-report xml:cov.xml \
        --cov-report term-missing \
        --cov=osdu_commons \
        -W ignore:::localstack.services.generic_proxy \
        $1

