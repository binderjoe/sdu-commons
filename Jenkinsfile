#!groovy

ROLE = 'arn:aws:iam::xxxxxxxxxxxx:role/iam-sharedservices-terraform-jenkins-dataplatform'

pipeline {
    agent {
        kubernetes {
            label "sdu-commons-pod-${UUID.randomUUID().toString()}"
            defaultContainer 'default'
            yaml """
---
apiVersion: v1
kind: Pod
metadata:
  namespace: shared-services
  annotations:
    iam.amazonaws.com/role: $ROLE
spec:
  containers:
    - image: python:3.6
      name: default
      tty: true
 """

        }
    }

    environment {
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_REGION = 'us-east-1'

        COGNITO_USER_NAME = 'test-user-001'
        COGNITO_USER_POOL_ID = 'us-east-1_'
        COGNITO_APP_CLIENT_ID = ''

        WORKFLOW_SERVICE_URL = 'https://workflow.aws-us-east-1.test-e2e.osdu-rds.com/v1/'
        DELIVERY_SERVICE_URL = 'https://delivery.aws-us-east-1.test-e2e.osdu-rds.com/api/v1/'
    }

    stages {
        stage('Install commons') {
            steps {
                sh 'python setup.py install'
            }
        }

        stage('Unit Tests') {
            steps {
                sh """
                pip install -r tests/requirements.txt
                python -m pytest \
                    --junitxml unit_tests_results.xml \
                    --cov-report xml:unit_tests_coverage.xml \
                    --cov-report term-missing \
                    --cov sdu_commons \
                    -W ignore:::localstack.services.generic_proxy \
                    tests
                """
            }
        }

        stage('Integration Tests') {
            steps {
                withCredentials([
                    string(credentialsId: 'COGNITO_APP_CLIENT_SECRET_PING_DEV', variable: 'COGNITO_APP_CLIENT_SECRET'),
                    string(credentialsId: 'COGNITO_USER_PASSWORD_PING_DEV_APPS', variable: 'COGNITO_USER_PASSWORD')
                ]) {
                    sh """
                    pip install -r integration_tests/requirements.txt
                    python -m pytest -svvv \
                        --junitxml integration_tests_results.xml \
                        --cov-report xml:integration_tests_coverage.xml \
                        --cov-report term-missing \
                        --cov sdu_commons \
                        -W ignore:::localstack.services.generic_proxy \
                        integration_tests
                    """
                }
            }
        }
    }

    post {
        always {
            junit '*_tests_results.xml'
            cobertura coberturaReportFile: '*_tests_coverage.xml'
        }
    }
}
