# variables below usually should not be changed
DOCKER_COMMAND = $(shell bash scripts/get_docker_command.sh)

.PHONY: help package deploy nuke clean test python_tests \
	python_requirements python_test_requirements docker

help:
	@echo 'Commands:'
	@echo 'venv - construct python virtual environment'
	# @echo 'package - creates deployment package'
	@echo 'test - run tests'
	@echo 'clean - clean files created by build'
	@echo 'distclean - clean files created by build and remove python venv and cache'

integration_test: python_integration_test
	@:

test: python_tests
	@:

clean:
	rm -rf .build
	rm -rf packages/
	rm -rf .bin
	find . -type f -name '*.pyc' | xargs rm -rf
	find . -type d -name '__pycache__' | xargs rm -rf
	find . -type f -name '*.egg-info' | xargs rm -rf
	find . -type d -name '*.egg-info' | xargs rm -rf
	find . -type d -name 'build' | xargs rm -rf
	find . -type d -name 'dist' | xargs rm -rf

distclean: clean
	rm -rf venv
	rm -rf .pytest_cache

python_integration_test: venv
	bash scripts/run_pytest.sh integration_tests

python_tests: venv
	bash scripts/run_pytest.sh tests

venv: setup.py tests/requirements.txt
	bash scripts/make_python_venv.sh
	touch venv # to have newer venv and avoid unnecessary repeating this command
