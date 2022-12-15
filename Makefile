ifneq (,$(wildcard ./.env))
    include .env
    export
endif
TWINE=$(shell which twine)
PYTHON3=$(shell which python3)
VERSION:=$(shell python3 gha_retrieve_version.py)
TAG:= "astronomer-cosmos-v$(VERSION)"

help:
	@grep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: ## check doc formatting
	$(PYTHON3) setup.py checkdocs

package: ## create a new pypi dist
	$(PYTHON3) setup.py sdist

test-ship: ## ship to test pypi dist
	$(TWINE) upload --repository testpypi dist/astronomer-cosmos-${VERSION}.tar.gz -u ${TWINE_USER} -p ${TWINE_TEST_PASS}

ship: ## ship pypi dist
ship: package
	$(TWINE) upload dist/astronomer-cosmos-${VERSION}.tar.gz -u ${TWINE_USER} -p ${TWINE_PASS}

.PHONY: delete-tag
delete-tag:
	- git tag -d $(TAG)
	- git push origin --delete $(TAG)

.PHONY: tag
tag: delete-tag
	git tag $(TAG)
	git push origin $(TAG)

.PHONY: build
build:
	python3 setup.py sdist

.PHONY: install_dev
install_dev:
	python -m pip install --upgrade pip
	python -m pip install -r dev-requirements.txt

.PHONY: pre-commit-install
pre-commit-install:
	pre-commit install
