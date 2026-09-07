SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

mvn = mvn

ifdef IS_CICD
    mvn = mvn --no-transfer-progress
endif

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BIN := $(MAKEFILE_PATH)/bin
OS := $(shell uname | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)
DOCKER_COMPOSE_VERSION := 2.34.0

MAVEN_GPG_PASSPHRASE ?=
MAVEN_OPTS ?=

SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

RELEASE_SKIP_TESTS ?= false
RELEASE_TARGET_TAG ?=
RELEASE_VERSION ?=

ifeq ($(ARCH),aarch64)
	DOCKER_COMPOSE_DOWNLOAD_URL := "https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(OS)-aarch64"
else ifeq ($(ARCH),x86_64)
	DOCKER_COMPOSE_DOWNLOAD_URL := "https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(OS)-x86_64"
else
	@printf 'Unknown architecture "%s"\n', "$(GOARCH)"
	@exit 69
endif

COMPOSE = bin/docker-compose -f $(MAKEFILE_PATH)/test/docker-compose.yml

SCYLLA_IMAGE := scylladb/scylla:2025.1
DOCKER_CACHE_DIR := $(MAKEFILE_PATH)/.docker-cache
DOCKER_CACHE_FILE := $(DOCKER_CACHE_DIR)/scylla-image.tar
CERT_CACHE_DIR := $(MAKEFILE_PATH)/.cert-cache
CERT_DIR := $(MAKEFILE_PATH)/test/scylla

.PHONY: clean verify lint lint-specs lint-fix compile compile-test compile-demo test test-unit test-integration test-demo release-prepare release release-dry-run

clean:
	${mvn} clean

verify:
	${mvn} verify

lint:
	${mvn} license:check
	${mvn} fmt:check
	${mvn} checkstyle:check
	${mvn} compile test-compile
	$(MAKE) lint-specs
	$(MAKE) lint-docs

lint-specs:
	${mvn} test -Dtest=FeatureSpecFormatTest,FeatureSpecVectorsTest,FeatureSpecDefaultsTest,FeatureSpecCanonicalEndpointVectorsTest,FeatureSpecNodeHealthTransitionsTest

lint-docs:
	${mvn} javadoc:test-javadoc javadoc:test-aggregate javadoc:test-aggregate-jar javadoc:test-jar javadoc:test-resource-bundle
	${mvn} javadoc:jar javadoc:aggregate javadoc:aggregate-jar javadoc:resource-bundle

lint-fix:
	${mvn} license:format
	${mvn} fmt:format

compile:
	${mvn} compile

compile-test:
	${mvn} test-compile

compile-demo:
	${mvn} test-compile

test-unit:
	${mvn} test

.PHONY: wait-for-alternator
wait-for-alternator:
	@echo "Waiting for Alternator to be ready..."
	@for i in $$(seq 1 60); do \
		if curl -sf http://172.39.0.2:9998/ >/dev/null 2>&1; then \
			echo "Alternator is ready (waited $${i}s)"; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "Timed out waiting for Alternator"; \
			exit 1; \
		fi; \
		sleep 1; \
	done

.PHONY: test-integration
test-integration: scylla-start wait-for-alternator
	INTEGRATION_TESTS=true ALTERNATOR_HOST=172.39.0.2 ALTERNATOR_PORT=9998 ALTERNATOR_HTTPS_PORT=9999 ALTERNATOR_CA_CERT_PATH=$$(pwd)/test/scylla/db.crt \
		${mvn} test -Dtest="**/*IT" -DfailIfNoTests=false -Dsurefire.timeout=300 || (make scylla-stop && exit 1)
	make scylla-stop

.PHONY: test-demo
test-demo: scylla-start wait-for-alternator
	${mvn} exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo2 -Dexec.classpathScope=test -Dexec.args="--endpoint http://172.39.0.2:9998" || (make scylla-stop && exit 1)
	${mvn} exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo3 -Dexec.classpathScope=test -Dexec.args="--endpoint http://172.39.0.2:9998" || (make scylla-stop && exit 1)
	make scylla-stop

.PHONY: test-all
test-all: scylla-start wait-for-alternator
	INTEGRATION_TESTS=true ALTERNATOR_HOST=172.39.0.2 ALTERNATOR_PORT=9998 ALTERNATOR_HTTPS_PORT=9999 ALTERNATOR_CA_CERT_PATH=$$(pwd)/test/scylla/db.crt \
		${mvn} test -Dtest="**/*IT" -DfailIfNoTests=false -Dsurefire.timeout=300 || (make scylla-stop && exit 1)
	${mvn} exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo2 -Dexec.classpathScope=test -Dexec.args="--endpoint http://172.39.0.2:9998" || (make scylla-stop && exit 1)
	${mvn} exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo3 -Dexec.classpathScope=test -Dexec.args="--endpoint http://172.39.0.2:9998" || (make scylla-stop && exit 1)
	make scylla-stop

.PHONY: release-prepare
release-prepare:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi

	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	NEXT_RELEASE=""
	if [[ -n "${RELEASE_VERSION}" ]]; then
		NEXT_RELEASE="-DreleaseVersion=${RELEASE_VERSION}"
	fi
	export MAVEN_OPTS
	${mvn} release:prepare -DpushChanges=false $${NEXT_RELEASE}

.PHONY: release
release:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/release-logs/ 2>/dev/null || true
	${mvn} release:perform -Drelease.autopublish=true > >(tee /tmp/release-logs/stdout.log) 2> >(tee /tmp/release-logs/stderr.log)

.PHONY: release-dry-run
release-dry-run:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	@if [[ -n "${RELEASE_SKIP_TESTS}" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/release-logs/ 2>/dev/null || true
	${mvn} release:perform > >(tee /tmp/release-logs/stdout.log) 2> >(tee /tmp/release-logs/stderr.log)

.prepare-environment-update-aio-max-nr:
	@if (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

.prepare-docker-compose: .prepare-bin
	@if [[ -f "$(BIN)/docker-compose" ]] && "$(BIN)/docker-compose" --version 2>/dev/null | grep "$(DOCKER_COMPOSE_VERSION)" >/dev/null; then
		echo "docker-compose $(DOCKER_COMPOSE_VERSION) is already installed"
	else
		echo "Downloading $(BIN)/docker-compose";
		curl --progress-bar -L $(DOCKER_COMPOSE_DOWNLOAD_URL) --output "$(BIN)/docker-compose";
		chmod +x "$(BIN)/docker-compose";
	fi

.prepare-bin:
	@[ -d "$(BIN)" ] || mkdir "$(BIN)";

.PHONY: .prepare-cert
.prepare-cert:
	@[ -f "${MAKEFILE_PATH}/test/scylla/db.key" ] || (echo "Prepare certificate" && cd ${MAKEFILE_PATH}/test/scylla/ && openssl req -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" -x509 -newkey rsa:4096 -keyout db.key -out db.crt -days 3650 -nodes -addext "subjectAltName=IP:172.39.0.2,IP:172.39.0.3,IP:172.39.0.4" && chmod 644 db.key)

.PHONY: scylla-start
scylla-start: cert-cache-load .prepare-docker-compose .prepare-environment-update-aio-max-nr docker-cache-load
	$(COMPOSE) up -d

.PHONY: scylla-stop
scylla-stop: .prepare-docker-compose
	$(COMPOSE) down

.PHONY: scylla-kill
scylla-kill: .prepare-docker-compose
	$(COMPOSE) kill

.PHONY: scylla-rm
scylla-rm: .prepare-docker-compose
	$(COMPOSE) rm -f

.PHONY: docker-pull
docker-pull:
	docker pull $(SCYLLA_IMAGE)

.PHONY: docker-cache-save
docker-cache-save: docker-pull
	@mkdir -p $(DOCKER_CACHE_DIR)
	docker save $(SCYLLA_IMAGE) -o $(DOCKER_CACHE_FILE)

.PHONY: docker-cache-load
docker-cache-load:
	@if [ -f "$(DOCKER_CACHE_FILE)" ]; then \
		echo "Loading Docker image from cache..."; \
		docker load -i $(DOCKER_CACHE_FILE); \
	else \
		echo "Cache file not found, pulling image..."; \
		$(MAKE) docker-pull; \
	fi

.PHONY: cert-cache-save
cert-cache-save: .prepare-cert
	@mkdir -p $(CERT_CACHE_DIR)
	cp $(CERT_DIR)/db.key $(CERT_DIR)/db.crt $(CERT_CACHE_DIR)/

.PHONY: cert-cache-load
cert-cache-load:
	@if [ -f "$(CERT_CACHE_DIR)/db.key" ] && [ -f "$(CERT_CACHE_DIR)/db.crt" ]; then \
		echo "Loading certificates from cache..."; \
		cp $(CERT_CACHE_DIR)/db.key $(CERT_CACHE_DIR)/db.crt $(CERT_DIR)/; \
		chmod 644 $(CERT_DIR)/db.key; \
	else \
		echo "Certificate cache not found, generating..."; \
		$(MAKE) .prepare-cert; \
	fi

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == 3.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		cp -f Makefile /tmp/tmp-Makefile
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
		mv -f /tmp/tmp-Makefile ./Makefile
	fi
