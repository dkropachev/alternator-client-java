SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

mvn = mvn

ifdef IS_CICD
    mvn = mvn --no-transfer-progress
endif

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BIN := $(MAKEFILE_PATH)/bin

SCYLLA_CCM_COMMIT := d15a2fab9d22fffad8a30c806a7c8e1632e58aae
SCYLLA_CCM_VENV := $(BIN)/scylla-ccm-$(SCYLLA_CCM_COMMIT)
PINNED_SCYLLA_CCM_PATH := $(SCYLLA_CCM_VENV)/bin/ccm
SCYLLA_CCM_INSTALL_LOCK := $(SCYLLA_CCM_VENV).install.lock
SCYLLA_CCM_INSTALL_MARKER := $(SCYLLA_CCM_VENV)/.install-complete
SCYLLA_CCM_PATH ?= $(PINNED_SCYLLA_CCM_PATH)
SCYLLA_VERSION ?= release:2025.2.5
SCYLLA_CCM_DIAGNOSTICS_DIR ?= $(MAKEFILE_PATH)/target/ccm

MAVEN_GPG_PASSPHRASE ?=
MAVEN_OPTS ?=

SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

RELEASE_SKIP_TESTS ?= false
RELEASE_TARGET_TAG ?=
RELEASE_VERSION ?=

.PHONY: clean verify lint lint-specs lint-docs lint-fix compile compile-test compile-demo test-unit test-integration test-all ccm-install release-prepare release release-dry-run checkout-one-commit-before

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

test-integration: ccm-install
	@for command in setsid kill ps; do
		executable=$$(type -P -- "$$command" || true)
		[[ -n "$$executable" && -x "$$executable" ]] || {
			echo "An external $$command executable is required for CCM integration tests" >&2
			exit 1
		}
	done
	mkdir -p -- "$(SCYLLA_CCM_DIAGNOSTICS_DIR)"
	INTEGRATION_TESTS=true \
		SCYLLA_VERSION="$(SCYLLA_VERSION)" \
		SCYLLA_CCM_PATH="$(SCYLLA_CCM_PATH)" \
		SCYLLA_CCM_DIAGNOSTICS_DIR="$(SCYLLA_CCM_DIAGNOSTICS_DIR)" \
		${mvn} test -Dtest=ClusterProvisioningIT -DfailIfNoTests=false \
		-Dtest.forkCount=1 -Dsurefire.timeout=1200
	INTEGRATION_TESTS=true \
		SCYLLA_VERSION="$(SCYLLA_VERSION)" \
		SCYLLA_CCM_PATH="$(SCYLLA_CCM_PATH)" \
		SCYLLA_CCM_DIAGNOSTICS_DIR="$(SCYLLA_CCM_DIAGNOSTICS_DIR)" \
		${mvn} test '-Dtest=**/*IT,!**/ClusterProvisioningIT' -DfailIfNoTests=false \
		-Dtest.forkCount=1 -Dsurefire.timeout=1200

test-all: test-integration

ccm-install:
	@ccm_works() {
		local executable=$$1
		[[ -f "$$executable" && -x "$$executable" ]] \
			&& "$$executable" create --help >/dev/null 2>&1
	}
	install_complete() {
		[[ -f "$(SCYLLA_CCM_INSTALL_MARKER)" \
			&& ! -L "$(SCYLLA_CCM_INSTALL_MARKER)" ]] || return 1
		[[ "$$(< "$(SCYLLA_CCM_INSTALL_MARKER)")" == "$(SCYLLA_CCM_COMMIT)" ]] || return 1
		ccm_works "$(PINNED_SCYLLA_CCM_PATH)"
	}
	if [[ "$(SCYLLA_CCM_PATH)" != "$(PINNED_SCYLLA_CCM_PATH)" ]]; then
		ccm_works "$(SCYLLA_CCM_PATH)" || {
			echo "SCYLLA_CCM_PATH is not a working CCM executable: $(SCYLLA_CCM_PATH)" >&2
			exit 1
		}
		echo "Using CCM executable: $(SCYLLA_CCM_PATH)"
		exit 0
	fi
	command -v flock >/dev/null 2>&1 || {
		echo "flock is required to install scylla-ccm" >&2
		exit 1
	}
	mkdir -p -- "$(BIN)"
	exec {install_lock_fd}>"$(SCYLLA_CCM_INSTALL_LOCK)"
	flock -x "$$install_lock_fd"
	if install_complete; then
		echo "Using CCM executable: $(PINNED_SCYLLA_CCM_PATH)"
		exit 0
	fi
	command -v uv >/dev/null 2>&1 || {
		echo "uv is required to install scylla-ccm: https://docs.astral.sh/uv/" >&2
		exit 1
	}
	uv venv --clear "$(SCYLLA_CCM_VENV)"
	uv pip install --python "$(SCYLLA_CCM_VENV)/bin/python" \
		"git+https://github.com/scylladb/scylla-ccm.git@$(SCYLLA_CCM_COMMIT)"
	ccm_works "$(PINNED_SCYLLA_CCM_PATH)" || {
		echo "Installed CCM entry point failed its launch check" >&2
		exit 1
	}
	temporary_marker=$$(mktemp "$(SCYLLA_CCM_VENV)/.install-complete.XXXXXXXX")
	printf '%s\n' "$(SCYLLA_CCM_COMMIT)" > "$$temporary_marker"
	chmod 600 -- "$$temporary_marker"
	mv -fT -- "$$temporary_marker" "$(SCYLLA_CCM_INSTALL_MARKER)"
	echo "Using CCM executable: $(PINNED_SCYLLA_CCM_PATH)"

release-prepare:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	NEXT_RELEASE=""
	if [[ -n "${RELEASE_VERSION}" ]]; then
		NEXT_RELEASE="-DreleaseVersion=${RELEASE_VERSION}"
	fi
	export MAVEN_OPTS
	${mvn} release:prepare -DpushChanges=false $${NEXT_RELEASE}

release:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/release-logs/ 2>/dev/null || true
	${mvn} release:perform -Drelease.autopublish=true > >(tee /tmp/release-logs/stdout.log) 2> >(tee /tmp/release-logs/stderr.log)

release-dry-run:
	@if [[ "${MAVEN_GPG_PASSPHRASE}" == "" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ -n "${RELEASE_SKIP_TESTS}" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/release-logs/ 2>/dev/null || true
	${mvn} release:perform > >(tee /tmp/release-logs/stdout.log) 2> >(tee /tmp/release-logs/stderr.log)

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == 3.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		cp -f Makefile /tmp/tmp-Makefile
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
		mv -f /tmp/tmp-Makefile ./Makefile
	fi
