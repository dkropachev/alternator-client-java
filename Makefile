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
SCYLLA_CCM_PATH ?= $(PINNED_SCYLLA_CCM_PATH)
SCYLLA_VERSION ?= release:2025.2
SCYLLA_CCM_STATE_ROOT ?=
SCYLLA_CCM_ID_LOCK_ROOT ?=
CCM_RESULTS_DIR ?= $(MAKEFILE_PATH)/target/ccm
CCM_RUN_DIR ?=

export SCYLLA_CCM_STATE_ROOT SCYLLA_CCM_ID_LOCK_ROOT CCM_RESULTS_DIR CCM_RUN_DIR

MAVEN_GPG_PASSPHRASE ?=
MAVEN_OPTS ?=

SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

RELEASE_SKIP_TESTS ?= false
RELEASE_TARGET_TAG ?=
RELEASE_VERSION ?=

define CCM_STATE_HELPERS
ccm_prepare_owned_directory() {
	local requested=$$1 parent
	parent=$$(readlink -m -- "$$(dirname -- "$$requested")") || return 1
	mkdir -p -- "$$parent" || return 1
	if [[ -e "$$requested" || -L "$$requested" ]]; then
		[[ -d "$$requested" && ! -L "$$requested" && -O "$$requested" ]] || {
			echo "Unsafe CCM-owned directory: $$requested" >&2
			return 1
		}
	else
		(umask 077; mkdir -- "$$requested") 2>/dev/null \
			|| [[ -d "$$requested" && ! -L "$$requested" && -O "$$requested" ]] \
			|| return 1
	fi
	chmod 700 -- "$$requested" || return 1
	readlink -f -- "$$requested"
}
ccm_host_root() {
	ccm_prepare_owned_directory "/tmp/alternator-client-java-ccm-$$(id -u)"
}
ccm_reject_build_tree() {
	local requested=$$1 description=$$2 resolved target_root
	resolved=$$(readlink -m -- "$$requested") || return 1
	target_root=$$(readlink -m -- "$(MAKEFILE_PATH)/target") || return 1
	case "$$resolved/" in
		"$$target_root/"|"$$target_root/"*)
			echo "Refusing CCM $$description beneath Maven build output: $$resolved" >&2
			return 1
			;;
	esac
}
ccm_state_root() {
	local requested host_root repository_key
	if [[ -n $${SCYLLA_CCM_STATE_ROOT:-} ]]; then
		requested=$$SCYLLA_CCM_STATE_ROOT
	else
		host_root=$$(ccm_host_root) || return 1
		repository_key=$$(stat -Lc '%d-%i' -- "$(MAKEFILE_PATH)") || return 1
		requested="$$host_root/$$repository_key"
	fi
	if [[ $${CCM_LEGACY_CLEANUP:-0} != 1 ]]; then
		ccm_reject_build_tree "$$requested" "state root" || return 1
	fi
	ccm_prepare_owned_directory "$$requested"
}
ccm_id_lock_root() {
	local requested host_root
	if [[ -n $${SCYLLA_CCM_ID_LOCK_ROOT:-} ]]; then
		requested=$$SCYLLA_CCM_ID_LOCK_ROOT
	else
		host_root=$$(ccm_host_root) || return 1
		requested="$$host_root/ccm-id-locks"
	fi
	ccm_reject_build_tree "$$requested" "ID lock root" || return 1
	ccm_prepare_owned_directory "$$requested"
}
ccm_scan_lock() {
	local root
	if [[ -n $${SCYLLA_CCM_STATE_ROOT:-} ]]; then
		root=$$(ccm_state_root) || return 1
	else
		root=$$(ccm_id_lock_root) || return 1
	fi
	printf '%s/.scan.lock\n' "$$root"
}
ccm_resolve_executable() {
	local candidate=$$1 resolved
	if [[ "$$candidate" == */* ]]; then
		resolved=$$(readlink -f -- "$$candidate") || return 1
	else
		resolved=$$(command -v -- "$$candidate") || return 1
		resolved=$$(readlink -f -- "$$resolved") || return 1
	fi
	[[ "$$resolved" == /* && -f "$$resolved" && -x "$$resolved" ]] || return 1
	printf '%s\n' "$$resolved"
}
ccm_process_fields() {
	local pid=$$1 stat_line remainder
	local -a fields
	[[ "$$pid" =~ ^[1-9][0-9]*$$ ]] || return 1
	{ IFS= read -r stat_line < "/proc/$$pid/stat"; } 2>/dev/null || return 1
	remainder=$${stat_line##*) }
	read -r -a fields <<< "$$remainder"
	[[ $${#fields[@]} -gt 19 ]] || return 1
	printf '%s %s %s %s\n' "$${fields[0]}" "$${fields[19]}" "$${fields[2]}" "$${fields[3]}"
}
ccm_process_matches_run() {
	local pid=$$1 run_directory=$$2 entry environment_fd
	[[ -r "/proc/$$pid/environ" && -O "/proc/$$pid" ]] || return 1
	exec {environment_fd}< "/proc/$$pid/environ" 2>/dev/null || return 1
	while IFS= read -r -d '' entry; do
		if [[ "$$entry" == "SCYLLA_CCM_RUN_DIR=$$run_directory" ]]; then
			exec {environment_fd}<&-
			return 0
		fi
	done <&$$environment_fd
	exec {environment_fd}<&-
	return 1
}
endef

.PHONY: clean verify lint lint-specs lint-fix compile compile-test compile-demo test test-unit test-integration test-all ccm-install ccm-clean-stale ccm-clean-run release-prepare release release-dry-run

clean:
	@if [[ "$$(uname -s)" == Linux && -r /proc/self/stat ]] \
		&& command -v flock >/dev/null 2>&1 && flock --version >/dev/null 2>&1 \
		&& command -v grep >/dev/null 2>&1 \
		&& command -v pgrep >/dev/null 2>&1 \
		&& command -v readlink >/dev/null 2>&1 \
		&& readlink -f -- "$(MAKEFILE_PATH)" >/dev/null 2>&1 \
		&& readlink -m -- "$(MAKEFILE_PATH)/target" >/dev/null 2>&1 \
		&& command -v stat >/dev/null 2>&1 \
		&& stat -Lc '%d-%i' -- "$(MAKEFILE_PATH)" >/dev/null 2>&1 \
		&& command -v timeout >/dev/null 2>&1; then
		CCM_REFUSE_ACTIVE=1 make --no-print-directory ccm-clean-stale
	fi
	${mvn} clean

verify:
	${mvn} verify

lint:
	${mvn} license:check
	${mvn} fmt:check
	${mvn} checkstyle:check
	${mvn} compile test-compile
	make lint-specs
	make lint-docs

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

.PHONY: test-integration
test-integration: ccm-install
	@$(CCM_STATE_HELPERS)
	[[ "$$(uname -s)" == Linux && -r /proc/self/stat ]] || {
		echo "CCM integration tests require Linux with procfs" >&2
		exit 1
	}
	for command in flock grep pgrep readlink setsid timeout; do
		command -v "$$command" >/dev/null 2>&1 || {
			echo "$$command is required for CCM integration tests" >&2
			exit 1
		}
			done
	env --default-signal=INT --default-signal=TERM --default-signal=HUP true 2>/dev/null || {
		echo "GNU env with --default-signal support is required" >&2
		exit 1
	}
	make --no-print-directory ccm-clean-stale
	state_root=$$(ccm_state_root)
	id_lock_root=$$(ccm_id_lock_root)
	ccm=$$(ccm_resolve_executable "$(SCYLLA_CCM_PATH)") || {
		echo "SCYLLA_CCM_PATH is not executable: $(SCYLLA_CCM_PATH)" >&2
		exit 1
	}
	results_root=$$(readlink -m -- "$(CCM_RESULTS_DIR)")
	case "$$results_root/" in
		"$$state_root"|"$$state_root"/*)
			echo "CCM results must be outside operational state: $$results_root" >&2
			exit 1
			;;
	esac
	mkdir -p -- "$$results_root"
	scan_lock=$$(ccm_scan_lock)
	exec 9> "$$scan_lock"
	flock -x 9
	run_directory=$$(mktemp -d "$$state_root/ccm-runtime.XXXXXXXX")
	run_name=$$(basename -- "$$run_directory")
	diagnostics_directory="$$results_root/$$run_name/diagnostics"
	(umask 077; mkdir -p -- "$$diagnostics_directory")
	chmod 700 -- "$$diagnostics_directory"
	owner_pid=$$BASHPID
	fields=$$(ccm_process_fields "$$owner_pid")
	read -r owner_state owner_start owner_group owner_session <<< "$$fields"
	boot_id=$$(< /proc/sys/kernel/random/boot_id)
	owner_file=$$(mktemp "$$run_directory/.OWNER.XXXXXXXX")
	printf 'pid=%s\nstart_ticks=%s\nboot_id=%s\n' \
		"$$owner_pid" "$$owner_start" "$$boot_id" > "$$owner_file"
	mv -f -- "$$owner_file" "$$run_directory/OWNER"
	flock -u 9
	exec 9>&-

	current_pid=
	current_start=
	current_status_file=
	current_ready_file=
	current_start_file=
	current_abort_file=
	current_release_file=
	current_group_file="$$run_directory/ACTIVE_GROUP"
	current_signal=
	snapshot_current_group() {
		group_pids=()
		group_starts=()
		local pid fields state start process_group session candidates pgrep_status
		[[ -n "$$current_pid" && -n "$$current_start" ]] || return 1
		fields=$$(ccm_process_fields "$$current_pid") || {
			echo "Maven group anchor $$current_pid disappeared before cleanup" >&2
			return 1
		}
		read -r state start process_group session <<< "$$fields"
		[[ "$$state" != Z && "$$start" == "$$current_start" \
			&& "$$process_group" == "$$current_pid" && "$$session" == "$$current_pid" ]] || {
			echo "Maven group anchor $$current_pid changed identity before cleanup" >&2
			return 1
		}
		pgrep_status=0
		candidates=$$(pgrep -g "$$current_pid" 2>/dev/null) || pgrep_status=$$?
		(( pgrep_status == 0 || pgrep_status == 1 )) || return 1
		while IFS= read -r pid; do
			[[ -n "$$pid" ]] || continue
			[[ "$$pid" != "$$BASHPID" && "$$pid" != "$$current_pid" && -O "/proc/$$pid" ]] \
				|| continue
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$process_group" == "$$current_pid" \
				&& "$$session" == "$$current_pid" ]] || continue
			group_pids+=("$$pid")
			group_starts+=("$$start")
		done <<< "$$candidates"
	}
	signal_current_group_members() {
		local signal_name=$$1 index pid fields state start process_group session
		for ((index = 0; index < $${#group_pids[@]}; index++)); do
			pid=$${group_pids[$$index]}
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$${group_starts[$$index]}" \
				&& "$$process_group" == "$$current_pid" && "$$session" == "$$current_pid" ]] \
				|| continue
			kill -s "$$signal_name" "$$pid" 2>/dev/null || true
		done
	}
	reap_current_group() {
		local iteration
		[[ -n "$$current_pid" ]] || return 0
		snapshot_current_group || return 1
		(( $${#group_pids[@]} == 0 )) && return 0
		signal_current_group_members "$${current_signal:-TERM}"
		for ((iteration = 0; iteration < 30; iteration++)); do
			snapshot_current_group || return 1
			(( $${#group_pids[@]} == 0 )) && return 0
			sleep 0.1
		done
		signal_current_group_members KILL
		for ((iteration = 0; iteration < 50; iteration++)); do
			snapshot_current_group || return 1
			(( $${#group_pids[@]} == 0 )) && return 0
			sleep 0.1
		done
		echo "Maven process group $$current_pid survived cleanup" >&2
		return 1
	}
	publish_phase_marker() {
		local marker=$$1
		if [[ -e "$$marker" || -L "$$marker" ]]; then
			[[ -f "$$marker" && ! -L "$$marker" && -O "$$marker" ]] || return 1
			return 0
		fi
		(umask 077; set -o noclobber; : > "$$marker") 2>/dev/null || {
			[[ -f "$$marker" && ! -L "$$marker" && -O "$$marker" ]] || return 1
		}
	}
	signal_current_anchor() {
		local fields state start process_group session
		[[ -n "$$current_pid" && -n "$$current_signal" ]] || return 0
		fields=$$(ccm_process_fields "$$current_pid") || return 0
		read -r state start process_group session <<< "$$fields"
		if [[ -z "$$current_start" ]]; then
			current_start=$$start
		fi
		[[ "$$state" == Z || "$$start" != "$$current_start" ]] \
			|| kill -s "$$current_signal" "$$current_pid" 2>/dev/null || true
	}
	release_current_anchor() {
		local fields state start process_group session iteration wait_status=0 failure=0
		[[ -n "$$current_pid" ]] || return 0
		publish_phase_marker "$$current_release_file" || failure=1
		fields=$$(ccm_process_fields "$$current_pid") || fields=
		if [[ -n "$$fields" ]]; then
			read -r state start process_group session <<< "$$fields"
			[[ -n "$$current_start" ]] || current_start=$$start
			for ((iteration = 0; iteration < 30; iteration++)); do
				fields=$$(ccm_process_fields "$$current_pid") || break
				read -r state start process_group session <<< "$$fields"
				[[ "$$state" != Z && "$$start" == "$$current_start" ]] || break
				sleep 0.1
			done
			fields=$$(ccm_process_fields "$$current_pid") || fields=
			if [[ -n "$$fields" ]]; then
				read -r state start process_group session <<< "$$fields"
				if [[ "$$state" != Z && "$$start" == "$$current_start" ]]; then
					kill -KILL "$$current_pid" 2>/dev/null || true
					failure=1
				fi
			fi
		fi
		wait "$$current_pid" 2>/dev/null || wait_status=$$?
		(( wait_status == 0 )) || failure=1
		rm -f -- "$$current_status_file" "$$current_ready_file" \
			"$$current_start_file" "$$current_abort_file" "$$current_release_file" \
			|| failure=1
		if (( failure == 0 )); then
			rm -f -- "$$current_group_file" || failure=1
		fi
		current_pid=
		current_start=
		current_status_file=
		current_ready_file=
		current_start_file=
		current_abort_file=
		current_release_file=
		current_signal=
		return "$$failure"
	}
	terminate_current() {
		local failure=0
		[[ -n "$$current_pid" ]] || return 0
		if [[ -n "$$current_signal" ]]; then
			signal_current_anchor
			publish_phase_marker "$$current_abort_file" || failure=1
		fi
		reap_current_group || failure=1
		release_current_anchor || failure=1
		return "$$failure"
	}
	finish_run() {
		local status=$$? cleanup_status=0
		trap - EXIT
		trap '' INT TERM HUP
		terminate_current || {
			(( status != 0 )) || status=125
		}
		flock -x "$$scan_lock" \
			make --no-print-directory ccm-clean-run \
					SCYLLA_CCM_STATE_ROOT="$$state_root" CCM_RUN_DIR="$$run_directory" \
					CCM_RESULTS_DIR="$$results_root" SCYLLA_CCM_PATH="$$ccm" \
					|| cleanup_status=$$?
		if (( status == 0 && cleanup_status != 0 )); then
			status=$$cleanup_status
		fi
		exit "$$status"
	}
	handle_signal() {
		local status=$$1
		trap '' INT TERM HUP
		current_signal=$$2
		terminate_current || true
		exit "$$status"
	}
	trap finish_run EXIT
	trap 'handle_signal 130 INT' INT
	trap 'handle_signal 143 TERM' TERM
	trap 'handle_signal 129 HUP' HUP

	maven_command=(${mvn})
	run_phase() {
		local phase_base fields state start process_group session status iteration
		phase_base=$$(mktemp "$$run_directory/.phase.XXXXXXXX") || return 125
		rm -- "$$phase_base" || return 125
		current_status_file="$$phase_base.status"
		current_ready_file="$$phase_base.ready"
		current_start_file="$$phase_base.start"
		current_abort_file="$$phase_base.abort"
		current_release_file="$$phase_base.release"
		current_signal=
		setsid env --default-signal=INT --default-signal=TERM --default-signal=HUP \
			INTEGRATION_TESTS=true SCYLLA_VERSION="$(SCYLLA_VERSION)" \
			SCYLLA_CCM_PATH="$$ccm" SCYLLA_CCM_RUN_DIR="$$run_directory" \
			SCYLLA_CCM_DIAGNOSTICS_DIR="$$diagnostics_directory" \
			SCYLLA_CCM_ID_LOCK_ROOT="$$id_lock_root" \
			bash -c '
				phase_status=$$1
				phase_ready=$$2
				phase_start=$$3
				phase_abort=$$4
				phase_release=$$5
				shift 5
				child_pid=
				requested_signal=
				forward_signal() {
					requested_signal=$$1
					[[ -z "$$child_pid" ]] || kill -s "$$requested_signal" "$$child_pid" 2>/dev/null || true
				}
				trap "forward_signal INT" INT
				trap "forward_signal TERM" TERM
				trap "forward_signal HUP" HUP
				temporary="$${phase_ready}.tmp.$$$$"
				umask 077
				printf "ready\n" > "$$temporary" || exit 125
				mv -f -- "$$temporary" "$$phase_ready" || exit 125
				while [[ ! -e "$$phase_start" && ! -e "$$phase_abort" ]]; do sleep 0.01; done
				[[ ! -e "$$phase_abort" && -z "$$requested_signal" ]] || exit 0
				set +e
				"$$@" &
				child_pid=$$!
				[[ -z "$$requested_signal" ]] || kill -s "$$requested_signal" "$$child_pid" 2>/dev/null || true
				while true; do
					wait "$$child_pid"
					status=$$?
					kill -0 "$$child_pid" 2>/dev/null || break
				done
				child_pid=
				temporary="$${phase_status}.tmp.$$$$"
				printf "%s\n" "$$status" > "$$temporary" || exit 125
				mv -f -- "$$temporary" "$$phase_status" || exit 125
				while [[ ! -e "$$phase_release" ]]; do sleep 0.01; done
				exit 0
			' ccm-phase "$$current_status_file" "$$current_ready_file" \
			"$$current_start_file" "$$current_abort_file" "$$current_release_file" \
			"$${maven_command[@]}" "$$@" &
		current_pid=$$!
		current_start=
		for ((iteration = 0; iteration < 100; iteration++)); do
			fields=$$(ccm_process_fields "$$current_pid") || {
				kill -0 "$$current_pid" 2>/dev/null || break
				sleep 0.01
				continue
			}
			read -r state start process_group session <<< "$$fields"
			if [[ "$$state" != Z && "$$process_group" == "$$current_pid" \
				&& "$$session" == "$$current_pid" ]]; then
				current_start=$$start
				break
			fi
			sleep 0.01
		done
		[[ -n "$$current_start" ]] || {
			echo "Unable to establish Maven process-group anchor" >&2
			terminate_current || true
			return 125
		}
		for ((iteration = 0; iteration < 100; iteration++)); do
			if [[ -f "$$current_ready_file" && ! -L "$$current_ready_file" \
				&& -O "$$current_ready_file" ]]; then
				break
			fi
			fields=$$(ccm_process_fields "$$current_pid") || break
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$$current_start" ]] || break
			sleep 0.01
		done
		[[ -f "$$current_ready_file" && ! -L "$$current_ready_file" \
			&& -O "$$current_ready_file" ]] || {
			echo "Maven process-group anchor did not become ready" >&2
			terminate_current || true
			return 125
		}
		[[ ! -e "$$current_group_file" && ! -L "$$current_group_file" ]] || {
			echo "Unsafe Maven process-group metadata" >&2
			terminate_current || true
			return 125
		}
		group_file=$$(mktemp "$$run_directory/.ACTIVE_GROUP.XXXXXXXX") || {
			terminate_current || true
			return 125
		}
		printf 'pgid=%s\nsid=%s\nleader_pid=%s\nleader_start_ticks=%s\nboot_id=%s\nmember=%s:%s\n' \
			"$$current_pid" "$$current_pid" "$$current_pid" "$$current_start" \
			"$$boot_id" "$$current_pid" "$$current_start" > "$$group_file" || {
			rm -f -- "$$group_file"
			terminate_current || true
			return 125
		}
		ln -- "$$group_file" "$$current_group_file" || {
			rm -f -- "$$group_file"
			terminate_current || true
			return 125
		}
		rm -- "$$group_file" || {
			terminate_current || true
			return 125
		}
		[[ ! -e "$$current_start_file" && ! -L "$$current_start_file" ]] || {
			echo "Unsafe Maven phase start marker" >&2
			terminate_current || true
			return 125
		}
		(umask 077; set -o noclobber; : > "$$current_start_file") 2>/dev/null || {
			echo "Unable to start Maven phase" >&2
			terminate_current || true
			return 125
		}
		while [[ ! -e "$$current_status_file" && ! -L "$$current_status_file" ]]; do
			fields=$$(ccm_process_fields "$$current_pid") || break
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$$current_start" \
				&& "$$process_group" == "$$current_pid" && "$$session" == "$$current_pid" ]] \
				|| break
			sleep 0.02
		done
		if [[ -f "$$current_status_file" && ! -L "$$current_status_file" \
			&& -O "$$current_status_file" ]]; then
			status=$$(< "$$current_status_file")
			[[ "$$status" =~ ^([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$$ ]] \
				|| status=125
		else
			status=125
		fi
		if ! terminate_current; then
			(( status != 0 )) || status=125
		fi
		return "$$status"
	}

	set +e
	run_phase test -Dtest=ClusterProvisioningIT -DfailIfNoTests=false \
		-Dtest.forkCount=1 -Dsurefire.timeout=1200
	run_status=$$?
	if (( run_status == 0 )); then
		run_phase test '-Dtest=**/*IT,!**/ClusterProvisioningIT' -DfailIfNoTests=false \
			-Dtest.forkCount=1 -Dsurefire.timeout=1200
		run_status=$$?
	fi
	exit "$$run_status"

.PHONY: test-all
test-all: test-integration

.PHONY: ccm-install
ccm-install:
	@$(CCM_STATE_HELPERS)
	if resolved=$$(ccm_resolve_executable "$(SCYLLA_CCM_PATH)" 2>/dev/null); then
		echo "Using CCM executable: $$resolved"
		exit 0
	fi
	if [[ "$(SCYLLA_CCM_PATH)" != "$(PINNED_SCYLLA_CCM_PATH)" ]]; then
		echo "SCYLLA_CCM_PATH is not executable or was not found on PATH: $(SCYLLA_CCM_PATH)" >&2
		exit 1
	fi
	if ! command -v uv >/dev/null 2>&1; then
		echo "uv is required to install scylla-ccm: https://docs.astral.sh/uv/"
		exit 1
	fi
	mkdir -p "$(BIN)"
	if [[ ! -x "$(SCYLLA_CCM_VENV)/bin/python" ]]; then uv venv "$(SCYLLA_CCM_VENV)"; fi
	uv pip install --python "$(SCYLLA_CCM_VENV)/bin/python" "git+https://github.com/scylladb/scylla-ccm.git@$(SCYLLA_CCM_COMMIT)"
	ccm_resolve_executable "$(PINNED_SCYLLA_CCM_PATH)" >/dev/null

.PHONY: ccm-clean-stale
ccm-clean-stale:
	@$(CCM_STATE_HELPERS)
	state_root=$$(ccm_state_root)
	scan_lock=$$(ccm_scan_lock)
	exec 9> "$$scan_lock"
	flock -x 9
	overall_status=0
	refuse_active=$${CCM_REFUSE_ACTIVE:-0}
	[[ "$$refuse_active" == 0 || "$$refuse_active" == 1 ]] || {
		echo "CCM_REFUSE_ACTIVE must be 0 or 1" >&2
		exit 2
	}
	current_boot=$$(< /proc/sys/kernel/random/boot_id)
	results_root=$$(readlink -m -- "$(CCM_RESULTS_DIR)")
	scan_roots=("$$state_root")
	scan_modes=(0)
	declare -A seen_scan_roots=(["$$state_root"]=1)
	if [[ -z $${SCYLLA_CCM_STATE_ROOT:-} ]]; then
		host_root=$$(ccm_host_root)
		if [[ -z $${seen_scan_roots[$$host_root]:-} ]]; then
			scan_roots+=("$$host_root")
			scan_modes+=(0)
			seen_scan_roots["$$host_root"]=1
		fi
		for candidate_root in "$$host_root"/*; do
			[[ -d "$$candidate_root" && ! -L "$$candidate_root" && -O "$$candidate_root" \
				&& "$$(basename -- "$$candidate_root")" =~ ^[0-9]+-[0-9]+$$ ]] || continue
			candidate_root=$$(readlink -f -- "$$candidate_root")
			if [[ -z $${seen_scan_roots[$$candidate_root]:-} ]]; then
				scan_roots+=("$$candidate_root")
				scan_modes+=(0)
				seen_scan_roots["$$candidate_root"]=1
			fi
		done
	fi
	# Older revisions stored operational state directly below target/ccm. Recover it
	# before Maven clean removes the only metadata capable of identifying survivors.
	if [[ -d "$$results_root" && ! -L "$$results_root" && -O "$$results_root" ]]; then
		results_root=$$(readlink -f -- "$$results_root")
		if [[ -z $${seen_scan_roots[$$results_root]:-} ]]; then
			scan_roots+=("$$results_root")
			scan_modes+=(1)
			seen_scan_roots["$$results_root"]=1
		fi
	fi
	for ((root_index = 0; root_index < $${#scan_roots[@]}; root_index++)); do
		scan_root=$${scan_roots[$$root_index]}
		legacy_mode=$${scan_modes[$$root_index]}
		root_lock="$$scan_root/.scan.lock"
		root_lock_fd=
		if [[ "$$root_lock" != "$$scan_lock" ]]; then
			if ! exec {root_lock_fd}> "$$root_lock" || ! flock -x "$$root_lock_fd"; then
				echo "Unable to lock CCM scan root: $$scan_root" >&2
				overall_status=1
				[[ -z "$$root_lock_fd" ]] || exec {root_lock_fd}>&-
				continue
			fi
		fi
		for run_directory in "$$scan_root"/ccm-runtime.*; do
		[[ -d "$$run_directory" && ! -L "$$run_directory" && -O "$$run_directory" ]] || continue
		if (( legacy_mode != 0 )) && [[ ! -d "$$run_directory/clusters" \
			&& ! -e "$$run_directory/OWNER" && ! -e "$$run_directory/OWNER_PID" \
			&& ! -e "$$run_directory/ACTIVE_GROUP" ]]; then
			has_emergency_group=0
			for group_file in "$$run_directory"/EMERGENCY_GROUP.*; do
				[[ -e "$$group_file" || -L "$$group_file" ]] && has_emergency_group=1
			done
			(( has_emergency_group != 0 )) || continue
		fi
		active=0
		unsafe=0
		owner_file="$$run_directory/OWNER"
		legacy_owner_file="$$run_directory/OWNER_PID"
		if [[ -e "$$owner_file" || -L "$$owner_file" ]]; then
			if [[ ! -f "$$owner_file" || -L "$$owner_file" || ! -O "$$owner_file" ]]; then
				unsafe=1
				pid=
			else
				pid= start_ticks= boot_id= invalid=0
				while IFS='=' read -r key value; do
					case "$$key" in
						pid) [[ -z "$$pid" ]] && pid=$$value || invalid=1 ;;
						start_ticks) [[ -z "$$start_ticks" ]] && start_ticks=$$value || invalid=1 ;;
						boot_id) [[ -z "$$boot_id" ]] && boot_id=$$value || invalid=1 ;;
						*) invalid=1 ;;
					esac
				done < "$$owner_file"
				[[ "$$pid" =~ ^[1-9][0-9]*$$ && "$$start_ticks" =~ ^[0-9]+$$ \
					&& "$$boot_id" =~ ^[0-9a-fA-F-]+$$ ]] || invalid=1
				if [[ "$$pid" =~ ^[1-9][0-9]*$$ ]]; then
					fields=$$(ccm_process_fields "$$pid") || {
						fields=
					[[ ! -e "/proc/$$pid/stat" ]] || unsafe=1
				}
				if (( invalid != 0 )); then
					if [[ -n "$$fields" && "$${fields%% *}" != Z ]]; then
						unsafe=1
					fi
				elif [[ "$$start_ticks" =~ ^[0-9]+$$ && "$$boot_id" == "$$current_boot" \
					&& -n "$$fields" ]]; then
					read -r state start process_group session <<< "$$fields"
						[[ "$$state" == Z || "$$start" != "$$start_ticks" ]] || active=1
					fi
				fi
			fi
		elif [[ -e "$$legacy_owner_file" || -L "$$legacy_owner_file" ]]; then
			if [[ ! -f "$$legacy_owner_file" || -L "$$legacy_owner_file" \
				|| ! -O "$$legacy_owner_file" ]]; then
				unsafe=1
			else
				pid=$$(tr -d '\r\n' < "$$legacy_owner_file")
				if [[ "$$pid" =~ ^[1-9][0-9]*$$ ]]; then
					fields=$$(ccm_process_fields "$$pid") || {
						fields=
						[[ ! -e "/proc/$$pid/stat" ]] || unsafe=1
					}
					[[ -z "$$fields" || "$${fields%% *}" == Z ]] || unsafe=1
				fi
			fi
		fi
		if (( unsafe != 0 )); then
			echo "Refusing unverifiable live CCM ownership: $$run_directory" >&2
			overall_status=1
			continue
		fi
		if (( active != 0 )); then
			if (( refuse_active != 0 )) \
				&& [[ "$$scan_root" == "$$state_root" || "$$legacy_mode" == 1 ]]; then
				echo "Refusing to clean while CCM run is active: $$run_directory" >&2
				overall_status=1
			else
				echo "Leaving active CCM run untouched: $$run_directory"
			fi
			continue
		fi
		make --no-print-directory ccm-clean-run \
			SCYLLA_CCM_STATE_ROOT="$$scan_root" CCM_RUN_DIR="$$run_directory" \
			CCM_RESULTS_DIR="$(CCM_RESULTS_DIR)" SCYLLA_CCM_PATH="$(SCYLLA_CCM_PATH)" \
			CCM_LEGACY_CLEANUP="$$legacy_mode" \
			|| overall_status=1
		done
		if [[ -n "$$root_lock_fd" ]]; then
			flock -u "$$root_lock_fd" || overall_status=1
			exec {root_lock_fd}>&- || overall_status=1
		fi
	done
	exit "$$overall_status"

ccm-clean-run:
	@$(CCM_STATE_HELPERS)
	for command in grep pgrep; do
		command -v "$$command" >/dev/null 2>&1 || {
			echo "$$command is required for CCM cleanup" >&2
			exit 1
		}
	done
	state_root=$$(ccm_state_root)
	[[ -n "$${CCM_RUN_DIR:-}" ]] || {
		echo "CCM_RUN_DIR is required" >&2
		exit 2
	}
	if [[ ! -e "$$CCM_RUN_DIR" && ! -L "$$CCM_RUN_DIR" ]]; then
		exit 0
	fi
	[[ -d "$$CCM_RUN_DIR" && ! -L "$$CCM_RUN_DIR" && -O "$$CCM_RUN_DIR" ]] || {
		echo "Refusing unsafe CCM run directory: $$CCM_RUN_DIR" >&2
		exit 1
	}
	run_directory=$$(readlink -f -- "$$CCM_RUN_DIR")
	run_name=$$(basename -- "$$run_directory")
	[[ "$$(dirname -- "$$run_directory")" == "$$state_root" \
		&& "$$run_name" =~ ^ccm-runtime\.[A-Za-z0-9]+$$ ]] || {
		echo "Refusing CCM run outside $$state_root: $$run_directory" >&2
		exit 1
	}
	results_root=$$(readlink -m -- "$(CCM_RESULTS_DIR)")
	legacy_cleanup=$${CCM_LEGACY_CLEANUP:-0}
	[[ "$$legacy_cleanup" == 0 || "$$legacy_cleanup" == 1 ]] || {
		echo "CCM_LEGACY_CLEANUP must be 0 or 1" >&2
		exit 2
	}
	[[ "$$results_root" != "$$state_root" || "$$legacy_cleanup" == 1 ]] || {
		echo "CCM results must be outside operational state: $$results_root" >&2
		exit 1
	}
	case "$$results_root/" in
		"$$run_directory"/*)
			echo "CCM results must be outside operational state: $$results_root" >&2
			exit 1
			;;
	esac
	if (( legacy_cleanup != 0 )); then
		diagnostics_directory="$$run_directory/diagnostics"
	else
		diagnostics_directory="$$results_root/$$run_name/diagnostics"
	fi
	load_recorded_group() {
		local metadata=$$1 line key value member_pid member_start invalid=0
		metadata_pgid=
		metadata_sid=
		metadata_leader_pid=
		metadata_leader_start=
		metadata_boot_id=
		metadata_member_pids=()
		metadata_member_starts=()
		[[ -f "$$metadata" && ! -L "$$metadata" && -O "$$metadata" ]] || return 1
		while IFS= read -r line || [[ -n "$$line" ]]; do
			[[ "$$line" == *=* ]] || {
				invalid=1
				continue
			}
			key=$${line%%=*}
			value=$${line#*=}
			case "$$key" in
				pgid) [[ -z "$$metadata_pgid" ]] && metadata_pgid=$$value || invalid=1 ;;
				sid) [[ -z "$$metadata_sid" ]] && metadata_sid=$$value || invalid=1 ;;
				leader_pid)
					[[ -z "$$metadata_leader_pid" ]] && metadata_leader_pid=$$value || invalid=1
					;;
				leader_start_ticks)
					[[ -z "$$metadata_leader_start" ]] && metadata_leader_start=$$value || invalid=1
					;;
				boot_id) [[ -z "$$metadata_boot_id" ]] && metadata_boot_id=$$value || invalid=1 ;;
				member)
					if [[ "$$value" =~ ^([1-9][0-9]*):([0-9]+)$$ ]]; then
						metadata_member_pids+=("$${BASH_REMATCH[1]}")
						metadata_member_starts+=("$${BASH_REMATCH[2]}")
					else
						invalid=1
					fi
					;;
				*) invalid=1 ;;
			esac
		done < "$$metadata"
		[[ "$$metadata_pgid" =~ ^[1-9][0-9]*$$ \
			&& "$$metadata_sid" == "$$metadata_pgid" \
			&& "$$metadata_leader_pid" == "$$metadata_pgid" \
			&& "$$metadata_leader_start" =~ ^[0-9]+$$ \
			&& ( -z "$$metadata_boot_id" || "$$metadata_boot_id" =~ ^[0-9a-fA-F-]+$$ ) \
			&& $$invalid == 0 ]] || return 1
	}
	recorded_member_is_known() {
		local pid=$$1 start=$$2 index
		if [[ "$$pid" == "$$metadata_leader_pid" && "$$start" == "$$metadata_leader_start" ]]; then
			return 0
		fi
		for ((index = 0; index < $${#metadata_member_pids[@]}; index++)); do
			[[ "$$pid" == "$${metadata_member_pids[$$index]}" \
				&& "$$start" == "$${metadata_member_starts[$$index]}" ]] && return 0
		done
		return 1
	}
	snapshot_recorded_group() {
		recorded_group_pids=()
		recorded_group_starts=()
		local candidates pgrep_status pid fields state start process_group session known=0 marked=0
		pgrep_status=0
		candidates=$$(pgrep -g "$$metadata_pgid" 2>/dev/null) || pgrep_status=$$?
		(( pgrep_status == 0 || pgrep_status == 1 )) || return 1
		while IFS= read -r pid; do
			[[ -n "$$pid" ]] || continue
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$process_group" == "$$metadata_pgid" \
				&& "$$session" == "$$metadata_sid" ]] || continue
			[[ -O "/proc/$$pid" ]] || return 1
			recorded_member_is_known "$$pid" "$$start" && known=1
			ccm_process_matches_run "$$pid" "$$run_directory" && marked=1
			recorded_group_pids+=("$$pid")
			recorded_group_starts+=("$$start")
		done <<< "$$candidates"
		(( $${#recorded_group_pids[@]} != 0 )) || return 2
		if (( recorded_group_verified == 0 )); then
			(( known != 0 )) || return 1
			[[ -n "$$metadata_boot_id" || $$marked != 0 ]] || return 1
		fi
		recorded_group_verified=1
		return 0
	}
	signal_recorded_group_members() {
		local signal_name=$$1 index pid fields state start process_group session
		for ((index = 0; index < $${#recorded_group_pids[@]}; index++)); do
			pid=$${recorded_group_pids[$$index]}
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$${recorded_group_starts[$$index]}" \
				&& "$$process_group" == "$$metadata_pgid" && "$$session" == "$$metadata_sid" ]] \
				|| continue
			kill -s "$$signal_name" "$$pid" 2>/dev/null || true
		done
	}
	terminate_recorded_group() {
		local metadata=$$1 current_boot status iteration
		load_recorded_group "$$metadata" || {
			echo "Refusing malformed CCM process-group metadata: $$metadata" >&2
			return 1
		}
		current_boot=$$(< /proc/sys/kernel/random/boot_id)
		if [[ -n "$$metadata_boot_id" && "$$metadata_boot_id" != "$$current_boot" ]]; then
			rm -f -- "$$metadata" "$$metadata.READY" "$$metadata.START"
			return
		fi
		recorded_group_verified=0
		snapshot_recorded_group
		status=$$?
		if (( status == 2 )); then
			rm -f -- "$$metadata" "$$metadata.READY" "$$metadata.START"
			return
		fi
		(( status == 0 )) || {
			echo "Unable to verify CCM process group $$metadata_pgid" >&2
			return 1
		}
		for ((iteration = 0; iteration < 30; iteration++)); do
			signal_recorded_group_members TERM
			sleep 0.1
			snapshot_recorded_group
			status=$$?
			(( status == 2 )) && break
			(( status == 0 )) || return 1
		done
		if (( status != 2 )); then
			for ((iteration = 0; iteration < 50; iteration++)); do
				signal_recorded_group_members KILL
				sleep 0.1
				snapshot_recorded_group
				status=$$?
				(( status == 2 )) && break
				(( status == 0 )) || return 1
			done
		fi
		if (( status != 2 )); then
			echo "CCM process group $$metadata_pgid survived cleanup" >&2
			return 1
		fi
		rm -f -- "$$metadata" "$$metadata.READY" "$$metadata.START"
	}
	terminate_recorded_groups() {
		local metadata metadata_name failure=0
		for metadata in "$$run_directory/ACTIVE_GROUP" "$$run_directory"/EMERGENCY_GROUP.*; do
			[[ -e "$$metadata" || -L "$$metadata" ]] || continue
			metadata_name=$$(basename -- "$$metadata")
			[[ "$$metadata_name" == ACTIVE_GROUP \
				|| "$$metadata_name" =~ ^EMERGENCY_GROUP\.[A-Za-z0-9]+$$ ]] || continue
			terminate_recorded_group "$$metadata" || failure=1
		done
		return "$$failure"
	}
	snapshot_run_processes() {
		process_pids=()
		process_starts=()
		local process environment_file matching_files pid fields state start process_group session
		local grep_status=0
		local -a environment_files=()
		for process in /proc/[0-9]*; do
			pid=$${process##*/}
			[[ "$$pid" != "$$BASHPID" && -O "$$process" \
				&& -r "$$process/environ" ]] || continue
			environment_files+=("$$process/environ")
		done
		(( $${#environment_files[@]} != 0 )) || return 0
		matching_files=$$(grep -l -zFx -- "SCYLLA_CCM_RUN_DIR=$$run_directory" \
			"$${environment_files[@]}" 2>/dev/null) || grep_status=$$?
		if (( grep_status > 1 )); then
			matching_files=
			for environment_file in "$${environment_files[@]}"; do
				[[ "$$environment_file" =~ ^/proc/([1-9][0-9]*)/environ$$ ]] || continue
				pid=$${BASH_REMATCH[1]}
				if ccm_process_matches_run "$$pid" "$$run_directory"; then
					matching_files+="$$environment_file"$$'\n'
				fi
			done
		fi
		while IFS= read -r environment_file; do
			[[ "$$environment_file" =~ ^/proc/([1-9][0-9]*)/environ$$ ]] || continue
			pid=$${BASH_REMATCH[1]}
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z ]] || continue
			ccm_process_matches_run "$$pid" "$$run_directory" || continue
			process_pids+=("$$pid")
			process_starts+=("$$start")
		done <<< "$$matching_files"
	}
	terminate_run_processes() {
		local index pid fields state start process_group session iteration
		snapshot_run_processes
		for ((index = 0; index < $${#process_pids[@]}; index++)); do
			pid=$${process_pids[$$index]}
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$${process_starts[$$index]}" ]] || continue
			ccm_process_matches_run "$$pid" "$$run_directory" || continue
			kill -TERM "$$pid" 2>/dev/null || true
		done
		for ((iteration = 0; iteration < 30; iteration++)); do
			snapshot_run_processes
			(( $${#process_pids[@]} == 0 )) && return 0
			sleep 0.1
		done
		for ((index = 0; index < $${#process_pids[@]}; index++)); do
			pid=$${process_pids[$$index]}
			fields=$$(ccm_process_fields "$$pid") || continue
			read -r state start process_group session <<< "$$fields"
			[[ "$$state" != Z && "$$start" == "$${process_starts[$$index]}" ]] || continue
			ccm_process_matches_run "$$pid" "$$run_directory" || continue
			kill -KILL "$$pid" 2>/dev/null || true
		done
		for ((iteration = 0; iteration < 50; iteration++)); do
			snapshot_run_processes
			(( $${#process_pids[@]} == 0 )) && return 0
			sleep 0.1
		done
		echo "Processes survived cleanup for $$run_directory: $${process_pids[*]}" >&2
		return 1
	}
	collect_diagnostics() {
		local config_directory=$$1 cluster_name=$$2
		local destination="$$diagnostics_directory/$$cluster_name-emergency"
		local node_directory node_name source failure=0
		(( diagnostics_ready != 0 )) || return 1
		mkdir -p -- "$$destination" || failure=1
		source="$$config_directory/ccm-commands.log"
		if [[ -e "$$source" || -L "$$source" ]]; then
			[[ -f "$$source" && ! -L "$$source" && -O "$$source" ]] \
				&& cp -- "$$source" "$$destination/" || failure=1
		fi
		source="$$config_directory/$$cluster_name/cluster.conf"
		if [[ -e "$$source" || -L "$$source" ]]; then
			[[ -f "$$source" && ! -L "$$source" && -O "$$source" ]] \
				&& cp -- "$$source" "$$destination/" || failure=1
		fi
		for node_directory in "$$config_directory/$$cluster_name"/node*; do
			[[ -d "$$node_directory" && ! -L "$$node_directory" ]] || continue
			node_name=$$(basename -- "$$node_directory")
			mkdir -p -- "$$destination/$$node_name" || failure=1
			for source in "$$node_directory/node.conf" "$$node_directory/conf/scylla.yaml"; do
				if [[ -e "$$source" || -L "$$source" ]]; then
					[[ -f "$$source" && ! -L "$$source" && -O "$$source" ]] \
						&& cp -- "$$source" "$$destination/$$node_name/" || failure=1
				fi
			done
			if [[ -e "$$node_directory/logs" || -L "$$node_directory/logs" ]]; then
				[[ -d "$$node_directory/logs" && ! -L "$$node_directory/logs" \
					&& -O "$$node_directory/logs" ]] \
					&& cp -a -- "$$node_directory/logs" "$$destination/$$node_name/" \
					|| failure=1
			fi
		done
		return "$$failure"
	}
	sanitize_node_references() {
		local cluster_directory=$$1 node_directory reference temporary
		for node_directory in "$$cluster_directory"/node*; do
			[[ -e "$$node_directory" || -L "$$node_directory" ]] || continue
			[[ -d "$$node_directory" && ! -L "$$node_directory" && -O "$$node_directory" \
				&& "$$(dirname -- "$$(readlink -f -- "$$node_directory")")" == "$$cluster_directory" ]] || {
				echo "Refusing unsafe CCM node directory: $$node_directory" >&2
				return 1
			}
			for reference in cassandra.pid scylla-agent.pid scylla-jmx.pid; do
				reference="$$node_directory/$$reference"
				[[ ! -e "$$reference" && ! -L "$$reference" ]] && continue
				[[ -f "$$reference" && ! -L "$$reference" && -O "$$reference" ]] || return 1
				rm -f -- "$$reference" || return 1
			done
			reference="$$node_directory/node.conf"
			[[ ! -e "$$reference" && ! -L "$$reference" ]] && continue
			[[ -f "$$reference" && ! -L "$$reference" && -O "$$reference" ]] || return 1
			temporary=$$(mktemp "$$node_directory/.node.conf.cleanup.XXXXXXXX") || return 1
			awk '{ key=$$0; sub(/[[:space:]]*:.*/, "", key); if (key != "pid" && key != "\047pid\047" && key != "\"pid\"") print }' \
				"$$reference" > "$$temporary" || return 1
			chmod --reference="$$reference" "$$temporary" || return 1
			mv -f -- "$$temporary" "$$reference" || return 1
		 done
	}
	release_id_reservations() {
		local id_lock_root reservation name owner
		id_lock_root=$$(ccm_id_lock_root) || return 1
		for reservation in "$$id_lock_root"/*.reservation \
			"$$id_lock_root"/.[1-9]*.reservation.*.tmp; do
			[[ -e "$$reservation" || -L "$$reservation" ]] || continue
			name=$$(basename -- "$$reservation")
			[[ "$$name" =~ ^[1-9][0-9]*\.reservation$$ \
				|| "$$name" =~ ^\.[1-9][0-9]*\.reservation\.[A-Za-z0-9]+\.tmp$$ ]] \
				|| continue
			[[ -f "$$reservation" && ! -L "$$reservation" && -O "$$reservation" ]] \
				|| continue
			owner=$$(< "$$reservation")
			[[ "$$owner" == "$$run_directory" ]] || continue
			rm -- "$$reservation" || return 1
		done
	}

	cleanup_failed=0
	resources_safe=1
	ccm_commands_safe=1
	diagnostics_ready=0
	terminate_recorded_groups || {
		cleanup_failed=1
		resources_safe=0
		ccm_commands_safe=0
	}
	terminate_run_processes || {
		cleanup_failed=1
		resources_safe=0
		ccm_commands_safe=0
	}
	if (umask 077; mkdir -p -- "$$diagnostics_directory") \
		&& chmod 700 -- "$$diagnostics_directory"; then
		diagnostics_ready=1
	else
		echo "Unable to prepare CCM diagnostics under $$results_root" >&2
		cleanup_failed=1
	fi
	clusters_directory="$$run_directory/clusters"
	if (( ccm_commands_safe != 0 )) \
		&& [[ -d "$$clusters_directory" && ! -L "$$clusters_directory" ]]; then
		clusters_directory=$$(readlink -f -- "$$clusters_directory")
		for config_directory in "$$clusters_directory"/*; do
			[[ -e "$$config_directory" || -L "$$config_directory" ]] || continue
			[[ -d "$$config_directory" && ! -L "$$config_directory" && -O "$$config_directory" \
				&& "$$(dirname -- "$$(readlink -f -- "$$config_directory")")" == "$$clusters_directory" ]] || {
				echo "Refusing unsafe CCM config directory: $$config_directory" >&2
				cleanup_failed=1
				resources_safe=0
				continue
			}
			config_directory=$$(readlink -f -- "$$config_directory")
			declare -A cluster_names=()
			if [[ -f "$$config_directory/CURRENT" && ! -L "$$config_directory/CURRENT" ]]; then
				cluster_name=$$(tr -d '\r\n' < "$$config_directory/CURRENT")
				[[ -z "$$cluster_name" ]] || cluster_names["$$cluster_name"]=1
			fi
			for cluster_config in "$$config_directory"/*/cluster.conf; do
				[[ -f "$$cluster_config" && ! -L "$$cluster_config" ]] || continue
				cluster_names["$$(basename -- "$$(dirname -- "$$cluster_config")")"]=1
			done
			for possible_cluster in "$$config_directory"/*; do
				[[ -d "$$possible_cluster" && ! -L "$$possible_cluster" ]] || continue
				for possible_node in "$$possible_cluster"/node*; do
					[[ -d "$$possible_node" && ! -L "$$possible_node" ]] || continue
					[[ -f "$$possible_node/node.conf" \
						|| -f "$$possible_node/conf/scylla.yaml" \
						|| -f "$$possible_node/cassandra.pid" \
						|| -f "$$possible_node/scylla-agent.pid" \
						|| -f "$$possible_node/scylla-jmx.pid" ]] || continue
					cluster_names["$$(basename -- "$$possible_cluster")"]=1
					break
				done
			done
			for cluster_name in "$${!cluster_names[@]}"; do
				[[ "$$cluster_name" =~ ^[A-Za-z0-9_-][A-Za-z0-9_.-]*$$ ]] || {
					echo "Refusing unsafe CCM cluster name: $$cluster_name" >&2
					cleanup_failed=1
					resources_safe=0
					continue
				}
				cluster_directory="$$config_directory/$$cluster_name"
				if [[ -e "$$cluster_directory" || -L "$$cluster_directory" ]]; then
					[[ -d "$$cluster_directory" && ! -L "$$cluster_directory" && -O "$$cluster_directory" \
						&& "$$(dirname -- "$$(readlink -f -- "$$cluster_directory")")" == "$$config_directory" ]] || {
						echo "Refusing unsafe CCM cluster directory: $$cluster_directory" >&2
						cleanup_failed=1
						resources_safe=0
						continue
					}
					cluster_directory=$$(readlink -f -- "$$cluster_directory")
				fi
				collect_diagnostics "$$config_directory" "$$cluster_name" || cleanup_failed=1
				sanitize_node_references "$$cluster_directory" || {
					cleanup_failed=1
					resources_safe=0
					continue
				}
				ccm=$$(ccm_resolve_executable "$(SCYLLA_CCM_PATH)") || {
					echo "CCM is unavailable while removing $$cluster_name" >&2
					cleanup_failed=1
					resources_safe=0
					continue
				}
				timeout --signal=TERM --kill-after=5s 120s \
					env SCYLLA_CCM_RUN_DIR="$$run_directory" \
					"$$ccm" remove --config-dir "$$config_directory" "$$cluster_name" || {
					timeout --signal=TERM --kill-after=5s 60s \
						env SCYLLA_CCM_RUN_DIR="$$run_directory" \
						"$$ccm" stop --config-dir "$$config_directory" || true
					timeout --signal=TERM --kill-after=5s 120s \
						env SCYLLA_CCM_RUN_DIR="$$run_directory" \
						"$$ccm" remove --config-dir "$$config_directory" "$$cluster_name" || true
				}
				if [[ -e "$$cluster_directory" || -L "$$cluster_directory" ]]; then
					echo "CCM cluster remains after cleanup: $$cluster_directory" >&2
					cleanup_failed=1
					resources_safe=0
				fi
				collect_diagnostics "$$config_directory" "$$cluster_name" || cleanup_failed=1
			done
			unset cluster_names
		done
	elif (( ccm_commands_safe == 0 )) \
		&& [[ -e "$$clusters_directory" || -L "$$clusters_directory" ]]; then
		echo "Skipping CCM removal because run processes remain unresolved" >&2
		cleanup_failed=1
		resources_safe=0
	elif [[ -e "$$clusters_directory" || -L "$$clusters_directory" ]]; then
		echo "Refusing unsafe CCM clusters directory: $$clusters_directory" >&2
		cleanup_failed=1
		resources_safe=0
	fi
	terminate_run_processes || {
		cleanup_failed=1
		resources_safe=0
	}
	if (( resources_safe != 0 )); then
		release_id_reservations || cleanup_failed=1
	fi
	if (( cleanup_failed != 0 )); then
		echo "CCM cleanup failed; preserving $$run_directory" >&2
		exit 1
	fi
	if (( legacy_cleanup != 0 )); then
		for entry in "$$run_directory"/* "$$run_directory"/.[!.]* "$$run_directory"/..?*; do
			[[ -e "$$entry" || -L "$$entry" ]] || continue
			[[ "$$entry" == "$$diagnostics_directory" ]] || rm -rf -- "$$entry"
		done
	else
		rm -rf -- "$$run_directory"
	fi

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

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == 3.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		cp -f Makefile /tmp/tmp-Makefile
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
		mv -f /tmp/tmp-Makefile ./Makefile
	fi
