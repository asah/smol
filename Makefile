SHELL := /bin/bash
EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

PG_CFLAGS=-Wno-declaration-after-statement

# Coverage build flags (gcov-compatible)
ifeq ($(COVERAGE),1)
PG_CFLAGS += --coverage -O0 -DSMOL_TEST_COVERAGE
SHLIB_LINK += --coverage
endif

# pg_regress tests: optimized to 30 essential tests (was 41)
# Reduced by 27% while maintaining 100% coverage via redundancy analysis
REGRESS = smol_100pct_coverage smol_between smol_build_edges smol_copy_coverage smol_coverage_batch_prefetch smol_coverage_complete smol_coverage_direct smol_coverage_gaps smol_deep_backward_navigation smol_duplicates smol_edge_coverage smol_empty_table smol_equality_stop smol_growth smol_include smol_include_rle_mismatch smol_int2 smol_multilevel_btree smol_options_coverage smol_parallel smol_parallel_build_test smol_prefetch_boundary smol_rightmost_descend smol_rle_edge_cases smol_rle_include_sizes smol_runtime_keys_coverage smol_synthetic_tests smol_text_include_guc smol_types smol_validate_catalog

# Load extension before each test to allow standalone test execution
REGRESS_OPTS = --load-extension=smol

# smol_debug_coverage smol_debug_log

# Use explicit path inside the Docker image; tests/builds run in Docker
PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell if [ -x $(PG_CONFIG) ]; then $(PG_CONFIG) --pgxs; fi)
include $(PGXS)

# Common Docker utilities for building/testing in a clean PG18 toolchain
.PHONY: dbuild drestart dexec dpsql dcodex psql

# compile the extension and create the docker image
dbuild:
	docker build -t smol .

# start the docker instance, killing any old one
dstart:
	if docker ps -a | grep smol; then echo "[docker] Killing old instance 'smol'"; docker rm -f smol; fi
	echo "[docker] Creating docker instance 'smol' from image 'smol'"
	docker run --init -m 4GB -d -u root --name smol -v "$$PWD":/home/postgres smol sleep infinity
	echo "[docker] Container 'smol' is ready. building..."
	docker exec -u postgres -w /home/postgres smol make build
	echo "[docker] starting postgresql..."
	docker exec -u postgres -w /home/postgres smol make start
	echo "[docker] done/ready."
	docker exec -u root -w /home/postgres smol bash -c "npm install -g @anthropic-ai/claude-code"

# jump into the docker instance e.g. to run top
# -e OPENAI_API_KEY="$(OPENAI_API_KEY)"
dexec:
	docker exec -it -u postgres -w /home/postgres smol bash

# jump into the docker instance e.g. to run top
dpsql:
	docker exec -it -u postgres -w /home/postgres smol psql

# jump into the docker and run codex - note long startup time while it reads
#dcodex:
#	docker exec smol bash -c "mkdir -p /root/.codex"
#	docker cp .codex/config.toml smol:/root/.codex/config.toml
#	echo -e "{\n \"OPENAI_API_KEY\": \"$(OPENAI_API_KEY)\"\n}\n" > .codex/auth.json
#	docker cp .codex/auth.json smol:/root/.codex/auth.json
#	docker exec -e OPENAI_API_KEY="$(OPENAI_API_KEY)" -it smol bash -c "codex -a never --sandbox danger-full-access \"you are running inside a Docker container; to understand the goals, restore your memory and know what to work on next, please read AGENTS.md and do what it says.\""


dclaude:
	docker exec -u postgres smol bash -c "mkdir -p .claude"
	docker cp $(HOME)/.claude/.credentials.json smol:/home/postgres/.claude/.credentials.json
	docker exec -u postgres -w /home/postgres  -it smol bash -c "claude --allowedTools 'Bash:*,ReadFile:*,WriteFile:(/home/postgres/*),DeleteFile:(/home/postgres/*),git,grep,ls,python,bash,psql,su,make' --model claude-sonnet-4-5-20250929 \"you are running inside a Docker container; to understand the goals, restore your memory and know what to work on next, please read *.md, smol.c, sql/*, bench/*. Read AGENTS.md and do what it says.\""

# ---------------------------------------------------------------------------
# Benchmarks - Pretty output with Python runner + legacy SQL benchmarks
# ---------------------------------------------------------------------------
.PHONY: bench bench-quick bench-full bench-thrash bench-repeats
.PHONY: bench-pressure bench-extreme bench-legacy

# Main benchmark targets using Python runner (new v2 suite)
bench-quick: start
	@echo "$(shell tput bold)Running quick benchmark suite...$(shell tput sgr0)"
	@python3 bench/runner.py --quick

bench-full: start
	@echo "$(shell tput bold)Running full comprehensive benchmark suite...$(shell tput sgr0)"
	@python3 bench/runner.py --full

# Convenience alias
bench: bench-quick

# Help target for benchmarks
bench-help:
	@echo ""
	@echo "$(shell tput bold)SMOL Benchmark Suite v2$(shell tput sgr0)"
	@echo "════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "$(shell tput bold)Quick Start:$(shell tput sgr0)"
	@echo "  make bench              # Run quick benchmark (alias for bench-quick)"
	@echo "  make bench-quick        # Quick suite (~30 sec, 17 workloads)"
	@echo "  make bench-full         # Full comprehensive suite (~15-20 min)"
	@echo ""
	@echo "$(shell tput bold)Features:$(shell tput sgr0)"
	@echo "  • Auto-scales based on shared_buffers"
	@echo "  • 5 workload classes (timeseries, dimension, events, sparse, composite)"
	@echo "  • Regression detection against baseline"
	@echo "  • Decision tree recommendations"
	@echo ""
	@echo "$(shell tput bold)Output:$(shell tput sgr0)"
	@echo "  • JSON results in bench/results/"
	@echo "  • Markdown reports"
	@echo "  • Actionable SMOL vs BTREE recommendations"
	@echo ""
	@echo "$(shell tput bold)Direct Python Usage:$(shell tput sgr0)"
	@echo "  python3 bench/runner.py --quick"
	@echo "  python3 bench/runner.py --full"
	@echo ""
	@echo "Legacy benchmarks archived in bench/archive/"
	@echo ""

# ---------------------------------------------------------------------------
# Inside-container targets
# Use these when you are already inside the smol Docker container.
# ---------------------------------------------------------------------------

.PHONY: buildclean build start stop psql pgcheck

# Resolve PostgreSQL bin dir from PG_CONFIG
PG_BIN := $(dir $(PG_CONFIG))
PGDATA := /home/postgres/pgdata

production: clean stop install start installcheck
	@echo "rebuilding and testing production build from scratch."

buildclean:
	@echo "cleaning current build: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE) clean

rebuild:
	@echo "Building and installing extension: make=$(MAKE)"
	@set -euo pipefail; \
	  touch smol.c && $(MAKE) build

build:
	@echo "Building extension: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE)

deldata:
	@echo "deleting $(PGDATA)..."
	@set -euo pipefail; \
	  /bin/rm -fr $(PGDATA)

start:
	@echo "Starting PostgreSQL in container (initdb if needed)"
	@set -euo pipefail; \
	  if [ ! -s $(PGDATA)/PG_VERSION ]; then \
	    $(PG_BIN)initdb -D $(PGDATA); \
	  fi; \
          echo "tuning postgresql.conf for the current env"; \
	  ./tune_pg.sh $(PGDATA)/postgresql.conf; \
	  sed -i 's/^#*shared_buffers = .*$$/shared_buffers = 64MB/' $(PGDATA)/postgresql.conf; \
	  chown postgres $(PGDATA)/postgresql.conf $(PGDATA)/postgresql.conf.bak*; \
	  if $(PG_BIN)pg_ctl -D $(PGDATA) status >/dev/null 2>&1; then \
	    echo "PostgreSQL already running"; \
	  else \
	    $(PG_BIN)pg_ctl -D $(PGDATA) -l /tmp/pg.log -w start; \
	  fi; \
	  echo "Waiting for server readiness..."; \
	  i=0; until $(PG_BIN)/pg_isready -h /tmp -p 5432 -d postgres -q || [ $$i -ge 60 ]; do sleep 1; i=$$((i+1)); done; [ $$i -lt 60 ]; \
	  echo "server ready."

stop-usrlocalpgsqldata:
	@if ps aux | grep '/usr/local/pgsql/bin/postgres -D /usr/local/pgsql/data' | grep -v grep; then \
            echo 'stopping PostgreSQL PGDATA=$(PGDATA)'; \
            pg_ctl -D /usr/local/pgsql/data stop; \
            sleep 1; \
        fi

stop-pgdata:
	@bash -c "if ps aux | grep '/usr/local/pgsql/bin/postgres -D $(PGDATA)' | grep -v grep; then echo 'stopping PostgreSQL PGDATA=$(PGDATA)'; pg_ctl -D '$(PGDATA)' stop; sleep 1; fi"

stop: stop-pgdata stop-usrlocalpgsqldata

# Interactive psql inside the container (run as postgres)
psql:
	$(PG_BIN)psql

pgcheck: build start
	@echo "Running regression tests (installcheck)"
	@set -euo pipefail; \
	  $(MAKE) installcheck; \
	  $(MAKE) stop

# ---------------------------------------------------------------------------
# Code Coverage Targets (Ubuntu 24.04/Docker only)
# ---------------------------------------------------------------------------
.PHONY: coverage coverage-build coverage-clean coverage-test coverage-html

# Clean coverage artifacts
coverage-clean:
	@echo "[coverage] Cleaning coverage data..."
	@rm -f *.gcda *.gcno *.gcov smol.so smol.o
	@rm -rf coverage_html
	@echo "[coverage] Coverage data cleaned."

# Build with coverage instrumentation
coverage-build: coverage-clean
	@echo "[coverage] Building with coverage instrumentation..."
	@COVERAGE=1 $(MAKE) 
	@sudo COVERAGE=1 $(MAKE) install
	@echo "[coverage] Coverage build complete."

# Run tests with coverage
coverage-test: stop start
	@set -euo pipefail; \
	  $(MAKE) installcheck; $(MAKE) stop

# Generate HTML coverage report using lcov
coverage-html:
	@echo "[coverage] Generating HTML coverage report..."
	@if ! command -v lcov >/dev/null 2>&1; then \
		echo "[coverage] lcov not found: installing..."; \
		sudo apt-get update -qq && sudo apt-get install -y -qq lcov >/dev/null 2>&1; \
	fi
	@lcov --capture --directory . --output-file coverage.info --quiet 2>/dev/null || true
	@lcov --remove coverage.info '/usr/*' --output-file coverage.info --quiet 2>/dev/null || true
	@genhtml coverage.info --output-directory coverage_html --quiet 2>/dev/null || true
	@if [ -d coverage_html ]; then \
		echo "[coverage] HTML report generated in coverage_html/index.html"; \
	else \
		echo "[coverage] HTML generation failed."; \
		echo "[coverage] Ensure smol.c.gcov exists or run 'make coverage' first."; \
	fi

# Complete coverage workflow - build, test, report, and check
coverage: coverage-clean coverage-build coverage-test
	@echo "[coverage] Generating coverage report..."
	@gcov -o . smol.c > /dev/null 2>&1 || echo "[coverage] gcov execution completed"
	@echo "[coverage] Checking coverage percentage..."
	@if [ ! -f smol.c.gcov ]; then \
		echo "[coverage] ERROR: No coverage data generated." >&2; \
		exit 2; \
	fi
	@scripts/calc_cov.sh
	@echo ""
	@echo "[coverage] Verifying 100% coverage target..."
	@scripts/calc_cov.sh | awk ' \
		/Excluded covered:/ { excl_cov = $$4 + 0 } \
		/Coverage: / { \
			gsub(/%/, "", $$2); \
			cov = $$2 + 0; \
		} \
		END { \
			failed = 0; \
			if (cov < 100.00) { \
				printf "[coverage] ✗ ERROR: Coverage is %.2f%%, target is 100.00%%\n", cov; \
				failed = 1; \
			} else { \
				printf "[coverage] ✓ Coverage is %.2f%% (meets target)\n", cov; \
			} \
			if (excl_cov > 0) { \
				printf "[coverage] ✗ ERROR: Found %d \"Excluded covered\" lines\n", excl_cov; \
				printf "[coverage]\n"; \
				printf "[coverage] What are \"Excluded covered\" lines?\n"; \
				printf "[coverage] ═══════════════════════════════════════════\n"; \
				printf "[coverage] These are lines marked with GCOV_EXCL_LINE (or in GCOV_EXCL_START/STOP blocks)\n"; \
				printf "[coverage] that are ACTUALLY covered by tests. This means:\n"; \
				printf "[coverage]   • The code IS being tested\n"; \
				printf "[coverage]   • But it is marked as excluded from coverage\n"; \
				printf "[coverage]   • This is a waste - the exclusion marker should be removed!\n"; \
				printf "[coverage]\n"; \
				printf "[coverage] How to fix:\n"; \
				printf "[coverage] ──────────\n"; \
				printf "[coverage] 1. Run: ./scripts/calc_cov.sh -e\n"; \
				printf "[coverage] 2. Review the listed lines with GCOV_EXCL markers\n"; \
				printf "[coverage] 3. Remove the GCOV_EXCL_LINE, GCOV_EXCL_START, or GCOV_EXCL_STOP markers\n"; \
				printf "[coverage] 4. Re-run: make coverage\n"; \
				printf "[coverage]\n"; \
				printf "[coverage] Why this matters:\n"; \
				printf "[coverage] ────────────────\n"; \
				printf "[coverage] Only truly untestable code should be excluded (e.g., unreachable error paths,\n"; \
				printf "[coverage] platform-specific code). If tests already cover it, the exclusion is misleading.\n"; \
				printf "[coverage]\n"; \
				failed = 1; \
			} \
			if (failed) exit 1; \
		} \
	'
	@echo ""
	@echo "[coverage] ✓ Coverage analysis complete!"
	@echo "[coverage] Review smol.c.gcov for line-by-line coverage"
	@echo "[coverage] Run 'scripts/calc_cov.sh -v' for uncovered line details"
	@echo "[coverage] Run 'scripts/calc_cov.sh -e' for excluded covered line details"
	@echo "[coverage] Run 'make coverage-html' for HTML report"


