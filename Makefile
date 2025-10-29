SHELL := /bin/bash
EXTENSION = smol
DATA = smol--1.0.sql
MODULE_big = smol

# Base object files
OBJS = smol.o smol_utils.o smol_build.o smol_scan.o

# Add coverage helper for inline functions in smol.h (coverage builds only)
ifeq ($(COVERAGE),1)
OBJS += smol_h_coverage.o
endif

PG_CFLAGS=-Wno-declaration-after-statement

# Coverage build flags (gcov-compatible)
ifeq ($(COVERAGE),1)
PG_CFLAGS += --coverage -O0 -DSMOL_TEST_COVERAGE
PG_CPPFLAGS += -DSMOL_TEST_COVERAGE
SHLIB_LINK += --coverage
endif

# pg_regress tests: Consolidated tests (3 production tests)
# Consolidates 30 original production tests into 3 files (smol_build merged into smol_core)
# smol_scan now includes position-based scan optimization tests
REGRESS_BASE = smol_core smol_scan smol_rle

# Coverage-only tests (10 consolidated tests)
# smol_coverage3 now includes all bloom filter edge cases (consolidates 8 tests into 1)
# Tuple buffering tests merged into smol_scan (production test)
REGRESS_COVERAGE_ONLY = smol_coverage1 smol_coverage2a smol_coverage2b smol_coverage2c smol_coverage2d smol_advanced smol_zone_maps_coverage smol_bloom_skip_coverage smol_int2_bloom_skip smol_int8_bloom_skip

# Full test list: 13 tests for coverage builds (3 production + 10 coverage-only), 3 for production
ifeq ($(COVERAGE),1)
REGRESS = $(REGRESS_BASE) $(REGRESS_COVERAGE_ONLY)
else
REGRESS = $(REGRESS_BASE)
endif

# Load extension before each test to allow standalone test execution
REGRESS_OPTS = --load-extension=smol

# smol_debug_coverage smol_debug_log

# Additional files to clean (coverage artifacts and test results)
EXTRA_CLEAN = *.gcov *.gcda *.gcno *.gcov.* coverage.info coverage_html results/

# Use explicit path inside the Docker image; tests/builds run in Docker
# Allow override via environment variable for CI
PG_CONFIG ?= /usr/local/pgsql/bin/pg_config
PGXS := $(shell if [ -x $(PG_CONFIG) ]; then $(PG_CONFIG) --pgxs; fi)
include $(PGXS)

# Override installcheck to optionally show regression.diffs on failure
# Set SHOW_DIFFS=1 to enable (useful for CI)
installcheck-with-diffs:
	@$(MAKE) installcheck || (exitcode=$$?; \
		if [ "$(SHOW_DIFFS)" = "1" ] && [ -f regression.diffs ]; then \
			echo ""; \
			echo "=================================================="; \
			echo "  Test failures detected - showing regression.diffs"; \
			echo "=================================================="; \
			cat regression.diffs; \
		fi; \
		exit $$exitcode)

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
	docker exec -u root smol chown -R postgres /home/postgres
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
	docker exec -u postgres -w /home/postgres  -it smol bash -c "claude --dangerously-skip-permissions --model claude-sonnet-4-5-20250929 \"you are running inside a Docker container; to understand the goals, restore your memory and know what to work on next, please read *.md, *.c, sql/*, bench/*.\""

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

production: stop clean install start
	@if $(MAKE) installcheck; then \
	  echo "rebuilding and testing production build from scratch."; \
	else \
	  if [ -f regression.diffs ]; then \
	    echo ""; \
	    echo "===== Test failure - First 100 lines of regression.diffs ====="; \
	    head -100 regression.diffs; \
	    echo "===== End of regression.diffs excerpt ====="; \
	    echo ""; \
	  fi; \
	  exit 1; \
	fi

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
	  ./scripts/tune_pg.sh $(PGDATA)/postgresql.conf; \
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
	  if $(MAKE) installcheck; then \
	    $(MAKE) stop; \
	  else \
	    if [ -f regression.diffs ]; then \
	      echo ""; \
	      echo "===== Test failure - First 100 lines of regression.diffs ====="; \
	      head -100 regression.diffs; \
	      echo "===== End of regression.diffs excerpt ====="; \
	      echo ""; \
	    fi; \
	    $(MAKE) stop; \
	    exit 1; \
	  fi

# ---------------------------------------------------------------------------
# Code Coverage Targets (Ubuntu 24.04/Docker only)
# ---------------------------------------------------------------------------
.PHONY: coverage coverage-build coverage-clean coverage-test coverage-html

# Clean coverage artifacts
coverage-clean:
	@echo "[coverage] Cleaning coverage data..."
	@$(MAKE) clean > /dev/null 2>&1
	@rm -rf coverage_html
	@echo "[coverage] Coverage data cleaned."

# Build with coverage instrumentation
coverage-build: stop coverage-clean
	@echo "[coverage] Building with coverage instrumentation..."
	@COVERAGE=1 $(MAKE)
	@sudo COVERAGE=1 $(MAKE) install
	@echo "[coverage] Coverage build complete."

# Run tests with coverage
# Note: Run each test separately to avoid stack overflow from accumulated gcov data.
# Each test run merges coverage data, preventing crashes in long test suites.
coverage-test: stop start
	@echo "[coverage] Running tests individually to prevent stack overflow..."
	@set -euo pipefail; \
	  failed_tests=""; \
	  all_tests="$(REGRESS_BASE) $(REGRESS_COVERAGE_ONLY)"; \
	  test_count=$$(echo $$all_tests | wc -w); \
	  test_num=0; \
	  for test in $$all_tests; do \
	    test_num=$$((test_num + 1)); \
	    echo "[coverage] Running test $$test_num/$$test_count: $$test"; \
	    if COVERAGE=1 $(MAKE) installcheck REGRESS=$$test > /tmp/test_$$test.log 2>&1; then \
	      timing=$$(grep "^ok.*$$test" /tmp/test_$$test.log | sed 's/.*\([0-9]\+ ms\)$$/\1/' || echo ""); \
	      if [ -n "$$timing" ]; then \
	        echo "[coverage] ✓ Test $$test passed ($$timing)"; \
	      else \
	        echo "[coverage] ✓ Test $$test passed"; \
	      fi; \
	    else \
	      echo "[coverage] ✗ Test $$test FAILED"; \
	      failed_tests="$$failed_tests $$test"; \
	      tail -10 /tmp/test_$$test.log; \
	      if [ -f regression.diffs ]; then \
	        echo ""; \
	        echo "[coverage] ===== First 100 lines of regression.diffs ====="; \
	        head -100 regression.diffs; \
	        echo "[coverage] ===== End of regression.diffs excerpt ====="; \
	        echo ""; \
	      fi; \
	    fi; \
	    $(PG_BIN)pg_ctl -D $(PGDATA) restart -w > /dev/null 2>&1; \
	    sleep 1; \
	  done; \
	  $(MAKE) stop; \
	  if [ -n "$$failed_tests" ]; then \
	    echo "[coverage] Failed tests:$$failed_tests"; \
	    exit 1; \
	  fi; \
	  echo "[coverage] All tests passed!"

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
	@gcov -o . smol.c smol_utils.c smol_build.c smol_scan.c > /dev/null 2>&1 || echo "[coverage] gcov execution completed"
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
		/Uncovered:/ { uncov = $$3 + 0 } \
		/Coverage: / { \
			gsub(/%/, "", $$2); \
			cov = $$2 + 0; \
		} \
		END { \
			failed = 0; \
			total_issues = excl_cov + uncov; \
			if (cov < 100.00) { \
				printf "[coverage] ✗ ERROR: Coverage is %.2f%%, target is 100.00%%\n", cov; \
				failed = 1; \
				if (total_issues > 0 && total_issues <= 100) { \
					printf "[coverage]\n"; \
					printf "[coverage] Showing details (total issues: %d):\n", total_issues; \
					printf "[coverage] ════════════════════════════════════════\n"; \
					system("./scripts/calc_cov.sh -v -e"); \
				} else if (total_issues > 100) { \
					printf "[coverage] Too many issues (%d) to show inline.\n", total_issues; \
				} \
				printf "[coverage]\n"; \
				printf "[coverage] Run ./scripts/calc_cov.sh -v -e to see details\n"; \
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


