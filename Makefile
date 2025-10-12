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

# pg_regress tests: keep regression small and fast
REGRESS = smol_basic smol_parallel smol_include smol_types smol_duplicates smol_rle_cache smol_text smol_twocol_uuid_int4 smol_twocol_date_int4 smol_twocol_int8 smol_io_efficiency smol_compression smol_explain_cost smol_edge_cases smol_backward_scan smol_parallel_scan smol_error_paths smol_coverage_direct smol_between smol_parallel_batch smol_debug_coverage smol_edge_coverage smol_rescan_buffer smol_validate smol_growth smol_debug_log smol_validate_errors smol_include_rle smol_loop_guard smol_rle_edge_cases smol_tree_navigation smol_rle_deep_coverage smol_key_rle_includes smol_copy_coverage smol_tall_trees smol_tall_tree_fanout smol_validate_catalog smol_int2 smol_parallel_full smol_easy_coverage smol_cost_nokey smol_options_coverage smol_text32_toolong smol_empty_table smol_rightmost_descend smol_rle_32k_limit smol_twocol_parallel_uuid_date smol_backward_varwidth smol_backward_equality smol_build_edges smol_include_rle_mismatch smol_text_multipage smol_zero_copy smol_synthetic_tests smol_rle_65k_boundary smol_coverage_gaps smol_100pct_coverage smol_key_rle_basic smol_catalog_corrupt smol_edgecases smol_force_parallel_rescan smol_multi_type smol_parallel_build smol_parallel_rescan_attempt smol_text_norle smol_cursor_features smol_coverage_100pct smol_coverage_complete smol_equality_stop smol_page_bounds_coverage smol_page_advance_bounds smol_backward_include_sizes smol_rle_include_sizes smol_runtime_keys_coverage smol_multilevel_btree smol_zerocopy_backward smol_deep_backward_navigation smol_text_include_guc smol_parallel_build_test

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
	docker run -m 4GB -d --name smol -v "$$PWD":/home/postgres smol sleep infinity
	echo "[docker] Container 'smol' is ready. building..."
	docker exec -u postgres -w /home/postgres smol make build
	echo "[docker] starting postgresql..."
	docker exec -u postgres -w /home/postgres smol make start
	echo "[docker] done/ready."

# jump into the docker instance e.g. to run top
dexec:
	docker exec -e OPENAI_API_KEY="$(OPENAI_API_KEY)" -it -w /home/postgres smol bash

# jump into the docker instance e.g. to run top
dpsql:
	docker exec -it -u postgres smol psql

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

# Main benchmark targets using Python runner (pretty output)
bench-quick: start
	@echo "$(shell tput bold)Running quick benchmark suite...$(shell tput sgr0)"
	@python3 bench/bench_runner.py --quick

bench-full: start
	@echo "$(shell tput bold)Running full comprehensive benchmark suite...$(shell tput sgr0)"
	@python3 bench/bench_runner.py --full

bench-thrash: start
	@echo "$(shell tput bold)Running thrashing test...$(shell tput sgr0)"
	@python3 bench/bench_runner.py --thrash

bench-repeats: start
	@echo "$(shell tput bold)Running benchmark with 5 repetitions...$(shell tput sgr0)"
	@python3 bench/bench_runner.py --quick --repeats 5

# Convenience alias
bench: bench-quick

# Help target for benchmarks
bench-help:
	@echo ""
	@echo "$(shell tput bold)SMOL Benchmark Suite$(shell tput sgr0)"
	@echo "════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "$(shell tput bold)Quick Start:$(shell tput sgr0)"
	@echo "  make bench              # Run quick benchmark (alias for bench-quick)"
	@echo "  make bench-quick        # Quick suite (3 tests, ~30s)"
	@echo "  make bench-full         # Full comprehensive suite (~2-5 min)"
	@echo "  make bench-thrash       # Cache efficiency test (large dataset)"
	@echo "  make bench-repeats      # Quick suite with 5 repetitions"
	@echo ""
	@echo "$(shell tput bold)Output:$(shell tput sgr0)"
	@echo "  • Pretty console output with color-coded performance"
	@echo "  • CSV files saved to results/ directory"
	@echo "  • Speedup ratios and compression statistics"
	@echo ""
	@echo "$(shell tput bold)Legacy SQL Benchmarks:$(shell tput sgr0)"
	@echo "  make bench-pressure     # Buffer pressure test (20M rows)"
	@echo "  make bench-extreme      # Extreme compression test"
	@echo "  make bench-legacy       # Both legacy tests"
	@echo ""
	@echo "$(shell tput bold)Direct Python Usage:$(shell tput sgr0)"
	@echo "  python3 bench/bench_runner.py --quick"
	@echo "  python3 bench/bench_runner.py --full --repeats 10"
	@echo ""
	@echo "See bench/README.md for detailed documentation"
	@echo ""

# ---------------------------------------------------------------------------
# Inside-container targets
# Use these when you are already inside the smol Docker container.
# ---------------------------------------------------------------------------

.PHONY: buildclean build start stop psql pgcheck

# Resolve PostgreSQL bin dir from PG_CONFIG
PG_BIN := $(dir $(PG_CONFIG))
PGDATA := /home/postgres/pgdata

buildclean:
	@echo "cleaning current build: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE) clean

rebuild:
	@echo "Building and installing extension: make=$(MAKE)"
	@set -euo pipefail; \
	  touch smol.c && $(MAKE) build

build:
	@echo "Building and installing extension: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE) && $(MAKE) install

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

stop:
	@echo "Stopping PostgreSQL in container"
	$(PG_BIN)pg_ctl -D $(PGDATA) -m fast -w stop >/dev/null 2>&1 || true

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
	@rm -f *.gcda *.gcno *.gcov smol.so
	@rm -rf coverage_html
	@echo "[coverage] Coverage data cleaned."

# Build with coverage instrumentation
coverage-build: coverage-clean
	@echo "[coverage] Building with coverage instrumentation..."
	@$(MAKE) clean
	@COVERAGE=1 $(MAKE) all
	@sudo COVERAGE=1 $(MAKE) install
	@echo "[coverage] Coverage build complete."

# Run tests with coverage
coverage-test:
	@echo "[coverage] Ensuring PostgreSQL is restarted with new extension..."
	@$(MAKE) stop
	@sleep 1
	@$(MAKE) start
	@echo "[coverage] Running regression tests with coverage..."
	@$(MAKE) installcheck || true
	@echo "[coverage] Stopping PostgreSQL to flush coverage data..."
	@$(MAKE) stop
	@sleep 3
	@echo "[coverage] Test run complete."

# Generate HTML coverage report using lcov
coverage-html:
	@echo "[coverage] Generating HTML coverage report..."
	@if ! command -v lcov >/dev/null 2>&1; then \
		echo "[coverage] Installing lcov..."; \
		apt-get update -qq && apt-get install -y -qq lcov >/dev/null 2>&1; \
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
	@scripts/calc_cov.sh | awk '/Coverage: / { \
		gsub(/%/, "", $$2); \
		cov = $$2 + 0; \
		if (cov < 100.00) { \
			printf "[coverage] ERROR: Coverage is %.2f%%, target is 100.00%%\n", cov; \
			exit 1; \
		} else { \
			printf "[coverage] ✓ Coverage is %.2f%% (meets target)\n", cov; \
		} \
	}'
	@echo ""
	@echo "[coverage] Coverage analysis complete!"
	@echo "[coverage] Review smol.c.gcov for line-by-line coverage"
	@echo "[coverage] Run 'scripts/calc_cov.sh --condensed' for uncovered line details"
	@echo "[coverage] Run 'make coverage-html' for HTML report"


