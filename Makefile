SHELL := /bin/bash
EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

PG_CFLAGS=-Wno-declaration-after-statement

# Coverage build flags (gcov-compatible)
ifeq ($(COVERAGE),1)
PG_CFLAGS += --coverage -O0
SHLIB_LINK += --coverage
endif

# pg_regress tests: keep regression small and fast
REGRESS = smol_basic smol_parallel smol_include smol_types smol_duplicates smol_rle_cache smol_text smol_twocol_uuid_int4 smol_twocol_date_int4 smol_io_efficiency smol_compression smol_explain_cost smol_edge_cases

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
	docker run -m 4GB -d --name smol -v "$$PWD":/workspace -w /workspace smol sleep infinity
	echo "[docker] Container 'smol' is ready. building..."
	docker exec smol make build
	echo "[docker] starting postgresql..."
	docker exec smol make start
	echo "[docker] symlinking /workspace to /home/postgres..."
	docker exec -w /home/postgres smol bash -c "/bin/ln -s /workspace/* ."
	echo "[docker] done/ready."

# jump into the docker instance e.g. to run top
dexec:
	docker exec -e OPENAI_API_KEY="$(OPENAI_API_KEY)" -it smol bash

# jump into the docker instance e.g. to run top
dpsql:
	docker exec -it -u postgres smol psql

# jump into the docker and run codex - note long startup time while it reads
dcodex:
	docker exec smol bash -c "mkdir -p /root/.codex"
	docker cp .codex/config.toml smol:/root/.codex/config.toml
	echo -e "{\n \"OPENAI_API_KEY\": \"$(OPENAI_API_KEY)\"\n}\n" > .codex/auth.json
	docker cp .codex/auth.json smol:/root/.codex/auth.json
	docker exec -e OPENAI_API_KEY="$(OPENAI_API_KEY)" -it smol bash -c "codex -a never --sandbox danger-full-access \"you are running inside a Docker container; to understand the goals, restore your memory and know what to work on next, please read AGENTS.md and do what it says.\""


dclaude:
	docker exec smol bash -c "mkdir -p /root/.claude"
	docker cp $(HOME)/.claude/.credentials.json smol:/root/.claude/.credentials.json
	docker exec  -it smol bash -c "claude --allowedTools 'Bash:*,ReadFile:*,WriteFile:(/workspace),DeleteFile:(/workspace),git,grep,ls,python,bash,psql,su,make' --model claude-sonnet-4-5-20250929 \"you are running inside a Docker container; to understand the goals, restore your memory and know what to work on next, please read *.md, smol.c, sql/*, bench/*. Read AGENTS.md and do what it says.\""

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
.PHONY: bench-quick bench-thrash bench-pressure bench-extreme bench-full

bench-quick: start
	@echo "[bench] Running quick benchmark suite (SMOL vs BTREE)..."
	@mkdir -p results
	@su - postgres -c "cd /workspace && /usr/local/pgsql/bin/psql -f bench/quick.sql" | tee results/bench-quick-$(shell date +%Y%m%d-%H%M%S).log
	@echo "[bench] Quick benchmark complete. See results/ for output."

bench-thrash: start
	@echo "[bench] Running thrash test (demonstrates cache efficiency with shared_buffers=64MB)..."
	@mkdir -p results
	@echo "[bench] This test shows SMOL fits in cache while BTREE requires disk I/O"
	@echo "[bench] Expected: BTREE reads ~1900 blocks from disk, SMOL reads 0"
	@su - postgres -c "cd /workspace && /usr/local/pgsql/bin/psql -f bench/thrash_clean.sql" | tee results/bench-thrash-$(shell date +%Y%m%d-%H%M%S).log
	@echo "[bench] Thrash test complete. See THRASH_TEST_SUMMARY.md for interpretation."

bench-pressure: start
	@echo "[bench] Running buffer pressure test (20M rows, demonstrates cache efficiency)..."
	@mkdir -p results
	@echo "[bench] This test uses EXPLAIN (ANALYZE, BUFFERS) to show I/O differences"
	@echo "[bench] Expected runtime: 3-5 minutes"
	@su - postgres -c "cd /workspace && /usr/local/pgsql/bin/psql -f bench/buffer_pressure.sql" | tee results/bench-pressure-$(shell date +%Y%m%d-%H%M%S).log
	@echo "[bench] Buffer pressure test complete. Check results/ for detailed I/O statistics."

bench-extreme: start
	@echo "[bench] Running EXTREME buffer pressure test (HIGHLY repetitive data)..."
	@mkdir -p results
	@echo "[bench] This test demonstrates maximum RLE compression advantage"
	@echo "[bench] 20M rows with only 1000 distinct keys → massive compression!"
	@echo "[bench] Expected: SMOL 8-10x smaller than BTREE"
	@echo "[bench] Expected runtime: 4-6 minutes"
	@su - postgres -c "cd /workspace && /usr/local/pgsql/bin/psql -v shared_buffers=64MB -f bench/extreme_pressure.sql" | tee results/bench-extreme-$(shell date +%Y%m%d-%H%M%S).log
	@echo "[bench] Extreme pressure test complete. Check results/ for compression and thrashing data."

bench-full: bench-quick bench-thrash bench-pressure bench-extreme
	@echo "[bench] Full benchmark suite complete."

# ---------------------------------------------------------------------------
# Inside-container targets
# Use these when you are already inside the smol Docker container.
# ---------------------------------------------------------------------------

.PHONY: buildclean build start stop psql pgcheck

# Resolve PostgreSQL bin dir from PG_CONFIG
PG_BIN := $(dir $(PG_CONFIG))
PGDATA := /var/lib/postgresql/data

buildclean:
	@echo "cleaning current build: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE) clean

rebuild:
	@echo "Building and installing extension: make=$(MAKE)"
	@set -euo pipefail; \
	  touch smol.c && $(MAKE) && $(MAKE) install

build:
	@echo "Building and installing extension: make=$(MAKE)"
	@set -euo pipefail; \
	  $(MAKE) && $(MAKE) install

deldata:
	@echo "deleting $(PGDATA)..."
	@set -euo pipefail; \
	  /bin/rm -fr $(PGDATA)

start:
	@# chmod to fix perms for the postgres user - stupid hack
	chmod 777 /workspace;
	@echo "Starting PostgreSQL in container (initdb if needed)"
	@set -euo pipefail; \
	  if [ ! -s $(PGDATA)/PG_VERSION ]; then \
	    su - postgres -c "$(PG_BIN)initdb -D $(PGDATA)"; \
	  fi; \
          echo "tuning postgresql.conf for the current env"; \
	  ./tune_pg.sh $(PGDATA)/postgresql.conf; \
	  sed -i 's/^#*shared_buffers = .*$$/shared_buffers = 64MB/' $(PGDATA)/postgresql.conf; \
	  chown postgres $(PGDATA)/postgresql.conf $(PGDATA)/postgresql.conf.bak*; \
	  if su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) status" >/dev/null 2>&1; then \
	    echo "PostgreSQL already running"; \
	  else \
	    su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) -l /tmp/pg.log -w start"; \
	  fi; \
	  echo "Waiting for server readiness..."; \
	  su - postgres -c 'i=0; until $(PG_BIN)/pg_isready -h /tmp -p 5432 -d postgres -q || [ $$i -ge 60 ]; do sleep 1; i=$$((i+1)); done; [ $$i -lt 60 ]'; \
	  echo "server ready."

stop:
	@echo "Stopping PostgreSQL in container"
	-@su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) -m fast -w stop" >/dev/null 2>&1 || true

# Interactive psql inside the container (run as postgres)
psql:
	@su - postgres -c "$(PG_BIN)psql"

pgcheck: build start
	@echo "Running regression tests (installcheck)"
	@set -euo pipefail; \
	  su - postgres -c "cd /workspace && make installcheck"; \
	  $(MAKE) stop

# ---------------------------------------------------------------------------
# Code Coverage Targets (Ubuntu 24.04/Docker only)
# ---------------------------------------------------------------------------
.PHONY: coverage coverage-build coverage-clean coverage-report coverage-html

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
	@COVERAGE=1 $(MAKE) all install
	@echo "[coverage] Coverage build complete."

# Run tests with coverage
coverage-test: start
	@echo "[coverage] Running regression tests with coverage..."
	@su - postgres -c "cd /workspace && make installcheck" || true
	@echo "[coverage] Stopping PostgreSQL to flush coverage data..."
	@$(MAKE) stop
	@sleep 1
	@echo "[coverage] Test run complete."

# Generate coverage report
coverage-report:
	@echo "[coverage] Generating coverage report..."
	@gcov -o . smol.c > /dev/null 2>&1 || echo "[coverage] gcov execution completed"
	@if [ -f smol.c.gcov ]; then \
		echo "[coverage] Coverage report generated: smol.c.gcov"; \
		echo ""; \
		echo "=== COVERAGE SUMMARY ==="; \
		awk ' \
			/^        -:/ { next } \
			/^    #####:/ { uncov++ } \
			/^        [0-9]+:/ { cov++ } \
			END { \
				total = cov + uncov; \
				if (total > 0) { \
					pct = (cov * 100.0) / total; \
					printf "Lines executed: %.2f%% (%d/%d)\n", pct, cov, total; \
				} \
			}' smol.c.gcov; \
		echo ""; \
		echo "=== UNCOVERED LINES ==="; \
		grep -n "^    #####:" smol.c.gcov | head -50 | awk -F: '{printf "Line %s: not executed\n", $$1}'; \
	else \
		echo "[coverage] ERROR: No coverage data found. Run 'make coverage-test' first."; \
	fi

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
		echo "[coverage] HTML generation failed. Using text report instead."; \
		$(MAKE) coverage-report; \
	fi

# Complete coverage workflow
coverage: coverage-build coverage-test coverage-report
	@echo ""
	@echo "[coverage] Full coverage analysis complete!"
	@echo "[coverage] Review smol.c.gcov for line-by-line coverage"
	@echo "[coverage] Run 'make coverage-html' for HTML report"


