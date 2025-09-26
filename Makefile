SHELL := /bin/bash
EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

PG_CFLAGS=-Wno-declaration-after-statement

# pg_regress tests: keep regression small and fast
REGRESS = smol_basic smol_twocol smol_twocol_uuid_int4 smol_twocol_date_int4 smol_parallel smol_include smol_types smol_dup smol_dup_more smol_more_cases smol_rle_cache smol_text

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
	docker run -d --name smol -v "$$PWD":/workspace -w /workspace smol sleep infinity
	echo "[docker] Container 'smol' is ready. building..."
	docker exec smol make build
	echo "[docker] starting postgresql..."
	docker exec smol make start
	echo "[docker] done/ready."

# jump into the docker instance e.g. to run top
dexec:
	docker exec -it smol bash

# jump into the docker instance e.g. to run top
dpsql:
	docker exec -it -u postgres smol psql

# jump into the docker and run codex - note long startup time while it reads
dcodex:
	echo "{" > .codex/auth.json
	echo "  \"OPENAI_API_KEY\": \"$(OPENAI_API_KEY)\"" >> .codex/auth.json
	echo "}" >> .codex/auth.json
	docker exec -it smol mkdir -p .codex
	docker cp .codex/auth.json smol:.codex
	docker cp .codex/config.toml smol:.codex
	docker exec -it smol bash -c "codex -a never --sandbox danger-full-access \"read AGENTS.md and do what it says\""

# ---------------------------------------------------------------------------
# Inside-container targets
# Use these when you are already inside the smol Docker container.
# ---------------------------------------------------------------------------

.PHONY: buildclean build start stop psql pgcheck bench-smol-btree-5m bench-smol-vs-btree bench bench-all bench-gap-scenario bench-clean bench-text-advantage text-adv-pretty text-adv-plot
.PHONY: rle-adv-pretty rle-adv-plot

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

bench-smol-btree-5m: build start
	@echo "Running SMOL vs BTREE benchmark (selectivity-based, single-key focus)"
	@set -euo pipefail; \
      chmod +x bench/smol_vs_btree_selectivity.sh; \
      su - postgres -c "env REUSE=$${REUSE:-1} ROWS=$${ROWS:-10000000} COLTYPE=$${COLTYPE:-int2} COLS=$${COLS:-2} SELECTIVITY=$${SELECTIVITY:-0.5} WORKERS=$${WORKERS:-5} TIMEOUT_SEC=$${TIMEOUT_SEC:-30} KILL_AFTER=$${KILL_AFTER:-5} BATCH=$${BATCH:-250000} bash /workspace/bench/smol_vs_btree_selectivity.sh"; \
      echo "Benchmark finished; leaving PostgreSQL running. Use 'make stop' when done."

# New, parameterized benchmark: SMOL vs BTREE with selectivity fraction and workers
# Usage ( container): make bench-smol-vs-btree ROWS=5000000 COLTYPE=int2 COLS=2 SELECTIVITY=0.1,0.5,0.9 WORKERS=0,2,5
bench-smol-vs-btree: build start
	@echo "Running SMOL vs BTREE (selectivity-based)"
	@set -euo pipefail; \
      chmod +x bench/smol_vs_btree_selectivity.sh; \
      su - postgres -c "env REUSE=$${REUSE:-1} ROWS=$${ROWS:-10000000} COLTYPE=$${COLTYPE:-int2} COLS=$${COLS:-2} SELECTIVITY=$${SELECTIVITY:-0.5} WORKERS=$${WORKERS:-5} TIMEOUT_SEC=$${TIMEOUT_SEC:-30} KILL_AFTER=$${KILL_AFTER:-5} BATCH=$${BATCH:-250000} bash /workspace/bench/smol_vs_btree_selectivity.sh"; \
      echo "Benchmark finished; leaving PostgreSQL running. Use 'make stop' when done."

# Fast variant: assumes PostgreSQL is already running and extension is built/installed.
# Skips build/start overhead and enables REUSE mode in the script to reuse table/indexes where possible.
.PHONY: bench-smol-vs-btree-fast
bench-smol-vs-btree-fast:
	@echo "Running SMOL vs BTREE (fast, reuse mode)"
	@set -euo pipefail; \
      chmod +x bench/smol_vs_btree_selectivity.sh; \
      su - postgres -c "env REUSE=$${REUSE:-1} ROWS=$${ROWS:-10000000} COLTYPE=$${COLTYPE:-int2} COLS=$${COLS:-2} SELECTIVITY=$${SELECTIVITY:-0.5} WORKERS=$${WORKERS:-5} TIMEOUT_SEC=$${TIMEOUT_SEC:-30} KILL_AFTER=$${KILL_AFTER:-5} BATCH=$${BATCH:-250000} bash /workspace/bench/smol_vs_btree_selectivity.sh"; \
	  echo "Fast benchmark finished; PostgreSQL left running. Use 'make stop' when done."

# Cleanup helper: drop all deterministic bench tables and related indexes by prefix
bench-clean: start
	@echo "Dropping bench tables matching 'bm_%' to reclaim space..."; \
	set -euo pipefail; \
	su - postgres -c "bash -lc '$(PG_BIN)psql -X -v ON_ERROR_STOP=1 -d postgres -qAt -c ''SELECT n.nspname||''.''||c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind = ''\''r''\'' AND c.relname LIKE ''\''bm\\_%''\'';'' | awk '{print "DROP TABLE IF EXISTS "$0" CASCADE;"}' | $(PG_BIN)psql -X -v ON_ERROR_STOP=1 -d postgres -q'"; \
	echo "Done."

# Alternative cleanup that avoids Makefile quoting issues; runs a small script inside container
.PHONY: bench-clean-alt
bench-clean-alt: start
	@echo "Dropping bench tables via bench/bench_clean.sh ..."; \
	su - postgres -c "bash /workspace/bench/bench_clean.sh"; \
	echo "Done."

# Gap-maximizing scenario target (single-key int2, COUNT(*))
bench-gap-scenario: build start
	@echo "Running gap-maximizing SMOL vs BTREE scenario (COUNT(*), key-only int2)"; \
	set -euo pipefail; \
	chmod +x bench/smol_gap_scenario.sh; \
	su - postgres -c "env ROWS=$${ROWS:-10000000} SELECTIVITY=$${SELECTIVITY:-0.5} WORKERS=$${WORKERS:-5} TIMEOUT_SEC=$${TIMEOUT_SEC:-60} KILL_AFTER=$${KILL_AFTER:-5} TBL=$${TBL:-gap_bench} bash /workspace/bench/smol_gap_scenario.sh"; \
	echo "Gap scenario finished; PostgreSQL left running. Use 'make stop' when done."

# Text key advantage benchmark (short identifiers <=32B, C collation)
bench-text-advantage: build start
	@echo "Running SMOL text key advantage benchmark (short identifiers)"; \
	set -euo pipefail; \
	chmod +x bench/smol_text_advantage.sh; \
	su - postgres -c "env ROWS=$${ROWS:-5000000} WORKERS=$${WORKERS:-5} INC=$${INC:-0} UNIQVALS=$${UNIQVALS:-100} SWEEP_UNIQVALS=$${SWEEP_UNIQVALS:-} DISTRIBUTION=$${DISTRIBUTION:-uniform} STRLEN=$${STRLEN:-16} SWEEP_STRLEN=$${SWEEP_STRLEN:-} TIMEOUT_SEC=$${TIMEOUT_SEC:-1800} KILL_AFTER=$${KILL_AFTER:-10} bash /workspace/bench/smol_text_advantage.sh"; \
	echo "Text benchmark finished; PostgreSQL left running. Use 'make stop' when done."

text-adv-pretty:
	@ROWS=$${ROWS:-} WORKERS=$${WORKERS:-} INC=$${INC:-} CSV=$${CSV:-results/text_adv.csv} \
	  python3 bench/pretty_text_adv.py

text-adv-plot:
	@ROWS=$${ROWS:-} WORKERS=$${WORKERS:-} INC=$${INC:-} CSV=$${CSV:-results/text_adv.csv} OUT=$${OUT:-results/text_adv_plot.png} \
	  python3 bench/plot_text_adv.py

# RLE + dupcache advantage benchmark (50M rows, 5 workers default)
.PHONY: bench-rle-advantage
bench-rle-advantage: build start
	@echo "Running SMOL RLE+dupcache advantage benchmark (includes, 5 workers)"; \
	set -euo pipefail; \
	chmod +x bench/smol_rle_dupcache_advantage.sh; \
	su - postgres -c "env ROWS=$${ROWS:-50000000} WORKERS=$${WORKERS:-5} INC=$${INC:-8} COLTYPE=$${COLTYPE:-int2} UNIQVALS=$${UNIQVALS:-1} SWEEP_UNIQVALS=$${SWEEP_UNIQVALS:-} DISTRIBUTION=$${DISTRIBUTION:-uniform} TIMEOUT_SEC=$${TIMEOUT_SEC:-1800} KILL_AFTER=$${KILL_AFTER:-10} REUSE=$${REUSE:-1} bash /workspace/bench/smol_rle_dupcache_advantage.sh"; \
	echo "Benchmark finished; PostgreSQL left running. Use 'make stop' when done."

# Pretty-print latest CSV (filter with ROWS=, WORKERS=, INC=)
rle-adv-pretty:
	@ROWS=$${ROWS:-} WORKERS=$${WORKERS:-} INC=$${INC:-} CSV=$${CSV:-results/rle_adv.csv} \
	  python3 bench/pretty_rle_adv.py

# Generate a simple bar plot from CSV (requires matplotlib)
rle-adv-plot:
	@ROWS=$${ROWS:-} WORKERS=$${WORKERS:-} INC=$${INC:-} CSV=$${CSV:-results/rle_adv.csv} OUT=$${OUT:-results/rle_adv_plot.png} \
	  python3 bench/plot_rle_adv.py

# Curated quick scenarios (fast reuse): single WORKERS per run, ROWS scaled by workers (5 -> 50M)
bench: build start
	@echo "Running curated SMOL vs BTREE scenarios (fast reuse)"; \
	set -euo pipefail; \
	for cols in 1 2; do \
	  for sel in 0.1 0.5 0.9; do \
	    for w in 1 2 5; do \
	      rows=$$(( w * 10000000 )); \
	      echo "# cols=$$cols sel=$$sel workers=$$w rows=$$rows"; \
	      SEL=$$sel COLS=$$cols ROWS=$$rows COLTYPE=$${COLTYPE:-int2} WORKERS=$$w COMPACT=$${COMPACT:-1} REUSE=$${REUSE:-1} $(MAKE) bench-smol-vs-btree-fast; \
	    done; \
	  done; \
	done; \
	echo "Done curated runs"; \
	true

# Exhaustive matrix (heavy): selectivity, type, workers, sizes, columns
bench-all: build start
	@echo "Running exhaustive SMOL vs BTREE matrix (this will take a long time)"; \
	set -euo pipefail; \
	SELS="0.0,0.1,0.25,0.5,0.75,0.9,0.95,0.99,1.0"; \
	for rows in 500000 5000000 50000000; do \
	  for ctype in int2 int4 int8; do \
	    for w in 1 2 5; do \
	      for cols in 1 2 4 8 16; do \
	        echo "# rows=$$rows type=$$ctype workers=$$w cols=$$cols"; \
	        ROWS=$$rows COLTYPE=$$ctype WORKERS=$$w COLS=$$cols SELECTIVITY=$$SELS REUSE=$${REUSE:-1} $(MAKE) bench-smol-vs-btree; \
	      done; \
	    done; \
	  done; \
	done; \
	echo "Done exhaustive runs"; \
	true
