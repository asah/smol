SHELL := /bin/bash
EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

# pg_regress tests: keep regression small and fast
REGRESS = smol_basic smol_twocol

# Use explicit path inside the Docker image; tests/builds run in Docker
PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell if [ -x $(PG_CONFIG) ]; then $(PG_CONFIG) --pgxs; fi)
include $(PGXS)

# Common Docker utilities for building/testing in a clean PG18 toolchain
.PHONY: dockerbuild dockercreate dockercodex

# compile the extension and create the docker image
dockerbuild:
	docker build -t smol .

# start the docker instance, killing any old one
dockerrestart:
	if docker ps -a | grep smol; then echo "[docker] Killing old instance 'smol'"; docker rm -f smol; fi
	echo "[docker] Creating docker instance 'smol' from image 'smol'"
	docker run -d --name smol -v "$$PWD":/workspace -w /workspace smol sleep infinity
	echo "[docker] Container 'smol' is ready."

# jump into the docker instance e.g. to run top
dockerexec:
	docker exec -it smol bash

# jump into the docker and run codex - note long startup time while it reads
dockercodex:
	echo "{ \"OPENAI_API_KEY\": \"$(OPENAI_API_KEY)\" }" > .codex/auth.json
	docker exec -it smol mkdir -p .codex
	docker cp .codex/auth.json smol:.codex
	docker cp .codex/config.toml smol:.codex
	docker exec -it smol codex -a never --sandbox danger-full-access "read AGENTS.md and do what it says"

# ---------------------------------------------------------------------------
# Inside-container targets
# Use these when you are already inside the smol Docker container.
# They mirror dockercheck and dockerbench-smol-btree-5m without using docker exec.
# ---------------------------------------------------------------------------

.PHONY: insidebuild insidestart insidestop insidecheck insidebench-smol-btree-5m

# Resolve PostgreSQL bin dir from PG_CONFIG
PG_BIN := $(dir $(PG_CONFIG))
PGDATA := /var/lib/postgresql/data

insidebuild:
	@echo "[inside] Building and installing extension in current container"
	@set -euo pipefail; \
	  $(MAKE) clean && $(MAKE) && $(MAKE) install

insidestart:
	@# chmod to fix perms for the postgres user - stupid hack
	chmod 777 /workspace;
	@echo "[inside] Starting PostgreSQL in container (initdb if needed)"
	@set -euo pipefail; \
	  if [ ! -s $(PGDATA)/PG_VERSION ]; then \
	    su - postgres -c "$(PG_BIN)initdb -D $(PGDATA)"; \
	  fi; \
          echo "tuning postgresql.conf for the current env"; \
	  ./tune_pg.sh $(PGDATA)/postgresql.conf; \
	  chown postgres $(PGDATA)/postgresql.conf $(PGDATA)/postgresql.conf.bak*; \
	  if su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) status" >/dev/null 2>&1; then \
	    echo "[inside] PostgreSQL already running"; \
	  else \
	    su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) -l /tmp/pg.log -w start"; \
	  fi; \
	  echo "[inside] Waiting for server readiness..."; \
	  su - postgres -c 'i=0; until $(PG_BIN)/pg_isready -h /tmp -p 5432 -d postgres -q || [ $$i -ge 60 ]; do sleep 1; i=$$((i+1)); done; [ $$i -lt 60 ]'; \
	  echo "[inside] server ready."

insidestop:
	@echo "[inside] Stopping PostgreSQL in container"
	-@su - postgres -c "$(PG_BIN)pg_ctl -D $(PGDATA) -m fast -w stop" >/dev/null 2>&1 || true

insidecheck: insidebuild insidestart
	@echo "[inside] Running regression tests (installcheck)"
	@set -euo pipefail; \
	  su - postgres -c "cd /workspace && make installcheck"; \
	  $(MAKE) insidestop

insidebench-smol-btree-5m: insidebuild insidestart
	@echo "[inside] Running SMOL vs BTREE benchmark (default 5M rows int2,int2)"
	@set -euo pipefail; \
      chmod +x bench/smol_vs_btree.sh; \
      su - postgres -c "env ROWS=$${ROWS:-5000000} THRESH=$${THRESH:-15000} TIMEOUT_SEC=$${TIMEOUT_SEC:-30} KILL_AFTER=$${KILL_AFTER:-5} BATCH=$${BATCH:-250000} SINGLECOL=$${SINGLECOL:-0} bash /workspace/bench/smol_vs_btree.sh"; \
      echo "[inside] Benchmark finished; leaving PostgreSQL running. Use 'make insidestop' when done."
