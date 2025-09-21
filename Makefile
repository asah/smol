EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

# pg_regress tests
REGRESS = smol_basic

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: dockerbuild dockercheck

dockerbuild:
	docker build -t smol-dev .

dockercheck: dockerbuild
	@echo "Running regression tests inside Docker (smol-dev)"
	docker run --rm -t -v "$$PWD":/workspace -w /workspace smol-dev bash -lc 'set -euo pipefail; \
	  make clean && make && make install; \
	  pg_ctlcluster 16 main start; \
	  su - postgres -c "cd /workspace && make installcheck"; \
	  pg_ctlcluster 16 main stop'

.PHONY: dockerbench-50m-par6
dockerbench-50m-par6: dockerbuild
	@echo "Running 50M SMALLINT benchmark (par=6) inside Docker"
	-@docker rm -f smol-bench >/dev/null 2>&1 || true
	docker run -d --name smol-bench -v "$$PWD":/workspace -w /workspace smol-dev sleep infinity >/dev/null
	docker exec smol-bench bash -lc 'set -euo pipefail; make clean && make && make install'
	docker exec smol-bench bash -lc 'pg_ctlcluster 16 main start'
	docker exec smol-bench bash -lc "su - postgres -c 'psql -v ON_ERROR_STOP=1 -c \"DROP EXTENSION IF EXISTS smol CASCADE; CREATE EXTENSION smol;\"'"
	docker exec smol-bench bash -lc 'mkdir -p /workspace/results'
	@echo "par=6, thr=5000"
	docker exec smol-bench bash -lc "su - postgres -c 'psql -v ON_ERROR_STOP=1 -v dtype=int2 -v rows=50000000 -v par_workers=6 -v maxval=32767 -v thr=5000 -f /workspace/bench/smol_bench.sql'" | tee results/bench_brc_50M_par6_thr5000.out
	@echo "par=6, thr=30000"
	docker exec smol-bench bash -lc "su - postgres -c 'psql -v ON_ERROR_STOP=1 -v dtype=int2 -v rows=50000000 -v par_workers=6 -v maxval=32767 -v thr=30000 -f /workspace/bench/smol_bench.sql'" | tee results/bench_brc_50M_par6_thr30000.out
	docker exec smol-bench bash -lc 'pg_ctlcluster 16 main stop'
	-@docker rm -f smol-bench >/dev/null 2>&1 || true
