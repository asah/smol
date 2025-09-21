EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

# pg_regress tests
REGRESS = smol_basic

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
