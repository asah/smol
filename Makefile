EXTENSION = smol
DATA = smol--1.0.sql
MODULES = smol

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
