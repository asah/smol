#!/bin/sh

psql -c 'SELECT pid, age(clock_timestamp(), query_start), usename, query FROM pg_stat_activity;'
