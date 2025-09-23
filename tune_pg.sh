#!/usr/bin/env bash
# tune_pg.sh - Auto-tune postgresql.conf for OLAP/HTAP on Linux (Ubuntu/Debian-friendly), works in containers too.
# Edits an existing postgresql.conf based on RAM, CPU, disk. Uses conservative defaults in Docker.
#
# Needed for SMOL development especiall;y inside Docker containers - SMOL index build falls over with tiny RAM.
#
# Env overrides (examples):
#   SHARED_BUFFERS_RATIO=0.25 EFFECTIVE_CACHE_RATIO=0.70 WORK_MEM_CAP_MB=128 MAINT_WORK_MEM_CAP_MB=1024
#   OLAP_STATS_TARGET=200 PARALLEL_GATHER_CAP=4
#
# Usage:
#   ./tune_pg.sh /path/to/postgresql.conf [--pgdata /path/to/PGDATA] [--dry-run]
set -euo pipefail

CONF_PATH="${1:-}"
if [[ -z "${CONF_PATH}" || "${CONF_PATH}" == -* ]]; then
  echo "Usage: $0 /path/to/postgresql.conf [--pgdata /path/to/PGDATA] [--dry-run]" >&2
  exit 1
fi
shift || true

PGDATA=""
DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pgdata) PGDATA="${2:-}"; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    *) echo "Unknown arg: $1" >&2; exit 1;;
  esac
done

if [[ ! -f "$CONF_PATH" ]]; then
  echo "postgresql.conf not found at: $CONF_PATH" >&2
  exit 1
fi

timestamp="$(date +%Y%m%d-%H%M%S)"
BACKUP_PATH="${CONF_PATH}.bak.${timestamp}"

# -------- Helpers --------
in_docker() {
  [[ -f "/.dockerenv" ]] && return 0
  # cgroup hint
  grep -qE '(docker|kubepods|containerd)' /proc/1/cgroup 2>/dev/null && return 0 || return 1
}

# Print or apply a conf change (idempotent). Accepts "key" "value".
apply_conf() {
  local key="$1"; shift
  local val="$*"
  # Escape slashes for sed
  local sval
  sval="$(printf '%s' "$val" | sed 's/[\/&]/\\&/g')"

  if grep -Eiq "^\s*#?\s*${key}\s*=" "$CONF_PATH"; then
    # Replace existing (commented or not)
    if [[ $DRY_RUN -eq 1 ]]; then
      echo "[plan] ${key} = ${val}"
    else
      sed -Ei "s|^\s*#?\s*(${key})\s*=.*|\1 = ${sval}|" "$CONF_PATH"
    fi
  else
    if [[ $DRY_RUN -eq 1 ]]; then
      echo "[plan] ${key} = ${val}   (append)"
    else
      echo "${key} = ${val}" >> "$CONF_PATH"
    fi
  fi
}

# Round to int
round() { awk "BEGIN{printf \"%d\", ($1)+0.5}"; }

# -------- Detect system resources --------
MEM_KB=$(grep -i MemTotal /proc/meminfo | awk '{print $2}')
MEM_GB=$(awk "BEGIN{printf \"%.2f\", ${MEM_KB}/1024/1024}")
CPU_CORES=$(nproc --all 2>/dev/null || echo 1)

# Ratios (tunable via env)
SHARED_BUFFERS_RATIO="${SHARED_BUFFERS_RATIO:-0.25}"        # 25% RAM for OLAP/HTAP
EFFECTIVE_CACHE_RATIO="${EFFECTIVE_CACHE_RATIO:-0.70}"      # 70% RAM (reduced in Docker below)
WORK_MEM_FRACTION_OF_RAM="${WORK_MEM_FRACTION_OF_RAM:-0.15}" # 15% RAM budgeted across connections
WORK_MEM_CAP_MB="${WORK_MEM_CAP_MB:-128}"                   # hard cap per session for safety
MAINT_WORK_MEM_CAP_MB="${MAINT_WORK_MEM_CAP_MB:-1024}"      # 1GB cap
OLAP_STATS_TARGET="${OLAP_STATS_TARGET:-200}"
PARALLEL_GATHER_CAP="${PARALLEL_GATHER_CAP:-4}"

if in_docker; then
  # Be more conservative inside containers to avoid host memory pressure
  EFFECTIVE_CACHE_RATIO="${EFFECTIVE_CACHE_RATIO_IN_DOCKER:-0.50}"
fi

# Read current max_connections to size work_mem; default 100 if missing
CUR_MAX_CONN=$(grep -Ei '^\s*#?\s*max_connections\s*=' "$CONF_PATH" | tail -1 | awk -F'=' '{print $2}' | awk '{print $1}' || true)
if [[ -z "${CUR_MAX_CONN}" ]]; then CUR_MAX_CONN=100; fi
if ! [[ "$CUR_MAX_CONN" =~ ^[0-9]+$ ]]; then CUR_MAX_CONN=100; fi

# Compute memory settings
# shared_buffers
SB_MB=$(round "$(awk "BEGIN{print (${MEM_KB}/1024)*${SHARED_BUFFERS_RATIO}"} )")
# cap shared_buffers a bit by default to avoid excessive allocation on small hosts
if (( SB_MB < 128 )); then SB_MB=128; fi
if (( SB_MB > 8192 )); then SB_MB=8192; fi  # 8GB cap by default

# effective_cache_size (advisory)
ECS_MB=$(round "$(awk "BEGIN{print (${MEM_KB}/1024)*${EFFECTIVE_CACHE_RATIO}"} )")
if (( ECS_MB < SB_MB )); then ECS_MB=$SB_MB; fi

# maintenance_work_mem
MWM_MB=$(round "$(awk "BEGIN{
  target = (${MEM_KB}/1024)*0.10;  # 10% of RAM
  if (target > ${MAINT_WORK_MEM_CAP_MB}) target = ${MAINT_WORK_MEM_CAP_MB};
  if (target < 64) target = 64;
  print target
}")")

# work_mem (budgeted fraction of RAM / connections)
WM_MB=$(round "$(awk "BEGIN{
  budget = (${MEM_KB}/1024)*${WORK_MEM_FRACTION_OF_RAM};
  perconn = budget/${CUR_MAX_CONN};
  if (perconn > ${WORK_MEM_CAP_MB}) perconn = ${WORK_MEM_CAP_MB};
  if (perconn < 4) perconn = 4;
  print perconn
}")")

# -------- Disk: rotational vs SSD and quick throughput test --------
STORAGE_CLASS="ssd"  # default assume SSD (common on servers/cloud)
DISK_MBPS=0
TARGET_DIR="${PGDATA:-$(dirname "$CONF_PATH")}"

# Try to infer underlying block device rotational flag
detect_rotational() {
  local dir="$1"
  local dev
  dev="$(df -P "$dir" | awk 'NR==2{print $1}')"
  # Handle mapped/overlay devices
  local base
  base="$(basename "$dev")"
  if [[ -e "/sys/block/$base/queue/rotational" ]]; then
    cat "/sys/block/$base/queue/rotational"
  elif [[ -L "/sys/class/block/$base" ]]; then
    local link
    link="$(readlink -f "/sys/class/block/$base")"
    if [[ -e "$link/queue/rotational" ]]; then
      cat "$link/queue/rotational"
    else
      echo ""
    fi
  else
    echo ""
  fi
}

rot=$(detect_rotational "$TARGET_DIR" 2>/dev/null || echo "")
if [[ "$rot" == "1" ]]; then STORAGE_CLASS="hdd"; fi

# quick write test (256MB) with fsync, avoid oflag=direct for portability
TMPFILE="${TARGET_DIR%/}/.pg_tune_disk_test.$$"
(
  sync
  t_start=$(date +%s%N)
  dd if=/dev/zero of="$TMPFILE" bs=1M count=256 conv=fsync,nocreat 2>/dev/null
  sync
  t_end=$(date +%s%N)
  elapsed_ns=$((t_end - t_start))
  if (( elapsed_ns > 0 )); then
    DISK_MBPS=$(awk -v dur="$elapsed_ns" 'BEGIN{printf "%.0f", (256*1e9)/dur}')
  else
    DISK_MBPS=0
  fi
) || true
rm -f "$TMPFILE" 2>/dev/null || true

# Decide storage class by speed if rotational unknown
if [[ -z "$rot" && $DISK_MBPS -gt 0 ]]; then
  if (( DISK_MBPS >= 500 )); then STORAGE_CLASS="ssd"; else STORAGE_CLASS="hdd"; fi
fi

# Set IO-related parameters
if [[ "$STORAGE_CLASS" == "ssd" ]]; then
  RANDOM_PAGE_COST="1.1"
  SEQ_PAGE_COST="1.0"
  EFFECTIVE_IO_CONCURRENCY="200"   # NVMe can go higher; 200 is safe & impactful
  MAX_WAL_SIZE_GB="8"
else
  RANDOM_PAGE_COST="1.5"
  SEQ_PAGE_COST="1.1"
  EFFECTIVE_IO_CONCURRENCY="2"
  MAX_WAL_SIZE_GB="4"
fi

# -------- Parallelism --------
MPW="$CPU_CORES"  # max_worker_processes
MPW_TOTAL="$CPU_CORES"  # max_parallel_workers
# per gather: up to half cores, but cap to PARALLEL_GATHER_CAP
MPW_GATHER=$(( CPU_CORES/2 ))
if (( MPW_GATHER < 2 )); then MPW_GATHER=2; fi
if (( MPW_GATHER > PARALLEL_GATHER_CAP )); then MPW_GATHER=$PARALLEL_GATHER_CAP; fi
if (( MPW_TOTAL < MPW_GATHER )); then MPW_TOTAL=$MPW_GATHER; fi

# -------- WAL/checkpoints (bias toward OLAP safety) --------
CHECKPOINT_TIMEOUT_MIN="15"        # 15 min
CHECKPOINT_COMPLETION_TARGET="0.9" # spread I/O

# -------- Apply changes --------
echo "Detected: ${MEM_GB} GiB RAM, ${CPU_CORES} cores, storage=${STORAGE_CLASS}, disk~${DISK_MBPS} MB/s" >&2
if [[ $DRY_RUN -eq 0 ]]; then
  cp "$CONF_PATH" "$BACKUP_PATH"
  echo "Backup created: $BACKUP_PATH" >&2
fi

# Memory
apply_conf shared_buffers "${SB_MB}MB"
apply_conf effective_cache_size "${ECS_MB}MB"
apply_conf work_mem "${WM_MB}MB"
apply_conf maintenance_work_mem "${MWM_MB}MB"

# OLAP/HTAP helpful toggles
apply_conf default_statistics_target "${OLAP_STATS_TARGET}"
apply_conf track_io_timing "on"
apply_conf wal_compression "on"
apply_conf random_page_cost "${RANDOM_PAGE_COST}"
apply_conf seq_page_cost "${SEQ_PAGE_COST}"
apply_conf effective_io_concurrency "${EFFECTIVE_IO_CONCURRENCY}"

# Parallelism
apply_conf max_worker_processes "${MPW}"
apply_conf max_parallel_workers "${MPW_TOTAL}"
apply_conf max_parallel_workers_per_gather "${MPW_GATHER}"
apply_conf parallel_leader_participation "on"

# WAL / checkpoints
apply_conf max_wal_size "${MAX_WAL_SIZE_GB}GB"
apply_conf checkpoint_timeout "${CHECKPOINT_TIMEOUT_MIN}min"
apply_conf checkpoint_completion_target "${CHECKPOINT_COMPLETION_TARGET}"

# JIT: generally helpful for big OLAP queries; keep thresholds high to avoid tiny-query overhead
apply_conf jit "on"
apply_conf jit_above_cost "100000"
apply_conf jit_inline_above_cost "200000"
apply_conf jit_optimize_above_cost "500000"

# Autovacuum remains ON for HTAP; give it some headroom on large hosts
if (( MEM_KB > 8*1024*1024 )); then # >8GB
  apply_conf autovacuum_work_mem "256MB"
fi

# Safety in containers (optional caps)
if in_docker; then
  # Avoid excessive shared buffers in tight cgroups
  :
fi

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Dry run complete. No changes written." >&2
else
  echo "Tuning complete. Restart PostgreSQL for settings to take effect." >&2
fi
