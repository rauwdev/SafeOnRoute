#!/bin/bash
set -e

TS=$(date +"%Y-%m-%d_%H%M%S")
OUT="/backups/backup_${PGDATABASE}_${TS}.sql"

echo "[INFO] Creating backup: $OUT"
pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > "$OUT"
echo "[OK] Backup created: $OUT"
ls -lh "$OUT"