#!/bin/bash
set -e

HBA_FILE="${PGDATA}/pg_hba.conf"

# Allow replica bootstrap connections in local Docker network.
if ! grep -q "^host replication all all trust$" "$HBA_FILE"; then
  echo "host replication all all trust" >> "$HBA_FILE"
fi
