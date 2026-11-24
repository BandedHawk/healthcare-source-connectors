#!/usr/bin/env bash
# A base backup of the PoC's Postgres (HIPAA.md §11.4, L4), run in the postgres container by poc.sh db_backup with
# STAMP (UTC time) and HOME_DIR (/var/lib/postgresql) set: pg_basebackup's tar, compressed, with the WAL it needs
# (base.tar.gz, pg_wal.tar.gz, backup_manifest), made in a staging directory on the data volume and then moved
# whole into the outbox, which the wal-shipper sends to s3://<backup bucket>/base/<STAMP>/. Refused if the volume
# hasn't twice the databases' size free.
set -euo pipefail
b="$HOME_DIR/backup"
need="$(psql -qAt -U postgres -d postgres -c 'SELECT sum(pg_database_size(oid)) FROM pg_database')"
free="$(df --output=avail -B1 "$HOME_DIR" | tail -n 1)"
if (( free < need * 2 )); then
  echo "base-backup.sh: $((free >> 20)) MiB free on the volume, $((need >> 20)) MiB of databases: too little" >&2
  exit 1
fi
rm -rf "$b/staging"
mkdir -p "$b/staging" "$b/outbox/base"
pg_basebackup -U postgres -D "$b/staging/$STAMP" -Ft -z -X stream --checkpoint=fast -l "poc.sh $STAMP"
mv "$b/staging/$STAMP" "$b/outbox/base/$STAMP"
