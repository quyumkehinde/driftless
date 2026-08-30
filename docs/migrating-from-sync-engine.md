# Migrating from stripe/sync-engine

Driftless mirrors sync-engine's table naming, and `driftless import` migrates a
sync-engine database in place, with no copying of your Postgres data out.

```bash
# stop sync-engine, then rename its schema out of the way (instant, metadata-only)
psql "$DATABASE_URL" -c 'ALTER SCHEMA stripe RENAME TO sync_engine'

driftless migrate up                    # creates the fresh stripe schema
driftless import --from-sync-engine     # copies rows, table by table
driftless verify --repair               # re-fetches true state for every object
driftless serve                         # from here on, it stays correct

# when satisfied:
psql "$DATABASE_URL" -c 'DROP SCHEMA sync_engine CASCADE'
```

Import reconstructs objects from sync-engine's typed columns and marks them
import-sourced. The `verify --repair` step then re-fetches every one of them
from Stripe, so the mirror ends up byte-true regardless of what the old
pipeline had dropped.
