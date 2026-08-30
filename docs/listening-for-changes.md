# Listening for changes

Every change Driftless applies fires `pg_notify` on the `driftless_changes`
channel, in the same transaction as the write, with a payload carrying ids
only:

```json
{"type": "subscription", "id": "sub_123"}
```

Notifications are not durable. A listener that was down misses the ring but
never the row, so the complete pattern is: catch up on everything changed since
your last cursor when you connect, then listen. Widen the catch-up query to
every table your worker reacts to, and persist the cursor yourself.

## Go (pgx)

```go
conn, _ := pgx.Connect(ctx, dbURL) // dedicated connection, not a pool
conn.Exec(ctx, "LISTEN driftless_changes")

// catch up on anything missed while nobody was listening
rows, _ := conn.Query(ctx,
	"SELECT 'subscription', id FROM stripe.subscriptions WHERE updated_at > $1", since)
// handle each row, then:

for {
	n, _ := conn.WaitForNotification(ctx) // {"type":"subscription","id":"sub_123"}
	// read the row, act
}
```

## Node (pg)

```js
const client = new pg.Client({ connectionString: dbURL }); // dedicated, not a pool
await client.connect();
await client.query("LISTEN driftless_changes");

// catch up on anything missed while nobody was listening
await client.query(
  "SELECT 'subscription' AS type, id FROM stripe.subscriptions WHERE updated_at > $1",
  [since]);
// handle each row, then:

client.on("notification", (msg) => {
  const { type, id } = JSON.parse(msg.payload); // read the row, act
});
```

## Python (psycopg 3)

```python
conn = psycopg.connect(db_url, autocommit=True)  # dedicated, not a pool
conn.execute("LISTEN driftless_changes")

# catch up on anything missed while nobody was listening
conn.execute(
    "SELECT 'subscription', id FROM stripe.subscriptions WHERE updated_at > %s", (since,))
# handle each row, then:

for notice in conn.notifies():
    change = json.loads(notice.payload)  # read the row, act
```

## Connection poolers

The listener needs a direct or session-pooled connection. Transaction-mode
poolers (pgbouncer, Supavisor) hand statements to arbitrary backend sessions,
so `LISTEN` through them silently receives nothing. Everything else in your app
can keep using the pooler.

## Supabase

This works on the direct connection. Alternatively, subscribe to changes on the
`stripe.*` tables with Supabase Realtime and skip LISTEN/NOTIFY entirely.
