# Recovery and execution boundaries

Workers register sessions and renew leases. Each claimed job carries a fencing epoch. When an owner loses its lease, a replacement can take over; a stale owner cannot commit its old result.

This protects committed runtime state, but does not provide exactly-once execution of external services. If a worker dies after a model responds but before the result commits, another attempt may call the model again.

The local Pi adapter runs a Pi coding-agent executable as a host process. PostgreSQL stores coordination state and, in this prototype, objects. The Pi process runs locally while the configured model provider may be remote. Containerized Wasm workers must use a separate namespace from native Pi jobs.
