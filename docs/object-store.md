# Independent object storage

Loom separates coordination from stored payloads. Tasks need both, but the two
interfaces can be backed by different systems.

| Interface | Responsibility | Default adapter |
| --- | --- | --- |
| `ControlStore` | Membership, leases, task scheduling, receipts, fencing, and durable actor state | PostgreSQL coordination rows |
| `ObjectStore` | Wasm components, task input/output objects, shuffle blocks/manifests, and final datasets | PostgreSQL blobs |

Small inline task inputs and actor state can still be part of control records.
Large or reusable task data should use `Input::Object` and immutable references.

```text
driver / worker ---- ControlStore ---- PostgreSQL coordination
       |
       +------------ ObjectStore ---- PostgreSQL blobs (default)
                                      RustFS / AWS S3 / R2

WasmEngine --------- ObjectStore      module loading only
```

## ObjectStore contract

The interface lives in `loom-contracts` and does not depend on a database or SDK:

```rust
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef>;
    async fn get(&self, object: &ObjectRef) -> Result<Bytes>;
    async fn size(&self, object: &ObjectRef) -> Result<u64> {
        Ok(self.get(object).await?.len() as u64)
    }
    async fn get_range(&self, object: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        slice_bytes(&self.get(object).await?, offset, length)
    }
}
```

`BlobStore` remains an alias for source compatibility. All current runtime,
shuffle, and adapter code uses the `ObjectStore` name.

The contract requires:

- Immutable objects, addressed by the SHA-256 digest of their exact bytes.
- Idempotent writes: identical bytes produce the same reference.
- A successful write is readable by every worker configured for that logical
  object-store namespace, including after the writing worker exits for a durable
  provider.
- A missing object returns `NotFound`; provider failure is reported, not silently
  redirected to a different store.
- Data remains available while tasks, retries, manifests, or users reference it.

“Temporary task data” does not imply short-lived local files or an arbitrary TTL.
A mapper can exit before a reducer consumes its blocks. The shared store must keep
those blocks available for downstream work and retries.

## Independent dependency injection

`Stores` composes the providers without mixing their implementations:

```rust
let control: Arc<dyn ControlStore> = /* coordination adapter */;
let objects: Arc<dyn ObjectStore> = /* independently chosen object adapter */;
let backend = Arc::new(Stores::new(control, objects));
```

`Stores` implements the existing `Backend` facade, so the execution runtime and
application code can retain their existing task APIs. It forwards membership and
execution calls exclusively to `control`, and object calls exclusively to `objects`.

The CLI, dashboard, and examples select a provider at their composition root:

```rust
let postgres = Arc::new(loom_backend_postgres::connect(&dsn, &namespace).await?);
let objects = loom_object_store_s3::configured(&namespace, postgres.clone())?;
let stores = Stores::new(postgres, objects);
```

`LOOM_OBJECT_STORE=postgres|rustfs|s3|r2` chooses the adapter. Driver and all
workers must be configured with the same logical object store; an `ObjectRef`
contains a digest, not an endpoint or credentials.

`WasmEngine::new` now accepts only `Arc<dyn ObjectStore>`. It does not need task or
membership access to fetch modules. Shuffle publication and block reads likewise
accept only the object interface. Membership and task commits remain together in
`ControlStore`, because lease/fencing checks must share a consistency domain.

## Implementations and limits

The default Compose configuration remains PostgreSQL for both roles. No new DSN
or infrastructure is required. Separating the interface does not itself move data
out of PostgreSQL.

`loom-object-store-memory::MemoryObjectStore` is an object-only adapter with no
ledger or scheduling implementation. It supports tests and single-process local
execution; cloning it shares the same in-memory namespace. Separate instances are
isolated. It is volatile and is not suitable for independent worker containers.

The separation is tested with PostgreSQL coordination, independent memory objects,
and real Wasm module execution using two worker sessions in one process. Tests
also verify that modules, input/output bytes, and shuffle blocks do not leak into
the coordination adapter's own blob storage.

`loom-object-store-s3` implements AWS S3, RustFS, and R2 with SHA-256 keys,
conditional creation, HEAD metadata, and native range reads. See the
[RustFS, AWS S3, and R2 guide](s3.md) for configuration, deployment, and tests.
Shared filesystems, multipart uploads, and streaming APIs are not implemented.

## Publication and cleanup

Payloads are written before the control store commits their reference. The stores
do not require a cross-system transaction: a failed/fenced commit may leave an
unreferenced immutable object, but must not expose a reference before its payload
is available. The existing retry and fencing protocol remains unchanged.

There is deliberately no `delete` method or automatic TTL yet. Safe cleanup needs
reference/lifecycle tracking across module references, task inputs, outputs, and
shuffle manifests. An external lifecycle rule must not expire still-referenced
objects. Objects currently remain stored indefinitely.

See [verification coverage](object-store-verification.md).

## Range reads

`ObjectStore::get_range(object, offset, length)` supports exact byte-range inputs.
Its default fallback preserves existing adapters; PostgreSQL, RustFS/S3/R2, and the object-only
memory adapter override it. `Input::Object` remains a complete-object read. See
[object slices](scaling-and-inputs.md) for examples, encoding rules, and limits.
