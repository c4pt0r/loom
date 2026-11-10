//! Storage-mediated hash shuffle. No database, transport, or Wasm dependency.
use loom_contracts::*;
use sha2::{Digest, Sha256};

pub const VERSION: u32 = 1;
pub const MAX_BLOCK_BYTES: usize = 64 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_MANIFEST_BLOCKS: usize = 4096;

fn invalid(message: impl ToString) -> Error {
    Error::Invalid(message.to_string())
}
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes> {
    let bytes = serde_json::to_vec(value).map_err(invalid)?;
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid("shuffle payload exceeds 4 MiB"));
    }
    Ok(bytes)
}
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid("shuffle payload exceeds 4 MiB"));
    }
    serde_json::from_slice(bytes).map_err(invalid)
}

/// Stable across languages, architectures, and process restarts. Empty keys are valid.
pub fn partition_for(key: &[u8], layout: &ShuffleLayout) -> Result<u32> {
    layout.validate()?;
    let hash = Sha256::digest(key);
    let value = u64::from_be_bytes(hash[..8].try_into().unwrap());
    Ok((value % u64::from(layout.partitions)) as u32)
}

/// Publish blocks first, then their manifest. Only the later fenced task commit
/// makes this manifest discoverable through a task dependency.
pub async fn publish(
    store: &dyn BlobStore,
    layout: &ShuffleLayout,
    output: &[u8],
) -> Result<ObjectRef> {
    layout.validate()?;
    let records: Vec<ShuffleRecord> = decode(output)?;
    let mut buckets = vec![Vec::new(); layout.partitions as usize];
    for record in records {
        let partition = partition_for(&record.key, layout)? as usize;
        let bytes = serde_json::to_vec(&record).map_err(invalid)?;
        if bytes.len() + 2 > MAX_BLOCK_BYTES {
            return Err(invalid("shuffle record exceeds 64 KiB encoded block limit"));
        }
        buckets[partition].push(bytes);
    }
    let mut manifest = ShuffleManifest {
        version: VERSION,
        layout: layout.clone(),
        partitions: vec![Vec::new(); layout.partitions as usize],
    };
    for (partition, records) in buckets.into_iter().enumerate() {
        let mut block = vec![b'['];
        let mut count = 0;
        for record in records {
            let comma = usize::from(count > 0);
            if block.len() + comma + record.len() + 1 > MAX_BLOCK_BYTES {
                block.push(b']');
                manifest.partitions[partition].push(write_block(store, block, count).await?);
                block = vec![b'['];
                count = 0;
            }
            if count > 0 {
                block.push(b',');
            }
            block.extend(record);
            count += 1;
        }
        if count > 0 {
            block.push(b']');
            manifest.partitions[partition].push(write_block(store, block, count).await?);
        }
    }
    store.put(encode(&manifest)?).await
}
async fn write_block(store: &dyn BlobStore, bytes: Bytes, records: u32) -> Result<ShuffleBlock> {
    let length = bytes.len() as u32;
    Ok(ShuffleBlock {
        object: store.put(bytes).await?,
        records,
        bytes: length,
    })
}

pub fn validate_manifest(manifest: &ShuffleManifest, expected: &ShuffleLayout) -> Result<()> {
    expected.validate()?;
    if manifest.version != VERSION
        || &manifest.layout != expected
        || manifest.partitions.len() != expected.partitions as usize
    {
        return Err(invalid("incompatible shuffle manifest version or layout"));
    }
    let blocks: usize = manifest.partitions.iter().map(Vec::len).sum();
    if blocks > MAX_MANIFEST_BLOCKS {
        return Err(invalid("too many shuffle blocks in manifest"));
    }
    for block in manifest.partitions.iter().flatten() {
        if block.records == 0 || block.bytes < 2 || block.bytes as usize > MAX_BLOCK_BYTES {
            return Err(invalid("invalid shuffle block metadata"));
        }
    }
    Ok(())
}

/// Validate only the selected partition's data; never fetch another partition's blocks.
pub async fn read_block(
    store: &dyn BlobStore,
    block: &ShuffleBlock,
    layout: &ShuffleLayout,
    partition: u32,
) -> Result<Vec<ShuffleRecord>> {
    let bytes = store.get(&block.object).await?;
    if bytes.len() != block.bytes as usize || bytes.len() > MAX_BLOCK_BYTES {
        return Err(invalid("shuffle block length mismatch"));
    }
    let records: Vec<ShuffleRecord> = decode(&bytes)?;
    if records.len() != block.records as usize {
        return Err(invalid("shuffle block record count mismatch"));
    }
    for record in &records {
        if partition_for(&record.key, layout)? != partition {
            return Err(invalid("shuffle key routed to wrong partition"));
        }
    }
    Ok(records)
}
