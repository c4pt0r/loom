use loom_contracts::{Error, ObjectRef, ObjectStore};
use loom_object_store_s3::S3Config;

/// Run unchanged against RustFS or a disposable AWS S3 bucket.
#[tokio::test]
#[ignore = "requires LOOM_S3_BUCKET and AWS credentials; optional LOOM_S3_ENDPOINT"]
async fn s3_contract() {
    let config = S3Config::from_env(false).unwrap();
    let namespace = format!("s3-contract-{}", uuid::Uuid::new_v4());
    let store = config.connect(&namespace).unwrap();
    let bytes: Vec<u8> = (0..8193).map(|i| (i % 256) as u8).collect();
    let object = store.put(bytes.clone()).await.unwrap();
    assert_eq!(store.put(bytes.clone()).await.unwrap(), object);
    let reopened = config.connect(&namespace).unwrap();
    assert_eq!(reopened.get(&object).await.unwrap(), bytes);
    assert_eq!(reopened.size(&object).await.unwrap(), 8193);
    assert_eq!(
        reopened.get_range(&object, 255, 700).await.unwrap(),
        bytes[255..955]
    );
    assert_eq!(
        reopened.get_range(&object, 8192, 1).await.unwrap(),
        bytes[8192..]
    );
    assert_eq!(
        reopened.get_range(&object, 8193, 0).await.unwrap(),
        Vec::<u8>::new()
    );
    for (offset, length) in [(8193, 1), (8194, 0), (8192, 2), (u64::MAX, 2)] {
        assert!(matches!(
            reopened.get_range(&object, offset, length).await,
            Err(Error::Invalid(_))
        ));
    }
    let empty = store.put(vec![]).await.unwrap();
    assert_eq!(store.get(&empty).await.unwrap(), Vec::<u8>::new());
    assert_eq!(store.size(&empty).await.unwrap(), 0);
    assert_eq!(
        store.get_range(&empty, 0, 0).await.unwrap(),
        Vec::<u8>::new()
    );
    let missing = ObjectRef::new("0".repeat(64));
    assert!(matches!(store.get(&missing).await, Err(Error::NotFound(_))));
    assert!(matches!(
        store.size(&missing).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store.get_range(&missing, 0, 0).await,
        Err(Error::NotFound(_))
    ));
    let other = config.connect(&(namespace.clone() + "-other")).unwrap();
    assert!(matches!(other.get(&object).await, Err(Error::NotFound(_))));
    let mut writers = tokio::task::JoinSet::new();
    // Independent clients race to publish the same new key with conditional writes.
    for _ in 0..8 {
        let client = config.connect(&namespace).unwrap();
        writers.spawn(async move {
            client
                .put(b"concurrent immutable publication".to_vec())
                .await
        });
    }
    let mut refs = vec![];
    while let Some(result) = writers.join_next().await {
        refs.push(result.unwrap().unwrap());
    }
    assert!(refs.iter().all(|r| *r == refs[0]));
    assert_eq!(
        reopened.get(&refs[0]).await.unwrap(),
        b"concurrent immutable publication"
    );
}
