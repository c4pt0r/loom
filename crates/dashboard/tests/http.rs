use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use loom_contracts::*;
use loom_dashboard::{Dashboard, router};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn read_only_routes_and_bounded_object_preview() {
    let store = Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::SystemClock,
    )));
    let object = store.put(vec![b'a'; 100000]).await.unwrap();
    let unicode = store.put("界".repeat(30000).into_bytes()).await.unwrap();
    let app = router(
        Dashboard {
            inspector: store.clone(),
            objects: store,
            namespace: "test".into(),
        },
        "127.0.0.1:8787".parse().unwrap(),
    );
    for (path, expected) in [
        ("/", StatusCode::OK),
        ("/api/config", StatusCode::OK),
        ("/api/inspect", StatusCode::OK),
        ("/api/inspect?limit=999", StatusCode::BAD_REQUEST),
        ("/api/job?id=absent", StatusCode::NOT_FOUND),
        ("/api/object?digest=invalid", StatusCode::BAD_REQUEST),
    ] {
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("host", "localhost:8787")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), expected, "{path}");
        assert_eq!(r.headers()["cache-control"], "no-store");
    }
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/object?digest={}", object.digest))
                .header("host", "127.0.0.1:8787")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let data: serde_json::Value =
        serde_json::from_slice(&to_bytes(r.into_body(), 100000).await.unwrap()).unwrap();
    assert_eq!(data["preview"]["bytes"], 100000);
    assert_eq!(data["preview"]["text"].as_str().unwrap().len(), 65536);
    assert_eq!(data["preview"]["truncated"], true);
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/object?digest={}", unicode.digest))
                .header("host", "localhost:8787")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let data: serde_json::Value =
        serde_json::from_slice(&to_bytes(r.into_body(), 200000).await.unwrap()).unwrap();
    assert_eq!(data["binary"], false); // A partial final UTF-8 codepoint is still a text preview.
    assert_eq!(data["preview"]["truncated"], true);
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/inspect")
                .header("host", "localhost:8787")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED);
    let r = app
        .oneshot(
            Request::builder()
                .uri("/api/inspect")
                .header("host", "untrusted.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
}
