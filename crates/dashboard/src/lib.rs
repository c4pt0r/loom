//! Read-only dashboard transport. No SQL, runtime mutation, or adapter dependency.
use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use loom_contracts::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct Dashboard {
    pub inspector: Arc<dyn InspectionStore>,
    pub objects: Arc<dyn ObjectStore>,
    pub namespace: String,
}
struct ApiError(Error);
impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            Error::Invalid(s) => (StatusCode::BAD_REQUEST, s),
            Error::NotFound(_) => (
                StatusCode::NOT_FOUND,
                "Resource not found in this namespace".into(),
            ),
            other => {
                eprintln!("dashboard backend: {other}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Backend unavailable. Displayed data may be stale; retry shortly.".into(),
                )
            }
        };
        (status, Json(serde_json::json!({"error":message}))).into_response()
    }
}
#[derive(Deserialize)]
struct IdQuery {
    id: String,
}
#[derive(Deserialize)]
struct ObjectQuery {
    digest: String,
}
#[derive(Serialize)]
struct ObjectPreview {
    digest: String,
    preview: TextPreview,
    binary: bool,
}
async fn inspect(
    State(s): State<Dashboard>,
    Query(q): Query<InspectQuery>,
) -> std::result::Result<Json<Inspection>, ApiError> {
    Ok(Json(s.inspector.inspect(q).await?))
}
async fn job(
    State(s): State<Dashboard>,
    Query(q): Query<IdQuery>,
) -> std::result::Result<Json<JobDetail>, ApiError> {
    Ok(Json(s.inspector.inspect_job(&q.id).await?))
}
async fn actor(
    State(s): State<Dashboard>,
    Query(q): Query<IdQuery>,
) -> std::result::Result<Json<ActorView>, ApiError> {
    Ok(Json(s.inspector.inspect_actor(&q.id).await?))
}
async fn object(
    State(s): State<Dashboard>,
    Query(q): Query<ObjectQuery>,
) -> std::result::Result<Json<ObjectPreview>, ApiError> {
    if q.digest.len() != 64 || !q.digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Invalid("Expected a SHA-256 object digest".into()).into());
    }
    let reference = ObjectRef::new(q.digest.clone());
    let size = s.objects.size(&reference).await?;
    let length = size.min(65536);
    let bytes = s.objects.get_range(&reference, 0, length).await?;
    if bytes.len() as u64 != length {
        return Err(Error::Invalid("Object provider returned an incomplete preview".into()).into());
    }
    let binary = std::str::from_utf8(&bytes)
        .err()
        .is_some_and(|e| e.error_len().is_some())
        || bytes.contains(&0);
    let text = if binary {
        bytes
            .iter()
            .take(256)
            .map(|b| format!("{b:02x} "))
            .collect::<String>()
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };
    Ok(Json(ObjectPreview {
        digest: q.digest,
        preview: TextPreview {
            text,
            bytes: size,
            truncated: size > if binary { 256 } else { 65536 },
        },
        binary,
    }))
}

pub fn router(state: Dashboard, bind: SocketAddr) -> Router {
    let allowed = Arc::new(vec![
        "localhost".to_string(),
        "127.0.0.1".into(),
        "[::1]".into(),
        bind.ip().to_string(),
    ]);
    let capacity = Arc::new(tokio::sync::Semaphore::new(16));
    Router::new()
        .route("/",get(||async {Html(include_str!("../web/index.html"))}))
        .route("/app.js",get(||async {([(header::CONTENT_TYPE,"text/javascript; charset=utf-8")],include_str!("../web/app.js"))}))
        .route("/style.css",get(||async {([(header::CONTENT_TYPE,"text/css; charset=utf-8")],include_str!("../web/style.css"))}))
        .route("/api/config",get(|State(s):State<Dashboard>|async move {Json(serde_json::json!({"namespace":s.namespace,"refresh_ms":3000,"read_only":true}))}))
        .route("/api/inspect",get(inspect))
        .route("/api/job",get(job))
        .route("/api/actor",get(actor))
        .route("/api/object",get(object))
        .with_state(state)
        .layer(middleware::from_fn(move |req:Request,next:Next| {
            let allowed=allowed.clone();let capacity=capacity.clone();
            async move {
                let host=req.headers().get(header::HOST).and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<axum::http::uri::Authority>().ok());
                if !host.is_some_and(|a|allowed.iter().any(|h|h==a.host())) {return (StatusCode::FORBIDDEN,"Use the configured dashboard address").into_response();}
                let Ok(_permit)=capacity.try_acquire_owned() else {return (StatusCode::SERVICE_UNAVAILABLE,"Dashboard busy; retry shortly").into_response();};
                let mut response=match tokio::time::timeout(Duration::from_secs(10),next.run(req)).await {
                    Ok(r)=>r,Err(_)=>(StatusCode::SERVICE_UNAVAILABLE,Json(serde_json::json!({"error":"Backend request timed out; displayed data is stale"}))).into_response(),
                };
                response.headers_mut().insert(header::CACHE_CONTROL,"no-store".parse().unwrap());
                response.headers_mut().insert(header::X_CONTENT_TYPE_OPTIONS,"nosniff".parse().unwrap());
                response.headers_mut().insert(header::CONTENT_SECURITY_POLICY,"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'".parse().unwrap());
                response
            }
        }))
}
pub async fn serve(state: Dashboard, bind: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    eprintln!(
        "DASHBOARD http://{bind} namespace={} (read-only)",
        state.namespace
    );
    axum::serve(listener, router(state, bind))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
