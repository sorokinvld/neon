//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

pub use reqwest_middleware::{ClientWithMiddleware, Error};
pub use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

use crate::http::conn_pool::GlobalConnPool;

use crate::{cancellation::CancelMap, config::ProxyConfig};
use futures::StreamExt;
use hyper::{
    server::{
        accept,
        conn::{AddrIncoming, AddrStream},
    },
    Body, Method, Request, Response,
};
use serde_json::{json, Value};

use std::{convert::Infallible, future::ready, sync::Arc};
use tls_listener::TlsListener;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use utils::http::{error::ApiError, json::json_response};

use super::websocket::serve_websocket;

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let conn_pool: Arc<GlobalConnPool> = GlobalConnPool::new(config);

    let tls_config = config.tls_config.as_ref().map(|cfg| cfg.to_server_config());
    let tls_acceptor: tokio_rustls::TlsAcceptor = match tls_config {
        Some(config) => config.into(),
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };

    let mut addr_incoming = AddrIncoming::from_listener(ws_listener)?;
    let _ = addr_incoming.set_nodelay(true);

    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming).filter(|conn| {
        if let Err(err) = conn {
            error!("failed to accept TLS connection for websockets: {err:?}");
            ready(false)
        } else {
            ready(true)
        }
    });

    let make_svc =
        hyper::service::make_service_fn(|stream: &tokio_rustls::server::TlsStream<AddrStream>| {
            let sni_name = stream.get_ref().1.sni_hostname().map(|s| s.to_string());
            let conn_pool = conn_pool.clone();

            async move {
                Ok::<_, Infallible>(hyper::service::service_fn(move |req: Request<Body>| {
                    let sni_name = sni_name.clone();
                    let conn_pool = conn_pool.clone();

                    async move {
                        let cancel_map = Arc::new(CancelMap::default());
                        let session_id = uuid::Uuid::new_v4();

                        request_handler(req, config, conn_pool, cancel_map, session_id, sni_name)
                            .instrument(info_span!(
                                "ws-client",
                                session = format_args!("{session_id}")
                            ))
                            .await
                    }
                }))
            }
        });

    hyper::Server::builder(accept::from_stream(tls_listener))
        .serve(make_svc)
        .with_graceful_shutdown(cancellation_token.cancelled())
        .await?;

    Ok(())
}

async fn request_handler(
    mut request: Request<Body>,
    config: &'static ProxyConfig,
    conn_pool: Arc<GlobalConnPool>,
    cancel_map: Arc<CancelMap>,
    session_id: uuid::Uuid,
    sni_hostname: Option<String>,
) -> Result<Response<Body>, ApiError> {
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        tokio::spawn(async move {
            if let Err(e) = serve_websocket(websocket, config, &cancel_map, session_id, host).await
            {
                error!("error in websocket connection: {e:?}");
            }
        });

        // Return the response so the spawned future can continue.
        Ok(response)
    } else if request.uri().path() == "/sql" && request.method() == Method::POST {
        let result = super::sql_over_http::handle(request, sni_hostname, conn_pool)
            .instrument(info_span!("sql-over-http"))
            .await;
        let status_code = match result {
            Ok(_) => hyper::StatusCode::OK,
            Err(_) => hyper::StatusCode::BAD_REQUEST,
        };
        let json = match result {
            Ok(r) => r,
            Err(e) => {
                let message = format!("{:?}", e);
                let code = match e.downcast_ref::<tokio_postgres::Error>() {
                    Some(e) => match e.code() {
                        Some(e) => serde_json::to_value(e.code()).unwrap(),
                        None => Value::Null,
                    },
                    None => Value::Null,
                };
                json!({ "message": message, "code": code })
            }
        };
        json_response(status_code, json).map(|mut r| {
            r.headers_mut().insert(
                "Access-Control-Allow-Origin",
                hyper::http::HeaderValue::from_static("*"),
            );
            r
        })
    } else {
        json_response(hyper::StatusCode::BAD_REQUEST, "query is not supported")
    }
}
