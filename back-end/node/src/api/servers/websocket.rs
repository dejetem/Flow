use crate::{api::servers::app_state::AppState, bootstrap::config::Config};
use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::get,
};
use errors::AppError;
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde_json::{Value, json};

pub async fn start(app_state: &AppState, config: &Config) -> Result<(), AppError> {
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(app_state.clone());

    let listener =
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.server.websocket_port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(app_state): State<AppState>,
) -> Response {
    ws.on_upgrade(|socket| websocket_connection(socket, app_state))
}

async fn websocket_connection(socket: WebSocket, app_state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            match msg {
                axum::extract::ws::Message::Text(text) => {
                    if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                        handle_websocket_message(&app_state, &mut sender, payload).await;
                    }
                }
                axum::extract::ws::Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }
    }
}

async fn handle_websocket_message(
    app_state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, axum::extract::ws::Message>,
    payload: Value,
) {
    let action = payload["action"].as_str().unwrap_or("");

    match action {
        "create_space" => {
            let node = app_state.node.read().await;
            let dir = payload["dir"].as_str().unwrap_or("/tmp/space");

            match node.create_space(dir).await {
                Ok(_) => {
                    let response = json!({
                        "action": "space_created",
                        "status": "success"
                    });
                    let _ = sender
                        .send(axum::extract::ws::Message::Text(
                            response.to_string().into(),
                        ))
                        .await;
                }
                Err(e) => {
                    let response = json!({
                        "action": "error",
                        "message": e.to_string(),
                        "status": "error"
                    });
                    let _ = sender
                        .send(axum::extract::ws::Message::Text(
                            response.to_string().into(),
                        ))
                        .await;
                }
            }
        }
        _ => {
            let response = json!({
                "action": "error",
                "message": "Unknown action",
                "status": "error"
            });
            let _ = sender
                .send(axum::extract::ws::Message::Text(
                    response.to_string().into(),
                ))
                .await;
        }
    }
}
