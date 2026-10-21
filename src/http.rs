//! HTTP: здоровье, метрики Prometheus и REST журнала — пути как у Java-шлюза.

use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::app::App;
use crate::db::{self, EventFilter};

/// Маршруты: `/actuator/*` открыты, `/api/*` — под токеном (если он задан).
pub fn router(app: Arc<App>) -> Router {
    let api = Router::new()
        .route("/api/health", get(api_health))
        .route("/api/status", get(status))
        .route("/api/ha", get(ha))
        .route("/api/scripts", get(scripts))
        .route("/api/events", get(events))
        .route("/api/events/type/{event_type}", get(events_by_type))
        .route("/api/events/severity/{severity}", get(events_by_severity))
        .route("/api/events/alarms", get(alarms))
        .route("/api/events/alarms/unacknowledged", get(unacknowledged))
        .route("/api/events/{id}/acknowledge", post(acknowledge))
        .route("/api/events/stats", get(stats))
        .route_layer(middleware::from_fn_with_state(app.clone(), require_token));
    Router::new()
        .route("/actuator/health", get(health))
        .route("/actuator/prometheus", get(prometheus))
        .merge(api)
        .with_state(app)
}

/// `/api/*` требует `Authorization: Bearer <GATEWAY_API_TOKEN>`, если токен задан (иначе открыт, как раньше).
async fn require_token(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    if let Some(token) = &app.settings.api_token {
        let header = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
        if !bearer_matches(header, token.expose()) {
            return (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE, "Bearer")], "нужен токен доступа\n")
                .into_response();
        }
    }
    next.run(req).await
}

/// Заголовок `Authorization` — `Bearer <token>`; сравнение за постоянное время, чтобы токен нельзя
/// было подобрать по времени ответа.
fn bearer_matches(header: Option<&str>, token: &str) -> bool {
    let Some(given) = header.and_then(|h| h.strip_prefix("Bearer ")) else { return false };
    let (a, b) = (given.trim().as_bytes(), token.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::bearer_matches;

    #[test]
    fn bearer_token_is_checked_exactly() {
        assert!(bearer_matches(Some("Bearer s3cret"), "s3cret"));
        assert!(!bearer_matches(Some("Bearer s3cre"), "s3cret"));
        assert!(!bearer_matches(Some("Bearer s3cret!"), "s3cret"));
        assert!(!bearer_matches(Some("s3cret"), "s3cret"), "без схемы Bearer");
        assert!(!bearer_matches(Some("Basic s3cret"), "s3cret"));
        assert!(!bearer_matches(Some("Bearer "), "s3cret"));
        assert!(!bearer_matches(None, "s3cret"));
    }
}

/// Живость процесса + состояние БД и связи с контроллерами (для глаз; статус UP, пока
/// процесс жив — как health Java-шлюза без индикаторов).
async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    let db = match &app.db {
        Some(pool) => {
            let ready = app.db_ready.load(std::sync::atomic::Ordering::Relaxed);
            let reason = match (db::ping(pool).await, ready) {
                (true, true) => None,
                (true, false) => Some("схема и теги ещё не синхронизированы, повтор в фоне"),
                (false, true) => Some("нет соединения с БД"),
                (false, false) => Some("БД недоступна с запуска шлюза: схема и теги не синхронизированы"),
            };
            match reason {
                None => json!({"status": "UP"}),
                Some(reason) => json!({"status": "DOWN", "details": {"reason": reason}}),
            }
        }
        None => json!({"status": "DISABLED"}),
    };
    let controllers: serde_json::Map<String, Value> = app
        .controllers
        .iter()
        .map(|c| (c.ctrl.name.clone(), json!(if c.is_connected() { "UP" } else { "DOWN" })))
        .collect();
    let kafka = match &app.kafka {
        Some(k) => json!({"status": if k.delivery().healthy() { "UP" } else { "DOWN" }}),
        None => json!({"status": "DISABLED"}),
    };
    Json(json!({"status": "UP", "components": {"db": db, "kafka": kafka, "controllers": controllers}}))
}

async fn prometheus(State(app): State<Arc<App>>) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], app.metrics.render()).into_response()
}

async fn api_health() -> Json<Value> {
    Json(json!({"status": "UP", "service": "SCADA Gateway"}))
}

async fn status(State(app): State<Arc<App>>) -> Json<Value> {
    let controllers: Vec<Value> = app
        .controllers
        .iter()
        .map(|c| {
            let mut status = json!({"id": c.ctrl.id.get(), "name": c.ctrl.name, "endpoint": c.ctrl.endpoint,
                        "protocol": format!("{:?}", c.ctrl.kind), "connected": c.is_connected(),
                        "tags": c.ctrl.tags.len()});
            // Проверка узлов OPC UA при последнем подключении: неизвестные узлы, типы, предел запроса.
            if let Some(report) = c.node_report() {
                status["opcua"] = report.summary(50);
            }
            status
        })
        .collect();
    Json(json!({
        "server": "SCADA Gateway",
        "status": "RUNNING",
        "time": chrono::Utc::now().to_rfc3339(),
        "uptimeSeconds": app.started.elapsed().as_secs(),
        "controllers": controllers,
    }))
}

/// Роль экземпляра в паре горячего резерва.
async fn ha(State(app): State<Arc<App>>) -> Json<Value> {
    let l = &app.leadership;
    Json(json!({
        "enabled": l.is_ha_enabled(),
        "instance": l.instance_id(),
        "role": if l.is_active() { "ACTIVE" } else { "STANDBY" },
        "since": l.since().to_rfc3339(),
        "group": l.group_id(),
    }))
}

/// Пользовательские скрипты обработки значений: привязки, ошибки, состояние перезагрузки.
async fn scripts(State(app): State<Arc<App>>) -> Json<Value> {
    Json(app.scripts.info())
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

fn no_db() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "БД шлюза не подключена"}))).into_response()
}

/// Ошибка БД: подробности (адрес, SQL) — в журнал, клиенту — только факт.
fn db_error(e: anyhow::Error) -> Response {
    tracing::error!("REST: ошибка БД: {e:#}");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "ошибка БД, подробности в журнале шлюза"})))
        .into_response()
}

async fn query_events(app: &App, filter: EventFilter<'_>, limit: Option<i64>) -> Response {
    let Some(pool) = &app.db else { return no_db() };
    match db::events(pool, filter, limit.unwrap_or(100)).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => db_error(e),
    }
}

async fn events(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::All, q.limit).await
}

async fn events_by_type(State(app): State<Arc<App>>, Path(t): Path<String>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::Type(&t), q.limit).await
}

async fn events_by_severity(
    State(app): State<Arc<App>>,
    Path(s): Path<String>,
    Query(q): Query<LimitQuery>,
) -> Response {
    query_events(&app, EventFilter::Severity(&s), q.limit).await
}

async fn alarms(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::Type("ALARM"), q.limit).await
}

async fn unacknowledged(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::UnacknowledgedCritical, q.limit).await
}

#[derive(Deserialize)]
struct AckQuery {
    #[serde(rename = "userId")]
    user_id: String,
}

/// Длиннее не помещается в `event_log.user_id`.
const MAX_USER_ID: usize = 255;

async fn acknowledge(State(app): State<Arc<App>>, Path(id): Path<i64>, Query(q): Query<AckQuery>) -> Response {
    let user = q.user_id.trim();
    if user.is_empty() || user.chars().count() > MAX_USER_ID {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("userId: от 1 до {MAX_USER_ID} символов")})))
            .into_response();
    }
    let Some(pool) = &app.db else { return no_db() };
    match db::acknowledge_alarm(pool, id, user).await {
        Ok(true) => Json(json!({"status": "ACKNOWLEDGED", "message": format!("Alarm {id} acknowledged by {user}")}))
            .into_response(),
        // Подтверждать нечего: оператор не должен считать аларм подтверждённым, если строки нет.
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("аларм {id} не найден (подтверждаются только события типа ALARM)")})),
        )
            .into_response(),
        Err(e) => db_error(e),
    }
}

async fn stats(State(app): State<Arc<App>>) -> Response {
    let Some(pool) = &app.db else { return no_db() };
    match db::event_stats(pool).await {
        Ok((total, errors, warnings, unacked)) => Json(json!({
            "total_events": total,
            "errors": errors,
            "warnings": warnings,
            "unacknowledged_alarms": unacked,
            "timestamp": chrono::Utc::now().timestamp_millis(),
        }))
        .into_response(),
        Err(e) => db_error(e),
    }
}
