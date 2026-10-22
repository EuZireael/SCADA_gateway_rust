//! События шлюза: журнал event_log в БД + публикация в `scada-events`.
//!
//! Источники (опрос, команды, heartbeat) кладут событие в очередь и идут дальше; писатель
//! в отдельной задаче отправляет события в Kafka и передаёт строки второй задаче, которая пачкой
//! вставляет их в БД. Задач две намеренно: недоступная БД держит вставку до таймаута пула, и в одной
//! задаче события в Kafka (смена роли пары, обрыв связи) запаздывали бы или терялись именно тогда,
//! когда они нужнее всего. Переполнение очередей — строка отбрасывается со счётчиком, но опрос и
//! Kafka не ждут никогда.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Не чаще одного предупреждения в `every` (при недоступной БД сбоев тысячи).
pub struct Throttle {
    every: Duration,
    last: std::sync::Mutex<Option<std::time::Instant>>,
}

impl Throttle {
    /// Не чаще одного раза в `every`.
    pub fn new(every: Duration) -> Self {
        Throttle { every, last: std::sync::Mutex::new(None) }
    }

    /// true — пора писать в журнал.
    pub fn ready(&self) -> bool {
        let mut last = self.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = std::time::Instant::now();
        if last.is_none_or(|t| now.duration_since(t) >= self.every) {
            *last = Some(now);
            true
        } else {
            false
        }
    }
}

use crate::db::{self, EventRow, TelemetryRow};
use crate::kafka::KafkaOut;
use crate::leadership::Leadership;
use crate::messages::EventMessage;
use crate::metrics::Metrics;
use crate::model::Timestamp;

/// Событие журнала шлюза: уходит в `event_log`, в Kafka и в REST.
#[derive(Debug, Clone)]
pub struct Event {
    /// Тип: `CONNECTION`, `QUALITY_CHANGE`, `COMMAND`, `ALARM`, `ALARM_CLEARED`, `HEARTBEAT`, `SCRIPT`, `SYSTEM`, `HA`,
    /// `CONFIG`.
    pub event_type: String,
    /// Источник.
    pub source: String,
    /// Важность: `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
    pub severity: String,
    /// Текст для оператора.
    pub message: String,
    /// Номер тега в БД, если событие о теге.
    pub tag_id: Option<i64>,
    /// Номер контроллера в БД, если событие о контроллере.
    pub controller_id: Option<i64>,
    /// Подробности — JSON-объект.
    pub details: Value,
    /// Момент события.
    pub time: Timestamp,
}

impl Event {
    /// Событие с пустыми подробностями и текущим временем.
    pub fn new(event_type: &str, source: &str, severity: &str, message: impl Into<String>) -> Self {
        Event {
            event_type: event_type.into(),
            source: source.into(),
            severity: severity.into(),
            message: message.into(),
            tag_id: None,
            controller_id: None,
            details: json!({}),
            time: Timestamp::now(),
        }
    }

    /// Подробности события (JSON-объект).
    pub fn details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }

    /// Привязать к контроллеру (номер ≤ 0 — не привязывать).
    pub fn controller(mut self, id: i64) -> Self {
        self.controller_id = (id > 0).then_some(id);
        self
    }

    /// Привязать к тегу (номер ≤ 0 — не привязывать).
    pub fn tag(mut self, id: i64) -> Self {
        self.tag_id = (id > 0).then_some(id);
        self
    }
}

/// Отправитель событий (дёшево клонируется).
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
}

impl EventSink {
    /// Поставить событие в очередь, не блокируясь: при переполнении оно отбрасывается и считается в `scada_events_dropped_total`.
    pub fn emit(&self, event: Event) {
        if self.tx.try_send(event).is_err() {
            self.metrics.events_dropped.inc();
        }
    }
}

/// Очередь событий на 10 000 штук: отправитель для всего шлюза и приёмник для писателя.
pub fn channel(metrics: Arc<Metrics>) -> (EventSink, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(10_000);
    (EventSink { tx, metrics }, rx)
}

/// Писатель событий: Kafka — сразу, БД — пачками до 500 строк.
pub async fn run_writer(
    mut rx: mpsc::Receiver<Event>,
    db: Option<PgPool>,
    kafka: Option<Arc<KafkaOut>>,
    leadership: Arc<Leadership>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    // БД — отдельной задачей и через ограниченную очередь пачек: см. описание модуля.
    let (db_tx, db_task) = match db {
        Some(pool) => {
            let (tx, db_rx) = mpsc::channel::<Vec<EventRow>>(256);
            (Some(tx), Some(tokio::spawn(run_event_db_writer(db_rx, pool, metrics.clone()))))
        }
        None => (None, None),
    };
    let mut batch: Vec<Event> = Vec::with_capacity(500);
    loop {
        // Ждём пачку событий или остановку; при остановке забираем всё, что успело встать в очередь, и выходим после обработки.
        let closed = tokio::select! {
            n = rx.recv_many(&mut batch, 500) => n == 0,
            _ = cancel.cancelled() => { rx.close(); while let Ok(e) = rx.try_recv() { batch.push(e); } true }
        };
        // Пара горячего резерва пишет в общую БД: помечаем, чей экземпляр записал событие.
        if leadership.is_ha_enabled() {
            let role = if leadership.is_active() { "ACTIVE" } else { "STANDBY" };
            for e in &mut batch {
                if let Value::Object(map) = &mut e.details {
                    map.insert("instance".into(), json!(leadership.instance_id()));
                    map.insert("role".into(), json!(role));
                }
            }
        }
        if let Some(kafka) = &kafka {
            for e in &batch {
                kafka.send_event(&EventMessage {
                    message_id: uuid::Uuid::new_v4().to_string(),
                    kind: "EVENT",
                    event_type: e.event_type.clone(),
                    source: e.source.clone(),
                    severity: e.severity.clone(),
                    message: e.message.clone(),
                    timestamp: e.time,
                    details: e.details.clone(),
                });
            }
        }
        if let Some(tx) = &db_tx {
            let rows: Vec<EventRow> = batch
                .iter()
                .map(|e| EventRow {
                    time: e.time.0,
                    event_type: e.event_type.clone(),
                    source: e.source.clone(),
                    severity: e.severity.clone(),
                    message: e.message.clone(),
                    tag_id: e.tag_id,
                    controller_id: e.controller_id,
                    details: (!e.details.as_object().is_some_and(|m| m.is_empty())).then(|| e.details.to_string()),
                })
                .collect();
            let lost = rows.len() as u64;
            if tx.try_send(rows).is_err() {
                metrics.db_rows_lost.with_label_values(&["events"]).inc_by(lost);
            }
        }
        batch.clear();
        if closed {
            break;
        }
    }
    drop(db_tx);
    if let Some(task) = db_task {
        let _ = task.await; // допишет очередь и выйдет, когда писатель закроет канал
    }
}

/// Вставка событий в БД. Недоступная БД — потерянные строки со счётчиком, а не остановка шлюза.
async fn run_event_db_writer(mut rx: mpsc::Receiver<Vec<EventRow>>, pool: PgPool, metrics: Arc<Metrics>) {
    let throttle = Throttle::new(Duration::from_secs(30));
    while let Some(rows) = rx.recv().await {
        if let Err(err) = db::insert_events(&pool, &rows).await {
            metrics.db_write_errors.with_label_values(&["events"]).inc();
            metrics.db_rows_lost.with_label_values(&["events"]).inc_by(rows.len() as u64);
            if throttle.ready() {
                warn!("Журнал: {} событий не записано в БД: {err:#}", rows.len());
            }
        }
    }
}

/// Очередь записи истории телеметрии (включается GATEWAY_PERSIST_TELEMETRY).
#[derive(Clone)]
pub struct TelemetrySink {
    tx: mpsc::Sender<Vec<TelemetryRow>>,
    metrics: Arc<Metrics>,
}

impl TelemetrySink {
    /// Пачка точек цикла опроса. Очередь полна (БД не успевает) — точки отбрасываются.
    pub fn push(&self, rows: Vec<TelemetryRow>) {
        let n = rows.len() as u64;
        if self.tx.try_send(rows).is_err() {
            self.metrics.telemetry_rows_dropped.inc_by(n);
        }
    }
}

/// Очередь строк истории: отправитель для обработчиков значений и приёмник для писателя.
pub fn telemetry_channel(metrics: Arc<Metrics>) -> (TelemetrySink, mpsc::Receiver<Vec<TelemetryRow>>) {
    let (tx, rx) = mpsc::channel(64);
    (TelemetrySink { tx, metrics }, rx)
}

/// Писатель истории: вставка пачками + удаление старше retention раз в 10 минут.
pub async fn run_telemetry_writer(
    mut rx: mpsc::Receiver<Vec<TelemetryRow>>,
    pool: PgPool,
    retention: Duration,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let throttle = Throttle::new(Duration::from_secs(30));
    let mut prune = tokio::time::interval(Duration::from_secs(600));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = prune.tick() => match db::prune_telemetry(&pool, retention).await {
                Ok(n) if n > 0 => tracing::info!("История: удалено {n} точек старше {} ч", retention.as_secs() / 3600),
                Ok(_) => {}
                Err(e) => warn!("История: очистка не удалась: {e:#}"),
            },
            rows = rx.recv() => match rows {
                Some(rows) => if let Err(e) = db::insert_telemetry(&pool, &rows).await {
                    metrics.db_write_errors.with_label_values(&["history"]).inc();
                    metrics.db_rows_lost.with_label_values(&["history"]).inc_by(rows.len() as u64);
                    if throttle.ready() {
                        warn!("История: {} точек не записано: {e:#}", rows.len());
                    }
                },
                None => return,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_lets_the_first_through_then_waits() {
        let t = Throttle::new(Duration::from_millis(80));
        assert!(t.ready());
        assert!(!t.ready());
        std::thread::sleep(Duration::from_millis(100));
        assert!(t.ready());
    }
}
