//! PostgreSQL: схема, синхронизация конфигурации, журнал событий, история телеметрии.
//!
//! Всё, что пишет в БД, идёт через очереди и отдельные задачи — опрос ПЛК никогда не ждёт
//! базу. (В Java-шлюзе потоки опроса писали в БД сами, и зависшая база останавливала
//! телеметрию целиком.)

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::{FromRow, Row};
use tracing::{info, warn};

use crate::config::{DbSettings, DbSsl};
use crate::model::{Controller, DbId, Tag, TagValue};

/// Пул БД. Соединения открываются по требованию: пул есть, даже если сервер недоступен, — запись событий и истории
/// просто не проходит (со счётчиком потерь), пока он не вернётся. Схему и конфигурацию готовит [`prepare`].
pub fn pool(settings: &DbSettings) -> PgPool {
    let mut options = PgConnectOptions::new()
        .host(&settings.host)
        .port(settings.port)
        .database(&settings.database)
        .username(&settings.username)
        .password(settings.password.expose())
        .ssl_mode(match settings.ssl {
            DbSsl::Disable => PgSslMode::Disable,
            DbSsl::Allow => PgSslMode::Allow,
            DbSsl::Prefer => PgSslMode::Prefer,
            DbSsl::Require => PgSslMode::Require,
            DbSsl::VerifyCa => PgSslMode::VerifyCa,
            DbSsl::VerifyFull => PgSslMode::VerifyFull,
        });
    if let Some(root) = &settings.ssl_root_cert {
        options = options.ssl_root_cert(root);
    }
    if settings.ssl.is_encrypted() {
        info!("🔐 БД: канал шифруется (sslmode={:?})", settings.ssl);
    } else if settings.ssl != DbSsl::Disable {
        warn!(
            "БД: TLS по возможности (sslmode={:?}), без проверки сервера; чтобы требовать шифрование — sslmode=require или строже",
            settings.ssl
        );
    }
    PgPoolOptions::new().max_connections(8).acquire_timeout(Duration::from_secs(10)).connect_lazy_with(options)
}

/// Подключиться и применить миграции схемы, без синхронизации конфигурации; неверный пароль и сертификат — ошибка сразу.
pub async fn connect(settings: &DbSettings) -> Result<PgPool> {
    let pool = pool(settings);
    pool.acquire().await.context("БД недоступна")?;
    sqlx::migrate!("./migrations").run(&pool).await.context("миграция схемы")?;
    Ok(pool)
}

/// Почему БД не подготовлена.
#[derive(Debug)]
pub enum PrepareError {
    /// Сервер недоступен (нет сети, ещё стартует) — пройдёт само, повторяем.
    Unavailable(sqlx::Error),
    /// Неверный пароль, непроверяемый сертификат, ошибка миграции — повтор не поможет.
    Failed(anyhow::Error),
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::Unavailable(e) => write!(f, "БД недоступна: {e}"),
            PrepareError::Failed(e) => write!(f, "{e:#}"),
        }
    }
}

/// Подготовить БД: дождаться соединения (не дольше таймаута пула, 10 с), применить миграции схемы и синхронизировать
/// конфигурацию (номера — в теги и контроллеры, см. [`sync_config`]).
pub async fn prepare(pool: &PgPool, controllers: &[Controller], all_yaml_names: &[String]) -> Result<(), PrepareError> {
    match pool.acquire().await {
        Ok(conn) => drop(conn),
        Err(e) if is_transient(&e) => return Err(PrepareError::Unavailable(e)),
        Err(e) => return Err(PrepareError::Failed(anyhow::Error::new(e).context("БД недоступна"))),
    }
    sqlx::migrate!("./migrations").run(pool).await.context("миграция схемы").map_err(PrepareError::Failed)?;
    sync_config(pool, controllers, all_yaml_names).await.context("синхронизация с YAML").map_err(PrepareError::Failed)
}

/// Ошибка, которая может пройти сама: сети нет, сервер ещё стартует. Неверный пароль, непроверяемый
/// сертификат и подобное повтором не лечатся — их показываем сразу, а не через минуту.
fn is_transient(e: &sqlx::Error) -> bool {
    match e {
        // Ошибка рукопожатия TLS (непроверяемый сертификат) приходит как `Io` с видом `InvalidData`; обрыв, отказ
        // в соединении и неразрешённое имя — другие виды, и они проходят сами.
        sqlx::Error::Io(io) => io.kind() != std::io::ErrorKind::InvalidData,
        sqlx::Error::PoolTimedOut => true,
        // 57P03 — cannot_connect_now: сервер запускается или восстанавливается.
        sqlx::Error::Database(d) => d.code().is_some_and(|c| c == "57P03"),
        _ => false,
    }
}

/// Синхронизировать controllers/tags с controllers.yaml (YAML — источник истины) и проставить
/// id из БД. Ключи upsert как у Java-шлюза: контроллер — по имени, тег — по nodeId в пределах
/// контроллера; исчезнувшие из YAML удаляются. Одной транзакцией; номера проставляются после её фиксации —
/// при откате (БД пропала посреди синхронизации) в тегах не остаётся номеров несуществующих строк.
pub async fn sync_config(pool: &PgPool, controllers: &[Controller], all_yaml_names: &[String]) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Пара горячего резерва на общей БД стартует одновременно: без блокировки оба экземпляра вставляли
    // бы одни и те же контроллеры (нарушение UNIQUE → падение старта). Второй ждёт первого и видит
    // готовые строки. Блокировка снимается с концом транзакции.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('scada-gateway:config-sync'))").execute(&mut *tx).await?;
    let mut counts = SyncCounts::default();
    let mut ids: Vec<(DbId, i64)> = Vec::new();
    for ctrl in controllers {
        let ctrl_id = upsert_controller(&mut tx, ctrl).await?;
        ids.push((ctrl.id.clone(), ctrl_id));
        sync_tags(&mut tx, ctrl, ctrl_id, &mut counts, &mut ids).await?;
    }
    // Контроллеры, которых больше нет в YAML (выключенные в YAML остаются), — вместе с тегами.
    let gone =
        sqlx::query("DELETE FROM tags WHERE controller_id IN (SELECT id FROM controllers WHERE NOT (name = ANY($1)))")
            .bind(all_yaml_names)
            .execute(&mut *tx)
            .await?;
    sqlx::query("DELETE FROM controllers WHERE NOT (name = ANY($1))").bind(all_yaml_names).execute(&mut *tx).await?;
    tx.commit().await?;
    for (cell, id) in ids {
        cell.set(id);
    }
    info!(
        "Синхронизация с YAML: тегов создано {}, обновлено {}, удалено {}",
        counts.created,
        counts.updated,
        counts.deleted + gone.rows_affected()
    );
    Ok(())
}

/// Счётчики синхронизации тегов с YAML (для строки в журнале).
#[derive(Default)]
struct SyncCounts {
    created: u64,
    updated: u64,
    deleted: u64,
}

/// Транзакция синхронизации.
type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// Контроллер по имени: вставить или обновить; id из БД.
async fn upsert_controller(tx: &mut Tx<'_>, ctrl: &Controller) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO controllers (name, endpoint, enabled, created_at, updated_at)
         VALUES ($1, $2, $3, now(), now())
         ON CONFLICT (name) DO UPDATE SET endpoint = EXCLUDED.endpoint, enabled = EXCLUDED.enabled, updated_at = now()
         RETURNING id",
    )
    .bind(&ctrl.name)
    .bind(&ctrl.endpoint)
    .bind(ctrl.enabled)
    .fetch_one(&mut **tx)
    .await
    .with_context(|| format!("контроллер {}", ctrl.name))?;
    Ok(row.get("id"))
}

/// Теги контроллера: существующие (по nodeId) обновить, новые вставить, пропавшие из YAML удалить; номера — в `ids`.
async fn sync_tags(
    tx: &mut Tx<'_>,
    ctrl: &Controller,
    ctrl_id: i64,
    counts: &mut SyncCounts,
    ids: &mut Vec<(DbId, i64)>,
) -> Result<()> {
    let existing: HashMap<String, i64> = sqlx::query("SELECT id, node_id FROM tags WHERE controller_id = $1")
        .bind(ctrl_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|r| (r.get::<String, _>("node_id"), r.get::<i64, _>("id")))
        .collect();
    let mut seen = Vec::with_capacity(ctrl.tags.len());
    for t in &ctrl.tags {
        let id = match existing.get(&t.node_id) {
            Some(&id) => {
                counts.updated += 1;
                update_tag(tx, id, t).await?;
                id
            }
            None => {
                counts.created += 1;
                insert_tag(tx, ctrl_id, t).await?
            }
        };
        ids.push((t.id.clone(), id));
        seen.push(t.node_id.clone());
    }
    let stale = sqlx::query("DELETE FROM tags WHERE controller_id = $1 AND NOT (node_id = ANY($2))")
        .bind(ctrl_id)
        .bind(&seen)
        .execute(&mut **tx)
        .await?;
    counts.deleted += stale.rows_affected();
    Ok(())
}

/// Обновить все поля тега по номеру (YAML — источник истины).
async fn update_tag(tx: &mut Tx<'_>, id: i64, t: &Tag) -> Result<()> {
    sqlx::query(
        "UPDATE tags SET name=$2, data_type=$3, polling_rate=$4, unit=$5, enabled=$6, min_value=$7,
         max_value=$8, channel_id=$9, device_name=$10, field_name=$11, device_type=$12, protocol=$13,
         modbus_address=$14, modbus_type=$15, modbus_unit_id=$16, writable=$17,
         history_deadband=$18, history_deadband_percent=$19, history_min_interval_ms=$20,
         history_max_interval_ms=$21, updated_at=now()
         WHERE id=$1",
    )
    .bind(id)
    .bind(&t.name)
    .bind(&t.data_type)
    .bind(t.polling_rate_ms as i64)
    .bind(&t.unit)
    .bind(t.enabled)
    .bind(t.min_value)
    .bind(t.max_value)
    .bind(t.channel_id)
    .bind(&t.device_name)
    .bind(&t.field_name)
    .bind(&t.device_type)
    .bind(&t.protocol_raw)
    .bind(t.modbus_address)
    .bind(&t.modbus_type)
    .bind(t.modbus_unit_id as i32)
    .bind(t.writable)
    .bind(t.history.deadband)
    .bind(t.history.deadband_percent)
    .bind(t.history.min_interval_ms.map(|v| v as i64))
    .bind(t.history.max_interval_ms.map(|v| v as i64))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Вставить тег и вернуть его номер; режим `record_device` не используется и всегда выключен.
async fn insert_tag(tx: &mut Tx<'_>, controller_id: i64, t: &Tag) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO tags (controller_id, node_id, name, data_type, polling_rate, unit, enabled, min_value,
         max_value, channel_id, device_name, field_name, device_type, protocol, modbus_address,
         modbus_type, modbus_unit_id, writable, record_device, history_deadband,
         history_deadband_percent, history_min_interval_ms, history_max_interval_ms,
         created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,false,$19,$20,$21,$22,now(),now())
         RETURNING id",
    )
    .bind(controller_id)
    .bind(&t.node_id)
    .bind(&t.name)
    .bind(&t.data_type)
    .bind(t.polling_rate_ms as i64)
    .bind(&t.unit)
    .bind(t.enabled)
    .bind(t.min_value)
    .bind(t.max_value)
    .bind(t.channel_id)
    .bind(&t.device_name)
    .bind(&t.field_name)
    .bind(&t.device_type)
    .bind(&t.protocol_raw)
    .bind(t.modbus_address)
    .bind(&t.modbus_type)
    .bind(t.modbus_unit_id as i32)
    .bind(t.writable)
    .bind(t.history.deadband)
    .bind(t.history.deadband_percent)
    .bind(t.history.min_interval_ms.map(|v| v as i64))
    .bind(t.history.max_interval_ms.map(|v| v as i64))
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.get("id"))
}

// ------------------------------------------------------------------- журнал событий --

/// Строка журнала для вставки.
pub struct EventRow {
    /// Момент события.
    pub time: DateTime<Utc>,
    /// Тип события (`CONNECTION`, `COMMAND`, `ALARM`…).
    pub event_type: String,
    /// Источник события.
    pub source: String,
    /// Важность: `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
    pub severity: String,
    /// Текст; в БД усекается до 500 символов.
    pub message: String,
    /// Номер тега в БД, если событие о теге.
    pub tag_id: Option<i64>,
    /// Номер контроллера в БД, если событие о контроллере.
    pub controller_id: Option<i64>,
    /// Подробности — JSON текстом; `None`, если их нет.
    pub details: Option<String>,
}

/// Пакетная вставка в event_log одним запросом (UNNEST).
pub async fn insert_events(pool: &PgPool, rows: &[EventRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let col = |f: fn(&EventRow) -> String| rows.iter().map(f).collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO event_log (event_time, event_type, source, severity, message, tag_id, controller_id, details, acknowledged)
         SELECT t.*, false FROM UNNEST($1::timestamptz[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[],
                                      $6::bigint[], $7::bigint[], $8::text[]) AS t",
    )
    .bind(rows.iter().map(|r| r.time).collect::<Vec<_>>())
    .bind(col(|r| r.event_type.clone()))
    .bind(col(|r| r.source.clone()))
    .bind(col(|r| r.severity.clone()))
    .bind(col(|r| r.message.chars().take(500).collect()))
    .bind(rows.iter().map(|r| r.tag_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.controller_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.details.clone()).collect::<Vec<_>>())
    .execute(pool)
    .await?;
    Ok(())
}

/// Событие журнала для REST — поля как у EventLogEntity Java-шлюза.
#[derive(Debug, Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct EventLogEntry {
    /// Номер строки журнала.
    pub id: i64,
    /// Момент события.
    pub event_time: DateTime<Utc>,
    /// Тип события.
    pub event_type: String,
    /// Источник.
    pub source: Option<String>,
    /// Важность.
    pub severity: Option<String>,
    /// Текст.
    pub message: Option<String>,
    /// Подробности (JSON текстом).
    pub details: Option<String>,
    /// Номер тега.
    pub tag_id: Option<i64>,
    /// Номер контроллера.
    pub controller_id: Option<i64>,
    /// Кто квитировал аларм.
    pub user_id: Option<String>,
    /// Квитирован ли аларм (для событий типа `ALARM`).
    pub acknowledged: Option<bool>,
}

/// Выборка журнала для REST. Всегда с LIMIT (Java-шлюз читал таблицу целиком).
pub async fn events(pool: &PgPool, filter: EventFilter<'_>, limit: i64) -> Result<Vec<EventLogEntry>> {
    /// Колонки журнала: общее начало всех выборок.
    const COLS: &str = "SELECT id, event_time, event_type, source, severity, message, details, tag_id, controller_id, user_id, acknowledged FROM event_log";
    let limit = limit.clamp(1, 1000);
    let rows = match filter {
        EventFilter::All => {
            sqlx::query_as(const_format::concatcp!(COLS, " ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        EventFilter::Type(t) => {
            sqlx::query_as(const_format::concatcp!(COLS, " WHERE event_type = $2 ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .bind(t)
                .fetch_all(pool)
                .await?
        }
        EventFilter::Severity(sev) => {
            sqlx::query_as(const_format::concatcp!(COLS, " WHERE severity = $2 ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .bind(sev)
                .fetch_all(pool)
                .await?
        }
        EventFilter::UnacknowledgedCritical => {
            sqlx::query_as(const_format::concatcp!(
                COLS,
                " WHERE severity = 'CRITICAL' AND acknowledged = false ORDER BY event_time DESC LIMIT $1"
            ))
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

/// Какие события вернуть из журнала.
pub enum EventFilter<'a> {
    /// Все.
    All,
    /// Только этого типа.
    Type(&'a str),
    /// Только этой важности.
    Severity(&'a str),
    /// Неквитированные события важности `CRITICAL`.
    UnacknowledgedCritical,
}

/// Подтвердить аларм (только события типа ALARM).
pub async fn acknowledge_alarm(pool: &PgPool, id: i64, user_id: &str) -> Result<bool> {
    let r =
        sqlx::query("UPDATE event_log SET acknowledged = true, user_id = $2 WHERE id = $1 AND event_type = 'ALARM'")
            .bind(id)
            .bind(user_id)
            .execute(pool)
            .await?;
    Ok(r.rows_affected() > 0)
}

/// Сводка по последним 10000 событиям — как /api/events/stats Java-шлюза, но на стороне БД.
pub async fn event_stats(pool: &PgPool) -> Result<(i64, i64, i64, i64)> {
    let row = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (WHERE severity IN ('ERROR','CRITICAL')) AS errors,
                count(*) FILTER (WHERE severity = 'WARNING') AS warnings
         FROM (SELECT severity FROM event_log ORDER BY event_time DESC LIMIT 10000) recent",
    )
    .fetch_one(pool)
    .await?;
    let unacked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_log WHERE severity = 'CRITICAL' AND acknowledged = false")
            .fetch_one(pool)
            .await?;
    Ok((row.get("total"), row.get("errors"), row.get("warnings"), unacked))
}

/// Отвечает ли БД (простой запрос с таймаутом 2 с); для `/actuator/health`.
pub async fn ping(pool: &PgPool) -> bool {
    tokio::time::timeout(Duration::from_secs(2), sqlx::query("SELECT 1").execute(pool))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

// -------------------------------------------------------------- история телеметрии --

/// Точка истории.
pub struct TelemetryRow {
    /// Номер тега в БД.
    pub tag_id: i64,
    /// Момент снятия значения.
    pub time: DateTime<Utc>,
    /// `GOOD` или `BAD`.
    pub quality: &'static str,
    /// Значение; `None` — кадр BAD.
    pub value: Option<TagValue>,
}

/// Пакетная вставка точек одним запросом (UNNEST): число → value, строка → value_str.
pub async fn insert_telemetry(pool: &PgPool, rows: &[TelemetryRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO telemetry (tag_id, \"time\", quality, value, value_str)
         SELECT * FROM UNNEST($1::bigint[], $2::timestamptz[], $3::varchar[], $4::float8[], $5::varchar[])",
    )
    .bind(rows.iter().map(|r| r.tag_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.time).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.quality).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.value.as_ref().and_then(TagValue::as_f64)).collect::<Vec<_>>())
    .bind(
        rows.iter()
            .map(|r| match &r.value {
                Some(TagValue::Text(s)) => Some(s.chars().take(255).collect::<String>()),
                _ => None,
            })
            .collect::<Vec<_>>(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Удалить историю старше retention.
pub async fn prune_telemetry(pool: &PgPool, retention: Duration) -> Result<u64> {
    let r = sqlx::query("DELETE FROM telemetry WHERE \"time\" < now() - make_interval(secs => $1)")
        .bind(retention.as_secs_f64())
        .execute(pool)
        .await?;
    Ok(r.rows_affected())
}
