//! Сборка и остановка процесса шлюза: `main.rs` только вызывает эти шаги по порядку.
//!
//! Порядок важен: БД и список контроллеров → роль в паре и Kafka → писатели журнала → скрипты → `App` →
//! выборы, опрос, команды, HTTP → ожидание сигнала → остановка задач.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::app::App;
use crate::command::{self, Dedup, Dispatcher};
use crate::config::{self, Settings};
use crate::events::{self, EventSink};
use crate::kafka::{self, KafkaOut};
use crate::leadership::Leadership;
use crate::metrics::Metrics;
use crate::model::{Controller, ControllerKind};
use crate::opcua_cert;
use crate::supervisor::supervise;
use crate::tag_settings::{self, TagSettingsStore};
use crate::{http, poller};

/// Сколько ждём остановки задач после сигнала.
const STOP_TIMEOUT: Duration = Duration::from_secs(8);
/// Пауза между попытками подготовить БД, недоступную при запуске.
const DB_RETRY: Duration = Duration::from_secs(5);
/// Не чаще — предупреждение «БД всё ещё недоступна».
const DB_RETRY_LOG_EVERY: Duration = Duration::from_secs(300);
/// Окно дублей команд: повторная доставка того же `commandId` не пишет в ПЛК дважды.
const DEDUP_TTL: Duration = Duration::from_secs(60);
/// Сколько последних идентификаторов команд помнить.
const DEDUP_MAX: usize = 1000;
/// Токен REST короче — предупреждение при запуске.
const MIN_TOKEN_CHARS: usize = 16;

/// Журнал в консоль: уровни из `RUST_LOG`, иначе `info` и тихие шумные библиотеки.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,opcua_client=error,opcua_crypto=error,opcua_core=warn,rdkafka=warn,sqlx=warn")
        }))
        .with_target(false)
        .compact()
        .init();
}

/// Контроллеры из YAML (с id тегов из БД, если она есть) и пул БД.
pub struct Station {
    /// Только включённые контроллеры; id тегов — из БД, если она есть.
    pub controllers: Vec<Controller>,
    /// Пул БД; `None` при `DB_ENABLED=false`. Есть и тогда, когда сервер при запуске был недоступен.
    pub db: Option<PgPool>,
    /// БД была недоступна при запуске: подготовить её в фоне ([`spawn_db_sync`]).
    pub pending_db: Option<PendingDb>,
}

/// Подготовка БД, отложенная до её появления: все контроллеры YAML (и выключенные — их строки в БД сохраняются) и имена.
/// Теги и контроллеры — копии тех, что работают в шлюзе, с теми же ячейками номеров: номера после синхронизации сразу
/// видны опросу, командам и истории.
pub struct PendingDb {
    controllers: Vec<Controller>,
    yaml_names: Vec<String>,
}

/// Прочитать `controllers.yaml`, синхронизировать теги с БД (id нужны командам и истории) и оставить включённые
/// контроллеры.
pub async fn load_station(settings: &Settings) -> Result<Station> {
    let servers = config::load_controllers(&settings.controllers_path)?;
    let yaml_names: Vec<String> = servers.iter().map(|s| s.name.clone()).collect();
    let mut controllers: Vec<Controller> = Vec::new();
    for s in &servers {
        match Controller::from_config(s) {
            Some(c) => controllers.push(c),
            None => warn!("Неизвестный протокол контроллера {}: {}", s.name, s.endpoint),
        }
    }
    // БД не на горячем пути: если она недоступна при запуске (сеть, сервер ещё стартует), шлюз запускается без неё и
    // готовит её в фоне. Неверный пароль, сертификат, ошибка миграции повтором не лечатся — отказ запуска, как раньше.
    let (db, pending_db) = match &settings.db {
        Some(db_settings) => {
            let pool = crate::db::pool(db_settings);
            let target = format!("{}:{}/{}", db_settings.host, db_settings.port, db_settings.database);
            match crate::db::prepare(&pool, &controllers, &yaml_names).await {
                Ok(()) => {
                    info!("БД {target} подключена");
                    (Some(pool), None)
                }
                Err(crate::db::PrepareError::Unavailable(e)) => {
                    warn!(
                        "БД {target} недоступна при запуске ({e}) — шлюз запускается без неё: телеметрия и команды \
                         работают, события и история не пишутся, команды по tagId — после синхронизации (по имени — \
                         сразу). Подключение — в фоне, каждые {} с",
                        DB_RETRY.as_secs()
                    );
                    (Some(pool), Some(PendingDb { controllers: controllers.clone(), yaml_names }))
                }
                Err(crate::db::PrepareError::Failed(e)) => return Err(e),
            }
        }
        None => {
            warn!("БД выключена (DB_ENABLED=false): журнал и история не пишутся");
            (None, None)
        }
    };
    let enabled: Vec<Controller> = controllers.into_iter().filter(|c| c.enabled).collect();
    let tag_count: usize = enabled.iter().map(|c| c.tags.len()).sum();
    info!("Загружено {} контроллеров, {tag_count} тегов", enabled.len());
    Ok(Station { controllers: enabled, db, pending_db })
}

/// БД, недоступная при запуске: ждать её в фоне, затем миграции и синхронизация конфигурации; номера тегов — в работающий
/// шлюз (индекс команд по `tagId` — заново), событие `SYSTEM`. Ошибку, которая сама не пройдёт, показываем и повторяем
/// реже — шлюз из-за БД не останавливается.
pub fn spawn_db_sync(app: &Arc<App>, pending: PendingDb, tasks: &mut Tasks) {
    let Some(pool) = app.db.clone() else { return };
    app.db_ready.store(false, std::sync::atomic::Ordering::Relaxed);
    let (app, cancel) = (app.clone(), tasks.cancel.clone());
    tasks.spawn(async move {
        let started = std::time::Instant::now();
        let mut last_log: Option<std::time::Instant> = None;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(DB_RETRY) => {}
            }
            match crate::db::prepare(&pool, &pending.controllers, &pending.yaml_names).await {
                Ok(()) => {
                    app.reindex_ids();
                    app.db_ready.store(true, std::sync::atomic::Ordering::Relaxed);
                    let message = format!(
                        "БД подключена через {} с после запуска: схема и конфигурация синхронизированы",
                        started.elapsed().as_secs()
                    );
                    info!("🗄 {message}");
                    app.events.emit(events::Event::new("SYSTEM", "Gateway", "INFO", message));
                    return;
                }
                Err(e) => {
                    if last_log.is_none_or(|t| t.elapsed() >= DB_RETRY_LOG_EVERY) {
                        warn!("БД всё ещё не подготовлена ({e}) — повтор каждые {} с", DB_RETRY.as_secs());
                        last_log = Some(std::time::Instant::now());
                    }
                }
            }
        }
    });
}

/// Роль экземпляра: одиночный (активен всегда) или резервируемый (роль решают выборы через Kafka).
pub fn build_leadership(settings: &Settings, metrics: &Metrics) -> Result<Arc<Leadership>> {
    let ha = &settings.gateway.ha;
    let gauge = metrics.ha_gauge(&ha.instance_id);
    if !ha.enabled {
        return Ok(Leadership::single(ha.instance_id.clone(), gauge));
    }
    if !settings.kafka.enabled {
        bail!("горячее резервирование (GATEWAY_HA_ENABLED) работает через Kafka: включите KAFKA_ENABLED");
    }
    Ok(Leadership::standby(ha.instance_id.clone(), ha.group_id.clone(), gauge))
}

/// Продюсер Kafka (и топики, если их надо создать); `None`, если Kafka выключена.
pub async fn build_kafka(
    settings: &Settings,
    metrics: &Arc<Metrics>,
    leadership: &Arc<Leadership>,
) -> Result<Option<Arc<KafkaOut>>> {
    if !settings.kafka.enabled {
        warn!("Kafka выключена (KAFKA_ENABLED=false)");
        return Ok(None);
    }
    kafka::ensure_topics(&settings.kafka).await;
    if !settings.kafka.client.0.is_empty() {
        info!("🔐 Kafka: {}", settings.kafka.client.summary());
    }
    Ok(Some(Arc::new(KafkaOut::new(&settings.kafka, metrics.clone(), leadership.clone())?)))
}

/// Задачи процесса: под надзором (упавшая перезапускается) и фоновые (писатели, HTTP — работают до отмены).
pub struct Tasks {
    /// Общий токен остановки: отмена будит все задачи.
    pub cancel: CancellationToken,
    metrics: Arc<Metrics>,
    events: EventSink,
    supervised: Vec<JoinHandle<()>>,
    background: JoinSet<()>,
}

impl Tasks {
    /// Пустой набор задач; `metrics` и `events` нужны надзирателю.
    pub fn new(metrics: Arc<Metrics>, events: EventSink) -> Self {
        Tasks { cancel: CancellationToken::new(), metrics, events, supervised: Vec::new(), background: JoinSet::new() }
    }

    /// Задача под надзором: `make` создаёт новый экземпляр при каждом (пере)запуске.
    pub fn supervise<F, Fut>(&mut self, name: &str, make: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.supervised.push(supervise(name, self.cancel.clone(), self.metrics.clone(), self.events.clone(), make));
    }

    /// Фоновая задача без надзора.
    pub fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) {
        self.background.spawn(task);
    }

    /// Отменить всё и подождать завершения (не дольше `STOP_TIMEOUT`).
    pub async fn stop(mut self) {
        self.cancel.cancel();
        let wait = async {
            for task in self.supervised {
                let _ = task.await;
            }
            while self.background.join_next().await.is_some() {}
        };
        if tokio::time::timeout(STOP_TIMEOUT, wait).await.is_err() {
            warn!("Не все задачи остановились за {} с", STOP_TIMEOUT.as_secs());
        }
    }
}

/// Сертификат OPC UA-клиента (если есть контроллеры OPC UA): выпустить или прочитать при запуске и следить за сроком.
/// Сертификат, заданный настройкой, или нужный защищённому каналу, но негодный — ошибка запуска; без защищённых каналов
/// — предупреждение (канал None работает и без сертификата).
pub async fn spawn_opcua_cert(
    settings: &Settings,
    controllers: &[Controller],
    metrics: &Arc<Metrics>,
    events: &EventSink,
    tasks: &mut Tasks,
) -> Result<()> {
    let opcua: Vec<&Controller> = controllers.iter().filter(|c| c.kind == ControllerKind::OpcUa).collect();
    if opcua.is_empty() {
        return Ok(());
    }
    let cert_settings = settings.gateway.opcua_cert.clone();
    let check = cert_settings.clone();
    match tokio::task::spawn_blocking(move || opcua_cert::ensure(&check)).await? {
        Ok(cert) => info!("🔐 {}", opcua_cert::describe(&cert, chrono::Utc::now())),
        Err(e) if cert_settings.own.is_some() || opcua.iter().any(|c| c.opc_security.is_secure()) => {
            return Err(e.context("сертификат OPC UA-клиента"));
        }
        Err(e) => {
            warn!("Сертификат OPC UA-клиента недоступен ({e:#}); защищённый канал без него не работает");
            return Ok(());
        }
    }
    let (metrics, events, cancel) = (metrics.clone(), events.clone(), tasks.cancel.clone());
    tasks.supervise("сертификат OPC UA", move || {
        opcua_cert::watch(cert_settings.clone(), metrics.clone(), events.clone(), cancel.clone())
    });
    Ok(())
}

/// Писатель истории телеметрии в БД (если БД есть и история включена); возвращает приёмник строк для `App`.
pub fn spawn_telemetry_writer(
    settings: &Settings,
    db: Option<&PgPool>,
    metrics: &Arc<Metrics>,
    tasks: &mut Tasks,
) -> Option<events::TelemetrySink> {
    let pool = db.filter(|_| settings.gateway.persist_telemetry)?;
    let (sink, rx) = events::telemetry_channel(metrics.clone());
    tasks.spawn(events::run_telemetry_writer(
        rx,
        pool.clone(),
        settings.gateway.telemetry_retention,
        metrics.clone(),
        tasks.cancel.clone(),
    ));
    info!("История телеметрии пишется в БД (retention {} ч)", settings.gateway.telemetry_retention.as_secs() / 3600);
    Some(sink)
}

/// Опрос каждого контроллера, журнал состояния и heartbeat — всё под надзором.
pub fn spawn_pollers(app: &Arc<App>, tasks: &mut Tasks) {
    let unchecked: Vec<&str> = app
        .controllers
        .iter()
        .filter(|h| h.ctrl.identity.is_none() && h.ctrl.kind != crate::model::ControllerKind::Modbus)
        .map(|h| h.ctrl.name.as_str())
        .collect();
    if !unchecked.is_empty() {
        warn!(
            "Имя контроллера не проверяется (нет identity в controllers.yaml): {} — перепутанный адрес или замена ПЛК \
             не будут замечены, команды уйдут не туда",
            unchecked.join(", ")
        );
    }
    for handle in &app.controllers {
        let (app, handle, cancel) = (app.clone(), handle.clone(), tasks.cancel.clone());
        let name = format!("опрос {}", handle.ctrl.name);
        tasks.supervise(&name, move || poller::run(app.clone(), handle.clone(), cancel.clone()));
    }
    let (health_app, cancel) = (app.clone(), tasks.cancel.clone());
    tasks.supervise("health-log", move || poller::health_log(health_app.clone(), cancel.clone()));
    let (beat_app, cancel) = (app.clone(), tasks.cancel.clone());
    tasks.supervise("heartbeat", move || poller::heartbeat(beat_app.clone(), cancel.clone()));
}

/// Приём команд из Kafka (если Kafka включена) и исполнители очередей команд по контроллерам.
pub fn spawn_commands(app: &Arc<App>, leadership: &Arc<Leadership>, tasks: &mut Tasks) {
    if app.kafka.is_none() {
        return;
    }
    // Один `Dedup` на все перезапуски задачи: после падения консьюмера повтор команды всё равно будет отброшен.
    let dedup = Arc::new(Dedup::new(DEDUP_TTL, DEDUP_MAX));
    // Очередь и исполнитель на каждый контроллер: консьюмер только раскладывает команды, не дожидаясь записи.
    let queues = Arc::new(Dispatcher::new(app.controllers.iter().map(|c| c.ctrl.name.as_str()), &app.metrics));
    for name in app.controllers.iter().map(|c| c.ctrl.name.clone()) {
        let (app, queues, cancel) = (app.clone(), queues.clone(), tasks.cancel.clone());
        tasks.supervise(&format!("команды {name}"), move || {
            let (app, queues, cancel, name) = (app.clone(), queues.clone(), cancel.clone(), name.clone());
            async move {
                queues
                    .run(&name, &cancel, |job| {
                        let app = app.clone();
                        async move { command::process(&app, job).await }
                    })
                    .await
            }
        });
    }
    let (app, leadership, cancel) = (app.clone(), leadership.clone(), tasks.cancel.clone());
    tasks.supervise("команды", move || {
        let (app, dedup, queues) = (app.clone(), dedup.clone(), queues.clone());
        kafka::consume_commands(
            app.settings.kafka.clone(),
            leadership.clone(),
            cancel.clone(),
            move |cmd, record_ts| {
                command::submit(&app, &dedup, &queues, cmd, record_ts);
                std::future::ready(())
            },
        )
    });
}

/// Настройки тегов из топика `channel.tag-settings` (если Kafka и настройки включены): период опроса и зона истории на лету.
pub fn spawn_tag_settings(app: &Arc<App>, tasks: &mut Tasks) {
    let kafka = &app.settings.kafka;
    if app.kafka.is_none() || !kafka.tag_settings_enabled {
        return;
    }
    let store = Arc::new(TagSettingsStore::new(&app.controllers, app.metrics.tag_settings_tags.clone()));
    let (app, cancel) = (app.clone(), tasks.cancel.clone());
    tasks.supervise("настройки тегов", move || {
        tag_settings::consume(app.settings.kafka.clone(), store.clone(), cancel.clone())
    });
}

/// HTTP: `/actuator/*` и `/api/*`; останавливается вместе с остальными задачами.
pub async fn spawn_http(app: Arc<App>, tasks: &mut Tasks) -> Result<()> {
    let (bind, port) = (app.settings.http_bind.clone(), app.settings.http_port);
    let listener = tokio::net::TcpListener::bind((bind.as_str(), port)).await.context("HTTP-порт занят")?;
    info!(
        "HTTP на {bind}:{port} (/actuator/health, /actuator/prometheus, /api/*: {})",
        if app.settings.api_token.is_some() {
            "по токену"
        } else {
            "без токена — задайте GATEWAY_API_TOKEN"
        }
    );
    if app.settings.api_token.as_ref().is_some_and(|t| t.expose().chars().count() < MIN_TOKEN_CHARS) {
        warn!(
            "GATEWAY_API_TOKEN короче {MIN_TOKEN_CHARS} символов: токен подбирается; возьмите случайный (openssl rand -hex 24)"
        );
    }
    let cancel = tasks.cancel.clone();
    tasks.spawn(async move {
        let server = axum::serve(listener, http::router(app)).with_graceful_shutdown(cancel.cancelled_owned());
        if let Err(e) = server.await {
            warn!("HTTP-сервер остановлен с ошибкой: {e}");
        }
    });
    Ok(())
}

/// Ждать SIGTERM или Ctrl-C.
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await.ok();
}
