//! Циклы опроса контроллеров — по задаче на контроллер.
//!
//! Период — минимальный pollingRate тегов контроллера (fixed rate). Обрыв: кадры BAD,
//! событие DISCONNECTED; переподключение — сама задача, по своему расписанию. Всё, что
//! пишет в БД или Kafka, ставится в очереди — цикл никогда не ждёт внешние системы.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::app::{App, ControllerHandle};
use crate::events::Event;
use crate::modbus::{self, ModbusClient};
use crate::model::{ControllerKind, Protocol, Quality, Reading, Tag, Timestamp};
use crate::opcua::{self, ConnectOptions, OpcConnection, ReadValueId};
use crate::pac::{self, PacConnection};
use crate::telemetry::{Processor, all_bad};

/// Опрос контроллера до остановки: цикл выбирается по протоколу. Запускается под надзором — выход раньше остановки считается сбоем.
pub async fn run(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let c = &handle.ctrl;
    info!("📡 {} ({:?}) {} — тегов {}, период {} мс", c.name, c.kind, c.endpoint, c.tags.len(), c.cycle_period_ms());
    app.events.emit(
        Event::new("CONNECTION", "Gateway", "INFO", format!("Connection to {}: CONNECTING", c.name))
            .controller(c.id)
            .details(json!({"endpoint": c.endpoint, "status": "CONNECTING"})),
    );
    match c.kind {
        ControllerKind::OpcUa => run_opcua(app, handle, cancel).await,
        ControllerKind::Modbus => run_modbus(app, handle, cancel).await,
        ControllerKind::Pac => run_pac(app, handle, cancel).await,
    }
}

/// Теги контроллера одного протокола.
fn tags_of(handle: &ControllerHandle, protocol: Protocol) -> Vec<Arc<Tag>> {
    handle.ctrl.tags.iter().filter(|t| t.protocol == protocol).cloned().collect()
}

/// Тикер цикла опроса с периодом контроллера; пропущенные тики не догоняются (следующий — через период после опоздавшего).
fn ticker(handle: &ControllerHandle) -> tokio::time::Interval {
    let mut t = interval(Duration::from_millis(handle.ctrl.cycle_period_ms()));
    t.set_missed_tick_behavior(MissedTickBehavior::Delay);
    t
}

/// Замер цикла опроса: на выходе из итерации пишет длительность в гистограмму и считает цикл,
/// не уложившийся в период (шлюз не успевает за контроллером).
struct CycleTimer {
    started: std::time::Instant,
    period: Duration,
    metrics: Arc<crate::metrics::Metrics>,
}

impl CycleTimer {
    /// Начать замер цикла: результат запишется при выходе из области видимости.
    fn start(app: &App, handle: &ControllerHandle) -> Self {
        CycleTimer {
            started: std::time::Instant::now(),
            period: Duration::from_millis(handle.ctrl.cycle_period_ms()),
            metrics: app.metrics.clone(),
        }
    }
}

impl Drop for CycleTimer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        self.metrics.poll_seconds.observe(elapsed.as_secs_f64());
        if elapsed > self.period {
            self.metrics.poll_overruns.inc();
        }
    }
}

/// Пауза с учётом остановки; false — остановка.
async fn pause(cancel: &CancellationToken, d: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = sleep(d) => true,
    }
}

// ------------------------------------------------------------------------ OPC UA --

/// Теги OPC UA контроллера с разобранными адресами узлов (тег с неверным адресом пропускается).
fn opcua_nodes(handle: &ControllerHandle) -> (Vec<Arc<Tag>>, Vec<ReadValueId>) {
    let mut tags = Vec::new();
    let mut nodes = Vec::new();
    for tag in tags_of(handle, Protocol::OpcUa) {
        match opcua::read_value_id(&tag.node_id) {
            Ok(n) => {
                nodes.push(n);
                tags.push(tag);
            }
            Err(e) => warn!("{}: тег {} пропущен: {e}", handle.ctrl.name, tag.name),
        }
    }
    (tags, nodes)
}

/// Записать в журнал режим защиты канала и предупредить о небезопасных сочетаниях.
fn log_opcua_security(name: &str, opts: &ConnectOptions) {
    let sec = &opts.security;
    if !sec.is_secure() && sec.username.is_none() {
        return;
    }
    info!(
        "{name}: OPC UA {:?}/{:?}{}",
        sec.policy,
        sec.mode,
        sec.username.as_ref().map(|u| format!(", пользователь {u}")).unwrap_or_default()
    );
    if sec.is_secure() && opts.trust_server_certs {
        warn!(
            "{name}: GATEWAY_OPCUA_TRUST_SERVER_CERTS — сертификат сервера не проверяется, защищённый канал не защищает от подмены сервера"
        );
    }
    if !sec.is_secure() && sec.username.is_some() {
        warn!("{name}: логин и пароль OPC UA идут по каналу без защиты (security: None) — задайте политику");
    }
}

/// Неудачная попытка подключения: журнал, кадры BAD, при долгой недоступности — линк DOWN.
fn on_connect_failed(
    app: &App,
    handle: &ControllerHandle,
    processor: &mut Processor,
    tags: &[Arc<Tag>],
    fails: u32,
    e: &anyhow::Error,
) {
    // Первая неудача и дальше раз в ~минуту — в журнал: из БД видно «висит давно».
    if fails == 1 || fails.is_multiple_of(6) {
        app.events.emit(
            Event::new(
                "CONNECTION",
                "Gateway",
                "WARNING",
                format!("Connection to {}: CONNECT_FAILED", handle.ctrl.name),
            )
            .controller(handle.ctrl.id)
            .details(json!({"status": "CONNECT_FAILED", "details": format!("попыток подряд: {fails} — {e:#}")})),
        );
    }
    if fails == 1 {
        warn!("❌ OPC UA {}: {e:#}", handle.ctrl.name)
    } else {
        debug!("OPC UA {}: попытка {fails}: {e:#}", handle.ctrl.name)
    }
    // Кадры BAD на каждой попытке, как у Modbus/PAC на каждом цикле: монитор, перезапущенный во время обрыва,
    // читает топик с конца и иначе не узнал бы, что значения недостоверны.
    processor.process(&all_bad(tags));
    if handle.is_stale(app.settings.gateway.stale_after) {
        app.mark_down(handle, &format!("нет подключения: {e:#}"));
    }
}

/// Что делать после цикла чтения.
enum Cycle {
    Continue,
    /// Сессию пересоздать (закрыта или давно нет удачных чтений).
    Reconnect,
}

/// Один цикл чтения узлов: обработка значений и состояние линка.
async fn read_opcua_cycle(
    app: &App,
    handle: &ControllerHandle,
    conn: &OpcConnection,
    processor: &mut Processor,
    tags: &[Arc<Tag>],
    nodes: &[ReadValueId],
) -> Cycle {
    if !conn.is_alive() {
        processor.process(&all_bad(tags));
        app.mark_down(handle, "сессия OPC UA закрыта");
        return Cycle::Reconnect;
    }
    let _cycle = CycleTimer::start(app, handle);
    match conn.read(nodes).await {
        Ok(values) => {
            let readings: Vec<Reading> = tags
                .iter()
                .zip(values.iter())
                .map(|(tag, dv)| {
                    let (value, quality, timestamp) = opcua::reading(dv);
                    Reading { tag: tag.clone(), value, quality, timestamp }
                })
                .collect();
            let good = readings.iter().filter(|r| r.quality == Quality::Good).count();
            processor.process(&readings);
            if good > 0 || readings.is_empty() {
                // Запрос прошёл и хотя бы один узел дал значение → связь есть (BAD у отдельных узлов связь не роняет).
                app.mark_up(handle);
            } else {
                // Все узлы BAD — сервер жив, а данных за ним нет (пропала шина или рантайм ПЛК, у OPC UA-фасада —
                // связь с прошивкой): значения замерли бы на мониторе при «живой» связи и без строки в журнале,
                // поэтому это обрыв.
                let status = values.first().and_then(|dv| dv.status).map(|s| s.to_string()).unwrap_or_default();
                app.mark_down(handle, &format!("все {} узлов вернули BAD ({status})", readings.len()));
            }
            Cycle::Continue
        }
        Err(e) => {
            processor.process(&all_bad(tags));
            app.mark_down(handle, &format!("OPC UA reads failing: {e:#}"));
            if handle.is_stale(app.settings.gateway.stale_after) {
                warn!(
                    "🔄 {}: нет удачных чтений > {} c — пересоздаю сессию",
                    handle.ctrl.name,
                    app.settings.gateway.stale_after.as_secs()
                );
                return Cycle::Reconnect;
            }
            Cycle::Continue
        }
    }
}

/// Цикл OPC UA: подключение → чтение до обрыва → закрытие и пересоздание сессии; пока подключения нет — кадры BAD на каждой попытке.
async fn run_opcua(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let gw = app.settings.gateway.clone();
    let (tags, nodes) = opcua_nodes(&handle);
    let mut processor = Processor::new(app.clone(), handle.clone());
    let opts = ConnectOptions::for_controller(&handle.ctrl.opc_security, &gw);
    log_opcua_security(&handle.ctrl.name, &opts);
    let mut fails: u32 = 0;

    while !cancel.is_cancelled() {
        let conn = match OpcConnection::connect_with(&handle.ctrl.endpoint, gw.opcua_op_timeout, &opts).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                fails += 1;
                on_connect_failed(&app, &handle, &mut processor, &tags, fails, &e);
                if !pause(&cancel, gw.reconnect_interval).await {
                    return;
                }
                continue;
            }
        };
        fails = 0;
        handle.touch();
        // С этого момента команды записи используют эту сессию.
        handle.set_opc_connection(Some(conn.clone()));
        info!("✅ OPC UA socket up: {} — опрос запущен", handle.ctrl.name);

        let mut tick = ticker(&handle);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
                _ = handle.poll_requested(gw.poll_after_write) => {}
            }
            if let Cycle::Reconnect = read_opcua_cycle(&app, &handle, &conn, &mut processor, &tags, &nodes).await {
                break;
            }
        }
        handle.set_opc_connection(None);
        // Закрываем сессию явно: иначе сервер держал бы её до своего таймаута.
        conn.close().await;
        if cancel.is_cancelled() || !pause(&cancel, Duration::from_secs(1)).await {
            return;
        }
    }
}

// ------------------------------------------------------------------------ Modbus --

/// Цикл Modbus: чтение плана каждый тик; сбой — кадры BAD, а соединение клиент сбрасывает сам.
async fn run_modbus(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let gw = &app.settings.gateway;
    let tags = tags_of(&handle, Protocol::Modbus);
    // План чтения; при дырах в карте регистров ПЛК клиент его дробит (см. ModbusClient::read).
    let mut blocks = modbus::plan_blocks(&tags);
    let (host, port) = modbus::endpoint(&handle.ctrl.endpoint);
    // unitId общий для контроллера (в конфиге один на всех тегах).
    let unit = tags.first().map(|t| t.modbus_unit_id).unwrap_or(1);
    let mut client = ModbusClient::new(host, port, unit, gw.modbus_op_timeout);
    let mut processor = Processor::new(app.clone(), handle.clone());
    let mut tick = ticker(&handle);
    info!("{}: Modbus — {} тегов в {} блоках FC03", handle.ctrl.name, tags.len(), blocks.len());

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        let _cycle = CycleTimer::start(&app, &handle);
        match client.read(&mut blocks).await {
            Ok(values) => {
                let ts = Timestamp::now(); // у Modbus нет времени источника — момент чтения
                let readings: Vec<Reading> = values
                    .into_iter()
                    .map(|(tag, value)| {
                        let quality = if value.is_some() { Quality::Good } else { Quality::Bad };
                        Reading { tag, value, quality, timestamp: ts }
                    })
                    .collect();
                processor.process(&readings);
                app.mark_up(&handle);
            }
            Err(e) => {
                processor.process(&all_bad(&tags));
                app.mark_down(&handle, &format!("Modbus reads failing: {e:#}"));
            }
        }
    }
}

// --------------------------------------------------------------------------- PAC --

/// Цикл PAC: соединение хранится в `handle.pac` и делится с командами; сбой — соединение сбрасывается и пересоздаётся на следующем тике.
async fn run_pac(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let gw = &app.settings.gateway;
    let tags = tags_of(&handle, Protocol::Pac);
    let (host, port) = pac::endpoint(&handle.ctrl.endpoint);
    let mut processor = Processor::new(app.clone(), handle.clone());
    let mut tick = ticker(&handle);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => { *handle.pac.lock().await = None; return; }
            _ = tick.tick() => {}
            // Запись прошла — прочитать сразу, а не через полпериода: оператор видит результат нажатия.
            _ = handle.poll_requested(gw.poll_after_write) => {}
        }
        let _cycle = CycleTimer::start(&app, &handle);
        // Замок соединения PAC: команды записи идут по тому же соединению и ждут, пока опрос его отпустит.
        let mut guard = handle.pac.lock().await;
        if guard.is_none() {
            match PacConnection::connect(&host, port, gw.pac_op_timeout).await {
                Ok(c) => *guard = Some(c),
                Err(e) => {
                    drop(guard);
                    processor.process(&all_bad(&tags));
                    app.mark_down(&handle, &format!("PAC: {e:#}"));
                    continue;
                }
            }
        }
        let conn = guard.as_mut().expect("соединение только что проверено");
        match conn.poll_states().await {
            Ok(()) => {
                let ts = Timestamp::now(); // у PAC нет времени источника — момент чтения
                let readings: Vec<Reading> = tags
                    .iter()
                    .map(|tag| {
                        let value = match (tag.device_name.as_deref(), tag.field_name.as_deref()) {
                            (Some(d), Some(f)) => conn.read_value(d, f, &tag.data_type),
                            _ => None,
                        };
                        let quality = if value.is_some() { Quality::Good } else { Quality::Bad };
                        Reading { tag: tag.clone(), value, quality, timestamp: ts }
                    })
                    .collect();
                // Значения сняты: соединение свободно для команд, пока идёт обработка.
                drop(guard);
                processor.process(&readings);
                app.mark_up(&handle);
            }
            Err(e) => {
                *guard = None; // пересоздадим на следующем цикле
                drop(guard);
                processor.process(&all_bad(&tags));
                app.mark_down(&handle, &format!("PAC reads failing: {e:#}"));
            }
        }
    }
}

/// Журнальная сводка здоровья связи раз в health_log_interval.
pub async fn health_log(app: Arc<App>, cancel: CancellationToken) {
    let mut t = interval(app.settings.gateway.health_log_interval);
    t.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = t.tick() => {}
        }
        let parts: Vec<String> = app
            .controllers
            .iter()
            .map(|c| format!("{} {}", if c.is_connected() { "🟢" } else { "🔴" }, c.ctrl.name))
            .collect();
        let up = app.controllers.iter().filter(|c| c.is_connected()).count();
        info!("📋 Здоровье связи: {up}/{} на связи [{}]", app.controllers.len(), parts.join(", "));
    }
}

/// HEARTBEAT в журнал и Kafka — признак «шлюз жив» для монитора.
pub async fn heartbeat(app: Arc<App>, cancel: CancellationToken) {
    let mut t = interval(app.settings.gateway.heartbeat_interval);
    t.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = t.tick() => {}
        }
        let uptime = app.started.elapsed().as_secs();
        app.events.emit(
            Event::new("HEARTBEAT", "Gateway", "INFO", format!("Gateway alive, uptime {uptime}s")).details(
                json!({"uptimeSeconds": uptime, "controllers": app.controllers.len(), "tags": app.tag_count()}),
            ),
        );
    }
}
