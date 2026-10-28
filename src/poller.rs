//! Циклы опроса контроллеров — по задаче на контроллер.
//!
//! Период — минимальный pollingRate тегов контроллера (fixed rate). Обрыв: кадры BAD,
//! событие DISCONNECTED; переподключение — сама задача, по своему расписанию. Всё, что
//! пишет в БД или Kafka, ставится в очереди — цикл никогда не ждёт внешние системы.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::app::{App, ControllerHandle};
use crate::config::IdentityConfig;
use crate::events::Event;
use crate::identity::{self, Mismatch};
use crate::modbus::{self, ModbusClient};
use crate::model::{Acquisition, ControllerKind, Protocol, Quality, Reading, Tag, Timestamp};
use crate::opcua::{
    self, ConnectOptions, DataValue, NodeReport, OpcConnection, PreparedNodes, ReadValueId, Subscription,
    SubscriptionEvent,
};
use crate::pac::{self, PacConnection};
use crate::schedule::PollSchedule;
use crate::telemetry::{Processor, all_bad};

/// Опрос контроллера до остановки: цикл выбирается по протоколу. Запускается под надзором — выход раньше остановки считается сбоем.
pub async fn run(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let c = &handle.ctrl;
    info!("📡 {} ({:?}) {} — тегов {}, период {} мс", c.name, c.kind, c.endpoint, c.tags.len(), c.cycle_period_ms());
    app.events.emit(
        Event::new("CONNECTION", "Gateway", "INFO", format!("Connection to {}: CONNECTING", c.name))
            .controller(c.id.get())
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

/// Сколько строк каждого списка проверки узлов показывать в журнале.
const REPORT_LOG_LINES: usize = 10;
/// Сколько строк каждого списка — в событии и `/api/status`.
const REPORT_DETAIL_LINES: usize = 50;

/// Итог проверки узлов после подключения: в журнал и событие `CONFIG` — только если он изменился (после
/// переподключения к той же программе ПЛК не повторяется), в `/api/status` — всегда последний.
fn report_nodes(app: &App, handle: &ControllerHandle, report: &NodeReport) {
    if !handle.set_node_report(report.clone()) {
        return;
    }
    let name = &handle.ctrl.name;
    if let Some(limit) = report.max_nodes_per_read.filter(|&n| n > 0 && (n as usize) < opcua::READ_CHUNK) {
        info!("{name}: сервер принимает до {limit} узлов в запросе — читаю пачками по {}", report.read_chunk);
    }
    for (list, what) in [
        (&report.unresolved, "адрес узла не разобран, теги не читаются"),
        (&report.unknown, "узлов нет на сервере, теги будут BAD"),
        (&report.unsupported, "значения узлов шлюз не передаёт, теги будут BAD"),
        (&report.mismatched, "dataType тегов не совпадает с типом узлов, значения идут в типе узла"),
    ] {
        if list.is_empty() {
            continue;
        }
        let shown = list.iter().take(REPORT_LOG_LINES).cloned().collect::<Vec<_>>().join("; ");
        let more = list.len().saturating_sub(REPORT_LOG_LINES);
        let tail = if more > 0 { format!("; и ещё {more}") } else { String::new() };
        warn!("⚠ {name}: {what} ({}): {shown}{tail}", list.len());
    }
    if report.has_problems() {
        let counts = format!(
            "адрес не разобран {}, нет на сервере {}, не передаётся {}, тип не совпадает {}",
            report.unresolved.len(),
            report.unknown.len(),
            report.unsupported.len(),
            report.mismatched.len()
        );
        app.events.emit(
            Event::new("CONFIG", "Gateway", "WARNING", format!("{name}: узлы OPC UA — {counts}"))
                .controller(handle.ctrl.id.get())
                .details(report.summary(REPORT_DETAIL_LINES)),
        );
    }
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
            .controller(handle.ctrl.id.get())
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

/// Контроллер назвал не то имя: журнал и событие (не чаще раза в минуту), кадры BAD, связь потеряна. Соединение уже закрыто;
/// повтор — по обычному расписанию подключения.
fn on_identity_mismatch(
    app: &App,
    handle: &ControllerHandle,
    processor: &mut Processor,
    tags: &[Arc<Tag>],
    mismatch: &Mismatch,
) {
    app.identity_mismatch(handle, mismatch);
    processor.process(&all_bad(tags));
    app.mark_down(handle, &format!("не тот контроллер: {mismatch}"));
}

/// Что делать после цикла чтения.
enum Cycle {
    Continue,
    /// Сессию пересоздать (закрыта или давно нет удачных чтений).
    Reconnect,
}

/// Один цикл чтения узлов: обработка значений и состояние линка. Читаются только «созревшие» теги (см. `schedule.rs`):
/// теги с настройкой «период» из `channel.tag-settings` — реже цикла.
async fn read_opcua_cycle(
    app: &App,
    handle: &ControllerHandle,
    conn: &OpcConnection,
    processor: &mut Processor,
    schedule: &mut PollSchedule,
    tags: &[Arc<Tag>],
    nodes: &[ReadValueId],
) -> Cycle {
    if !conn.is_alive() {
        processor.process(&all_bad(tags));
        app.mark_down(handle, "сессия OPC UA закрыта");
        return Cycle::Reconnect;
    }
    let now = Instant::now();
    let due: Vec<usize> = (0..tags.len()).filter(|&i| schedule.take_due(&tags[i], now)).collect();
    if due.is_empty() {
        return Cycle::Continue; // в этом цикле читать нечего: у всех тегов период длиннее цикла
    }
    let full = due.len() == tags.len();
    let subset: Option<(Vec<Arc<Tag>>, Vec<ReadValueId>)> = (!full)
        .then(|| (due.iter().map(|&i| tags[i].clone()).collect(), due.iter().map(|&i| nodes[i].clone()).collect()));
    let (read_tags, read_nodes): (&[Arc<Tag>], &[ReadValueId]) = match &subset {
        Some((t, n)) => (t, n),
        None => (tags, nodes),
    };
    let _cycle = CycleTimer::start(app, handle);
    match conn.read(read_nodes).await {
        Ok(values) => {
            let readings: Vec<Reading> = read_tags
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
            } else if full {
                // Все узлы BAD — сервер жив, а данных за ним нет (пропала шина или рантайм ПЛК, у OPC UA-фасада —
                // связь с прошивкой): значения замерли бы на мониторе при «живой» связи и без строки в журнале,
                // поэтому это обрыв. Если читалась только часть тегов, вывода о связи не делаем: редкие теги могут
                // быть все недостоверны при исправной связи.
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

/// Сколько неудачных попыток подряд идут с короткой паузой, прежде чем перейти на редкие (≈ минута при паузе 0,2 с).
const FAST_ATTEMPTS: u32 = 300;
/// Пауза между попытками к контроллеру, который недоступен давно: журнал и сеть не засоряются многодневным простоем.
const SLOW_PAUSE: Duration = Duration::from_secs(2);
/// Наибольшая пауза после отказов сервера подряд.
const REFUSAL_PAUSE_MAX: Duration = Duration::from_secs(30);

/// Пауза перед следующей попыткой подключения к OPC UA после `fails` неудач подряд: `base`
/// (`GATEWAY_SUPERVISE_INTERVAL_MS`, по умолчанию 0,2 с) первые `FAST_ATTEMPTS` попыток, дальше не короче `SLOW_PAUSE`.
/// Короткая пауза нужна для быстрого возврата: контроллер, вернувшийся через несколько секунд, подхватывается сразу;
/// медленный режим — защита от шума при многодневной недоступности (каждая попытка пишет в журнал).
fn reconnect_pause(fails: u32, base: Duration) -> Duration {
    if fails <= FAST_ATTEMPTS { base } else { base.max(SLOW_PAUSE) }
}

/// Подключиться не удалось из-за связи (порт закрыт, обрыв, таймаут), а не потому, что сервер ответил отказом (неверный
/// пароль, защита, лимит сессий). Отказ сервера повторять часто нельзя: каждая попытка может оставить на сервере
/// неполностью открытую сессию, и частые попытки исчерпают лимит сессий контроллера для всех клиентов.
fn is_link_failure(e: &anyhow::Error) -> bool {
    let text = format!("{e:#}");
    [
        "BadCommunicationError",
        "BadConnectionClosed",
        "BadNotConnected",
        "BadServerHalted",
        "Connection refused",
        "Could not connect",
        "таймаут",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

/// Пауза после `refusals`-го отказа сервера подряд: `SLOW_PAUSE`, дальше вдвое дольше с каждым отказом, но не дольше
/// `REFUSAL_PAUSE_MAX`. Клиент OPC UA при отказе закрывает канал, но не сессию, и сервер держит её до своего таймаута
/// (до 60 с): с постоянной паузой 2 с неверный пароль занимал все 20 сессий сервера за 40 с, и сервер отказывал
/// остальным клиентам. С ростом паузы брошенных сессий не больше шести.
fn refusal_pause(refusals: u32, base: Duration) -> Duration {
    let doubled = SLOW_PAUSE.saturating_mul(1 << refusals.saturating_sub(1).min(4));
    base.max(doubled.min(REFUSAL_PAUSE_MAX))
}

/// Пауза после неудачной попытки подключения: короткая при потере связи, растущая при отказе сервера (`refusals` —
/// отказов подряд, считая этот).
fn pause_after_failure(e: &anyhow::Error, fails: u32, refusals: u32, base: Duration) -> Duration {
    if is_link_failure(e) { reconnect_pause(fails, base) } else { refusal_pause(refusals, base) }
}

/// Цикл OPC UA: подключение → чтение до обрыва → закрытие и пересоздание сессии; пока подключения нет — кадры BAD на каждой попытке.
async fn run_opcua(app: Arc<App>, handle: Arc<ControllerHandle>, cancel: CancellationToken) {
    let gw = app.settings.gateway.clone();
    let all_tags = tags_of(&handle, Protocol::OpcUa);
    let mut processor = Processor::new(app.clone(), handle.clone());
    let mut schedule = PollSchedule::new(handle.ctrl.tags.len(), Duration::from_millis(handle.ctrl.cycle_period_ms()));
    let opts = ConnectOptions::for_controller(&handle.ctrl.opc_security, &gw);
    log_opcua_security(&handle.ctrl.name, &opts);
    let mut fails: u32 = 0;
    let mut refusals: u32 = 0;

    while !cancel.is_cancelled() {
        // Подключение и разбор узлов тегов (адреса, типы, предел запроса) — заново на каждой сессии: программу ПЛК
        // могли поменять. Сервер, переставший отвечать при разборе, — та же неудача подключения.
        let connected = match OpcConnection::connect_with(&handle.ctrl.endpoint, gw.opcua_op_timeout, &opts).await {
            Ok(mut c) => match c.prepare(&all_tags).await {
                Ok(prepared) => match &handle.ctrl.identity {
                    // Имя контроллера — до первой команды: чужой контроллер не читаем и не пишем.
                    Some(identity) => match c.verify_identity(identity).await {
                        Ok(()) => Ok((c, prepared)),
                        Err(e) => {
                            c.close().await;
                            Err(e)
                        }
                    },
                    None => Ok((c, prepared)),
                },
                Err(e) => {
                    c.close().await;
                    Err(e)
                }
            },
            Err(e) => Err(e),
        };
        let (conn, PreparedNodes { tags, nodes, unresolved, report }) = match connected {
            Ok((c, prepared)) => (Arc::new(c), prepared),
            Err(e) => {
                fails += 1;
                if let Some(mismatch) = e.downcast_ref::<Mismatch>() {
                    on_identity_mismatch(&app, &handle, &mut processor, &all_tags, mismatch);
                } else {
                    on_connect_failed(&app, &handle, &mut processor, &all_tags, fails, &e);
                }
                if !is_link_failure(&e) {
                    refusals += 1;
                }
                if !pause(&cancel, pause_after_failure(&e, fails, refusals, gw.reconnect_interval)).await {
                    return;
                }
                continue;
            }
        };
        fails = 0;
        refusals = 0;
        handle.touch();
        app.identity_verified(&handle);
        report_nodes(&app, &handle, &report);
        // Теги, которые не читаются (адрес не разобран), — BAD сразу: иначе монитор показывал бы последнее значение.
        if !unresolved.is_empty() {
            processor.process(&all_bad(&unresolved));
        }
        // С этого момента команды записи используют эту сессию.
        handle.set_opc_connection(Some(conn.clone()));
        info!("✅ OPC UA socket up: {} — опрос запущен", handle.ctrl.name);

        // Подписки вместо опроса (`acquisition: subscription`); не удались — этот сеанс идёт опросом, подключение не рвётся.
        let subscription = if handle.ctrl.acquisition == Acquisition::Subscription {
            start_subscription(&app, &handle, &conn, &tags, &nodes).await
        } else {
            None
        };
        if let Some(subscription) = subscription {
            run_subscription(&app, &handle, &conn, &mut processor, &mut schedule, &tags, &nodes, subscription, &cancel)
                .await;
        } else {
            let mut tick = ticker(&handle);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => {}
                    _ = handle.poll_requested(gw.poll_after_write) => {}
                }
                if let Cycle::Reconnect =
                    read_opcua_cycle(&app, &handle, &conn, &mut processor, &mut schedule, &tags, &nodes).await
                {
                    break;
                }
            }
        }
        handle.set_opc_connection(None);
        // Закрываем сессию явно: иначе сервер держал бы её до своего таймаута.
        conn.close().await;
        if cancel.is_cancelled() || !pause(&cancel, gw.reconnect_interval.min(Duration::from_secs(1))).await {
            return;
        }
    }
}

// -------------------------------------------------------------------- подписки OPC UA --

/// Как часто без изменений и без запросов проверять связь (одно чтение `ServerStatus.State`), если цикл опроса короче.
const HEALTH_CHECK_MIN: Duration = Duration::from_secs(5);

/// Период, с которым сервер читает узел тега (`samplingInterval`): настройка «период» из `channel.tag-settings`, иначе `pollingRate`
/// тега, иначе цикл контроллера.
fn sampling_ms(tag: &Tag, cycle_ms: u64) -> f64 {
    let ms = tag
        .tuning
        .poll_period()
        .map(|d| d.as_millis() as u64)
        .or_else(|| (tag.polling_rate_ms > 0).then_some(tag.polling_rate_ms))
        .unwrap_or(cycle_ms);
    ms.max(1) as f64
}

/// Создать подписки на все узлы. `None` — не удалось (причина в журнале и событии `CONFIG`): сеанс идёт опросом.
async fn start_subscription(
    app: &App,
    handle: &ControllerHandle,
    conn: &OpcConnection,
    tags: &[Arc<Tag>],
    nodes: &[ReadValueId],
) -> Option<Subscription> {
    let name = &handle.ctrl.name;
    let cycle_ms = handle.ctrl.cycle_period_ms();
    let sampling: Vec<f64> = tags.iter().map(|t| sampling_ms(t, cycle_ms)).collect();
    match conn.subscribe(nodes, &sampling, cycle_ms as f64).await {
        Ok(sub) => {
            info!(
                "📬 {name}: подписки OPC UA — {} узлов в {} подписках, публикация раз в {} мс{}",
                nodes.len() - sub.refused.len(),
                sub.subscriptions,
                sub.publishing_ms,
                if sub.refused.is_empty() {
                    String::new()
                } else {
                    format!(", сервер отказал для {} узлов (теги BAD)", sub.refused.len())
                }
            );
            Some(sub)
        }
        Err(e) => {
            warn!("⚠ {name}: подписки не созданы ({e:#}) — опрашиваю запросами Read");
            app.events.emit(
                Event::new(
                    "CONFIG",
                    "Gateway",
                    "WARNING",
                    format!("{name}: подписки OPC UA не созданы — опрос запросами Read"),
                )
                .controller(handle.ctrl.id.get())
                .details(json!({"acquisition": "subscription", "fallback": "poll", "error": format!("{e:#}")})),
            );
            None
        }
    }
}

/// Показание тега из последнего значения узла.
fn reading_of(tag: &Arc<Tag>, dv: &DataValue) -> Reading {
    let (value, quality, timestamp) = opcua::reading(dv);
    Reading { tag: tag.clone(), value, quality, timestamp }
}

/// Прочитать все узлы запросом и положить в кэш (начальные значения и опрос после записи); `true` — хотя бы одно значение хорошее.
async fn refresh_cache(
    app: &App,
    handle: &ControllerHandle,
    conn: &OpcConnection,
    processor: &mut Processor,
    tags: &[Arc<Tag>],
    nodes: &[ReadValueId],
    cache: &mut [Option<DataValue>],
) -> bool {
    match conn.read(nodes).await {
        Ok(values) => {
            let readings: Vec<Reading> = tags.iter().zip(values.iter()).map(|(tag, dv)| reading_of(tag, dv)).collect();
            for (slot, dv) in cache.iter_mut().zip(values) {
                *slot = Some(dv);
            }
            let good = readings.iter().any(|r| r.quality == Quality::Good);
            processor.process(&readings);
            if good || readings.is_empty() {
                app.mark_up(handle);
            }
            good
        }
        Err(e) => {
            debug!("{}: чтение всех узлов не удалось: {e:#}", handle.ctrl.name);
            false
        }
    }
}

/// Цикл подписок до обрыва или остановки. Изменения узлов идут в обработку сразу, как пришли; раз в цикл контроллера в неё
/// же подаются последние известные значения «созревших» тегов — как при опросе (полная переотправка, качество), но без чтения
/// из сети. Связь: приходят ли уведомления; если их нет дольше [`HEALTH_CHECK_MIN`] (или трёх циклов) — одно чтение состояния сервера.
#[allow(clippy::too_many_arguments)] // состояние цикла опроса передаётся как есть, как у read_opcua_cycle
async fn run_subscription(
    app: &App,
    handle: &ControllerHandle,
    conn: &OpcConnection,
    processor: &mut Processor,
    schedule: &mut PollSchedule,
    tags: &[Arc<Tag>],
    nodes: &[ReadValueId],
    mut sub: Subscription,
    cancel: &CancellationToken,
) {
    let gw = &app.settings.gateway;
    let cycle = Duration::from_millis(handle.ctrl.cycle_period_ms());
    let health_after = (cycle * 3).max(HEALTH_CHECK_MIN);
    let mut cache: Vec<Option<DataValue>> = vec![None; tags.len()];
    for &(index, status) in &sub.refused {
        cache[index] = Some(DataValue { status: Some(status), ..Default::default() });
    }
    // Значения узлов, которые подписка пришлёт сама, — но сразу после подписки уже есть что показать, и связь проверена.
    refresh_cache(app, handle, conn, processor, tags, nodes, &mut cache).await;
    let mut last_activity = Instant::now();
    let mut tick = ticker(handle);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            event = sub.events.recv() => {
                let Some(event) = event else { return };
                let mut readings = Vec::new();
                let mut closed = None;
                let mut handle_event = |event: SubscriptionEvent| match event {
                    SubscriptionEvent::Data { index, value } => {
                        if let Some(tag) = tags.get(index) {
                            readings.push(reading_of(tag, &value));
                            cache[index] = Some(value);
                        }
                    }
                    SubscriptionEvent::Closed(status) => closed = Some(status),
                };
                handle_event(event);
                // Всё, что накопилось, — одной пачкой в обработку.
                while let Ok(more) = sub.events.try_recv() {
                    handle_event(more);
                }
                if !readings.is_empty() {
                    last_activity = Instant::now();
                    let good = readings.iter().any(|r| r.quality == Quality::Good);
                    processor.process(&readings);
                    if good {
                        app.mark_up(handle);
                    }
                }
                if let Some(status) = closed {
                    warn!("{}: сервер закрыл подписку ({status}) — пересоздаю сессию", handle.ctrl.name);
                    processor.process(&all_bad(tags));
                    app.mark_down(handle, &format!("подписка закрыта сервером ({status})"));
                    return;
                }
            }
            _ = tick.tick() => {
                if !conn.is_alive() {
                    processor.process(&all_bad(tags));
                    app.mark_down(handle, "сессия OPC UA закрыта");
                    return;
                }
                let now = Instant::now();
                let due: Vec<Reading> = tags
                    .iter()
                    .zip(cache.iter())
                    .filter_map(|(tag, dv)| dv.as_ref().map(|dv| (tag, dv)))
                    .filter(|(tag, _)| schedule.take_due(tag, now))
                    .map(|(tag, dv)| reading_of(tag, dv))
                    .collect();
                if !due.is_empty() {
                    processor.process(&due);
                }
                if cache.iter().all(|dv| dv.as_ref().is_some_and(|dv| !dv.status().is_good())) && !cache.is_empty() {
                    // Сервер жив, а данных за ним нет — тот же вывод, что при опросе: обрыв.
                    app.mark_down(handle, "все узлы вернули BAD");
                } else if last_activity.elapsed() >= health_after {
                    match conn.read(&[ReadValueId::from(opcua::NodeId::from(opcua::VariableId::Server_ServerStatus_State))]).await {
                        Ok(_) => {
                            last_activity = Instant::now();
                            app.mark_up(handle);
                        }
                        Err(e) => {
                            processor.process(&all_bad(tags));
                            app.mark_down(handle, &format!("OPC UA reads failing: {e:#}"));
                            if handle.is_stale(gw.stale_after) {
                                warn!(
                                    "🔄 {}: нет связи > {} c — пересоздаю сессию",
                                    handle.ctrl.name,
                                    gw.stale_after.as_secs()
                                );
                                return;
                            }
                        }
                    }
                }
            }
            _ = handle.poll_requested(gw.poll_after_write) => {
                // После записи: свежие значения всех узлов запросом (уведомления придут не раньше интервала публикации).
                refresh_cache(app, handle, conn, processor, tags, nodes, &mut cache).await;
                last_activity = Instant::now();
            }
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
    let mut schedule = PollSchedule::new(handle.ctrl.tags.len(), Duration::from_millis(handle.ctrl.cycle_period_ms()));
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
                // Блоки регистров читаются целиком (так дешевле), а в обработку идут только «созревшие» теги.
                let now = Instant::now();
                let readings: Vec<Reading> = values
                    .into_iter()
                    .filter(|(tag, _)| schedule.take_due(tag, now))
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
    let mut schedule = PollSchedule::new(handle.ctrl.tags.len(), Duration::from_millis(handle.ctrl.cycle_period_ms()));
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
            let connected = PacConnection::connect(&host, port, gw.pac_op_timeout).await.and_then(|c| {
                // Имя PAC — до первой команды: к чужому контроллеру соединение не сохраняется.
                match &handle.ctrl.identity {
                    Some(IdentityConfig { pac_name: Some(expected), .. }) => {
                        identity::check("имя PAC (PAC_name)", expected, c.pac_name().as_deref())?;
                        Ok(c)
                    }
                    _ => Ok(c),
                }
            });
            match connected {
                Ok(c) => {
                    app.identity_verified(&handle);
                    *guard = Some(c)
                }
                Err(e) if e.downcast_ref::<Mismatch>().is_some() => {
                    drop(guard);
                    if let Some(mismatch) = e.downcast_ref::<Mismatch>() {
                        on_identity_mismatch(&app, &handle, &mut processor, &tags, mismatch);
                    }
                    continue;
                }
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
                // Снимок PAC приходит целиком, а в обработку идут только «созревшие» теги.
                let now = Instant::now();
                let readings: Vec<Reading> = tags
                    .iter()
                    .filter(|tag| schedule.take_due(tag, now))
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn reconnect_is_fast_at_first_and_slows_down_for_a_long_outage() {
        let base = Duration::from_millis(200);
        assert_eq!(reconnect_pause(1, base), base);
        assert_eq!(reconnect_pause(FAST_ATTEMPTS, base), base);
        assert_eq!(reconnect_pause(FAST_ATTEMPTS + 1, base), SLOW_PAUSE);
        assert_eq!(reconnect_pause(1_000_000, base), SLOW_PAUSE);
    }

    #[test]
    fn a_server_refusal_is_not_retried_quickly_but_a_lost_link_is() {
        let base = Duration::from_millis(200);
        let refused =
            anyhow!("OPC UA opc.tcp://x:4840: соединение не установлено (Ok(BadCommunicationError (2147811328)))");
        let denied =
            anyhow!("OPC UA opc.tcp://x:4840: соединение не установлено (Ok(BadIdentityTokenRejected (2149253120)))");
        let too_many =
            anyhow!("OPC UA opc.tcp://x:4840: соединение не установлено (Ok(BadTooManySessions (2153119744)))");
        let hung = anyhow!("OPC UA opc.tcp://x:4840: таймаут 5000 мс (handshake завис)");
        assert_eq!(pause_after_failure(&refused, 1, 0, base), base);
        assert_eq!(pause_after_failure(&hung, 1, 3, base), base);
        assert_eq!(pause_after_failure(&denied, 1, 1, base), SLOW_PAUSE);
        assert_eq!(pause_after_failure(&too_many, 1, 1, base), SLOW_PAUSE);
        assert_eq!(pause_after_failure(&denied, 3, 3, base), Duration::from_secs(8));
    }

    #[test]
    fn the_pause_after_server_refusals_doubles_up_to_half_a_minute() {
        let base = Duration::from_millis(200);
        let pauses: Vec<u64> = (1..=7).map(|n| refusal_pause(n, base).as_secs()).collect();
        assert_eq!(pauses, [2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(refusal_pause(u32::MAX, base), REFUSAL_PAUSE_MAX);
        // Сколько попыток успевает за таймаут сессии на сервере (60 с) — столько сессий могут висеть брошенными.
        let mut at = Duration::ZERO;
        let attempts = (1..).take_while(|&n| {
            let started = at;
            at += refusal_pause(n, base);
            started < Duration::from_secs(60)
        });
        assert!(attempts.count() <= 6);
    }

    #[test]
    fn a_longer_configured_pause_is_never_shortened() {
        let ten = Duration::from_secs(10);
        assert_eq!(reconnect_pause(1, ten), ten);
        assert_eq!(reconnect_pause(FAST_ATTEMPTS + 1, ten), ten);
        assert_eq!(refusal_pause(1, ten), ten);
        assert_eq!(refusal_pause(4, ten), Duration::from_secs(16));
    }
}
