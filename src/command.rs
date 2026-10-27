//! Команды записи из `scada-commands`: очередь своего контроллера, маршрутизация по протоколу, права, приведение
//! типа, запись в ПЛК и результат в `scada-command-results`. Статусы — как у Java-шлюза. Команда с `type: "PAC_ALARM"` —
//! действие над аварией ПЛК ([`crate::pac_alarms`]) в той же очереди контроллера.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prometheus::{Histogram, IntGauge};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::app::{App, ControllerHandle, TagRef};
use crate::events::Event;
use crate::messages::{CommandMessage, CommandResultMessage, command_age_ms};
use crate::metrics::Metrics;
use crate::model::{self, Protocol, Quality, TagValue, Timestamp};
use crate::opcua::{self, NodeId};
use crate::pac::lua;
use crate::pac_alarms;

/// Исход команды.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// Статус: `APPLIED`, `REJECTED_*` или `FAILED_*`.
    pub status: &'static str,
    /// Пояснение для оператора.
    pub message: String,
    /// Записанное значение (в `appliedValue` результата); есть только при успехе.
    pub applied: Option<TagValue>,
}

impl Outcome {
    /// Успех: записанное значение попадает в результат.
    fn applied(value: TagValue) -> Self {
        Outcome {
            status: "APPLIED", message: format!("Записано значение {}", value), applied: Some(value)
        }
    }

    /// Применено; оператор видит своё значение, а в ПЛК ушло пересчитанное скриптом канала.
    fn applied_converted(plc: TagValue, operator: Option<TagValue>) -> Self {
        match operator {
            Some(op) if op != plc => Outcome {
                status: "APPLIED",
                message: format!("Записано значение {op} (в ПЛК: {plc})"),
                applied: Some(op),
            },
            _ => Outcome::applied(plc),
        }
    }

    /// Отказ или сбой: статус и пояснение, записанного значения нет.
    pub(crate) fn fail(status: &'static str, message: impl Into<String>) -> Self {
        Outcome { status, message: message.into(), applied: None }
    }

    /// Команда применена (`APPLIED`).
    pub fn success(&self) -> bool {
        self.status == "APPLIED"
    }
}

/// Значение команды (JSON) → значение по типу тега. Строки с числом принимаются.
pub fn coerce(data_type: &str, value: &Value) -> Result<TagValue, String> {
    // Строка "NaN" или "inf" разбирается как число, а в ПЛК нечисловое значение уйти не должно.
    let number = || {
        let n = match value {
            Value::Number(n) => n.as_f64().ok_or_else(|| format!("{n} не число"))?,
            Value::Bool(b) => f64::from(u8::from(*b)),
            Value::String(s) => s.trim().parse::<f64>().map_err(|_| format!("'{s}' не число"))?,
            other => return Err(format!("{other} не число")),
        };
        if n.is_finite() { Ok(n) } else { Err(format!("{n} не конечное число")) }
    };
    if model::is_bool(data_type) {
        return match value {
            Value::Bool(b) => Ok(TagValue::Bool(*b)),
            Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(TagValue::Bool(true)),
            Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(TagValue::Bool(false)),
            _ => number().map(|n| TagValue::Bool(n != 0.0)),
        };
    }
    if model::is_int(data_type) {
        return number().map(|n| TagValue::Int(n.trunc() as i64));
    }
    if model::is_float(data_type) {
        return number().map(TagValue::F64);
    }
    if model::is_string(data_type) {
        return Ok(TagValue::Text(match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }));
    }
    match value {
        Value::Bool(b) => Ok(TagValue::Bool(*b)),
        Value::String(s) => Ok(TagValue::Text(s.clone())),
        Value::Number(n) => {
            Ok(n.as_i64().map(TagValue::Int).unwrap_or_else(|| TagValue::F64(n.as_f64().unwrap_or(0.0))))
        }
        other => Err(format!("{other} не значение")),
    }
}

/// Тег команды и его контроллер: по `tagId`, иначе по `tagName`.
fn resolve(app: &App, cmd: &CommandMessage) -> Result<TagRef, String> {
    match (cmd.tag_id, cmd.tag_name.as_deref().filter(|n| !n.trim().is_empty())) {
        (Some(id), _) => app.tag_by_id(id).ok_or_else(|| format!("Тег не найден: {id}")),
        (None, Some(name)) => app.tag_by_name(name).ok_or_else(|| format!("Тег не найден по имени: {name}")),
        (None, None) => Err("Команда без tagId и tagName".to_string()),
    }
}

/// Вид команды по полю `type`.
enum Kind {
    /// Нет `type` или `TAG_WRITE`: запись тега (так шлёт и прежний монитор).
    TagWrite,
    /// `PAC_ALARM`: действие над аварией ПЛК.
    PacAlarm,
    /// Иное: `REJECTED_INVALID`, пояснение — что пришло.
    Unknown(String),
}

fn kind(cmd: &CommandMessage) -> Kind {
    match &cmd.kind {
        Value::Null => Kind::TagWrite,
        Value::String(s) if s == "TAG_WRITE" => Kind::TagWrite,
        Value::String(s) if s == "PAC_ALARM" => Kind::PacAlarm,
        other => Kind::Unknown(format!("Неизвестный вид команды type = {other}")),
    }
}

/// Контроллер команды `PAC_ALARM`; `Err` — исход, команда до ПЛК не доходит.
fn alarm_target(app: &App, cmd: &CommandMessage) -> Result<(Arc<ControllerHandle>, pac_alarms::AlarmCommand), Outcome> {
    let alarm = pac_alarms::parse_command(cmd).map_err(|m| Outcome::fail("REJECTED_INVALID", m))?;
    Ok((pac_alarms::controller(app, &alarm.controller_id)?, alarm))
}

/// Выполнить команду.
pub async fn execute(app: &App, cmd: &CommandMessage) -> Outcome {
    match kind(cmd) {
        Kind::TagWrite => {}
        Kind::PacAlarm => {
            return match alarm_target(app, cmd) {
                Ok((controller, alarm)) => pac_alarms::execute(&controller, &alarm).await,
                Err(outcome) => outcome,
            };
        }
        Kind::Unknown(msg) => return Outcome::fail("REJECTED_INVALID", msg),
    }
    let TagRef { tag, controller } = match resolve(app, cmd) {
        Ok(r) => r,
        Err(msg) => return Outcome::fail("REJECTED_UNKNOWN_TAG", msg),
    };
    // Modbus — только чтение по протоколу: у holding-регистра нет признака «только чтение»,
    // и ошибка в конфиге (writable: true) молча перезаписала бы регистр прибора.
    if tag.protocol == Protocol::Modbus {
        return Outcome::fail(
            "REJECTED_NOT_WRITABLE",
            format!("Modbus-контроллер только на чтение, запись запрещена: {}", tag.name),
        );
    }
    if !tag.writable {
        return Outcome::fail(
            "REJECTED_NOT_WRITABLE",
            format!("Тег только для чтения (датчик), запись запрещена: {}", tag.name),
        );
    }
    let data_type = cmd.data_type.as_deref().unwrap_or(&tag.data_type);
    if let Err(outcome) = check_limits(&tag, data_type, &cmd.value) {
        return outcome;
    }
    // Скрипт канала с write(): значение оператора (инженерные единицы) → значение для ПЛК, до приведения
    // типа. Упал — ошибка значения, в ПЛК ничего не уходит.
    let scripted = app.scripts.has_chain(&tag.name);
    let plc_json = match app.scripts.to_plc(&tag, &cmd.value) {
        Ok(v) => v,
        Err(e) => {
            return Outcome::fail("REJECTED_TYPE_MISMATCH", format!("Значение не преобразовано скриптом канала: {e}"));
        }
    };
    let value = match coerce(data_type, &plc_json) {
        Ok(v) => v,
        Err(e) => return Outcome::fail("REJECTED_TYPE_MISMATCH", format!("Значение не приводится к типу тега: {e}")),
    };
    // Оператору — его значение; в ПЛК ушло пересчитанное скриптом.
    let operator_value = if scripted { coerce(data_type, &cmd.value).ok() } else { None };

    let write = Write { tag: &tag, data_type, value, operator_value };
    match tag.protocol {
        Protocol::OpcUa => write_opcua(app, &controller, write).await,
        Protocol::Pac => write_pac(app, &controller, write).await,
        Protocol::Modbus => unreachable!("отсечено выше"),
    }
}

/// Пределы `writeMin`/`writeMax` тега, включительно. Проверяется значение оператора — до скрипта канала, в тех же единицах,
/// что телеметрия и пределы аларма; приведённое к типу (у целого тега 85,7 → 85 — то, что и будет записано).
fn check_limits(tag: &model::Tag, data_type: &str, value: &Value) -> Result<(), Outcome> {
    if tag.write_min.is_none() && tag.write_max.is_none() {
        return Ok(());
    }
    let coerced = coerce(data_type, value)
        .map_err(|e| Outcome::fail("REJECTED_TYPE_MISMATCH", format!("Значение не приводится к типу тега: {e}")))?;
    // Строка в обход пределов не проходит: у тега с пределами значение команды — только число.
    let Some(number) = coerced.as_f64() else {
        return Err(Outcome::fail(
            "REJECTED_TYPE_MISMATCH",
            format!("У тега заданы пределы записи, значение должно быть числом: {coerced}"),
        ));
    };
    let below = tag.write_min.is_some_and(|lo| number < lo);
    let above = tag.write_max.is_some_and(|hi| number > hi);
    if !below && !above {
        return Ok(());
    }
    let limits = match (tag.write_min, tag.write_max) {
        (Some(lo), Some(hi)) => format!("от {lo} до {hi}"),
        (Some(lo), None) => format!("не меньше {lo}"),
        (None, Some(hi)) => format!("не больше {hi}"),
        (None, None) => unreachable!("проверено выше"),
    };
    Err(Outcome::fail("REJECTED_OUT_OF_RANGE", format!("Значение {number} вне пределов записи тега: {limits}")))
}

/// Приведённая к типу тега команда, готовая к записи в ПЛК.
struct Write<'a> {
    tag: &'a model::Tag,
    data_type: &'a str,
    /// В ПЛК.
    value: TagValue,
    /// Что видит оператор, если скрипт канала пересчитал значение.
    operator_value: Option<TagValue>,
}

/// Запись по OPC UA; при `GATEWAY_COMMANDS_VERIFY_MS` — с проверкой эффекта.
async fn write_opcua(app: &App, controller: &crate::app::ControllerHandle, w: Write<'_>) -> Outcome {
    let Write { tag, data_type, value, operator_value } = w;
    let Some(conn) = controller.opc_connection() else {
        return Outcome::fail("FAILED_NO_CONNECTION", "Контроллер не подключён");
    };
    let node = match conn.node(&tag.node_id) {
        Ok(n) => n,
        Err(e) => return Outcome::fail("REJECTED_UNKNOWN_TAG", format!("Адрес узла не разобран: {e:#}")),
    };
    // Тип значения — тип узла на сервере (сервер проверяет его строго); если он неизвестен — dataType тега.
    let variant = match node.variant(data_type, &value) {
        Ok(v) => v,
        Err(e) => {
            let of = if node.kind.is_some() { "узла" } else { "тега" };
            return Outcome::fail("REJECTED_TYPE_MISMATCH", format!("Значение не приводится к типу {of}: {e:#}"));
        }
    };
    let verify = app.settings.gateway.command_verify;
    // Значение до записи нужно, чтобы отличить «запись не подействовала» от «значение и так было таким».
    let before = if verify.is_zero() { None } else { read_back(&conn, &node.id).await };
    match conn.write(node.id.clone(), variant).await {
        Ok(status) if status.is_good() => {
            info!("✍ OPC UA записано {} = {}", tag.name, value);
            controller.request_poll();
            if let Some(before) = before {
                // Даём ПЛК применить запись: программа ПЛК обрабатывает её на своём цикле, а не мгновенно.
                tokio::time::sleep(verify).await;
                if let Some(after) = read_back(&conn, &node.id).await
                    && !same_value(&after, &value)
                    && same_value(&after, &before)
                {
                    warn!("⚠ {}: запись принята, но значение осталось {after} (записано {value})", tag.name);
                    return Outcome::fail(
                        "FAILED_NOT_APPLIED",
                        format!(
                            "Контроллер принял запись, но значение не изменилось: {after} (записано {value}); \
                             вероятно, программа не разрешает действие при текущих условиях"
                        ),
                    );
                }
            }
            Outcome::applied_converted(value, operator_value)
        }
        Ok(status) => Outcome::fail(opcua::classify_write_status(status), format!("OPC UA отклонил запись: {status}")),
        Err(e) => Outcome::fail("FAILED_WRITE", format!("Ошибка записи: {e:#}")),
    }
}

/// Запись по PAC: `set_cmd` устройство.поле = значение; код 0 — принято. При `GATEWAY_COMMANDS_VERIFY_MS` — с проверкой
/// эффекта, как у OPC UA: код 0 говорит только «команду приняли», а значением может владеть программа ПЛК.
async fn write_pac(app: &App, controller: &crate::app::ControllerHandle, w: Write<'_>) -> Outcome {
    let Write { tag, data_type, value, operator_value } = w;
    let (Some(device), Some(field)) = (tag.device_name.as_deref(), tag.field_name.as_deref()) else {
        return Outcome::fail("REJECTED_UNKNOWN_TAG", format!("У PAC-тега нет device/field для команды: {}", tag.name));
    };
    // Текст команды собирается тем же кодом, что и при записи: значение, которое в него не помещается (строка, NaN,
    // отрицательное для менеджера рецептов), отклоняется здесь, до захвата соединения.
    if let Err(e) = lua::command_text(device, field, &value) {
        return Outcome::fail("REJECTED_TYPE_MISMATCH", format!("{e}"));
    }
    let verify = app.settings.gateway.command_verify;
    let mut guard = controller.pac.lock().await;
    let Some(conn) = guard.as_mut() else {
        return Outcome::fail("FAILED_WRITE", "Команда PAC не выполнена (нет активного соединения)");
    };
    // Значение до записи — из последнего снимка опроса: им отличают «запись не подействовала» от «значение и так было таким».
    let before = if verify.is_zero() { None } else { conn.read_value(device, field, data_type) };
    match conn.exec_command(device, field, &value).await {
        Ok(0) => {
            info!("✍ PAC записано {device}.{field} = {}", value);
            if let Some(before) = before {
                // Даём ПЛК применить команду на своём цикле; соединение на это время отпускаем — опрос не ждёт.
                drop(guard);
                tokio::time::sleep(verify).await;
                let mut guard = controller.pac.lock().await;
                if let Some(conn) = guard.as_mut()
                    && conn.poll_states().await.is_ok()
                    && let Some(after) = conn.read_value(device, field, data_type)
                    && !same_value(&after, &value)
                    && same_value(&after, &before)
                {
                    warn!("⚠ {}: команда принята, но значение осталось {after} (записано {value})", tag.name);
                    return Outcome::fail(
                        "FAILED_NOT_APPLIED",
                        format!(
                            "Контроллер принял команду, но значение не изменилось: {after} (записано {value}); \
                             вероятно, программа не разрешает действие при текущих условиях"
                        ),
                    );
                }
            } else {
                drop(guard); // опрос ждёт это же соединение
            }
            // Опросить сразу, не дожидаясь следующего цикла: монитор видит результат записи без задержки периода.
            controller.request_poll();
            Outcome::applied_converted(value, operator_value)
        }
        Ok(code) => Outcome::fail("FAILED_WRITE", format!("PAC не выполнил команду {device}.{field} (код {code})")),
        Err(e) => {
            *guard = None; // связь оборвалась — опрос переподключится
            Outcome::fail("FAILED_WRITE", format!("Команда PAC не выполнена: {e:#}"))
        }
    }
}

/// Текущее значение узла для проверки эффекта; ошибка чтения — «неизвестно», команду не портит.
async fn read_back(conn: &opcua::OpcConnection, node: &NodeId) -> Option<TagValue> {
    let values = conn.read(&[opcua::ReadValueId::from(node)]).await.ok()?;
    let dv = values.first()?;
    match opcua::reading(dv) {
        (Some(v), Quality::Good, _) => Some(v),
        _ => None,
    }
}

/// Равенство значений с допуском для вещественных (после округления в ПЛК).
fn same_value(a: &TagValue, b: &TagValue) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => (x - y).abs() <= 1e-6 * x.abs().max(y.abs()).max(1.0),
        _ => a == b,
    }
}

/// Окно недавних commandId: повторная доставка той же команды не пишет в ПЛК дважды.
pub struct Dedup {
    seen: Mutex<(HashMap<String, Instant>, VecDeque<String>)>,
    ttl: Duration,
    max: usize,
}

impl Dedup {
    /// Окно из `max` последних идентификаторов, помнящих `ttl`.
    pub fn new(ttl: Duration, max: usize) -> Self {
        Dedup { seen: Mutex::new((HashMap::new(), VecDeque::new())), ttl, max }
    }

    /// true — дубль в пределах TTL; иначе запоминает id.
    pub fn is_duplicate(&self, id: &str) -> bool {
        let mut guard = self.seen.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (map, order) = &mut *guard;
        let now = Instant::now();
        if map.get(id).is_some_and(|t| now.duration_since(*t) < self.ttl) {
            return true;
        }
        map.insert(id.to_string(), now);
        order.push_back(id.to_string());
        // Окно ограничено по числу записей: старейшие вытесняются, память не растёт.
        while order.len() > self.max {
            if let Some(old) = order.pop_front() {
                map.remove(&old);
            }
        }
        false
    }
}

/// Команда, принятая из топика и ждущая исполнения в очереди своего контроллера.
#[derive(Debug)]
pub struct Job {
    /// Команда.
    pub cmd: CommandMessage,
    /// Метка записи Kafka, мс (по часам продюсера монитора) — по ней считается возраст.
    pub record_timestamp_ms: Option<i64>,
    /// Когда команда встала в очередь.
    queued: Instant,
}

impl Job {
    /// Команда, только что принятая из топика.
    pub fn new(cmd: CommandMessage, record_timestamp_ms: Option<i64>) -> Self {
        Job { cmd, record_timestamp_ms, queued: Instant::now() }
    }
}

/// Очередь одного контроллера. Приёмник — под асинхронным мьютексом: исполнитель, перезапущенный надзирателем после
/// паники, продолжает ту же очередь.
struct Queue {
    tx: mpsc::UnboundedSender<Job>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Job>>>,
    depth: IntGauge,
    wait: Histogram,
}

/// Очереди команд по контроллерам, у каждой свой исполнитель: контроллер, который не отвечает на запись, задерживает
/// только свои команды. Внутри контроллера команды исполняются по одной в порядке топика, как и раньше: от порядка
/// зависит итоговое значение, а проверка эффекта сравнивает значения до и после записи.
pub struct Dispatcher {
    queues: HashMap<String, Queue>,
}

impl Dispatcher {
    /// Очереди для контроллеров станции; длина и ожидание — в `scada_command_queue` и `scada_command_wait_seconds`.
    pub fn new<'a>(controllers: impl IntoIterator<Item = &'a str>, metrics: &Metrics) -> Self {
        let queues = controllers
            .into_iter()
            .map(|name| {
                let (tx, rx) = mpsc::unbounded_channel();
                let queue = Queue {
                    tx,
                    rx: Arc::new(tokio::sync::Mutex::new(rx)),
                    depth: metrics.command_queue.with_label_values(&[name]),
                    wait: metrics.command_wait_seconds.with_label_values(&[name]),
                };
                (name.to_string(), queue)
            })
            .collect();
        Dispatcher { queues }
    }

    /// Поставить команду в очередь контроллера; `Err` с командой — такого контроллера нет.
    pub fn enqueue(&self, controller: &str, job: Job) -> Result<(), Box<Job>> {
        let Some(queue) = self.queues.get(controller) else { return Err(Box::new(job)) };
        queue.tx.send(job).map_err(|e| Box::new(e.0))?;
        queue.depth.inc();
        Ok(())
    }

    /// Исполнитель очереди контроллера: берёт команды по одной и ждёт исполнения каждой, пока не отменён.
    pub async fn run<F, Fut>(&self, controller: &str, cancel: &CancellationToken, process: F)
    where
        F: Fn(Job) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let Some(queue) = self.queues.get(controller) else { return };
        let mut rx = queue.rx.lock().await;
        loop {
            let job = tokio::select! {
                _ = cancel.cancelled() => return,
                job = rx.recv() => match job {
                    Some(job) => job,
                    None => return,
                },
            };
            queue.depth.dec();
            queue.wait.observe(job.queued.elapsed().as_secs_f64());
            process(job).await;
        }
    }
}

/// Имя тега команды для журнала: `tagName`, иначе `#tagId`; у `PAC_ALARM` — авария и контроллер.
fn tag_label(cmd: &CommandMessage) -> String {
    if let Kind::PacAlarm = kind(cmd) {
        let n = |v: &Value| if v.is_null() { "?".to_string() } else { v.to_string() };
        return format!(
            "аварии {}:{}:{} контроллера {}",
            n(&cmd.object_type),
            n(&cmd.object_number),
            n(&cmd.alarm_number),
            cmd.controller_id.as_str().unwrap_or("?")
        );
    }
    cmd.tag_name.clone().or_else(|| cmd.tag_id.map(|id| format!("#{id}"))).unwrap_or_else(|| "?".into())
}

/// Что делает команда, для журнала: значение записи или действие над аварией.
fn what(cmd: &CommandMessage) -> String {
    match kind(cmd) {
        Kind::PacAlarm => cmd.action.as_str().map_or_else(|| cmd.action.to_string(), str::to_string),
        _ => cmd.value.to_string(),
    }
}

/// Принять команду из топика (вызывается по порядку топика): повтор — пропуск, тег не найден — ответ сразу, иначе —
/// в очередь контроллера тега.
pub fn submit(app: &App, dedup: &Dedup, queues: &Dispatcher, cmd: CommandMessage, record_timestamp_ms: Option<i64>) {
    if let Some(id) = cmd.command_id.as_deref().filter(|id| !id.is_empty())
        && dedup.is_duplicate(id)
    {
        warn!("⏭ Команда {id} уже обработана недавно — пропуск");
        return;
    }
    info!("← команда: {} = {}, от {}", tag_label(&cmd), what(&cmd), cmd.requested_by.as_deref().unwrap_or("?"));
    let controller = match kind(&cmd) {
        Kind::TagWrite => resolve(app, &cmd)
            .map(|TagRef { controller, .. }| controller)
            .map_err(|msg| Outcome::fail("REJECTED_UNKNOWN_TAG", msg)),
        Kind::PacAlarm => alarm_target(app, &cmd).map(|(controller, _)| controller),
        Kind::Unknown(msg) => Err(Outcome::fail("REJECTED_INVALID", msg)),
    };
    let controller = match controller {
        Ok(controller) => controller,
        Err(outcome) => return finish(app, &cmd, outcome),
    };
    if let Err(job) = queues.enqueue(&controller.ctrl.name, Job::new(cmd, record_timestamp_ms)) {
        let msg = format!("Нет очереди команд контроллера {}", controller.ctrl.name);
        finish(app, &job.cmd, Outcome::fail("FAILED_WRITE", msg));
    }
}

/// Исполнить команду из очереди: возраст, запись, результат.
pub async fn process(app: &App, job: Job) {
    let cmd = &job.cmd;
    // Команда, пролежавшая дольше предела (шлюзы стояли оба или очередь контроллера стояла за зависшей записью), в ПЛК
    // не уходит: монитор её давно списал по таймауту, и запоздалая запись была бы для оператора неожиданной. Команды,
    // пришедшие за время переключения пары (секунды), исполняются. Возраст — по часам монитора: нужен NTP.
    let max_age_ms = app.settings.gateway.command_max_age.as_millis() as i64;
    let age = command_age_ms(cmd, job.record_timestamp_ms, chrono::Utc::now().timestamp_millis());
    let outcome = match age {
        Some(age) if age > max_age_ms => {
            warn!(
                "⌛ Команда {} по {} устарела ({age} мс) — в ПЛК не отправлена",
                cmd.command_id.as_deref().unwrap_or("?"),
                tag_label(cmd)
            );
            Outcome::fail(
                "REJECTED_EXPIRED",
                format!("Команда устарела: {} c при пределе {} c", age / 1000, max_age_ms / 1000),
            )
        }
        _ => execute(app, cmd).await,
    };
    finish(app, cmd, outcome);
}

/// Исход команды — в метрику, журнал, `scada-command-results` и событие `COMMAND` (у `PAC_ALARM` — `PAC_ALARM_COMMAND`).
fn finish(app: &App, cmd: &CommandMessage, outcome: Outcome) {
    app.metrics.commands.with_label_values(&[outcome.status]).inc();
    if !outcome.success() {
        warn!(
            "Команда {} по {}: {} — {}",
            cmd.command_id.as_deref().unwrap_or("?"),
            tag_label(cmd),
            outcome.status,
            outcome.message
        );
    }
    let alarm = matches!(kind(cmd), Kind::PacAlarm);
    if let Some(kafka) = &app.kafka {
        kafka.send_result(&CommandResultMessage {
            command_id: cmd.command_id.clone(),
            tag_id: cmd.tag_id,
            tag_name: if alarm { alarm_result_name(app, cmd) } else { cmd.tag_name.clone() },
            status: outcome.status,
            success: outcome.success(),
            message: outcome.message.clone(),
            applied_value: outcome.applied.clone(),
            timestamp: Timestamp::now(),
        });
    }
    let severity = if outcome.success() { "INFO" } else { "WARNING" };
    let requested_by = cmd.requested_by.as_deref().unwrap_or("?");
    let event = if alarm {
        Event::new(
            "PAC_ALARM_COMMAND",
            "CommandConsumer",
            severity,
            format!("Команда {} по {} от {requested_by}: {}", what(cmd), tag_label(cmd), outcome.status),
        )
        .details(json!({
            "controllerId": cmd.controller_id, "action": cmd.action, "objectType": cmd.object_type,
            "objectNumber": cmd.object_number, "alarmNumber": cmd.alarm_number, "requestedBy": cmd.requested_by,
            "status": outcome.status,
        }))
    } else {
        Event::new(
            "COMMAND",
            "CommandConsumer",
            severity,
            format!("Команда {} = {} от {requested_by}: {}", cmd.tag_name.as_deref().unwrap_or("?"), cmd.value,
                    outcome.status),
        )
        .details(json!({"tagId": cmd.tag_id, "value": cmd.value.to_string(), "requestedBy": cmd.requested_by, "status": outcome.status}))
    };
    app.events.emit(event);
}

/// `tagName` результата `PAC_ALARM` — путь проекта контроллера (как `path` снимка); контроллер без пути — его id из
/// команды. Контроллер не найден — id из команды, не строка — нет имени (ключ записи тогда `commandId`).
fn alarm_result_name(app: &App, cmd: &CommandMessage) -> Option<String> {
    let id = cmd.controller_id.as_str()?;
    let path = app
        .controllers
        .iter()
        .find(|h| pac_alarms::controller_id(&h.ctrl) == id)
        .and_then(|h| pac_alarms::path(&h.ctrl));
    Some(path.unwrap_or_else(|| id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerce_by_tag_type() {
        assert_eq!(coerce("INT32", &json!(1)), Ok(TagValue::Int(1)));
        assert_eq!(coerce("INT32", &json!(2.9)), Ok(TagValue::Int(2)));
        assert_eq!(coerce("INT32", &json!("5")), Ok(TagValue::Int(5)));
        assert_eq!(coerce("FLOAT", &json!(1)), Ok(TagValue::F64(1.0)));
        assert_eq!(coerce("BOOLEAN", &json!("true")), Ok(TagValue::Bool(true)));
        assert_eq!(coerce("BOOLEAN", &json!(0)), Ok(TagValue::Bool(false)));
        assert_eq!(coerce("STRING", &json!("REC")), Ok(TagValue::Text("REC".into())));
        assert!(coerce("INT32", &json!("abc")).is_err());
        for bad in ["NaN", "nan", "inf", "-Infinity"] {
            for t in ["FLOAT", "INT32", "BOOLEAN"] {
                assert!(coerce(t, &json!(bad)).is_err(), "{t} {bad}");
            }
        }
        assert!(coerce("FLOAT", &json!(null)).is_err());
    }

    fn tag_with_limits(data_type: &str, write_min: Option<f64>, write_max: Option<f64>) -> model::Tag {
        let cfg = serde_json::from_value(json!({
            "name": "t", "nodeId": "ns=2;s=1", "dataType": data_type, "enabled": true, "writable": true,
            "writeMin": write_min, "writeMax": write_max,
        }))
        .unwrap();
        model::Tag::from_config(&cfg)
    }

    fn limits_status(tag: &model::Tag, value: Value) -> Result<(), &'static str> {
        check_limits(tag, &tag.data_type, &value).map_err(|o| o.status)
    }

    #[test]
    fn write_limits_are_inclusive() {
        let tag = tag_with_limits("FLOAT", Some(0.0), Some(85.5));
        for ok in [json!(0), json!(85.5), json!("85.5"), json!(42.1)] {
            assert_eq!(limits_status(&tag, ok.clone()), Ok(()), "{ok}");
        }
        for out in [json!(-0.001), json!(85.50001), json!(850), json!("850")] {
            assert_eq!(limits_status(&tag, out.clone()), Err("REJECTED_OUT_OF_RANGE"), "{out}");
        }
        let err = check_limits(&tag, "FLOAT", &json!(850)).unwrap_err();
        assert!(!err.success() && err.applied.is_none());
        assert_eq!(err.message, "Значение 850 вне пределов записи тега: от 0 до 85.5");
    }

    #[test]
    fn write_limits_reject_non_numbers_and_need_no_check_without_limits() {
        let tag = tag_with_limits("FLOAT", None, Some(100.0));
        // NaN и бесконечность не сравниваются с пределом — это ошибка значения, а не «в пределах».
        for bad in [json!("NaN"), json!("inf"), json!("-Infinity"), json!(null), json!("abc")] {
            assert_eq!(limits_status(&tag, bad.clone()), Err("REJECTED_TYPE_MISMATCH"), "{bad}");
        }
        // Тип из команды не обходит пределы: строка вместо числа отклоняется.
        let as_text = check_limits(&tag, "STRING", &json!("1000")).unwrap_err();
        assert_eq!(as_text.status, "REJECTED_TYPE_MISMATCH");
        assert_eq!(limits_status(&tag, json!(-1e9)), Ok(()), "нижнего предела нет");
        assert_eq!(
            check_limits(&tag, "FLOAT", &json!(100.5)).unwrap_err().message,
            "Значение 100.5 вне пределов записи тега: не больше 100"
        );
        // Целый тег: проверяется то, что будет записано (дробная часть отбрасывается).
        let int = tag_with_limits("INT32", Some(1.0), None);
        assert_eq!(limits_status(&int, json!(1.9)), Ok(()));
        assert_eq!(limits_status(&int, json!(0.9)), Err("REJECTED_OUT_OF_RANGE"));
        // Без пределов значение здесь не проверяется вовсе — дальше идёт прежний путь.
        assert_eq!(limits_status(&tag_with_limits("STRING", None, None), json!("любой")), Ok(()));
    }

    fn job(id: &str) -> Job {
        Job::new(serde_json::from_value(json!({"commandId": id, "tagName": "t", "value": 1})).unwrap(), None)
    }

    /// Журнал исполнения: какой контроллер какую команду взял.
    type Log = Arc<Mutex<Vec<(String, String)>>>;

    fn record(log: &Log, controller: &str, job: &Job) {
        log.lock().unwrap().push((controller.to_string(), job.cmd.command_id.clone().unwrap()));
    }

    /// Контроллер «A» не отвечает: его исполнитель застрял на первой команде; команды «B» исполняются сразу и по порядку,
    /// следующие команды «A» ждут в своей очереди.
    #[tokio::test]
    async fn hung_controller_does_not_delay_commands_to_others() {
        let metrics = Metrics::new();
        let queues = Arc::new(Dispatcher::new(["A", "B"], &metrics));
        let (log, cancel): (Log, _) = (Log::default(), CancellationToken::new());
        for name in ["A", "B"] {
            let (queues, log, cancel) = (queues.clone(), log.clone(), cancel.clone());
            tokio::spawn(async move {
                queues
                    .run(name, &cancel, |job| {
                        record(&log, name, &job);
                        async move {
                            if name == "A" {
                                std::future::pending::<()>().await; // запись не возвращается
                            }
                        }
                    })
                    .await
            });
        }
        for (controller, id) in [("A", "a1"), ("B", "b1"), ("A", "a2"), ("B", "b2")] {
            assert!(queues.enqueue(controller, job(id)).is_ok());
        }
        assert!(queues.enqueue("C", job("c1")).is_err(), "нет такого контроллера");
        let done = tokio::time::timeout(Duration::from_secs(2), async {
            while log.lock().unwrap().len() < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(done.is_ok(), "команды B ждали A: {:?}", log.lock().unwrap());
        let taken: Vec<_> = log.lock().unwrap().iter().filter(|(c, _)| c == "B").map(|(_, id)| id.clone()).collect();
        assert_eq!(taken, ["b1", "b2"]);
        assert_eq!(metrics.command_queue.with_label_values(&["A"]).get(), 1, "a2 ждёт за зависшей a1");
        assert_eq!(metrics.command_queue.with_label_values(&["B"]).get(), 0);
        assert_eq!(metrics.command_wait_seconds.with_label_values(&["B"]).get_sample_count(), 2);
        cancel.cancel();
    }

    /// Команды одного контроллера — строго по одной и в порядке поступления, даже если исполнение занимает разное время.
    #[tokio::test]
    async fn commands_of_one_controller_run_one_at_a_time_in_order() {
        let metrics = Metrics::new();
        let queues = Arc::new(Dispatcher::new(["A"], &metrics));
        let (log, cancel): (Log, _) = (Log::default(), CancellationToken::new());
        let busy = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = {
            let (queues, log, cancel, busy) = (queues.clone(), log.clone(), cancel.clone(), busy.clone());
            tokio::spawn(async move {
                queues
                    .run("A", &cancel, |job| {
                        let (log, busy) = (log.clone(), busy.clone());
                        async move {
                            assert_eq!(busy.fetch_add(1, std::sync::atomic::Ordering::SeqCst), 0, "две команды сразу");
                            let n: u64 = job.cmd.command_id.as_deref().unwrap()[1..].parse().unwrap();
                            tokio::time::sleep(Duration::from_millis((20 - n) % 7)).await;
                            record(&log, "A", &job);
                            busy.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    })
                    .await
            })
        };
        let ids: Vec<String> = (0..20).map(|i| format!("c{i}")).collect();
        for id in &ids {
            queues.enqueue("A", job(id)).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while log.lock().unwrap().len() < ids.len() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("все команды исполнены");
        let order: Vec<String> = log.lock().unwrap().iter().map(|(_, id)| id.clone()).collect();
        assert_eq!(order, ids);
        cancel.cancel();
        worker.await.expect("исполнитель не паниковал");
    }

    /// Исполнитель, перезапущенный после остановки (паника, перезапуск надзирателем), продолжает ту же очередь.
    #[tokio::test]
    async fn restarted_worker_continues_the_same_queue() {
        let metrics = Metrics::new();
        let queues = Arc::new(Dispatcher::new(["A"], &metrics));
        let log = Log::default();
        let start = |cancel: CancellationToken| {
            let (queues, log) = (queues.clone(), log.clone());
            tokio::spawn(async move {
                queues
                    .run("A", &cancel, |job| {
                        record(&log, "A", &job);
                        async {}
                    })
                    .await
            })
        };
        let first = CancellationToken::new();
        let worker = start(first.clone());
        queues.enqueue("A", job("x1")).unwrap();
        while log.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        first.cancel();
        worker.await.unwrap();
        queues.enqueue("A", job("x2")).unwrap();
        assert_eq!(metrics.command_queue.with_label_values(&["A"]).get(), 1);
        let second = CancellationToken::new();
        let worker = start(second.clone());
        while log.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(log.lock().unwrap()[1].1, "x2");
        second.cancel();
        worker.await.unwrap();
    }

    #[test]
    fn dedup_window() {
        let d = Dedup::new(Duration::from_secs(60), 2);
        assert!(!d.is_duplicate("a"));
        assert!(d.is_duplicate("a"));
        assert!(!d.is_duplicate("b"));
        assert!(!d.is_duplicate("c")); // вытесняет "a"
        assert!(!d.is_duplicate("a"));
    }

    mod routing {
        use super::*;
        use crate::app::AppDeps;
        use crate::config::{ServerConfig, Settings};
        use crate::events;
        use crate::kafka::KafkaOut;
        use crate::leadership::Leadership;
        use crate::model::Controller;
        use crate::script::Scripts;

        struct Rig {
            app: Arc<App>,
            kafka: Arc<KafkaOut>,
            events_rx: mpsc::Receiver<Event>,
            queues: Dispatcher,
            dedup: Dedup,
        }

        fn controller(yaml: &str) -> Controller {
            Controller::from_config(&serde_yaml_ng::from_str::<ServerConfig>(yaml).unwrap()).unwrap()
        }

        /// OPC UA-контроллер с `id`, PAC- и Modbus-контроллер; аварии ПЛК — по `alarms`.
        fn rig(alarms: bool) -> Rig {
            let mut settings = Settings::from_env().expect("настройки по умолчанию");
            settings.gateway.pac_alarms_enabled = alarms;
            let metrics = Arc::new(Metrics::new());
            let leadership = Leadership::single("test".into(), metrics.ha_gauge("test"));
            let kafka =
                Arc::new(KafkaOut::new(&settings.kafka, metrics.clone(), leadership.clone()).expect("продюсер"));
            let (events, events_rx) = events::channel(metrics.clone());
            let tanks = controller(
                "id: ptusa-танки\nname: Танки\nendpoint: opc.tcp://h:4840\nenabled: true\ntags:\n\
                 - {name: BN1.TANKS.TANK1.ST, nodeId: \"ns=2;s=1\", dataType: INT32, enabled: true, writable: true}\n\
                 - {name: BN1.TANKS.TANK2.ST, nodeId: \"ns=2;s=2\", dataType: INT32, enabled: true}\n",
            );
            let pac = controller(
                "name: PAC\nendpoint: pac://h:10000\nenabled: true\ntags:\n\
                 - {name: P.V1.ST, nodeId: V1.ST, dataType: INT32, enabled: true}\n",
            );
            let wago = controller(
                "name: WAGO\nendpoint: modbus://h:502\nenabled: true\ntags:\n\
                 - {name: W.R1.V, nodeId: \"modbus:40001\", protocol: modbus, modbusAddress: 40001, dataType: INT32, \
                 enabled: true}\n",
            );
            let queues = Dispatcher::new(["Танки", "PAC", "WAGO"], &metrics);
            let deps = AppDeps {
                metrics,
                kafka: Some(kafka.clone()),
                events,
                telemetry: None,
                db: None,
                leadership,
                scripts: Scripts::none(),
            };
            let app = Arc::new(App::new(settings, deps, vec![tanks, pac, wago]));
            Rig { app, kafka, events_rx, queues, dedup: Dedup::new(Duration::from_secs(60), 100) }
        }

        impl Rig {
            fn submit(&self, body: Value) {
                submit(&self.app, &self.dedup, &self.queues, serde_json::from_value(body).unwrap(), None);
            }

            /// Результаты команд: (ключ записи, тело).
            fn results(&self) -> Vec<(String, Value)> {
                let topic = &self.app.settings.kafka.topics.command_results;
                std::mem::take(&mut *self.kafka.captured.lock().unwrap())
                    .into_iter()
                    .filter(|(t, _, _)| t == topic)
                    .map(|(_, key, body)| (key, serde_json::from_str(&body).unwrap()))
                    .collect()
            }
        }

        fn alarm(id: &str, controller: &str) -> Value {
            json!({"commandId": id, "type": "PAC_ALARM", "controllerId": controller, "action": "ACCEPT",
                   "objectType": 102, "objectNumber": 2, "alarmNumber": 3, "requestedBy": "оператор"})
        }

        #[tokio::test]
        async fn pac_alarm_commands_are_checked_and_queued_to_their_controller() {
            let mut r = rig(true);
            let mut no_number = alarm("c1", "ptusa-танки");
            no_number["alarmNumber"] = Value::Null;
            r.submit(no_number);
            r.submit(alarm("c2", "нет-такого"));
            r.submit(alarm("c3", "WAGO"));
            r.submit(json!({"commandId": "c4", "type": "RESET", "tagName": "BN1.TANKS.TANK1.ST", "value": 1}));
            let got: Vec<(String, String, String)> = r
                .results()
                .into_iter()
                .map(|(key, b)| {
                    (key, b["status"].as_str().unwrap().to_owned(), b["message"].as_str().unwrap().to_owned())
                })
                .collect();
            // Ключ результата — путь проекта контроллера (общий префикс тегов), у неизвестного контроллера — id из
            // команды; у команды неизвестного вида — как у записи: tagName из запроса.
            assert_eq!((got[0].0.as_str(), got[0].1.as_str()), ("BN1.TANKS", "REJECTED_INVALID"));
            assert!(got[0].2.contains("alarmNumber"), "{:?}", got[0]);
            assert_eq!((got[1].0.as_str(), got[1].1.as_str()), ("нет-такого", "REJECTED_UNKNOWN_TAG"));
            assert_eq!((got[2].0.as_str(), got[2].1.as_str()), ("W.R1", "REJECTED_UNKNOWN_TAG"), "у Modbus аварий нет");
            assert_eq!((got[3].0.as_str(), got[3].1.as_str()), ("BN1.TANKS.TANK1.ST", "REJECTED_INVALID"));
            assert!(got[3].2.contains("RESET"), "{:?}", got[3]);
            assert_eq!(got.len(), 4);
            let events: Vec<String> =
                std::iter::from_fn(|| r.events_rx.try_recv().ok()).map(|e| e.event_type).collect();
            assert_eq!(events, ["PAC_ALARM_COMMAND", "PAC_ALARM_COMMAND", "PAC_ALARM_COMMAND", "COMMAND"]);

            // Правильная команда встаёт в очередь своего контроллера (OPC UA и PAC); без соединения — нет связи.
            for (id, queue, path) in [("ptusa-танки", "Танки", "BN1.TANKS"), ("PAC", "PAC", "P.V1")] {
                r.submit(alarm(&format!("ok-{queue}"), id));
                assert!(r.results().is_empty(), "{id}: ждёт в очереди");
                let (back, done) = run_one(r, queue).await;
                r = back;
                assert_eq!(done.len(), 1, "{id}");
                assert_eq!(done[0].0, path);
                assert_eq!(done[0].1["status"], "FAILED_NO_CONNECTION", "{id}: {}", done[0].1);
                assert_eq!(done[0].1["appliedValue"], Value::Null);
            }
        }

        /// Исполнить одну команду из очереди контроллера; стенд и результаты.
        async fn run_one(r: Rig, queue: &'static str) -> (Rig, Vec<(String, Value)>) {
            let cancel = CancellationToken::new();
            let (app, stop) = (r.app.clone(), cancel.clone());
            let worker = async move {
                r.queues
                    .run(queue, &stop, |job| {
                        let app = app.clone();
                        let stop = stop.clone();
                        async move {
                            process(&app, job).await;
                            stop.cancel();
                        }
                    })
                    .await;
                r
            };
            let r = tokio::time::timeout(Duration::from_secs(5), worker).await.expect("команда исполнена");
            let done = r.results();
            (r, done)
        }

        #[test]
        fn tag_writes_keep_working_with_or_without_type() {
            let r = rig(true);
            r.submit(json!({"commandId": "w1", "tagName": "BN1.TANKS.TANK1.ST", "value": 1}));
            r.submit(json!({"commandId": "w2", "type": "TAG_WRITE", "tagName": "BN1.TANKS.TANK1.ST", "value": 2}));
            assert!(r.results().is_empty(), "обе записи в очереди контроллера");
            r.submit(json!({"commandId": "w3", "type": "TAG_WRITE", "tagName": "нет", "value": 2}));
            assert_eq!(r.results()[0].1["status"], "REJECTED_UNKNOWN_TAG");
        }

        #[test]
        fn pac_alarm_commands_are_rejected_when_alarms_are_off() {
            let r = rig(false);
            r.submit(alarm("c1", "ptusa-танки"));
            let got = r.results();
            assert_eq!(got[0].1["status"], "REJECTED_UNKNOWN_TAG");
            assert!(got[0].1["message"].as_str().unwrap().contains("GATEWAY_PAC_ALARMS_ENABLED"));
        }
    }
}
