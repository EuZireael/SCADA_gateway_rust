//! Команды записи из `scada-commands`: маршрутизация по протоколу, права, приведение типа,
//! запись в ПЛК и результат в `scada-command-results`. Статусы — как у Java-шлюза.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tracing::{info, warn};

use crate::app::{App, TagRef};
use crate::events::Event;
use crate::messages::{CommandMessage, CommandResultMessage, command_age_ms};
use crate::model::{self, Protocol, TagValue, Timestamp};
use crate::opcua;

/// Исход команды.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    pub status: &'static str,
    pub message: String,
    pub applied: Option<TagValue>,
}

impl Outcome {
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

    fn fail(status: &'static str, message: impl Into<String>) -> Self {
        Outcome { status, message: message.into(), applied: None }
    }

    pub fn success(&self) -> bool {
        self.status == "APPLIED"
    }
}

/// Значение команды (JSON) → значение по типу тега. Строки с числом принимаются.
pub fn coerce(data_type: &str, value: &Value) -> Result<TagValue, String> {
    let number = || match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| format!("{n} не число")),
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        Value::String(s) => s.trim().parse::<f64>().map_err(|_| format!("'{s}' не число")),
        other => Err(format!("{other} не число")),
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

/// Выполнить команду.
pub async fn execute(app: &App, cmd: &CommandMessage) -> Outcome {
    let found = match (cmd.tag_id, cmd.tag_name.as_deref().filter(|n| !n.trim().is_empty())) {
        (Some(id), _) => app.tag_by_id(id).ok_or_else(|| format!("Тег не найден: {id}")),
        (None, Some(name)) => app.tag_by_name(name).ok_or_else(|| format!("Тег не найден по имени: {name}")),
        (None, None) => Err("Команда без tagId и tagName".to_string()),
    };
    let TagRef { tag, controller } = match found {
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

    match tag.protocol {
        Protocol::OpcUa => {
            let Some(conn) = controller.opc_connection() else {
                return Outcome::fail("FAILED_NO_CONNECTION", "Контроллер не подключён");
            };
            let prepared =
                opcua::parse_node_id(&tag.node_id).and_then(|n| Ok((n, opcua::to_variant(data_type, &value)?)));
            let (node, variant) = match prepared {
                Ok(p) => p,
                Err(e) => {
                    return Outcome::fail(
                        "REJECTED_TYPE_MISMATCH",
                        format!("Значение не приводится к типу тега: {e:#}"),
                    );
                }
            };
            match conn.write(node, variant).await {
                Ok(status) if status.is_good() => {
                    info!("✍ OPC UA записано {} = {}", tag.name, value);
                    controller.request_poll();
                    Outcome::applied_converted(value, operator_value)
                }
                Ok(status) => {
                    Outcome::fail(opcua::classify_write_status(status), format!("OPC UA отклонил запись: {status}"))
                }
                Err(e) => Outcome::fail("FAILED_WRITE", format!("Ошибка записи: {e:#}")),
            }
        }
        Protocol::Pac => {
            let (Some(device), Some(field)) = (tag.device_name.as_deref(), tag.field_name.as_deref()) else {
                return Outcome::fail(
                    "REJECTED_UNKNOWN_TAG",
                    format!("У PAC-тега нет device/field для команды: {}", tag.name),
                );
            };
            // В set_cmd значение подставляется в Lua-текст — только число/bool, строк нет.
            if matches!(value, TagValue::Text(_)) {
                return Outcome::fail("REJECTED_TYPE_MISMATCH", "PAC принимает только числовые значения");
            }
            let mut guard = controller.pac.lock().await;
            let Some(conn) = guard.as_mut() else {
                return Outcome::fail("FAILED_WRITE", "Команда PAC не выполнена (нет активного соединения)");
            };
            match conn.exec_command(device, field, &value).await {
                Ok(0) => {
                    info!("✍ PAC записано {device}.{field} = {}", value);
                    drop(guard); // опрос ждёт это же соединение
                    controller.request_poll();
                    Outcome::applied_converted(value, operator_value)
                }
                Ok(code) => {
                    Outcome::fail("FAILED_WRITE", format!("PAC не выполнил команду {device}.{field} (код {code})"))
                }
                Err(e) => {
                    *guard = None; // связь оборвалась — опрос переподключится
                    Outcome::fail("FAILED_WRITE", format!("Команда PAC не выполнена: {e:#}"))
                }
            }
        }
        Protocol::Modbus => unreachable!("отсечено выше"),
    }
}

/// Окно недавних commandId: повторная доставка той же команды не пишет в ПЛК дважды.
pub struct Dedup {
    seen: Mutex<(HashMap<String, Instant>, VecDeque<String>)>,
    ttl: Duration,
    max: usize,
}

impl Dedup {
    pub fn new(ttl: Duration, max: usize) -> Self {
        Dedup { seen: Mutex::new((HashMap::new(), VecDeque::new())), ttl, max }
    }

    /// true — дубль в пределах TTL; иначе запоминает id.
    pub fn is_duplicate(&self, id: &str) -> bool {
        let mut guard = self.seen.lock().expect("mutex dedup");
        let (map, order) = &mut *guard;
        let now = Instant::now();
        if map.get(id).is_some_and(|t| now.duration_since(*t) < self.ttl) {
            return true;
        }
        map.insert(id.to_string(), now);
        order.push_back(id.to_string());
        while order.len() > self.max {
            if let Some(old) = order.pop_front() {
                map.remove(&old);
            }
        }
        false
    }
}

/// Обработать одну команду: дубли, возраст, запись, метрика, результат, событие.
/// `record_timestamp_ms` — метка записи Kafka (по часам продюсера монитора).
pub async fn handle(app: &App, dedup: &Dedup, cmd: CommandMessage, record_timestamp_ms: Option<i64>) {
    if let Some(id) = cmd.command_id.as_deref().filter(|id| !id.is_empty())
        && dedup.is_duplicate(id)
    {
        warn!("⏭ Команда {id} уже обработана недавно — пропуск");
        return;
    }
    let tag = cmd.tag_name.clone().or_else(|| cmd.tag_id.map(|id| format!("#{id}"))).unwrap_or_else(|| "?".into());
    info!("← команда: {tag} = {}, от {}", cmd.value, cmd.requested_by.as_deref().unwrap_or("?"));
    // Команда, пролежавшая в топике дольше предела (шлюзы стояли оба), в ПЛК не уходит: монитор её давно
    // списал по таймауту, и запоздалая запись была бы для оператора неожиданной. Команды, пришедшие за
    // время переключения пары (секунды), исполняются. Возраст — по часам монитора: нужен NTP.
    let max_age_ms = app.settings.gateway.command_max_age.as_millis() as i64;
    let age = command_age_ms(&cmd, record_timestamp_ms, chrono::Utc::now().timestamp_millis());
    let outcome = match age {
        Some(age) if age > max_age_ms => {
            warn!(
                "⌛ Команда {} по {tag} устарела ({age} мс) — в ПЛК не отправлена",
                cmd.command_id.as_deref().unwrap_or("?")
            );
            Outcome::fail(
                "REJECTED_EXPIRED",
                format!("Команда устарела: {} c при пределе {} c", age / 1000, max_age_ms / 1000),
            )
        }
        _ => execute(app, &cmd).await,
    };
    app.metrics.commands.with_label_values(&[outcome.status]).inc();
    if !outcome.success() {
        warn!(
            "Команда {} по {tag}: {} — {}",
            cmd.command_id.as_deref().unwrap_or("?"),
            outcome.status,
            outcome.message
        );
    }
    if let Some(kafka) = &app.kafka {
        kafka.send_result(&CommandResultMessage {
            command_id: cmd.command_id.clone(),
            tag_id: cmd.tag_id,
            tag_name: cmd.tag_name.clone(),
            status: outcome.status,
            success: outcome.success(),
            message: outcome.message.clone(),
            applied_value: outcome.applied.clone(),
            timestamp: Timestamp::now(),
        });
    }
    app.events.emit(
        Event::new(
            "COMMAND",
            "CommandConsumer",
            if outcome.success() { "INFO" } else { "WARNING" },
            format!("Команда {} = {} от {}: {}", cmd.tag_name.as_deref().unwrap_or("?"), cmd.value,
                    cmd.requested_by.as_deref().unwrap_or("?"), outcome.status),
        )
        .details(json!({"tagId": cmd.tag_id, "value": cmd.value.to_string(), "requestedBy": cmd.requested_by, "status": outcome.status})),
    );
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
        assert!(coerce("FLOAT", &json!(null)).is_err());
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
}
