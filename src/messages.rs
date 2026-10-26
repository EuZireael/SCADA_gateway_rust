//! Сообщения Kafka — поля и формат как у Java-шлюза (контракт с монитором).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{Quality, TagValue, Timestamp};

/// Телеметрия → `scada.tags`, ключ = путь канала. Ровно три поля (аналог OPC UA DataValue):
/// всё статическое монитор берёт из своего реестра по ключу.
#[derive(Debug, Serialize)]
pub struct TelemetryMessage<'a> {
    /// Типизированное значение; `null` — кадр потери связи (quality=BAD). Ссылка: сообщение живёт
    /// до сериализации, копировать значение (строки) на каждую публикацию незачем.
    pub value: Option<&'a TagValue>,
    /// Достоверность: `GOOD` или `BAD` (при `BAD` значение обычно `null`).
    pub quality: Quality,
    /// Момент снятия значения (sourceTimestamp OPC UA, момент чтения Modbus/PAC).
    pub timestamp: Timestamp,
}

impl TelemetryMessage<'_> {
    /// Тот же JSON, что даёт `serde_json::to_vec`, но в переданный буфер и без промежуточных выделений
    /// (самый частый путь шлюза: десятки тысяч сообщений в секунду). Байты совпадают — тест ниже.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"{\"value\":");
        match self.value {
            Some(TagValue::Int(i)) => {
                let _ = std::io::Write::write_fmt(out, format_args!("{i}"));
            }
            Some(v) => {
                if serde_json::to_writer(&mut *out, v).is_err() {
                    out.extend_from_slice(b"null");
                }
            }
            None => out.extend_from_slice(b"null"),
        }
        out.extend_from_slice(b",\"quality\":\"");
        out.extend_from_slice(self.quality.as_str().as_bytes());
        out.extend_from_slice(b"\",\"timestamp\":");
        self.timestamp.write_epoch_seconds(out);
        out.push(b'}');
    }
}

/// Событие → `scada-events`, ключ = eventType.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMessage {
    /// Уникальный идентификатор сообщения (UUID): по нему потребитель отбрасывает дубли.
    pub message_id: String,
    /// Константа `"EVENT"` — тип сообщения для потребителя.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Тип события: `CONNECTION`, `COMMAND`, `HEARTBEAT`, `HA`, `SCRIPT`, `SYSTEM`, `ALARM`, `CONTROLLER_IDENTITY_MISMATCH`…
    pub event_type: String,
    /// Источник: `Gateway`, `CommandConsumer`, `HotStandby`, `Scripts`.
    pub source: String,
    /// Важность: `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
    pub severity: String,
    /// Текст для оператора.
    pub message: String,
    /// Момент события.
    pub timestamp: Timestamp,
    /// Подробности в свободной форме (JSON-объект); для пары резервирования дополнены `instance` и `role`.
    pub details: Value,
}

/// Аларм → `scada-alarms`, ключ = путь канала. cleared=true — снятие.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmMessage {
    /// Уникальный идентификатор сообщения (UUID).
    pub message_id: String,
    /// Константа `"ALARM"`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Идентификатор аларма (`ALARM_<id тега>_<условие>_<мс>`); снятие несёт тот же, что и возникновение.
    pub alarm_id: String,
    /// Номер тега в БД шлюза (`null` без БД).
    pub tag_id: Option<i64>,
    /// Имя тега (ключ сообщения).
    pub tag_name: String,
    /// Важность: `MINOR`, `MAJOR`, `CRITICAL` — зависит от глубины выхода за предел.
    pub severity: String,
    /// Текст для оператора («High value: 105.00 > 100.00 °C»).
    pub message: String,
    /// Нарушенный предел (`minValue` или `maxValue`).
    pub threshold: f64,
    /// Значение в момент события.
    pub current_value: f64,
    /// Момент события.
    pub timestamp: Timestamp,
    /// Квитирован ли аларм (при создании `false`).
    pub acknowledged: bool,
    /// `true` — снятие аларма (значение вернулось в норму с гистерезисом 2 % диапазона).
    pub cleared: bool,
    /// Номер контроллера в БД.
    pub controller_id: Option<i64>,
    /// Имя контроллера.
    pub controller_name: Option<String>,
}

/// Команда из `scada-commands`. Монитор адресует тег по имени (tagName); tagId — для
/// старого Monitor Srv. Незнакомые поля игнорируются. Команда с `type: "PAC_ALARM"` — не запись тега, а действие над
/// аварией ПЛК (`controllerId`, `action`, `objectType`, `objectNumber`, `alarmNumber`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandMessage {
    /// Идентификатор команды от монитора: по нему шлюз отбрасывает повтор.
    pub command_id: Option<String>,
    /// Номер тега в БД (для старого монитора); основной способ адресации — `tagName`.
    pub tag_id: Option<i64>,
    /// Имя тега (совпадает с `name` в `controllers.yaml`).
    pub tag_name: Option<String>,
    /// Подсказка типа. Монитор её не шлёт: тип берётся из конфигурации тега.
    pub data_type: Option<String>,
    /// Новое значение: число, bool или строка; приводится к типу тега.
    #[serde(default)]
    pub value: Value,
    /// Кто отправил команду (оператор), для журнала.
    pub requested_by: Option<String>,
    /// Момент отправки команды (ISO-8601 или epoch-секунды) — для возраста, если у записи Kafka нет метки.
    pub timestamp: Option<Value>,
    /// Вид команды: нет или `TAG_WRITE` — запись тега, `PAC_ALARM` — действие над аварией ПЛК. Как пришёл (не строка —
    /// неизвестный вид, `REJECTED_INVALID`).
    #[serde(rename = "type", default)]
    pub kind: Value,
    /// `PAC_ALARM`: контроллер — `id` из `controllers.yaml` (не задан — имя).
    #[serde(default)]
    pub controller_id: Value,
    /// `PAC_ALARM`: `ACCEPT`, `SUPPRESS` или `UNSUPPRESS`.
    #[serde(default)]
    pub action: Value,
    /// `PAC_ALARM`: тип объекта (`id_type` ptusa) — целое; проверяется при исполнении.
    #[serde(default)]
    pub object_type: Value,
    /// `PAC_ALARM`: номер объекта (`id_n`).
    #[serde(default)]
    pub object_number: Value,
    /// `PAC_ALARM`: номер аварии в объекте (`id_object_alarm_number`).
    #[serde(default)]
    pub alarm_number: Value,
}

impl CommandMessage {
    /// Метка из тела команды в миллисекундах epoch.
    pub fn sent_at_ms(&self) -> Option<i64> {
        match self.timestamp.as_ref()? {
            Value::String(s) => chrono::DateTime::parse_from_rfc3339(s).ok().map(|t| t.timestamp_millis()),
            Value::Number(n) => n.as_f64().map(|secs| (secs * 1000.0) as i64),
            _ => None,
        }
    }
}

/// Возраст команды, мс: по метке записи Kafka (её ставит продюсер монитора), иначе по `timestamp`
/// в теле. `None` — возраст неизвестен (такую команду исполняем).
pub fn command_age_ms(cmd: &CommandMessage, record_timestamp_ms: Option<i64>, now_ms: i64) -> Option<i64> {
    record_timestamp_ms.filter(|t| *t > 0).or_else(|| cmd.sent_at_ms()).map(|sent| now_ms - sent)
}

/// Авария ПЛК в снимке `scada.pac-alarms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PacAlarm {
    /// `<objectType>:<objectNumber>:<alarmNumber>`, у критических ошибок прошивки — `C:<objectNumber>`.
    pub key: String,
    /// Тип объекта (`id_type`); у критических `null`.
    pub object_type: Option<i64>,
    /// Номер объекта, серийный номер устройства или параметр критической ошибки (`id_n`).
    pub object_number: i64,
    /// Номер аварии в объекте; у критических `null`.
    pub alarm_number: Option<i64>,
    /// Текст для оператора, как его прислал ПЛК.
    pub description: String,
    /// Группа: `тревога`, `сообщение`, `ответ`…
    pub group: String,
    /// Приоритет ptusa: 0 FATAL, 100 ошибка связи, 250 тревога, 500 сообщение, 750 ответ, 1000 напоминание.
    pub priority: i64,
    /// `ALARM`, `RETURN` (причина ушла, не квитирована) или `ACCEPT` (квитирована, причина есть).
    pub state: String,
    /// Подавлена.
    pub suppressed: bool,
}

/// Снимок списка аварий контроллера → `scada.pac-alarms` (compact), ключ = id контроллера.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PacAlarmsMessage<'a> {
    /// `id` контроллера из `controllers.yaml` (не задан — имя).
    pub controller_id: &'a str,
    /// Имя контроллера.
    pub controller_name: &'a str,
    /// Путь проекта в мониторе: `alarmsPath` или общий префикс имён тегов контроллера.
    pub path: &'a str,
    /// `false` — связи с ПЛК нет; `alarms` — последний известный список.
    pub online: bool,
    /// Счётчик изменений списка у ПЛК (`id` ptusa) последнего удачного чтения.
    pub pac_version: Option<i64>,
    /// Момент последнего удачного чтения (ISO-8601, мс, UTC).
    pub polled_at: Option<String>,
    /// Аварии в порядке ПЛК.
    pub alarms: &'a [PacAlarm],
}

/// Результат команды → `scada-command-results`, ключ = tagName (или commandId).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResultMessage {
    /// Идентификатор команды из запроса.
    pub command_id: Option<String>,
    /// Номер тега из запроса.
    pub tag_id: Option<i64>,
    /// Имя тега из запроса.
    pub tag_name: Option<String>,
    /// Исход: `APPLIED`, `REJECTED_*` или `FAILED_*` (полный перечень — `docs/OPERATIONS.md`).
    pub status: &'static str,
    /// `true` только при `APPLIED`.
    pub success: bool,
    /// Пояснение для оператора (причина отказа, что записано).
    pub message: String,
    /// Значение, которое увидел оператор: своё, если скрипт канала пересчитал значение для ПЛК.
    pub applied_value: Option<TagValue>,
    /// Момент результата.
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts() -> Timestamp {
        Timestamp(chrono::Utc.timestamp_opt(1790233762, 61_208_074).unwrap())
    }

    #[test]
    fn telemetry_wire_is_exactly_three_fields() {
        let v = TagValue::F32(1.07);
        let msg = TelemetryMessage { value: Some(&v), quality: Quality::Good, timestamp: ts() };
        assert_eq!(
            serde_json::to_string(&msg).unwrap(),
            r#"{"value":1.07,"quality":"GOOD","timestamp":1790233762.061208074}"#
        );
    }

    #[test]
    fn hand_written_json_is_byte_identical_to_serde() {
        use chrono::TimeZone;
        let stamps = [
            ts(),
            Timestamp(chrono::Utc.timestamp_opt(1, 5).unwrap()),
            Timestamp(chrono::Utc.timestamp_opt(1790233762, 999_999_999).unwrap()),
            Timestamp(chrono::Utc.timestamp_opt(0, 0).unwrap()),
        ];
        let values = [
            None,
            Some(TagValue::Bool(true)),
            Some(TagValue::Bool(false)),
            Some(TagValue::Int(0)),
            Some(TagValue::Int(-42)),
            Some(TagValue::Int(i64::MAX)),
            Some(TagValue::F32(64.7)),
            Some(TagValue::F32(-0.0)),
            Some(TagValue::F32(f32::NAN)),
            Some(TagValue::F32(1.0e-7)),
            Some(TagValue::F64(1.0e21)),
            Some(TagValue::F64(f64::INFINITY)),
            Some(TagValue::F64(0.1)),
            Some(TagValue::Text("строка \"с\" кавычками\n и \\ слэшем".into())),
            Some(TagValue::Text(String::new())),
        ];
        for value in &values {
            for quality in [Quality::Good, Quality::Bad] {
                for timestamp in stamps {
                    let msg = TelemetryMessage { value: value.as_ref(), quality, timestamp };
                    let mut mine = Vec::new();
                    msg.write_json(&mut mine);
                    assert_eq!(String::from_utf8(mine).unwrap(), serde_json::to_string(&msg).unwrap(), "{value:?}");
                }
            }
        }
    }

    #[test]
    fn bad_frame_has_null_value() {
        let msg = TelemetryMessage { value: None, quality: Quality::Bad, timestamp: ts() };
        assert!(serde_json::to_string(&msg).unwrap().starts_with(r#"{"value":null,"quality":"BAD""#));
    }

    #[test]
    fn command_from_monitor_is_parsed() {
        let raw = r#"{"commandId":"c1","tagName":"A.B.LINE1V0.M","value":1,"requestedBy":"scada-runtime","timestamp":"2026-09-24T08:00:00Z","extra":true}"#;
        let cmd: CommandMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(cmd.command_id.as_deref(), Some("c1"));
        assert_eq!(cmd.tag_name.as_deref(), Some("A.B.LINE1V0.M"));
        assert_eq!(cmd.value, serde_json::json!(1));
        assert!(cmd.tag_id.is_none() && cmd.data_type.is_none());
    }

    #[test]
    fn command_age_prefers_record_timestamp_then_body() {
        let mut cmd: CommandMessage =
            serde_json::from_str(r#"{"commandId":"c","tagName":"T","value":1,"timestamp":"2026-09-24T08:00:00Z"}"#)
                .unwrap();
        let sent = chrono::DateTime::parse_from_rfc3339("2026-09-24T08:00:00Z").unwrap().timestamp_millis();
        assert_eq!(command_age_ms(&cmd, None, sent + 61_000), Some(61_000), "по телу команды");
        assert_eq!(
            command_age_ms(&cmd, Some(sent + 50_000), sent + 61_000),
            Some(11_000),
            "метка записи Kafka приоритетнее"
        );
        cmd.timestamp = None;
        assert_eq!(command_age_ms(&cmd, None, sent), None, "нет метки — возраст неизвестен");
        cmd.timestamp = Some(serde_json::json!(1790233762.5));
        assert_eq!(cmd.sent_at_ms(), Some(1790233762500));
    }

    #[test]
    fn result_uses_java_field_names() {
        let r = CommandResultMessage {
            command_id: Some("c1".into()),
            tag_id: None,
            tag_name: Some("T".into()),
            status: "APPLIED",
            success: true,
            message: "ok".into(),
            applied_value: Some(TagValue::Int(1)),
            timestamp: ts(),
        };
        let json: Value = serde_json::to_value(&r).unwrap();
        for field in ["commandId", "tagId", "tagName", "status", "success", "message", "appliedValue", "timestamp"] {
            assert!(json.get(field).is_some(), "нет поля {field}");
        }
    }
}
