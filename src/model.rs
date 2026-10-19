//! Доменная модель: контроллер, тег, значение, метка времени.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

use crate::config::{HistoryConfig, ServerConfig, TagConfig};
use crate::tag_settings::TagTuning;

/// Протокол, по которому тег читается и пишется.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// OPC UA: узлы по `nodeId`, чтение пачками, запись `Write`.
    OpcUa,
    /// Modbus TCP: holding-регистры (FC03), только чтение.
    Modbus,
    /// PAC driver-master (ptusa): значения из Lua-снимка, запись `set_cmd`.
    Pac,
}

/// Тип контроллера — по схеме endpoint (`opc.tcp://`, `modbus://`, `pac://`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerKind {
    /// Контроллер с адресом `opc.tcp://…`.
    OpcUa,
    /// Контроллер с адресом `modbus://…`.
    Modbus,
    /// Контроллер с адресом `pac://…`.
    Pac,
}

impl ControllerKind {
    /// Тип по началу адреса. По началу, а не по вхождению: хост `pac://modbus-gw:502` — это PAC, а не Modbus.
    pub fn from_endpoint(endpoint: &str) -> Option<Self> {
        let e = endpoint.trim().to_ascii_lowercase();
        if e.starts_with("opc.tcp://") {
            Some(Self::OpcUa)
        } else if e.starts_with("modbus://") {
            Some(Self::Modbus)
        } else if e.starts_with("pac://") {
            Some(Self::Pac)
        } else {
            None
        }
    }

    /// Протокол тегов, которые опрашивает контроллер этого типа.
    pub fn protocol(self) -> Protocol {
        match self {
            Self::OpcUa => Protocol::OpcUa,
            Self::Modbus => Protocol::Modbus,
            Self::Pac => Protocol::Pac,
        }
    }
}

/// Тег (канал) с адресацией и правами — неизменяемая часть конфигурации.
#[derive(Debug, Clone)]
pub struct Tag {
    /// PK тега в БД шлюза (0 — работа без БД).
    pub id: i64,
    /// Порядковый номер среди включённых тегов контроллера: по нему обработчик хранит состояние
    /// тега (качество, последнее опубликованное значение…) вектором, а не хеш-таблицей по имени.
    pub slot: usize,
    /// Полный путь канала = Kafka-key телеметрии = адрес команды монитора.
    pub name: String,
    /// Адрес в контроллере: OPC UA — `ns=2;s=6`; PAC и Modbus — любой уникальный ключ. Уникален в пределах контроллера: по нему тег связывается со строкой БД.
    pub node_id: String,
    /// Номер канала в базе монитора (справочно, пишется в БД).
    pub channel_id: Option<i64>,
    /// Прибор PAC (`LINE1V0`); у остальных протоколов — метаданные для монитора и скриптов.
    pub device_name: Option<String>,
    /// Поле прибора PAC (`ST`, `RT_PAR_F[12]`); у остальных протоколов — метаданные.
    pub field_name: Option<String>,
    /// Тип прибора (`V` — клапан, `M` — мотор…); метаданные.
    pub device_type: Option<String>,
    /// Протокол чтения и записи, выведенный из `protocol`, `nodeId` и `modbusAddress` (см. [`classify`]).
    pub protocol: Protocol,
    /// Значение `protocol` из YAML как написано (пишется в БД).
    pub protocol_raw: String,
    /// Тип данных как в YAML; по префиксу определяет приведение значения команды и декодирование Modbus (см. [`is_int`] и соседей).
    pub data_type: String,
    /// Желаемый период опроса, мс; 0 — не задан. Период цикла контроллера — минимум по его тегам.
    pub polling_rate_ms: u64,
    /// Включён ли тег; выключенные теги в [`Controller::tags`] не попадают.
    pub enabled: bool,
    /// Можно ли писать команды. Modbus не пишется никогда, что бы ни стояло здесь.
    pub writable: bool,
    /// Единица измерения (справочно, в событиях алармов).
    pub unit: Option<String>,
    /// Нижний предел для аларма (`GATEWAY_ALARMS_ENABLED`).
    pub min_value: Option<f64>,
    /// Верхний предел для аларма.
    pub max_value: Option<f64>,
    /// Адрес holding-регистра в нотации 4xxxx (40001…); только Modbus.
    pub modbus_address: Option<i32>,
    /// Тип регистра из YAML (справочно, пишется в БД; декодирование идёт по `data_type`).
    pub modbus_type: Option<String>,
    /// Modbus Unit ID; по умолчанию 1.
    pub modbus_unit_id: u8,
    /// Переопределение фильтра истории (`history:` в controllers.yaml); пусто — умолчания.
    pub history: HistoryConfig,
    /// Настройки из топика `channel.tag-settings` (период опроса, зона истории); меняются на лету, пусто — YAML.
    pub tuning: Arc<TagTuning>,
}

impl Tag {
    /// Тег из записи YAML. `id` и `slot` проставляются позже: `id` — после синхронизации с БД, `slot` — при сборке контроллера.
    pub fn from_config(cfg: &TagConfig) -> Self {
        let protocol_raw = cfg.protocol.clone().unwrap_or_else(|| "opcua".into());
        Tag {
            id: 0,
            slot: 0,
            name: cfg.name.clone(),
            node_id: cfg.node_id.clone(),
            channel_id: cfg.channel_id,
            device_name: cfg.device_name.clone(),
            field_name: cfg.field_name.clone(),
            device_type: cfg.device_type.clone(),
            protocol: classify(&protocol_raw, &cfg.node_id, cfg.modbus_address),
            protocol_raw,
            data_type: cfg.data_type.clone(),
            polling_rate_ms: cfg.polling_rate,
            enabled: cfg.enabled,
            writable: cfg.writable,
            unit: cfg.unit.clone(),
            min_value: cfg.min_value,
            max_value: cfg.max_value,
            modbus_address: cfg.modbus_address,
            modbus_type: cfg.modbus_type.clone(),
            modbus_unit_id: cfg.modbus_unit_id.unwrap_or(1),
            history: cfg.history.clone().unwrap_or_default(),
            tuning: Arc::default(),
        }
    }
}

/// Протокол тега — как TagProtocols Java-шлюза: явный modbus/pac, иначе OPC UA при
/// protocol=opcua или непустом nodeId; фолбэк Modbus по положительному адресу.
pub fn classify(protocol: &str, node_id: &str, modbus_address: Option<i32>) -> Protocol {
    match protocol.to_ascii_lowercase().as_str() {
        "modbus" => Protocol::Modbus,
        "pac" => Protocol::Pac,
        "opcua" => Protocol::OpcUa,
        _ if modbus_address.is_some_and(|a| a > 0) => Protocol::Modbus,
        _ if !node_id.is_empty() => Protocol::OpcUa,
        _ => Protocol::OpcUa,
    }
}

/// Политика безопасности канала OPC UA (`security:` в controllers.yaml).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpcPolicy {
    /// Без защиты (по умолчанию).
    #[default]
    None,
    /// Basic128Rsa15 (устаревшая; для старых ПЛК).
    Basic128Rsa15,
    /// Basic256 (устаревшая).
    Basic256,
    /// Basic256Sha256 — минимально рекомендуемая.
    Basic256Sha256,
    /// Aes128_Sha256_RsaOaep.
    Aes128Sha256RsaOaep,
    /// Aes256_Sha256_RsaPss.
    Aes256Sha256RsaPss,
}

/// Режим защиты сообщений OPC UA.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpcMode {
    /// Без подписи и шифрования (только вместе с политикой None).
    #[default]
    None,
    /// Сообщения подписываются.
    Sign,
    /// Сообщения подписываются и шифруются (по умолчанию для защищённых политик).
    SignAndEncrypt,
}

/// Защита соединения с OPC UA-контроллером: политика и режим канала, логин и пароль пользователя.
/// Пароль не печатается ни в `Debug`, ни в журнале.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct OpcSecurity {
    /// Политика безопасности канала.
    pub policy: OpcPolicy,
    /// Режим защиты сообщений.
    pub mode: OpcMode,
    /// Логин пользователя; задаётся вместе с паролем.
    pub username: Option<String>,
    /// Пароль пользователя. В `Debug` и журнал не попадает.
    pub password: Option<String>,
}

impl std::fmt::Debug for OpcSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpcSecurity")
            .field("policy", &self.policy)
            .field("mode", &self.mode)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl OpcSecurity {
    /// Канал защищён (подпись и/или шифрование).
    pub fn is_secure(&self) -> bool {
        self.policy != OpcPolicy::None
    }

    /// Разбор `security:` (`None`, `Basic256Sha256`, `Basic256Sha256_Sign`, `Aes256_Sha256_RsaPss/SignAndEncrypt`…):
    /// политика, затем необязательный режим `Sign` / `SignAndEncrypt` (по умолчанию — SignAndEncrypt).
    /// Регистр, `_`, `-`, `/` и пробелы значения не имеют. Пара логин/пароль — только вместе.
    pub fn parse(security: Option<&str>, username: Option<&str>, password: Option<&str>) -> Result<Self, String> {
        let username = username.filter(|u| !u.is_empty());
        let password = password.filter(|p| !p.is_empty());
        if username.is_some() != password.is_some() {
            return Err("username и password задаются вместе".into());
        }
        let raw = security.unwrap_or("").trim();
        let norm: String =
            raw.chars().filter(|c| !matches!(c, '_' | '-' | '/' | ' ')).collect::<String>().to_ascii_lowercase();
        let (policy_part, mode) = if let Some(p) = norm.strip_suffix("signandencrypt") {
            (p, Some(OpcMode::SignAndEncrypt))
        } else if let Some(p) = norm.strip_suffix("sign") {
            (p, Some(OpcMode::Sign))
        } else {
            (norm.as_str(), None)
        };
        let policy = match policy_part {
            "" | "none" => OpcPolicy::None,
            "basic128rsa15" => OpcPolicy::Basic128Rsa15,
            "basic256" => OpcPolicy::Basic256,
            "basic256sha256" => OpcPolicy::Basic256Sha256,
            "aes128sha256rsaoaep" => OpcPolicy::Aes128Sha256RsaOaep,
            "aes256sha256rsapss" => OpcPolicy::Aes256Sha256RsaPss,
            _ => {
                return Err(format!(
                    "security: {raw:?} — ожидается None или политика (Basic256Sha256, Aes128_Sha256_RsaOaep, Aes256_Sha256_RsaPss, \
                     Basic256, Basic128Rsa15) с необязательным режимом _Sign / _SignAndEncrypt"
                ));
            }
        };
        let mode = match (policy, mode) {
            (OpcPolicy::None, None) => OpcMode::None,
            (OpcPolicy::None, Some(_)) => return Err(format!("security: {raw:?} — у политики None нет режима")),
            (_, mode) => mode.unwrap_or(OpcMode::SignAndEncrypt),
        };
        Ok(OpcSecurity { policy, mode, username: username.map(str::to_string), password: password.map(str::to_string) })
    }
}

/// Контроллер со своими тегами.
#[derive(Debug, Clone)]
pub struct Controller {
    /// PK контроллера в БД шлюза (0 — работа без БД).
    pub id: i64,
    /// Уникальное имя контроллера (ключ строки в таблице `controllers`).
    pub name: String,
    /// Адрес со схемой (`opc.tcp://`, `modbus://`, `pac://`).
    pub endpoint: String,
    /// Протокол контроллера, выведенный из схемы адреса.
    pub kind: ControllerKind,
    /// Включён ли контроллер.
    pub enabled: bool,
    /// Только включённые теги.
    pub tags: Vec<Arc<Tag>>,
    /// Защита соединения (только OPC UA).
    pub opc_security: OpcSecurity,
}

impl Controller {
    /// Контроллер из записи YAML; `None`, если схема адреса неизвестна. Берутся только включённые теги, каждому присваивается слот.
    pub fn from_config(cfg: &ServerConfig) -> Option<Self> {
        Some(Controller {
            id: 0,
            name: cfg.name.clone(),
            endpoint: cfg.endpoint.clone(),
            kind: ControllerKind::from_endpoint(&cfg.endpoint)?,
            enabled: cfg.enabled,
            opc_security: cfg.opc_security().unwrap_or_default(),
            tags: cfg
                .tags
                .iter()
                .filter(|t| t.enabled)
                .enumerate()
                .map(|(slot, t)| Arc::new(Tag { slot, ..Tag::from_config(t) }))
                .collect(),
        })
    }

    /// Период опроса: минимальный pollingRate тегов, не чаще 100 мс. (Java-шлюз брал
    /// min с 1000 мс и опрашивал раз в секунду при pollingRate 2000.)
    pub fn cycle_period_ms(&self) -> u64 {
        self.tags.iter().map(|t| t.polling_rate_ms).filter(|&r| r > 0).min().unwrap_or(1000).max(100)
    }
}

// ------------------------------------------------------------------- типы данных --

/// Имя типа тега начинается с префикса (INT покрывает INT16/INT32/INTEGER).
fn has_prefix(data_type: &str, prefix: &str) -> bool {
    data_type.trim().to_ascii_uppercase().starts_with(prefix)
}

/// Логический ли тип (`BOOL…`).
pub fn is_bool(data_type: &str) -> bool {
    has_prefix(data_type, "BOOL")
}

/// Целый ли тип (`INT…`: INT16, INT32, INTEGER).
pub fn is_int(data_type: &str) -> bool {
    has_prefix(data_type, "INT")
}

/// Вещественный ли тип (`FLOAT…`, `REAL…`, `DOUBLE…`).
pub fn is_float(data_type: &str) -> bool {
    has_prefix(data_type, "FLOAT") || has_prefix(data_type, "REAL") || has_prefix(data_type, "DOUBLE")
}

/// Строковый ли тип (`STRING…`).
pub fn is_string(data_type: &str) -> bool {
    has_prefix(data_type, "STRING")
}

/// Значение тега. На проводе — типизированный JSON (число/bool/строка): монитор строит
/// график по числу. F32 сериализуется кратчайшим представлением float (64.7, а не
/// 64.69999694824219), как Jackson у Java-шлюза.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TagValue {
    /// Логическое значение (`true`/`false` на проводе).
    Bool(bool),
    /// Целое; все целые типы ПЛК приводятся к `i64`.
    Int(i64),
    /// Вещественное одинарной точности (OPC UA `Float`, Modbus `FLOAT`).
    F32(f32),
    /// Вещественное двойной точности.
    F64(f64),
    /// Строка.
    Text(String),
}

impl TagValue {
    /// Значение числом: bool — 0/1, строка — `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            TagValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            TagValue::Int(i) => Some(*i as f64),
            TagValue::F32(f) => Some(*f as f64),
            TagValue::F64(f) => Some(*f),
            TagValue::Text(_) => None,
        }
    }

    /// Число ли это (bool и строка — нет): только числа участвуют в зонах нечувствительности и алармах.
    pub fn is_numeric(&self) -> bool {
        matches!(self, TagValue::Int(_) | TagValue::F32(_) | TagValue::F64(_))
    }
}

impl std::fmt::Display for TagValue {
    /// Как на проводе: число/bool — JSON-литерал, строка — без кавычек.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagValue::Text(s) => f.write_str(s),
            other => f.write_str(&serde_json::to_string(other).unwrap_or_default()),
        }
    }
}

/// Качество отсчёта.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    /// Значение достоверно.
    Good,
    /// Значение недостоверно (обрыв, плохой статус узла); на проводе обычно `value = null`.
    Bad,
}

impl Quality {
    /// Имя качества на проводе: `GOOD` или `BAD`.
    pub fn as_str(self) -> &'static str {
        match self {
            Quality::Good => "GOOD",
            Quality::Bad => "BAD",
        }
    }
}

impl Serialize for Quality {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Момент времени на проводе — дробные epoch-секунды с наносекундами
/// (`1790233762.061208074`), байт-в-байт как Instant у Jackson в Java-шлюзе.
/// Монитор понимает и это, и ISO-8601.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub DateTime<Utc>);

impl Timestamp {
    /// Текущий момент.
    pub fn now() -> Self {
        Timestamp(Utc::now())
    }

    /// Метка текстом: `секунды.наносекунды` (девять знаков), как её читает монитор.
    pub fn epoch_seconds_text(&self) -> String {
        let mut out = Vec::with_capacity(32);
        self.write_epoch_seconds(&mut out);
        String::from_utf8(out).unwrap_or_default()
    }

    /// То же текстом в буфер, без выделений: `секунды.наносекунды` (девять знаков).
    pub fn write_epoch_seconds(&self, out: &mut Vec<u8>) {
        use std::io::Write;
        let _ = write!(out, "{}.{:09}", self.0.timestamp(), self.0.timestamp_subsec_nanos());
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let raw =
            serde_json::value::RawValue::from_string(self.epoch_seconds_text()).map_err(serde::ser::Error::custom)?;
        raw.serialize(s)
    }
}

/// Одно снятое значение тега.
#[derive(Debug, Clone)]
pub struct Reading {
    /// Тег, с которого снято значение.
    pub tag: Arc<Tag>,
    /// `None` — значение не снято (BAD).
    pub value: Option<TagValue>,
    /// Достоверность значения.
    pub quality: Quality,
    /// Момент снятия: время источника, если он его отдаёт (OPC UA), иначе момент чтения шлюзом.
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn value_serializes_typed() {
        assert_eq!(serde_json::to_string(&TagValue::F32(64.7)).unwrap(), "64.7");
        assert_eq!(serde_json::to_string(&TagValue::Int(1)).unwrap(), "1");
        assert_eq!(serde_json::to_string(&TagValue::Bool(true)).unwrap(), "true");
        assert_eq!(serde_json::to_string(&TagValue::Text("REC".into())).unwrap(), "\"REC\"");
    }

    #[test]
    fn timestamp_is_epoch_seconds_with_nanos() {
        let ts = Timestamp(Utc.timestamp_opt(1790233762, 61_208_074).unwrap());
        assert_eq!(serde_json::to_string(&ts).unwrap(), "1790233762.061208074");
    }

    #[test]
    fn data_types_by_prefix() {
        assert!(is_int("INT32") && is_int("int16") && is_int("INTEGER"));
        assert!(is_float("FLOAT") && is_float("REAL") && is_float("DOUBLE"));
        assert!(is_bool("BOOLEAN") && is_string("STRING"));
        assert!(!is_int("FLOAT"));
    }

    #[test]
    fn protocol_classification_matches_java() {
        assert_eq!(classify("pac", "pac:385", None), Protocol::Pac);
        assert_eq!(classify("modbus", "modbus:40001", Some(40001)), Protocol::Modbus);
        assert_eq!(classify("opcua", "ns=2;s=6", None), Protocol::OpcUa);
        assert_eq!(classify("", "", Some(40005)), Protocol::Modbus);
    }

    #[test]
    fn security_is_parsed_flexibly() {
        let p = |s: &str| OpcSecurity::parse(Some(s), None, None);
        assert_eq!(p("None").unwrap(), OpcSecurity::default());
        assert_eq!(OpcSecurity::parse(None, None, None).unwrap(), OpcSecurity::default());
        let s = p("Basic256Sha256").unwrap();
        assert_eq!(
            (s.policy, s.mode),
            (OpcPolicy::Basic256Sha256, OpcMode::SignAndEncrypt),
            "режим по умолчанию — шифрование"
        );
        assert_eq!(p("basic256sha256_sign").unwrap().mode, OpcMode::Sign);
        assert_eq!(p("Aes256_Sha256_RsaPss/SignAndEncrypt").unwrap().policy, OpcPolicy::Aes256Sha256RsaPss);
        assert_eq!(p("Aes128-Sha256-RsaOaep Sign").unwrap().policy, OpcPolicy::Aes128Sha256RsaOaep);
        assert!(p("Basic999").is_err(), "неизвестная политика — ошибка, а не молчаливое «без защиты»");
        assert!(p("None_Sign").is_err());
        assert!(p("Sign").is_err());
    }

    #[test]
    fn credentials_come_in_pairs_and_stay_out_of_debug() {
        assert!(OpcSecurity::parse(None, Some("u"), None).is_err());
        assert!(OpcSecurity::parse(None, None, Some("p")).is_err());
        let s = OpcSecurity::parse(Some("Basic256Sha256"), Some("operator"), Some("s3cret!")).unwrap();
        assert!(s.is_secure());
        let shown = format!("{s:?}");
        assert!(shown.contains("operator") && !shown.contains("s3cret"), "{shown}");
        assert_eq!(
            OpcSecurity::parse(None, Some(""), Some("")).unwrap().username,
            None,
            "пустые логин и пароль — не заданы"
        );
    }

    #[test]
    fn cycle_period_uses_polling_rate() {
        let mut t = TagConfig {
            name: "x".into(),
            node_id: "n".into(),
            channel_id: None,
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: None,
            data_type: "FLOAT".into(),
            polling_rate: 2000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: None,
            max_value: None,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
            history: None,
        };
        let mut s = ServerConfig {
            id: None,
            name: "c".into(),
            endpoint: "opc.tcp://h:4840".into(),
            security: None,
            username: None,
            password: None,
            enabled: true,
            tags: vec![t.clone()],
        };
        assert_eq!(Controller::from_config(&s).unwrap().cycle_period_ms(), 2000);
        t.polling_rate = 20;
        s.tags.push(t);
        assert_eq!(Controller::from_config(&s).unwrap().cycle_period_ms(), 100);
    }
}
