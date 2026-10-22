//! `controllers.yaml`: контроллеры и теги, подстановка `${VAR:default}`, проверка станции.

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Корень `controllers.yaml`.
#[derive(Debug, Deserialize)]
pub struct ControllersFile {
    /// Секция `opcua` — название осталось от Java-шлюза; в ней контроллеры любого протокола.
    pub opcua: ControllersSection,
}

/// Секция `opcua`.
#[derive(Debug, Deserialize)]
pub struct ControllersSection {
    /// Контроллеры станции.
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
}

/// Один контроллер из YAML (OPC UA / Modbus / PAC — по схеме endpoint).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    /// Идентификатор из YAML; не используется, разбирается для совместимости формата.
    #[allow(dead_code)] // id разбирается для совместимости формата с Java-шлюзом
    pub id: Option<String>,
    /// Уникальное имя контроллера; по нему он связывается с таблицей `controllers`.
    pub name: String,
    /// Адрес со схемой: `opc.tcp://…`, `modbus://…`, `pac://…` — по ней выбирается протокол.
    pub endpoint: String,
    /// Политика и режим канала OPC UA: `None` (по умолчанию), `Basic256Sha256`, `Aes256_Sha256_RsaPss_Sign`…
    pub security: Option<String>,
    /// Пользователь OPC UA (вместе с `password`; пароль удобно держать в `${PLC_PASSWORD}`).
    pub username: Option<String>,
    /// Пароль пользователя OPC UA; в файл лучше писать `${ПЕРЕМЕННАЯ}`.
    pub password: Option<String>,
    /// Как получать значения OPC UA: `poll` (по умолчанию — Read каждый цикл) или `subscription` (подписки).
    pub acquisition: Option<String>,
    /// Включён ли контроллер; по умолчанию `false` — выключенный не опрашивается и не проверяется.
    #[serde(default)]
    pub enabled: bool,
    /// Чем контроллер называет себя; сверяется при каждом подключении (см. [`IdentityConfig`]).
    pub identity: Option<IdentityConfig>,
    /// Теги контроллера.
    #[serde(default)]
    pub tags: Vec<TagConfig>,
}

/// Как опознать контроллер при подключении: имя, которое он называет сам. Не совпало — шлюз закрывает соединение, теги
/// идут с качеством `BAD`, команды в этот контроллер не уходят.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityConfig {
    /// PAC: `PAC_name` из ответа на `GET_INFO_ON_CONNECT`. OPC UA: ожидаемое значение узла `nodeId` (у фасада ptusa-opcua
    /// и симулятора — `SYSTEM.PAC_NAME`).
    pub pac_name: Option<String>,
    /// OPC UA: узел, значение которого — имя контроллера (с `pacName`). Адрес `ns=2;s=…` или `nsu=<URI>;s=…`.
    pub node_id: Option<String>,
    /// OPC UA: `ApplicationUri` сервера (первый элемент `Server.ServerArray`).
    pub application_uri: Option<String>,
}

impl IdentityConfig {
    /// Проверка при старте: пустое или неподходящее протоколу правило — ошибка (иначе оператор считал бы, что сверка идёт).
    fn validate(&self, name: &str, endpoint: &str) -> Result<()> {
        let fields =
            [("pacName", &self.pac_name), ("nodeId", &self.node_id), ("applicationUri", &self.application_uri)];
        for (field, value) in fields {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                bail!("контроллер {name:?}: identity.{field} пуст — задайте значение или уберите поле");
            }
        }
        if endpoint.starts_with("opc.tcp://") {
            if self.pac_name.is_some() != self.node_id.is_some() {
                bail!(
                    "контроллер {name:?}: identity.pacName и identity.nodeId для OPC UA задаются вместе (узел и его ожидаемое значение)"
                );
            }
            if self.pac_name.is_none() && self.application_uri.is_none() {
                bail!("контроллер {name:?}: identity пуст — задайте pacName с nodeId или applicationUri");
            }
        } else if endpoint.starts_with("pac://") {
            if self.node_id.is_some() || self.application_uri.is_some() {
                bail!(
                    "контроллер {name:?}: у PAC сверяется только identity.pacName (nodeId и applicationUri — для OPC UA)"
                );
            }
            if self.pac_name.is_none() {
                bail!("контроллер {name:?}: identity пуст — задайте pacName");
            }
        } else {
            bail!(
                "контроллер {name:?}: identity поддерживается для PAC и OPC UA (endpoint {endpoint}); у Modbus прибор себя не называет"
            );
        }
        Ok(())
    }
}

impl ServerConfig {
    /// Защита соединения с OPC UA-контроллером из `security`/`username`/`password`.
    pub fn opc_security(&self) -> Result<crate::model::OpcSecurity> {
        crate::model::OpcSecurity::parse(self.security.as_deref(), self.username.as_deref(), self.password.as_deref())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Проверка при старте: молча проигнорированные настройки защиты — дыра, поэтому ошибка в `security`
    /// или защита у протокола, который её не поддерживает (Modbus, PAC), останавливают запуск.
    pub fn validate(&self) -> Result<()> {
        let security = self.opc_security().with_context(|| format!("контроллер {:?}", self.name))?;
        let is_opcua = self.endpoint.starts_with("opc.tcp://");
        if !is_opcua && (security.is_secure() || security.username.is_some()) {
            bail!(
                "контроллер {:?}: security/username/password поддерживаются только для OPC UA (endpoint {}); \
                 для Modbus и PAC защиты на уровне протокола нет — не задавайте их, чтобы не думать, что канал защищён",
                self.name,
                self.endpoint
            );
        }
        let acquisition = crate::model::Acquisition::parse(self.acquisition.as_deref())
            .map_err(|e| anyhow::anyhow!("контроллер {:?}: {e}", self.name))?;
        if !is_opcua && acquisition == crate::model::Acquisition::Subscription {
            bail!(
                "контроллер {:?}: acquisition: subscription поддерживается только для OPC UA (endpoint {})",
                self.name,
                self.endpoint
            );
        }
        if let Some(identity) = &self.identity {
            identity.validate(&self.name, &self.endpoint)?;
        }
        Ok(())
    }
}

/// Один тег/канал из YAML — поля как у TagConfig Java-шлюза.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagConfig {
    /// Полный путь канала: ключ сообщения Kafka и адрес команды монитора; уникален на всю станцию.
    pub name: String,
    /// Адрес в контроллере (OPC UA `ns=2;s=6`; у PAC и Modbus — уникальный ключ); уникален в пределах контроллера.
    pub node_id: String,
    /// Номер канала в базе монитора.
    pub channel_id: Option<i64>,
    /// Прибор PAC; у других протоколов — метаданные.
    pub device_name: Option<String>,
    /// Поле прибора PAC (`ST`, `RT_PAR_F[12]`); допустимы буквы, цифры, `_`, `[`, `]`, `.`.
    pub field_name: Option<String>,
    /// Тип прибора (метаданные).
    pub device_type: Option<String>,
    /// `opcua` (по умолчанию), `modbus` или `pac`; должен совпасть с протоколом контроллера.
    pub protocol: Option<String>,
    /// Тип данных: `BOOL…`, `INT…`, `FLOAT…`/`REAL…`/`DOUBLE…`, `STRING…`.
    pub data_type: String,
    /// Желаемый период опроса, мс; период цикла контроллера — наименьший положительный из его тегов.
    #[serde(default)]
    pub polling_rate: u64,
    /// Включён ли тег; по умолчанию `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Можно ли писать командой; Modbus не пишется никогда.
    #[serde(default)]
    pub writable: bool,
    /// Единица измерения.
    pub unit: Option<String>,
    /// Нижний предел для аларма.
    pub min_value: Option<f64>,
    /// Верхний предел для аларма.
    pub max_value: Option<f64>,
    /// Наименьшее значение, которое можно записать командой (включительно); меньшее — `REJECTED_OUT_OF_RANGE`.
    pub write_min: Option<f64>,
    /// Наибольшее значение, которое можно записать командой (включительно).
    pub write_max: Option<f64>,
    /// Адрес holding-регистра 4xxxx (40001…105536); обязателен у Modbus-тега.
    pub modbus_address: Option<i32>,
    /// Тип регистра (справочно).
    pub modbus_type: Option<String>,
    /// Modbus Unit ID (по умолчанию 1); один на контроллер.
    pub modbus_unit_id: Option<u8>,
    /// Переопределение фильтра истории для тега; не заданные поля — умолчания `gateway.history.*`.
    pub history: Option<HistoryConfig>,
}

/// Блок `history:` тега: `{deadband: 0.5, deadbandPercent: 1, minIntervalMs: 60000, maxIntervalMs: 600000}`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryConfig {
    /// Абсолютная зона истории тега; не задано — `GATEWAY_HISTORY_DEADBAND`.
    pub deadband: Option<f64>,
    /// Относительная зона истории тега, %.
    pub deadband_percent: Option<f64>,
    /// Минимальный интервал между точками истории тега, мс.
    pub min_interval_ms: Option<u64>,
    /// «Пульс» истории тега, мс: точка раз в столько, даже если значение стоит; 0 — выключен.
    pub max_interval_ms: Option<u64>,
}

/// Прочитать controllers.yaml с подстановкой `${VAR:default}` из окружения.
pub fn load_controllers(path: &std::path::Path) -> Result<Vec<ServerConfig>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("не прочитан {}", path.display()))?;
    let expanded = expand_placeholders(&raw, |k| std::env::var(k).ok())
        .with_context(|| format!("{}: не заданы обязательные переменные", path.display()))?;
    parse_controllers(&expanded)
}

/// Разобрать YAML и проверить станцию целиком ([`validate_station`] и проверки каждого включённого контроллера). Подстановки `${…}` к этому моменту уже выполнены.
pub fn parse_controllers(yaml: &str) -> Result<Vec<ServerConfig>> {
    let file: ControllersFile = serde_yaml_ng::from_str(yaml).context("controllers.yaml не разобран")?;
    for server in file.opcua.servers.iter().filter(|s| s.enabled) {
        server.validate()?;
    }
    validate_station(&file.opcua.servers)?;
    Ok(file.opcua.servers)
}

/// Проверка конфигурации в целом (включённые контроллеры): имена контроллеров и тегов уникальны, протокол
/// контроллера известен. Имя тега — ключ сообщения Kafka и адрес команды: два тега с одним именем делали бы
/// телеметрию и команды неоднозначными, а контроллер с неизвестным протоколом молча не опрашивался бы.
pub fn validate_station(servers: &[ServerConfig]) -> Result<()> {
    let mut problems: Vec<String> = Vec::new();
    let mut controller_names = std::collections::HashSet::new();
    let mut tag_owner: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut duplicates: Vec<String> = Vec::new();
    for server in servers.iter().filter(|s| s.enabled) {
        if !controller_names.insert(server.name.as_str()) {
            problems.push(format!("контроллер {:?} описан дважды", server.name));
        }
        if crate::model::ControllerKind::from_endpoint(&server.endpoint).is_none() {
            problems.push(format!(
                "контроллер {:?}: неизвестный протокол в endpoint {:?} (ожидается opc.tcp://, modbus:// или pac://)",
                server.name, server.endpoint
            ));
        }
        for tag in server.tags.iter().filter(|t| t.enabled) {
            if tag.name.trim().is_empty() {
                problems.push(format!("контроллер {:?}: у включённого тега пустое имя", server.name));
            } else if let Some(first) = tag_owner.insert(tag.name.as_str(), server.name.as_str()) {
                duplicates.push(format!("{} ({first} и {})", tag.name, server.name));
            }
        }
        check_tags(server, &mut problems);
    }
    if !duplicates.is_empty() {
        problems.push(format!(
            "имена тегов должны быть уникальны (это ключ Kafka и адрес команды), повторяются {}: {}{}",
            duplicates.len(),
            duplicates.iter().take(5).cloned().collect::<Vec<_>>().join("; "),
            if duplicates.len() > 5 { "; …" } else { "" }
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        bail!("конфигурация контроллеров не принята:\n  - {}", problems.join("\n  - "))
    }
}

/// Допустимый адрес поля PAC: имя прибора или поля, элемент массива `RT_PAR_F[12]`, вложенность `PAR_MAIN[1].P`.
/// Адрес подставляется в Lua-текст команды `set_cmd`, поэтому кавычки, пробелы и управляющие символы запрещены.
fn is_pac_identifier(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '[' | ']' | '.'))
}

/// Проверки тегов одного включённого контроллера: молча не работающий тег хуже отказа запуска.
fn check_tags(server: &ServerConfig, problems: &mut Vec<String>) {
    use crate::model::{ControllerKind, Protocol, classify};
    let Some(kind) = ControllerKind::from_endpoint(&server.endpoint) else { return };
    let (mut node_ids, mut modbus_units) = (std::collections::HashSet::new(), std::collections::BTreeSet::new());
    for tag in server.tags.iter().filter(|t| t.enabled && !t.name.trim().is_empty()) {
        let who = format!("контроллер {:?}, тег {:?}", server.name, tag.name);
        let protocol = classify(tag.protocol.as_deref().unwrap_or("opcua"), &tag.node_id, tag.modbus_address);
        // Контроллер опрашивает только теги своего протокола; остальные никогда не читались бы.
        if protocol != kind.protocol() {
            problems.push(format!(
                "{who}: протокол тега {protocol:?} не совпадает с протоколом контроллера {:?} — тег не опрашивался бы",
                kind.protocol()
            ));
            continue;
        }
        // Адрес узла — ключ синхронизации с БД: два тега с одним адресом получили бы один id и общую историю.
        if !node_ids.insert(tag.node_id.as_str()) {
            problems.push(format!("{who}: nodeId {:?} уже занят другим тегом контроллера", tag.node_id));
        }
        check_write_limits(tag, &who, problems);
        match protocol {
            Protocol::Pac => {
                for (what, value) in [("deviceName", &tag.device_name), ("fieldName", &tag.field_name)] {
                    match value.as_deref() {
                        Some(v) if is_pac_identifier(v) => {}
                        Some(v) => problems.push(format!(
                            "{who}: {what} {v:?} содержит недопустимые символы (допустимы буквы, цифры, _ [ ] .)"
                        )),
                        None => problems.push(format!("{who}: для PAC-тега не задан {what}")),
                    }
                }
            }
            Protocol::Modbus => {
                // Holding-регистры 40001…105536 (адрес в протоколе — 16 бит); за границей адрес молча обрезался бы.
                let wide = crate::model::is_float(&tag.data_type);
                match tag.modbus_address {
                    Some(a) if (40001..=105536 - i32::from(wide)).contains(&a) => {}
                    Some(a) => problems.push(format!("{who}: modbusAddress {a} вне диапазона 40001…105536")),
                    None => problems.push(format!("{who}: для Modbus-тега не задан modbusAddress")),
                }
                modbus_units.insert(tag.modbus_unit_id.unwrap_or(1));
            }
            Protocol::OpcUa => {}
        }
    }
    if modbus_units.len() > 1 {
        problems.push(format!(
            "контроллер {:?}: у тегов разные modbusUnitId {modbus_units:?} — шлюз читает контроллер с одним",
            server.name
        ));
    }
}

/// Пределы записи тега: конечные числа, нижний не больше верхнего, тег числовой. Пределы у строки или логического
/// тега не проверялись бы — отказ запуска лучше видимости защиты.
fn check_write_limits(tag: &TagConfig, who: &str, problems: &mut Vec<String>) {
    if tag.write_min.is_none() && tag.write_max.is_none() {
        return;
    }
    if !crate::model::is_int(&tag.data_type) && !crate::model::is_float(&tag.data_type) {
        problems.push(format!(
            "{who}: writeMin/writeMax задаются только у числового тега (INT…, FLOAT…, REAL…, DOUBLE…), dataType {:?}",
            tag.data_type
        ));
        return;
    }
    for (what, value) in [("writeMin", tag.write_min), ("writeMax", tag.write_max)] {
        if let Some(v) = value.filter(|v| !v.is_finite()) {
            problems.push(format!("{who}: {what} {v} — не конечное число"));
            return;
        }
    }
    if let (Some(lo), Some(hi)) = (tag.write_min, tag.write_max)
        && lo > hi
    {
        problems.push(format!("{who}: writeMin {lo} больше writeMax {hi}"));
    }
}

/// Подстановка в стиле Spring: `${NAME:default}` — со значением по умолчанию (пустое `${NAME:}`
/// тоже допустимо); `${NAME}` без умолчания **обязательна** — без неё ошибка, а не пустая строка:
/// адрес контроллера `opc.tcp://:4840` шлюз молча не обнаружил бы до первой попытки подключения.
pub fn expand_placeholders(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut missing: Vec<String> = Vec::new();
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let expr = &after[..end];
                match expr.split_once(':') {
                    Some((name, default)) => out.push_str(&lookup(name).unwrap_or_else(|| default.to_string())),
                    None => match lookup(expr) {
                        Some(v) => out.push_str(&v),
                        None => missing.push(expr.to_string()),
                    },
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        bail!("задайте переменные окружения: {}", missing.join(", "));
    }
    Ok(out)
}
