//! `simulator station-config`: конфигурация симулятора из базы каналов станции, все теги — по OPC UA.
//!
//! Источник истины по составу каналов — `config/stations/<станция>.yaml` (его генерирует
//! `tools/station_config.sh` из `.cdbx` монитора). Симулятор должен отдавать **ровно эти** каналы, с теми же
//! адресами узлов (`ns=2;s=<прибор>.<поле>`), что и OPC UA-фасад эмулятора мойки (`ptusa-opcua/`): тогда шлюзу
//! для обоих стендов нужен один и тот же файл станции.
//!
//! Правила сборки:
//! * тег на каждый канал базы (включая выключенные в `.cdbx`: сервер их отдаёт, шлюз просто не опрашивает);
//! * протокол — всегда `opcua`; адрес — `s=` из `nodeId` канала;
//! * если такой прибор и поле были в прежней (трёхпротокольной) конфигурации — оттуда берутся источник значения
//!   из архива, правила записи прошивки, единица и начальное значение: поведение, снятое с эмулятора мойки,
//!   не теряется. Остальные каналы получают нулевое значение своего типа и доступ по `writable` канала;
//! * Modbus и PAC в результат не попадают, поэтому их серверы симулятор не поднимает.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_yaml_ng::{Mapping, Value as Y};

use crate::plc::normalized;

/// Приставка `nodeId` OPC UA-канала станции: пространство имён симулятора всегда второе.
const NODE_PREFIX: &str = "ns=2;s=";

/// Ключи прежнего тега, которые пересобираются заново или относятся только к Modbus/шуму.
const DROPPED: &[&str] = &[
    "name",
    "address",
    "type",
    "protocol",
    "device",
    "field",
    "dev_type",
    "access",
    "initial",
    "modbus_address",
    "modbus_type",
    "noise_enabled",
    "drift_enabled",
];

/// Станция: файл `config/stations/*.yaml` (нужны только контроллеры OPC UA и их теги).
#[derive(Deserialize)]
struct StationFile {
    opcua: StationOpcua,
}

#[derive(Deserialize)]
struct StationOpcua {
    servers: Vec<StationServer>,
}

#[derive(Deserialize)]
struct StationServer {
    tags: Vec<StationTag>,
}

/// Канал станции.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StationTag {
    node_id: String,
    device_name: String,
    field_name: String,
    device_type: String,
    data_type: String,
    #[serde(default)]
    writable: bool,
    #[serde(default = "yes")]
    enabled: bool,
}

fn yes() -> bool {
    true
}

/// Что получилось при сборке: для сводки и тестов.
#[derive(Debug, Default, PartialEq)]
pub struct Report {
    /// Каналов станции (теги результата).
    pub channels: usize,
    /// Из них включённых в базе каналов (опрашиваются шлюзом).
    pub enabled: usize,
    /// Каналов, перенесённых из прежней конфигурации с архивом и правилами прошивки.
    pub inherited: usize,
    /// Каналов без прежнего описания: значение статическое.
    pub fresh: usize,
    /// Условий записи, которые пришлось убрать: поле-условие в станции не существует.
    pub dropped_requires: usize,
    /// Тегов прежней конфигурации, которых нет в базе каналов (в результат не попадают).
    pub legacy_unused: usize,
}

/// Тип симулятора по типу канала станции.
fn sim_type(data_type: &str) -> Result<&'static str> {
    Ok(match data_type {
        "FLOAT" => "float",
        "INT32" => "int",
        "STRING" => "string",
        "BOOL" => "bool",
        "BYTE" => "byte",
        other => bail!("неизвестный dataType канала: {other:?}"),
    })
}

/// Нулевое значение типа (для канала без прежнего описания).
fn zero(sim_type: &str) -> Y {
    match sim_type {
        "float" => Y::Number(serde_yaml_ng::Number::from(0.0)),
        "string" => Y::String(String::new()),
        "bool" => Y::Bool(false),
        _ => Y::Number(0.into()),
    }
}

/// Скаляр или вложенная структура в строчной (flow) записи YAML: строки в двойных кавычках.
fn flow(v: &Y) -> String {
    match v {
        Y::Null => "null".into(),
        Y::Bool(b) => b.to_string(),
        Y::Number(n) => n.to_string(),
        Y::String(s) => {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        }
        Y::Sequence(items) => format!("[{}]", items.iter().map(flow).collect::<Vec<_>>().join(", ")),
        Y::Mapping(m) => {
            format!("{{{}}}", m.iter().map(|(k, v)| format!("{}: {}", flow(k), flow(v))).collect::<Vec<_>>().join(", "))
        }
        Y::Tagged(t) => flow(&t.value),
    }
}

/// Прежние теги по паре (прибор, поле без пробелов) и вся прежняя конфигурация (реплей, период цикла).
struct Legacy {
    tags: HashMap<(String, String), Mapping>,
    root: Mapping,
}

fn str_of(m: &Mapping, key: &str) -> Option<String> {
    match m.get(key)? {
        Y::String(s) => Some(s.clone()),
        Y::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

impl Legacy {
    fn parse(yaml: &str) -> Result<Self> {
        let root: Mapping = serde_yaml_ng::from_str(yaml).context("прежняя конфигурация не разобрана")?;
        let mut tags = HashMap::new();
        let blocks = root
            .get("plc")
            .and_then(|p| p.get("data_blocks"))
            .and_then(Y::as_sequence)
            .context("в прежней конфигурации нет plc.data_blocks")?;
        for block in blocks {
            for tag in block.get("tags").and_then(Y::as_sequence).into_iter().flatten() {
                let Some(m) = tag.as_mapping() else { continue };
                if let (Some(d), Some(f)) = (str_of(m, "device"), str_of(m, "field")) {
                    tags.entry((d, normalized(&f))).or_insert_with(|| m.clone());
                }
            }
        }
        Ok(Legacy { tags, root })
    }
}

/// Одна запись `tags:` в строчной записи.
fn tag_line(pairs: &[(String, Y)]) -> String {
    let body = pairs.iter().map(|(k, v)| format!("{k}: {}", flow(v))).collect::<Vec<_>>().join(", ");
    format!("        - {{{body}}}")
}

/// Собрать текст конфигурации симулятора. Чистая функция: файлы читает [`run`].
pub fn build(station_yaml: &str, legacy_yaml: &str, source: &str) -> Result<(String, Report)> {
    let station: StationFile = serde_yaml_ng::from_str(station_yaml).context("файл станции не разобран")?;
    let legacy = Legacy::parse(legacy_yaml)?;
    let channels: Vec<&StationTag> = station.opcua.servers.iter().flat_map(|s| &s.tags).collect();

    let mut report = Report { channels: channels.len(), ..Report::default() };
    let known: HashSet<(String, String)> =
        channels.iter().map(|c| (c.device_name.clone(), normalized(&c.field_name))).collect();
    let mut used: HashSet<(String, String)> = HashSet::new();
    let mut lines = Vec::with_capacity(channels.len());
    for ch in &channels {
        let address = ch
            .node_id
            .strip_prefix(NODE_PREFIX)
            .with_context(|| format!("nodeId {:?}: ожидается {NODE_PREFIX}<прибор>.<поле>", ch.node_id))?;
        let ty = sim_type(&ch.data_type)?;
        let key = (ch.device_name.clone(), normalized(&ch.field_name));
        let old = legacy.tags.get(&key);
        if ch.enabled {
            report.enabled += 1;
        }
        let access = match old.and_then(|m| str_of(m, "access")) {
            Some(a) => a,
            None if ch.writable => "RW".into(),
            None => "RO".into(),
        };
        let mut pairs: Vec<(String, Y)> = vec![
            ("name".into(), Y::String(address.into())),
            ("address".into(), Y::String(address.into())),
            ("type".into(), Y::String(ty.into())),
            ("protocol".into(), Y::String("opcua".into())),
            ("device".into(), Y::String(ch.device_name.clone())),
            ("field".into(), Y::String(ch.field_name.clone())),
            ("dev_type".into(), Y::String(ch.device_type.clone())),
            ("access".into(), Y::String(access)),
            ("initial".into(), old.and_then(|m| m.get("initial")).cloned().unwrap_or_else(|| zero(ty))),
        ];
        if let Some(old) = old {
            report.inherited += 1;
            used.insert(key);
            let replays = str_of(old, "generator").as_deref() == Some("replay");
            for (k, v) in old {
                let Some(k) = k.as_str() else { continue };
                if DROPPED.contains(&k) {
                    continue;
                }
                if k == "write_requires" && !requires_exists(v, &ch.device_name, &known) {
                    report.dropped_requires += 1;
                    continue;
                }
                pairs.push((k.into(), v.clone()));
            }
            // Источник по умолчанию — прежний адрес тега (числовой id канала): закрепляем его явно.
            if replays
                && old.get("replay_source").is_none()
                && let Some(a) = str_of(old, "address").or_else(|| str_of(old, "name"))
            {
                pairs.push(("replay_source".into(), Y::String(a)));
            }
        } else {
            report.fresh += 1;
        }
        lines.push(tag_line(&pairs));
    }
    report.legacy_unused = legacy.tags.keys().filter(|k| !used.contains(*k)).count();

    let plc = legacy.root.get("plc").and_then(Y::as_mapping).context("plc")?;
    let text = |k: &str, d: &str| str_of(plc, k).unwrap_or_else(|| d.into());
    let replay = legacy.root.get("replay").map(flow).unwrap_or_else(|| "{enabled: false}".into());
    let mut out = String::new();
    out.push_str(&format!(
        "# ============================================================================\n\
         # Симулятор станции: ВСЕ каналы базы каналов — по OPC UA (Modbus и PAC не поднимаются).\n\
         # СГЕНЕРИРОВАНО `simulator station-config` из {source} и прежней трёхпротокольной\n\
         # конфигурации (replay_config.yaml) — не править руками: поправьте источник и пересоберите.\n\
         #   каналов {}, включено в базе {}; с прежним описанием (архив, правила прошивки) {}, новых (статических) {}\n\
         # Адрес узла — ns=2;s=<прибор>.<поле>, как у OPC UA-фасада эмулятора мойки (ptusa-opcua/): шлюзу нужен\n\
         # один и тот же config/stations/*.yaml. Режим трёх протоколов (тесты) — replay_config.yaml.\n\
         # ============================================================================\n",
        report.channels, report.enabled, report.inherited, report.fresh
    ));
    out.push_str(&format!(
        "plc:\n  id: {}\n  name: {}\n  endpoint: {}\n  update_rate: {}\n  data_blocks:\n    - db_number: 1\n      name: {}\n      tags:\n",
        text("id", "STATION"),
        flow(&Y::String(format!("{} OPC UA", text("id", "STATION")))),
        text("endpoint", "opc.tcp://0.0.0.0:4840"),
        text("update_rate", "0.5"),
        text("id", "STATION").replace('-', "_"),
    ));
    for line in &lines {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str(&format!("replay: {replay}\n"));
    Ok((out, report))
}

/// Условие записи ссылается на поле, которое есть среди каналов станции.
fn requires_exists(requires: &Y, own_device: &str, known: &HashSet<(String, String)>) -> bool {
    let Some(m) = requires.as_mapping() else { return false };
    let device = str_of(m, "device").unwrap_or_else(|| own_device.into());
    str_of(m, "field").is_some_and(|f| known.contains(&(device, normalized(&f))))
}

/// `simulator station-config <станция.yaml> <прежняя.yaml> <выход.yaml>`.
pub fn run(station: &Path, legacy: &Path, out: &Path) -> Result<()> {
    let read = |p: &Path| std::fs::read_to_string(p).with_context(|| p.display().to_string());
    let source = station.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let (text, r) = build(&read(station)?, &read(legacy)?, &format!("config/stations/{source}"))?;
    std::fs::write(out, text).with_context(|| out.display().to_string())?;
    println!(
        "{}: каналов {} (включено {}), с прежним описанием {}, новых {}; \
         условий записи убрано {}, прежних тегов вне базы каналов {}",
        out.display(),
        r.channels,
        r.enabled,
        r.inherited,
        r.fresh,
        r.dropped_requires,
        r.legacy_unused
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::plc::Plc;

    const STATION: &str = r#"
opcua:
  servers:
    - id: s
      name: s
      endpoint: "opc.tcp://${PLC_HOST}:4840"
      enabled: true
      tags:
        - {name: "A.V1.ST", nodeId: "ns=2;s=V1.ST", deviceName: "V1", fieldName: "ST", deviceType: "V", protocol: opcua, dataType: INT32, pollingRate: 1000, enabled: true, writable: true}
        - {name: "A.V1.M", nodeId: "ns=2;s=V1.M", deviceName: "V1", fieldName: "M", deviceType: "V", protocol: opcua, dataType: INT32, pollingRate: 1000, enabled: true, writable: true}
        - {name: "A.AI1.V", nodeId: "ns=2;s=AI1.V", deviceName: "AI1", fieldName: "V", deviceType: "AI", protocol: opcua, dataType: FLOAT, pollingRate: 1000, enabled: false, writable: false}
        - {name: "A.O.PAR[ 1 ]", nodeId: "ns=2;s=O.PAR[1]", deviceName: "O", fieldName: "PAR[ 1 ]", deviceType: "OBJECT", protocol: opcua, dataType: STRING, pollingRate: 1000, enabled: true, writable: true}
"#;

    const LEGACY: &str = r#"
plc: {id: X-1, name: "X One", endpoint: "opc.tcp://0.0.0.0:4840", update_rate: 0.5, data_blocks: [
  {db_number: 1, name: X, tags: [
    {name: "5", address: "5", type: int, protocol: modbus, modbus_address: 3, modbus_type: int16, device: V1, field: ST, dev_type: V, access: RW, initial: 0, write_requires: {field: M, equals: 1}, write_state: true},
    {name: "6", address: "6", type: int, protocol: opcua, device: V1, field: M, dev_type: V, access: RW, initial: 1},
    {name: "7", address: "7", type: float, protocol: opcua, device: AI1, field: V, dev_type: AI, access: RO, initial: 0.0, generator: replay},
    {name: "8", address: "8", type: float, protocol: opcua, device: GONE, field: V, dev_type: AI, access: RO, initial: 0.0},
  ]}]}
replay: {enabled: true, data_path: data/archive_replay.bin.gz, speed: 1.0, loop: true}
"#;

    #[test]
    fn every_channel_becomes_an_opcua_tag_with_the_node_address() {
        let (text, r) = build(STATION, LEGACY, "config/stations/t.yaml").unwrap();
        assert_eq!(
            r,
            Report { channels: 4, enabled: 3, inherited: 3, fresh: 1, dropped_requires: 0, legacy_unused: 1 }
        );
        let cfg: config::Config = serde_yaml_ng::from_str(&text).unwrap();
        let tags: Vec<_> = cfg.plc.data_blocks.iter().flat_map(|b| &b.tags).collect();
        assert_eq!(tags.len(), 4);
        assert!(tags.iter().all(|t| t.protocol.as_deref() == Some("opcua") && t.modbus_address.is_none()));
        let by = |a: &str| tags.iter().find(|t| t.address() == a).unwrap();
        assert_eq!(by("V1.ST").data_type, "int");
        assert_eq!(
            by("O.PAR[1]").field.as_deref(),
            Some("PAR[ 1 ]"),
            "поле — как в базе каналов, адрес — без пробелов"
        );
        // Прежнее поведение прошивки переносится: условие записи и «состояние».
        assert!(by("V1.ST").write_state && by("V1.ST").write_requires.is_some());
        // Источник реплея закреплён прежним адресом, потому что у нового адреса его нет.
        assert_eq!(by("AI1.V").replay_source.as_deref(), Some("7"));
        // Канал без прежнего описания: ноль своего типа и доступ по writable канала.
        assert_eq!(by("O.PAR[1]").access.as_deref(), Some("RW"));
        assert!(Plc::new(&cfg, None).is_ok());
    }

    #[test]
    fn write_requires_on_a_missing_field_is_dropped() {
        let station = STATION
            .replace("\"V1.M\"", "\"V1.X\"")
            .replace("ns=2;s=V1.M", "ns=2;s=V1.X")
            .replace("fieldName: \"M\"", "fieldName: \"X\"");
        let (text, r) = build(&station, LEGACY, "t").unwrap();
        assert_eq!(r.dropped_requires, 1);
        let cfg: config::Config = serde_yaml_ng::from_str(&text).unwrap();
        assert!(Plc::new(&cfg, None).is_ok(), "без условия-сироты ПЛК собирается");
    }

    #[test]
    fn unknown_channel_type_or_node_format_is_an_error() {
        assert!(build(&STATION.replace("INT32", "INT64"), LEGACY, "t").is_err());
        assert!(build(&STATION.replace("ns=2;s=V1.ST", "ns=3;s=V1.ST"), LEGACY, "t").is_err());
    }

    /// Файл в репозитории — ровно то, что даёт генератор из базы каналов станции: ни один канал не потерян,
    /// все по OPC UA, Modbus и PAC нет. Расхождение — перегенерировать (`simulator station-config`).
    #[test]
    fn repository_config_covers_the_whole_channel_database_over_opcua() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let station = std::fs::read_to_string(root.join("../config/stations/BN1_MCA1.yaml")).unwrap();
        let legacy = std::fs::read_to_string(root.join("config/replay_config.yaml")).unwrap();
        let committed = std::fs::read_to_string(root.join("config/station_opcua.yaml")).unwrap();
        let (fresh, r) = build(&station, &legacy, "config/stations/BN1_MCA1.yaml").unwrap();
        assert_eq!(committed, fresh, "station_opcua.yaml устарел: `simulator station-config`");
        assert_eq!((r.channels, r.enabled), (2647, 1834));

        let cfg = config::load(&root.join("config/station_opcua.yaml")).unwrap();
        let tags: Vec<_> = cfg.plc.data_blocks.iter().flat_map(|b| &b.tags).collect();
        assert_eq!(tags.len(), 2647);
        assert!(tags.iter().all(|t| t.protocol.as_deref() == Some("opcua")));
        let plc = Plc::new(&cfg, None).unwrap();
        let st = plc.stats();
        assert_eq!((st.opcua, st.modbus, st.pac), (2647, 0, 0));
    }
}
