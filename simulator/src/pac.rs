//! PAC-контроллер симулятора — сервер протокола driver-master (Savushkin/ptusa).
//!
//! Говорит на «родном» протоколе PAC так, как его видно у эмулятора ptusa 2026.4.2.1 (сборка
//! ptusa_main под ПК, проект BN1-МСА1): после accept — приветствие `PAC accept`, дальше кадры
//! `'s' + ServiceID + FrameType + pidx + BE16-длина + payload`, тело ОТВЕТА = zlib(Lua), статус
//! успеха 12. Версия протокола 104 (zlib + UTF-8); QuickLZ (легаси v102) не поддерживается.
//!
//! Сервер держит СНИМОК значений тегов (его обновляет цикл симулятора) и на запрос строит из него
//! Lua. Реализованные команды (device_communicator::CMD):
//!   GET_INFO_ON_CONNECT(10)  — версия протокола, имя PAC, CRC параметров (handshake);
//!   GET_DEVICES(100)         — объектная модель (устройства → поля);
//!   GET_DEVICES_STATES(101)  — снимок по приборам: `t={LINE1V0={M=0, ST=1}, ...}` (основной опрос);
//!   EXEC_DEVICE_COMMAND(102) — запись: Lua-команда `set_cmd` применяется к RW-тегу, в ответ код
//!                              результата LE16 (0 — применено, 1 — нет такого RW-поля);
//!   GET_PAC_ERRORS(103)      — список аварий ([`crate::alarms`]) Lua-текстом, как у прошивки;
//!   SET_PAC_ERROR_CMD(104)   — действие над аварией: строка `errors_manager:get_instance():set_cmd( cmd, t, n, a )`,
//!                              в ответ код LE16 (0 — принято, 1 — строка не распознана).
//! Остальное — пустой ответ со статусом успеха, как у ptusa.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use regex::Regex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::alarms::Alarms;
use crate::value::Value;

/// Версия протокола, которую сервер называет при handshake.
const PROTOCOL_VERSION: u32 = 104;
/// Команда: версия протокола и имя PAC.
pub const CMD_GET_INFO_ON_CONNECT: u8 = 10;
/// Команда: объектная модель (приборы и поля).
pub const CMD_GET_DEVICES: u8 = 100;
/// Команда: снимок состояния приборов.
pub const CMD_GET_DEVICES_STATES: u8 = 101;
/// Команда: выполнить команду прибора.
pub const CMD_EXEC_DEVICE_COMMAND: u8 = 102;
/// Команда: список аварий PAC.
pub const CMD_GET_PAC_ERRORS: u8 = 103;
/// Команда: действие над аварией (квитировать, подавить, снять подавление).
pub const CMD_SET_PAC_ERROR_CMD: u8 = 104;

/// Приветствие, которое сервер присылает сразу после подключения.
pub const BANNER: &[u8] = b"PAC accept";
/// Первый байт каждого кадра.
const NET_ID: u8 = b's';
/// Так помечает успешный ответ ptusa.
pub const STATUS_OK: u8 = 12;
/// Драйвер трактует `ответ[1] == 7` как ошибку.
const STATUS_ERROR: u8 = 7;
/// Длина заголовка запроса, байт.
const REQUEST_HEADER_LEN: usize = 6;
/// Длина ответа — 2 байта → сжатое тело ≤ 65535 байт.
const MAX_BODY: usize = 0xFFFF;

/// Код результата EXEC_DEVICE_COMMAND (LE16), как у ptusa: 0 — применено, 1 — нет.
pub const EXEC_APPLIED: [u8; 2] = [0, 0];
pub const EXEC_FAILED: [u8; 2] = [1, 0];

/// Поле прибора PAC.
#[derive(Debug, Clone)]
pub struct PacField {
    /// Имя поля (`ST`, `RT_PAR_F[12]`).
    pub field: String,
    /// node.id канала (ключ снимка).
    pub address: String,
}

/// Прибор PAC и его поля.
#[derive(Debug, Clone)]
pub struct PacDevice {
    /// Имя прибора (`LINE1V0`).
    pub device: String,
    /// Поля прибора.
    pub fields: Vec<PacField>,
}

/// Запись драйвера: (прибор, поле, индекс, значение) → применено ли.
pub type WriteFn = dyn Fn(&str, &str, Option<u32>, &Value) -> bool + Send + Sync;

/// Модель PAC: приборы, текущий снимок значений и функция записи, которой симулятор применяет команды драйвера.
pub struct PacModel {
    name: String,
    params_crc: u32,
    devices: Vec<PacDevice>,
    values: Mutex<HashMap<String, Value>>,
    on_write: Box<WriteFn>,
    alarms: Arc<Alarms>,
}

impl PacModel {
    /// Модель без значений и без аварий; снимок кладёт цикл симулятора.
    pub fn new(name: &str, devices: Vec<PacDevice>, on_write: Box<WriteFn>) -> Self {
        PacModel {
            name: name.to_string(),
            params_crc: 0,
            devices,
            values: Mutex::new(HashMap::new()),
            on_write,
            alarms: Arc::default(),
        }
    }

    /// Аварии берутся из общего списка ПЛК (тот же, что у OPC UA-сервера).
    pub fn with_alarms(self, alarms: Arc<Alarms>) -> Self {
        PacModel { alarms, ..self }
    }

    /// Обновить снимок значений (node_id → значение). Цикл симулятора зовёт на каждом шаге.
    pub fn update_snapshot(&self, values: HashMap<String, Value>) {
        *self.values.lock().expect("снимок") = values;
    }

    /// Диспетчер команды драйвера. Возвращает (статус, тело ДО сжатия).
    pub fn handle_command(&self, cmd: u8, payload: &[u8]) -> (u8, Vec<u8>) {
        match cmd {
            CMD_GET_INFO_ON_CONNECT => (STATUS_OK, self.build_info()),
            CMD_GET_DEVICES => (STATUS_OK, self.build_states()),
            CMD_GET_DEVICES_STATES => (STATUS_OK, self.build_states()),
            CMD_EXEC_DEVICE_COMMAND => {
                let applied = self.apply_command(payload.get(1..).unwrap_or_default());
                (STATUS_OK, if applied { EXEC_APPLIED } else { EXEC_FAILED }.to_vec())
            }
            CMD_GET_PAC_ERRORS => {
                let mut lua = self.alarms.lua_text().into_bytes();
                lua.push(0);
                (STATUS_OK, lua)
            }
            CMD_SET_PAC_ERROR_CMD => {
                let accepted = self.alarm_command(payload.get(1..).unwrap_or_default());
                (STATUS_OK, if accepted { EXEC_APPLIED } else { EXEC_FAILED }.to_vec())
            }
            other => {
                // ptusa на незнакомую команду отвечает пустым телом со статусом успеха.
                warn!("PAC: неизвестная команда {other}");
                (STATUS_OK, Vec::new())
            }
        }
    }

    /// Ответ на handshake: версия протокола, имя, контрольная сумма параметров (C-строка).
    fn build_info(&self) -> Vec<u8> {
        let name = self.name.replace('\\', "\\\\").replace('"', "\\\"");
        format!(
            "protocol_version = {PROTOCOL_VERSION}; PAC_name = \"{name}\"; is_reset_params = 0;params_CRC={};\n",
            self.params_crc
        )
        .into_bytes()
    }

    /// Формат ответов devices/states: 2 байта devices_request_id (LE; конфигурация не меняется —
    /// константа 1) + Lua-текст + завершающий нуль. Нуль нужен: драйвер старой SCADA (EasyDriver) разжимает ответ в
    /// общий буфер и исполняет его как C-строку (`luaL_dostring`), а без нуля за коротким ответом читается хвост
    /// прошлого длинного (снимок «Ошибка получения состояния объектов PAC … unfinished string»).
    fn with_request_id(lua: String) -> Vec<u8> {
        let mut out = 1u16.to_le_bytes().to_vec();
        out.extend(lua.into_bytes());
        out.push(0);
        out
    }

    /// Снимок по приборам, как device_manager::save_device у ptusa:
    /// `t={ LINE1V0={M=0, ST=1}, OBJECT1={CMD=0, RT_PAR_F={[12]=0.5}}, ... }`. Канал читают как
    /// `t[прибор][поле]`; поле-массив `RT_PAR_F[12]` уходит элементом таблицы `RT_PAR_F`.
    fn build_states(&self) -> Vec<u8> {
        let values = self.values.lock().expect("снимок");
        let array_field = array_field_re();
        let mut lua = String::from("t=\n\t{\n");
        // Рецептурный менеджер у прошивки — массив `t.RECMAN[n]`, а не приборы с именем `RECMAN[n]`.
        let mut recman: BTreeMap<u32, String> = BTreeMap::new();
        for d in &self.devices {
            let mut fields: Vec<String> = Vec::new();
            let mut arrays: Vec<(String, BTreeMap<u32, &Value>)> = Vec::new();
            for f in &d.fields {
                let Some(value) = values.get(&f.address) else { continue };
                if let Some(m) = array_field.captures(&f.field)
                    && let Ok(idx) = m["idx"].parse::<u32>()
                {
                    let name = m["name"].to_string();
                    match arrays.iter_mut().find(|(n, _)| *n == name) {
                        Some((_, items)) => {
                            items.insert(idx, value);
                        }
                        None => arrays.push((name, BTreeMap::from([(idx, value)]))),
                    }
                } else {
                    fields.push(format!("{}={}", lua_key(&f.field), lua_value(value)));
                }
            }
            for (name, items) in arrays {
                let inner: Vec<String> = items.iter().map(|(i, v)| format!("[{i}]={}", lua_value(v))).collect();
                fields.push(format!("{}={{{}}}", lua_key(&name), inner.join(", ")));
            }
            if fields.is_empty() {
                continue;
            }
            match recman_index(&d.device) {
                Some(n) => {
                    recman.insert(n, fields.join(", "));
                }
                None => lua.push_str(&format!("\t{}={{{}}},\n", lua_key(&d.device), fields.join(", "))),
            }
        }
        if !recman.is_empty() {
            lua.push_str("\tRECMAN={\n");
            for (n, body) in recman {
                lua.push_str(&format!("\t\t[{n}]={{{body}}},\n"));
            }
            lua.push_str("\t},\n");
        }
        lua.push_str("\t}\n");
        Self::with_request_id(lua)
    }

    /// CMD_EXEC_DEVICE_COMMAND: разобрать `set_cmd` и применить к RW-тегу.
    fn apply_command(&self, raw: &[u8]) -> bool {
        let text = String::from_utf8_lossy(raw);
        let Some(m) = set_cmd_re().captures(&text) else {
            info!("PAC: команда записи не распознана: {text:?}");
            return false;
        };
        let (dev, field) = (&m["dev"], &m["field"]);
        let index = m["idx"].parse::<u32>().ok();
        let value = parse_scalar(&m["val"]);
        info!("PAC: запись {dev}.{field} = {value:?}");
        (self.on_write)(dev, field, index, &value)
    }

    /// CMD_SET_PAC_ERROR_CMD: `errors_manager:get_instance():set_cmd( cmd, t, n, a )` → команда над аварией. Код вне
    /// `100`/`200`/`201` или другая строка — не принято (у прошивки — ошибка Lua).
    fn alarm_command(&self, raw: &[u8]) -> bool {
        let text = String::from_utf8_lossy(raw);
        let Some(m) = alarm_cmd_re().captures(text.trim_end_matches('\0')) else {
            info!("PAC: команда аварии не распознана: {text:?}");
            return false;
        };
        let action = match &m["cmd"] {
            "100" => "ACCEPT",
            "200" => "SUPPRESS",
            "201" => "UNSUPPRESS",
            _ => return false,
        };
        let number = |name: &str| m[name].parse::<i32>();
        let (Ok(t), Ok(n), Ok(a)) = (number("t"), number("n"), number("a")) else { return false };
        info!("PAC: {action} аварии {t}:{n}:{a}");
        self.alarms.command(action, t, n, a)
    }
}

/// Строка команды аварии: двоеточие перед `get_instance` обязательно (статический метод tolua).
fn alarm_cmd_re() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^errors_manager:get_instance\(\):set_cmd\(\s*(?P<cmd>\d+)\s*,\s*(?P<t>-?\d+)\s*,\s*(?P<n>-?\d+)\s*,\s*(?P<a>-?\d+)\s*\)$",
        )
        .expect("регэксп")
    })
}

/// Поле-массив канала: `RT_PAR_F[12]` или `PAR_MAIN[1].P_CZAD_S` (хвост — подпись канала).
fn array_field_re() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(?P<name>[^\[\]]+)\[(?P<idx>\d+)\]").expect("регэксп"))
}

/// Разбор Lua-команды записи: `__1V1:set_cmd('ST', 1, 1)` или `..., 'value')`; драйвер старой SCADA (EasyDriver) пишет её в двойных
/// кавычках и с пробелами: `__OBJECT2:set_cmd( "S_PAR_F", 1, 10 )`, а рецептурный менеджер как `__RECMAN[1]:set_cmd( "PAR", 5, 2.5 )`.
/// Ведущие подчёркивания (у имён с цифры) съедаются: dev='1V1', field='ST', idx=1, val='1'.
fn set_cmd_re() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"_*(?P<dev>\w+(?:\[\d+\])?):set_cmd\(\s*['"](?P<field>\w+)['"]\s*,\s*(?P<idx>\d+)\s*,\s*['"]?(?P<val>[^'")]+?)['"]?\s*\)"#,
        )
            .expect("регэксп")
    })
}

/// Значение из текста команды: целое, вещественное или строка.
fn parse_scalar(text: &str) -> Value {
    if let Ok(i) = text.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = text.parse::<f64>() {
        Value::Float(f)
    } else {
        Value::Text(text.to_string())
    }
}

/// `RECMAN[3]` → `Some(3)`: рецептурный менеджер.
fn recman_index(device: &str) -> Option<u32> {
    device.strip_prefix("RECMAN[")?.strip_suffix(']')?.parse().ok()
}

/// Ключ Lua-таблицы: идентификатор как есть (LINE1V0), иначе `["1V1"]`.
fn lua_key(name: &str) -> String {
    let mut chars = name.chars();
    let ident = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ident { name.to_string() } else { format!("[\"{}\"]", name.replace('\\', "\\\\").replace('"', "\\\"")) }
}

/// Значение тега → Lua-литерал. bool → 1/0 (в протоколе это T_NUMBER), float — компактно (`%.6g`),
/// строка (рецепт, список программ) — в кавычках, как `CUR_REC='…'` у ptusa.
fn lua_value(value: &Value) -> String {
    match value {
        Value::Bool(b) => u8::from(*b).to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format_g6(*f),
        Value::Text(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'").replace('\n', "\\n")),
    }
}

/// Как `f"{x:.6g}"` в Python: 6 значащих цифр, без хвостовых нулей, экспонента при exp < −4 или ≥ 6.
pub fn format_g6(x: f64) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.into();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let sci = format!("{x:.5e}"); // d.dddddeN — уже округлено до 6 значащих
    let (mantissa, exp) = sci.split_once('e').expect("экспонента");
    let exp: i32 = exp.parse().expect("порядок");
    let strip = |s: &str| -> String {
        if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.').to_string() } else { s.to_string() }
    };
    if !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", strip(mantissa), exp.abs())
    } else {
        strip(&format!("{:.*}", (5 - exp).max(0) as usize, x))
    }
}

/// zlib-сжатие тела ответа (`compress2`, как у драйвера).
fn compress(body: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(body).expect("zlib");
    enc.finish().expect("zlib")
}

/// Кадр ответа: 's', статус, pidx (эхо), BE16-длина сжатого тела, zlib(body).
fn response(pidx: u8, status: u8, body: &[u8]) -> Vec<u8> {
    let packed = if status == STATUS_OK && !body.is_empty() { compress(body) } else { Vec::new() };
    if packed.len() > MAX_BODY {
        tracing::error!("PAC: сжатый ответ {}B > {MAX_BODY} — не влезает в длину", packed.len());
        return vec![NET_ID, STATUS_ERROR, pidx, 0, 0];
    }
    let mut out = vec![NET_ID, status, pidx, (packed.len() >> 8) as u8, (packed.len() & 0xFF) as u8];
    out.extend(packed);
    out
}

/// Сколько раз клиенты запросили снимок состояния (GET_DEVICES_STATES); раз в 5 с пишется в журнал — по нему считают, сколько
/// значений в секунду забирают у симулятора (число каналов × запросов в секунду).
static STATE_REQUESTS: AtomicU64 = AtomicU64::new(0);

/// Обслужить одно соединение драйвера: приветствие, затем кадры «запрос — ответ» до закрытия.
async fn handle(mut stream: TcpStream, model: Arc<PacModel>) -> Result<()> {
    let peer = stream.peer_addr().ok();
    info!("PAC: драйвер подключился {peer:?}");
    stream.write_all(BANNER).await?;
    loop {
        let mut header = [0u8; REQUEST_HEADER_LEN];
        if stream.read_exact(&mut header).await.is_err() {
            break;
        }
        if header[0] != NET_ID {
            warn!("PAC: неверный заголовок кадра от {peer:?} — рву связь");
            break;
        }
        let pidx = header[3];
        let length = (usize::from(header[4]) << 8) | usize::from(header[5]);
        let mut payload = vec![0u8; length];
        if length > 0 && stream.read_exact(&mut payload).await.is_err() {
            break;
        }
        let cmd = payload.first().copied().unwrap_or(0);
        if cmd == CMD_GET_DEVICES_STATES {
            STATE_REQUESTS.fetch_add(1, Ordering::Relaxed);
        }
        let (status, body) = model.handle_command(cmd, &payload);
        stream.write_all(&response(pidx, status, &body)).await?;
    }
    info!("PAC: драйвер отключился {peer:?}");
    Ok(())
}

/// Принимает соединения, пока жив процесс.
pub async fn serve(listener: TcpListener, model: Arc<PacModel>) {
    if let Ok(addr) = listener.local_addr() {
        info!("PAC (driver-master v{PROTOCOL_VERSION}): порт {}", addr.port());
    }
    tokio::spawn(async {
        let mut last = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let now = STATE_REQUESTS.load(Ordering::Relaxed);
            if now != last {
                info!("PAC: запросов состояния за 5 с: {}", now - last);
                last = now;
            }
        }
    });
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let model = model.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, model).await {
                        debug!("PAC: соединение закрыто: {e}");
                    }
                });
            }
            Err(e) => {
                warn!("PAC: accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Мини-«драйвер» PAC для отладки (как `tools/pac_probe.py` прежнего симулятора): handshake, объектная
/// модель и снимок состояний любого PAC — симулятора или настоящей прошивки ptusa — печатаются Lua-текстом.
pub async fn probe(host: &str, port: u16) -> Result<()> {
    let mut client = crate::pac_client::PacClient::connect(host, port).await?;
    println!(
        "== GET_INFO_ON_CONNECT ==\n{}",
        String::from_utf8_lossy(&client.request(CMD_GET_INFO_ON_CONNECT, &[]).await?)
    );
    println!("== GET_DEVICES ==\n{}", client.devices_lua().await?);
    println!("== GET_DEVICES_STATES ==\n{}", client.states_lua().await?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn devices() -> Vec<PacDevice> {
        let f = |field: &str, address: &str| PacField { field: field.into(), address: address.into() };
        vec![
            PacDevice { device: "LINE1V0".into(), fields: vec![f("M", "460"), f("ST", "385")] },
            PacDevice {
                device: "OBJECT1".into(),
                fields: vec![
                    f("RT_PAR_F[12]", "96"),
                    f("RT_PAR_F[7]", "91"),
                    f("PAR_MAIN[1].P_CZAD_S", "6"),
                    f("CUR_REC", "78"),
                ],
            },
            PacDevice { device: "1V1".into(), fields: vec![f("ST", "9001")] },
        ]
    }

    fn values() -> HashMap<String, Value> {
        HashMap::from([
            ("460".into(), Value::Int(0)),
            ("385".into(), Value::Int(1)),
            ("96".into(), Value::Float(0.5)),
            ("91".into(), Value::Float(1.25)),
            ("6".into(), Value::Float(1.0)),
            ("78".into(), Value::Text("Танк №1 'сырой'".into())),
            ("9001".into(), Value::Bool(true)),
        ])
    }

    fn model(on_write: Box<WriteFn>) -> PacModel {
        let m = PacModel::new("BN1-МСА1", devices(), on_write);
        m.update_snapshot(values());
        m
    }

    fn states_lua(m: &PacModel) -> String {
        let (status, body) = m.handle_command(CMD_GET_DEVICES_STATES, &[CMD_GET_DEVICES_STATES]);
        assert_eq!(status, STATUS_OK);
        assert_eq!(&body[..2], &1u16.to_le_bytes(), "первые 2 байта — devices_request_id");
        String::from_utf8(body[2..].to_vec()).unwrap().trim_end_matches('\0').to_string()
    }

    #[test]
    fn states_are_keyed_by_device_like_ptusa() {
        let lua = states_lua(&model(Box::new(|_, _, _, _| false)));
        assert!(lua.starts_with("t=\n\t{\n"), "{lua}");
        assert!(lua.contains("\tLINE1V0={M=0, ST=1},\n"), "{lua}");
        assert!(!lua.contains("tags["), "адресация по channelId осталась в прошлом");
    }

    #[test]
    fn array_fields_become_lua_tables() {
        let lua = states_lua(&model(Box::new(|_, _, _, _| false)));
        // PAR_MAIN[1].P_CZAD_S — хвост после ] подпись канала, в ПЛК это PAR_MAIN[1].
        assert!(lua.contains("PAR_MAIN={[1]=1}"), "{lua}");
        assert!(lua.contains("RT_PAR_F={[7]=1.25, [12]=0.5}"), "{lua}");
    }

    #[test]
    fn strings_are_quoted_and_names_escaped() {
        let lua = states_lua(&model(Box::new(|_, _, _, _| false)));
        assert!(lua.contains("CUR_REC='Танк №1 \\'сырой\\''"), "{lua}");
        assert!(lua.contains("[\"1V1\"]={ST=1}"), "имя с цифры — не Lua-идентификатор: {lua}");
    }

    #[test]
    fn devices_answer_like_states_and_ends_with_nul() {
        // Прошивка на GET_DEVICES отвечает тем же снимком `t={…}`, что и на GET_DEVICES_STATES, а тело — C-строка:
        // драйвер старой SCADA исполняет его как `luaL_dostring` и без нуля читает хвост прошлого ответа.
        let m = model(Box::new(|_, _, _, _| false));
        let (_, devices) = m.handle_command(CMD_GET_DEVICES, &[CMD_GET_DEVICES]);
        let (_, states) = m.handle_command(CMD_GET_DEVICES_STATES, &[CMD_GET_DEVICES_STATES]);
        assert_eq!(devices, states);
        assert_eq!(devices.last(), Some(&0u8));
        let lua = String::from_utf8(devices[2..].to_vec()).unwrap();
        assert!(lua.starts_with("t=\n"), "{lua}");
        assert!(lua.contains("LINE1V0={M=0, ST=1}"), "{lua}");
    }

    #[test]
    fn info_names_protocol_and_pac() {
        let m = model(Box::new(|_, _, _, _| false));
        let (_, info) = m.handle_command(CMD_GET_INFO_ON_CONNECT, &[CMD_GET_INFO_ON_CONNECT]);
        let info = String::from_utf8(info).unwrap();
        assert!(info.starts_with("protocol_version = 104;"), "{info}");
        assert!(info.contains("PAC_name = \"BN1-МСА1\""), "{info}");
    }

    #[test]
    fn alarms_are_listed_and_commanded_like_ptusa() {
        let alarms = Arc::new(Alarms::default());
        let m = model(Box::new(|_, _, _, _| false)).with_alarms(alarms.clone());
        let number = alarms.raise(2);
        let (status, list) = m.handle_command(CMD_GET_PAC_ERRORS, &[CMD_GET_PAC_ERRORS, 0]);
        assert_eq!((status, list.last()), (STATUS_OK, Some(&0u8)));
        let lua = String::from_utf8(list).unwrap();
        assert!(lua.starts_with("alarms[ 0 ] = \n  {}alarms[ 0 ] = \n"), "{lua}");
        assert!(lua.contains(&format!("id_n=2,\nid_object_alarm_number={number},\nid_type=102,")), "{lua}");
        let run = |text: &str| {
            let mut payload = vec![CMD_SET_PAC_ERROR_CMD];
            payload.extend(text.as_bytes());
            payload.push(0);
            m.handle_command(CMD_SET_PAC_ERROR_CMD, &payload)
        };
        for bad in [
            "errors_manager.get_instance():set_cmd( 100, 102, 2, 1 )",
            "errors_manager:get_instance():set_cmd( 300, 102, 2, 1 )",
            "errors_manager:get_instance():set_cmd( 100, 102, 2, 'x' )",
        ] {
            assert_eq!(run(bad), (STATUS_OK, EXEC_FAILED.to_vec()), "{bad}");
        }
        assert!(alarms.lua_text().contains("id_n=2"), "неверные команды сообщение не трогают");
        let accept = format!("errors_manager:get_instance():set_cmd( 100, 102, 2, {number} )");
        assert_eq!(run(&accept), (STATUS_OK, EXEC_APPLIED.to_vec()));
        assert!(!alarms.lua_text().contains("id_n=2"), "квитированное сообщение ушло");
        assert_eq!(run(&accept), (STATUS_OK, EXEC_APPLIED.to_vec()), "над несуществующим — молча, как у ptusa");
    }

    #[test]
    fn recipe_manager_is_one_array_table() {
        // У прошивки рецептурный менеджер — массив `t.RECMAN[n]`; драйвер читает `RECMAN[1].PAR[5]` как `t.RECMAN[1].PAR[5]`.
        let f = |field: &str, address: &str| PacField { field: field.into(), address: address.into() };
        let devices = vec![
            PacDevice { device: "RECMAN[1]".into(), fields: vec![f("CMD", "1"), f("PAR[2]", "2")] },
            PacDevice { device: "RECMAN[2]".into(), fields: vec![f("CMD", "3")] },
        ];
        let m = PacModel::new("X", devices, Box::new(|_, _, _, _| false));
        m.update_snapshot(HashMap::from([
            ("1".into(), Value::Int(0)),
            ("2".into(), Value::Float(1.5)),
            ("3".into(), Value::Int(7)),
        ]));
        let lua = states_lua(&m);
        assert!(lua.contains("RECMAN={\n\t\t[1]={CMD=0, PAR={[2]=1.5}},\n\t\t[2]={CMD=7},\n\t},"), "{lua}");
        assert!(!lua.contains("RECMAN[1]"), "{lua}");
    }

    #[test]
    fn exec_command_reports_result_code() {
        type Logged = (String, String, Option<u32>, Value);
        let applied: Arc<Mutex<Vec<Logged>>> = Arc::default();
        let log = applied.clone();
        let m = model(Box::new(move |dev, field, idx, val| {
            log.lock().unwrap().push((dev.into(), field.into(), idx, val.clone()));
            dev == "LINE1V0"
        }));
        let run = |text: &str| {
            let mut payload = vec![CMD_EXEC_DEVICE_COMMAND];
            payload.extend(text.as_bytes());
            m.handle_command(CMD_EXEC_DEVICE_COMMAND, &payload)
        };
        assert_eq!(run("__LINE1V0:set_cmd('M', 1, 1)"), (STATUS_OK, EXEC_APPLIED.to_vec()));
        assert_eq!(applied.lock().unwrap()[0], ("LINE1V0".into(), "M".into(), Some(1), Value::Int(1)));
        assert_eq!(run("__NO_SUCH:set_cmd('M', 1, 1)").1, EXEC_FAILED.to_vec());
        assert_eq!(run("garbage").1, EXEC_FAILED.to_vec());
        // Поле-массив — индексом отдельным аргументом; дробное и строковое значение.
        run("__LINE1V0:set_cmd('RT_PAR_F', 12, 7.5)");
        run("__LINE1V0:set_cmd('CUR_REC', 1, 'abc')");
        let log = applied.lock().unwrap();
        assert_eq!(log[2], ("LINE1V0".into(), "RT_PAR_F".into(), Some(12), Value::Float(7.5)));
        assert_eq!(log[3].3, Value::Text("abc".into()));
        drop(log);
        // Формат драйвера старой SCADA (EasyDriver): двойные кавычки, пробелы, рецептурный менеджер `RECMAN[n]`.
        run(r#"__LINE1V0:set_cmd( "RT_PAR_F", 3, 10 )"#);
        run(r#"__LINE1V0:set_cmd( "CUR_REC", 1, "abc" )"#);
        run(r#"__RECMAN[2]:set_cmd( "PAR", 5, 2.5 )"#);
        let log = applied.lock().unwrap();
        assert_eq!(log[4], ("LINE1V0".into(), "RT_PAR_F".into(), Some(3), Value::Int(10)));
        assert_eq!(log[5].3, Value::Text("abc".into()));
        assert_eq!(log[6], ("RECMAN[2]".into(), "PAR".into(), Some(5), Value::Float(2.5)));
    }

    #[test]
    fn unknown_command_is_empty_success() {
        assert_eq!(model(Box::new(|_, _, _, _| false)).handle_command(55, &[55]), (STATUS_OK, Vec::new()));
    }

    #[test]
    fn g6_matches_python_formatting() {
        for (x, s) in [
            (0.0, "0"),
            (1.0, "1"),
            (21.5, "21.5"),
            (0.5, "0.5"),
            (1.25, "1.25"),
            (100_000.0, "100000"),
            (999_999.5, "1e+06"),
            (1_234_567.0, "1.23457e+06"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (-6.54321987, "-6.54322"),
            (0.1 + 0.2, "0.3"),
            (1e21, "1e+21"),
        ] {
            assert_eq!(format_g6(x), s, "{x}");
        }
    }

    #[tokio::test]
    async fn socket_exchange_starts_with_banner() {
        let m = Arc::new(model(Box::new(|_, _, _, _| false)));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve(listener, m));
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut banner = vec![0u8; BANNER.len()];
        s.read_exact(&mut banner).await.unwrap();
        assert_eq!(banner, BANNER);

        s.write_all(&[b's', 1, 1, 7, 0, 1, CMD_GET_INFO_ON_CONNECT]).await.unwrap();
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await.unwrap();
        assert_eq!(&hdr[..3], &[b's', STATUS_OK, 7], "эхо pidx и статус успеха ptusa");
        let mut packed = vec![0u8; (usize::from(hdr[3]) << 8) | usize::from(hdr[4])];
        s.read_exact(&mut packed).await.unwrap();
        let mut info = String::new();
        flate2::read::ZlibDecoder::new(&packed[..]).read_to_string(&mut info).unwrap();
        assert!(info.starts_with("protocol_version = 104;"), "{info}");
        assert!(info.contains("PAC_name = \"BN1-МСА1\""), "{info}");

        // Неверный заголовок — соединение рвётся.
        // Клиент на том же сервере (и тест запроса целиком).
        let mut client = crate::pac_client::PacClient::connect("127.0.0.1", port).await.unwrap();
        assert!(client.states_lua().await.unwrap().contains("LINE1V0={M=0, ST=1}"));
        assert_eq!(
            client.exec("__LINE1V0:set_cmd('M', 1, 1)").await.unwrap(),
            1,
            "в этой модели on_write всегда false"
        );

        s.write_all(&[b'x', 0, 0, 0, 0, 0]).await.unwrap();
        let mut rest = Vec::new();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(3), s.read_to_end(&mut rest)).await.unwrap().unwrap(),
            0
        );
    }
}
