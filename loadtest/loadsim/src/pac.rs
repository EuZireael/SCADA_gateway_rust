//! PAC-серверы нагрузочного стенда: N «контроллеров» по M полей на протоколе driver-master («EasyDriver»).
//!
//! Тот же протокол, что у настоящей прошивки ptusa и у драйвера старой SCADA (savushkin-r-d/driver):
//! после accept — приветствие `PAC accept`, дальше кадры `'s' + ServiceID + FrameType + pidx + BE16-длина + payload`,
//! ответ — `'s' + статус 12 + pidx + BE16-длина + zlib(тело)`. Реализовано то, что нужно шлюзу: handshake
//! (GET_INFO_ON_CONNECT), снимок состояний (GET_DEVICES_STATES) и запись (EXEC_DEVICE_COMMAND). Длина сжатого
//! тела — 16 бит, поэтому один снимок не больше 65 535 байт: это предел протокола на число полей контроллера.
//!
//! Поле номер `j` — `F<j % 20>` прибора `D<j / 20>`: снимок `t = { D0 = { F0 = 1.5, F1 = 2, … }, D1 = … }`.

use std::io::Write;
use std::sync::{Arc, Mutex};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Полей в одном приборе.
pub const FIELDS_PER_DEVICE: usize = 20;
const BANNER: &[u8] = b"PAC accept";
const STATUS_OK: u8 = 12;
const STATUS_ERROR: u8 = 7;
const CMD_INFO: u8 = 10;
const CMD_STATES: u8 = 101;
const CMD_EXEC: u8 = 102;
/// Потолок сжатого тела ответа: длина кадра — 16 бит.
const MAX_BODY: usize = 0xFFFF;

/// Значения полей одного «контроллера»: тип поля — по номеру (каждое третье целое), как у узлов OPC UA.
pub struct Fields {
    pub values: Mutex<Vec<f64>>,
}

/// Целое ли поле номер `j` (как `is_float` у OPC UA-источника).
fn is_int(j: usize) -> bool {
    j.is_multiple_of(3)
}

/// Lua-снимок `t = { … }` со всеми полями; перед текстом — два байта devices_request_id (как у ptusa).
fn snapshot(values: &[f64]) -> Vec<u8> {
    let mut out = String::with_capacity(values.len() * 12 + 64);
    out.push_str("t=\n\t{\n");
    for (d, chunk) in values.chunks(FIELDS_PER_DEVICE).enumerate() {
        out.push_str(&format!("\tD{d}={{"));
        for (k, v) in chunk.iter().enumerate() {
            let j = d * FIELDS_PER_DEVICE + k;
            if k > 0 {
                out.push_str(", ");
            }
            if is_int(j) {
                out.push_str(&format!("F{k}={}", *v as i64));
            } else {
                out.push_str(&format!("F{k}={v}"));
            }
        }
        out.push_str("},\n");
    }
    out.push_str("\t}\n");
    let mut body = 1u16.to_le_bytes().to_vec();
    body.extend(out.into_bytes());
    body
}

fn compress(body: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(body).expect("zlib");
    enc.finish().expect("zlib")
}

/// Кадр ответа; слишком большой снимок — статус ошибки (как у протокола: длина 16 бит).
fn response(pidx: u8, body: &[u8]) -> Vec<u8> {
    let packed = compress(body);
    if packed.len() > MAX_BODY {
        return vec![b's', STATUS_ERROR, pidx, 0, 0];
    }
    let mut out = vec![b's', STATUS_OK, pidx, (packed.len() >> 8) as u8, (packed.len() & 0xFF) as u8];
    out.extend(packed);
    out
}

/// Применить команду `__D3:set_cmd('F7', 1, 42)`: записать значение в поле. Код результата: 0 — применено, 1 — нет.
fn exec(text: &str, fields: &Fields) -> [u8; 2] {
    let Some(rest) = text.trim_start_matches('_').strip_prefix('D') else { return [1, 0] };
    let Some((dev, rest)) = rest.split_once(":set_cmd('F") else { return [1, 0] };
    let Some((field, rest)) = rest.split_once('\'') else { return [1, 0] };
    let value = rest.rsplit(',').next().unwrap_or("").trim().trim_end_matches(')').trim();
    let (Ok(d), Ok(k), Ok(v)) = (dev.parse::<usize>(), field.parse::<usize>(), value.parse::<f64>()) else {
        return [1, 0];
    };
    let mut values = fields.values.lock().expect("значения");
    match values.get_mut(d * FIELDS_PER_DEVICE + k) {
        Some(slot) => {
            *slot = v;
            [0, 0]
        }
        None => [1, 0],
    }
}

async fn handle(mut stream: TcpStream, name: String, fields: Arc<Fields>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    stream.write_all(BANNER).await?;
    loop {
        let mut header = [0u8; 6];
        stream.read_exact(&mut header).await?;
        if header[0] != b's' {
            return Ok(());
        }
        let len = (usize::from(header[4]) << 8) | usize::from(header[5]);
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        let pidx = header[3];
        let body: Vec<u8> = match payload.first().copied() {
            Some(CMD_INFO) => {
                format!("protocol_version = 104; PAC_name = \"{name}\"; is_reset_params = 0;params_CRC=0;\n").into_bytes()
            }
            Some(CMD_STATES) => snapshot(&fields.values.lock().expect("значения")),
            Some(CMD_EXEC) => exec(&String::from_utf8_lossy(&payload[1..]), &fields).to_vec(),
            _ => Vec::new(),
        };
        stream.write_all(&response(pidx, &body)).await?;
    }
}

/// Поднять PAC-сервер на порту; возвращает общее состояние полей (его меняет цикл источника).
pub async fn start(port: u16, name: String, tags: usize) -> anyhow::Result<Arc<Fields>> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let fields = Arc::new(Fields {
        values: Mutex::new((0..tags).map(|j| if is_int(j) { j as f64 } else { j as f64 + 0.5 }).collect()),
    });
    let shared = fields.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { continue };
            let (name, fields) = (name.clone(), shared.clone());
            tokio::spawn(async move {
                let _ = handle(stream, name, fields).await;
            });
        }
    });
    Ok(fields)
}

/// Сжатый размер снимка из `tags` полей, байт (для проверки предела протокола).
pub fn packed_size(tags: usize) -> usize {
    let values: Vec<f64> = (0..tags).map(|j| if is_int(j) { j as f64 } else { j as f64 + 0.5 }).collect();
    compress(&snapshot(&values)).len()
}

/// Новое значение поля номер `j` на шаге `round` (меняется каждый раз, как у OPC UA-источника).
pub fn changed(j: usize, round: u64) -> f64 {
    if is_int(j) { (j as i64).wrapping_add(round as i64) as f64 } else { j as f64 + (round % 1000) as f64 * 0.37 }
}
