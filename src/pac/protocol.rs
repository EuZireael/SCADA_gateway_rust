//! Проводной протокол driver-master (PAC Savushkin/ptusa), версия 104 (zlib + UTF-8).
//!
//! Сверено с эмулятором ptusa 2026.4.2.1: после accept — приветствие [`BANNER`], дальше
//! кадры. ЗАПРОС (6 байт заголовка): `'s', ServiceID, FrameSingle, pidx, lenHi, lenLo` +
//! payload. ОТВЕТ (5 байт): `'s', status, pidx, lenHi, lenLo` + zlib(body). Успех ptusa
//! помечает статусом 12, ошибку — 7 (драйвер смотрит только на ошибку).

use std::io::Read;
#[cfg(test)]
use std::io::Write;

use flate2::read::ZlibDecoder;
#[cfg(test)]
use flate2::{Compression, write::ZlibEncoder};

/// Версия протокола driver-master, с которой сверена реализация (при другой — предупреждение, работа продолжается).
pub const PROTOCOL_VERSION: i64 = 104;
/// Приветствие, которое PAC присылает сразу после подключения.
pub const BANNER: &[u8] = b"PAC accept";
/// Первый байт каждого кадра (`'s'`).
pub const NET_ID: u8 = b's';
/// Идентификатор службы в запросе.
pub const SERVICE_ID: u8 = 1;
/// Признак одиночного кадра: запрос не разбит на части.
pub const FRAME_SINGLE: u8 = 1;
/// Статус ответа «ошибка» (успех ptusa помечает статусом 12; драйвер смотрит только на ошибку).
pub const STATUS_ERROR: u8 = 7;
/// Длина заголовка запроса, байт.
pub const REQUEST_HEADER_LEN: usize = 6;
/// Длина заголовка ответа, байт.
pub const RESPONSE_HEADER_LEN: usize = 5;
/// В теле ответов devices/states первые 2 байта — devices_request_id.
pub const LUA_START_OFFSET: usize = 2;

/// Команда: версия протокола и имя PAC (handshake).
pub const CMD_GET_INFO_ON_CONNECT: u8 = 10;
/// Команда: снимок состояния всех приборов (Lua-таблица `t`).
pub const CMD_GET_DEVICES_STATES: u8 = 101;
/// Команда: выполнить команду прибора (`set_cmd`).
pub const CMD_EXEC_DEVICE_COMMAND: u8 = 102;
/// Команда: список аварий прошивки (Lua-таблица `alarms[0]`); доп. байт — `project_descr_id` (0).
pub const CMD_GET_PAC_ERRORS: u8 = 103;
/// Команда: действие над аварией (`errors_manager:get_instance():set_cmd(…)`).
pub const CMD_SET_PAC_ERROR_CMD: u8 = 104;

/// Кадр запроса: заголовок + `cmd` + доп. данные.
pub fn build_request(pidx: u8, cmd: u8, extra: &[u8]) -> Vec<u8> {
    let len = 1 + extra.len();
    let mut frame = Vec::with_capacity(REQUEST_HEADER_LEN + len);
    frame.extend_from_slice(&[NET_ID, SERVICE_ID, FRAME_SINGLE, pidx, (len >> 8) as u8, len as u8]);
    frame.push(cmd);
    frame.extend_from_slice(extra);
    frame
}

/// Потолок распакованного ответа. Кадр PAC ≤ 64 КБ (длина — 16 бит), а zlib сжимает до ~1000:1, так что
/// одним кадром без ограничения можно было бы раздуть память шлюза в десятки МБ ещё до исполнения Lua.
/// Настоящий снимок станции (2647 каналов) — ~20 КБ.
pub const MAX_INFLATED_BYTES: usize = 8 * 1024 * 1024;

/// zlib-распаковка тела ответа (C++-драйвер жмёт compress2 — стандартный zlib), не больше
/// [`MAX_INFLATED_BYTES`].
pub fn inflate(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() * 4);
    ZlibDecoder::new(data).take(MAX_INFLATED_BYTES as u64 + 1).read_to_end(&mut out)?;
    if out.len() > MAX_INFLATED_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ответ PAC после распаковки больше {} МБ", MAX_INFLATED_BYTES / (1024 * 1024)),
        ));
    }
    Ok(out)
}

/// zlib-сжатие (для тестов и фейкового PAC).
#[cfg(test)]
pub fn deflate(data: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(data).expect("запись в Vec не падает");
    enc.finish().expect("запись в Vec не падает")
}

/// Код результата EXEC_DEVICE_COMMAND и SET_PAC_ERROR_CMD (LE16): 0 — применено, иначе команда не выполнилась.
pub fn exec_result_code(body: &[u8]) -> u16 {
    if body.len() >= 2 { u16::from_le_bytes([body[0], body[1]]) } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_header_layout() {
        let f = build_request(7, CMD_GET_DEVICES_STATES, &[]);
        assert_eq!(f, vec![b's', 1, 1, 7, 0, 1, 101]);
    }

    #[test]
    fn request_length_is_big_endian_16() {
        let extra = vec![b'x'; 299];
        let f = build_request(1, CMD_EXEC_DEVICE_COMMAND, &extra);
        assert_eq!((f[4], f[5]), (0x01, 0x2C)); // 300 = 0x012C
        assert_eq!(f.len(), REQUEST_HEADER_LEN + 300);
    }

    #[test]
    fn zlib_round_trip() {
        let original = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\t}\n".as_bytes();
        assert_eq!(inflate(&deflate(original)).unwrap(), original);
    }

    #[test]
    fn zlib_bomb_is_rejected() {
        // 32 МБ нулей сжимаются в десятки КБ — помещаются в один кадр PAC.
        let bomb = deflate(&vec![0u8; 32 * 1024 * 1024]);
        assert!(bomb.len() < 65535, "бомба помещается в один кадр: {} Б", bomb.len());
        let err = inflate(&bomb).unwrap_err();
        assert!(err.to_string().contains("МБ"), "{err}");
        // Граница: ровно потолок проходит.
        assert_eq!(inflate(&deflate(&vec![7u8; MAX_INFLATED_BYTES])).unwrap().len(), MAX_INFLATED_BYTES);
    }

    #[test]
    fn exec_result_code_is_le16() {
        assert_eq!(exec_result_code(&[0, 0]), 0);
        assert_eq!(exec_result_code(&[1, 0]), 1);
        assert_eq!(exec_result_code(&[]), 0);
    }
}
