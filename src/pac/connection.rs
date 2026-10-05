//! Одно TCP-соединение с PAC-контроллером: сокет + свой Lua-стейт.
//!
//! Цикл: [`PacConnection::connect`] (с приветствием PAC) → handshake (версия/имя) →
//! [`PacConnection::poll_states`] (снимок всех приборов) → [`PacConnection::read_value`] по
//! тегам. Запись — [`PacConnection::write_command`]. Первый запрос шлём сразу: ptusa рвёт
//! соединение, если после приветствия клиент молчит дольше ~300 мс.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::warn;

use super::lua::{self, PacLua};
use super::protocol::*;
use crate::model::TagValue;

pub struct PacConnection {
    stream: TcpStream,
    lua: PacLua,
    op_timeout: Duration,
    pidx: u8,
    handshaked: bool,
}

impl PacConnection {
    /// Открыть сокет, принять приветствие PAC и сразу сделать handshake.
    pub async fn connect(host: &str, port: u16, op_timeout: Duration) -> Result<Self> {
        let stream = timeout(op_timeout, TcpStream::connect((host, port)))
            .await
            .with_context(|| format!("PAC {host}:{port}: таймаут подключения"))?
            .with_context(|| format!("PAC {host}:{port}: нет подключения"))?;
        stream.set_nodelay(true).ok();
        let mut conn = PacConnection { stream, lua: PacLua::new()?, op_timeout, pidx: 0, handshaked: false };
        // Приветствие «PAC accept» шлёт ptusa_main, но не plc-simulator: ждать его здесь нельзя
        // (симулятор молчит до запроса → таймаут). Оно дочитывается в request(), как в Java-шлюзе.
        conn.handshake().await?;
        Ok(conn)
    }

    /// GET_INFO_ON_CONNECT: версия протокола и имя PAC.
    async fn handshake(&mut self) -> Result<()> {
        let info = self.request(CMD_GET_INFO_ON_CONNECT, &[]).await?;
        self.lua.exec(&lua_text(&info))?;
        let ver = self.lua.protocol_version();
        if ver != PROTOCOL_VERSION {
            warn!("PAC: версия протокола {ver} (поддерживается {PROTOCOL_VERSION})");
        }
        self.handshaked = true;
        Ok(())
    }

    /// GET_DEVICES_STATES: снимок всех приборов → исполнить Lua (наполняет `t`).
    pub async fn poll_states(&mut self) -> Result<()> {
        if !self.handshaked {
            self.handshake().await?;
        }
        let body = self.request(CMD_GET_DEVICES_STATES, &[]).await?;
        let off = if body.len() >= LUA_START_OFFSET { LUA_START_OFFSET } else { 0 };
        self.lua.exec(&lua_text(&body[off..]))
    }

    /// Значение поля прибора из снимка `t[device][field]`.
    pub fn read_value(&self, device: &str, field: &str, data_type: &str) -> Option<TagValue> {
        self.lua.read(device, field, data_type)
    }

    /// Значение тега из снимка: `tags[channelId]` (симулятор) или `t[device][field]` (ptusa).
    pub fn read_tag(&self, channel_id: Option<i64>, device: Option<&str>, field: Option<&str>, data_type: &str) -> Option<TagValue> {
        self.lua.read_tag(channel_id, device, field, data_type)
    }

    /// EXEC_DEVICE_COMMAND: `__<device>:set_cmd('<field>', 1, <value>)`. Возвращает код
    /// результата PAC (LE16): 0 — применено, иначе команда не выполнилась (соединение при
    /// этом живо). Err — сбой связи.
    pub async fn exec_command(&mut self, device: &str, field: &str, value: &TagValue) -> Result<u16> {
        let cmd = format!("__{device}:set_cmd('{field}', 1, {})", lua::scalar(value));
        let result = self.request(CMD_EXEC_DEVICE_COMMAND, cmd.as_bytes()).await?;
        Ok(exec_result_code(&result))
    }

    /// То же, но отказ PAC — ошибка.
    #[cfg(test)]
    pub async fn write_command(&mut self, device: &str, field: &str, value: &TagValue) -> Result<()> {
        match self.exec_command(device, field, value).await? {
            0 => Ok(()),
            code => bail!("PAC не выполнил команду {device}.{field} (код {code})"),
        }
    }

    /// Отправить команду, получить и распаковать тело ответа.
    async fn request(&mut self, cmd: u8, extra: &[u8]) -> Result<Vec<u8>> {
        self.pidx = self.pidx.wrapping_add(1);
        let frame = build_request(self.pidx, cmd, extra);
        timeout(self.op_timeout, self.stream.write_all(&frame)).await.context("PAC: таймаут отправки")??;

        let mut hdr = [0u8; RESPONSE_HEADER_LEN];
        self.read_exact(&mut hdr).await?;
        if hdr[..] == BANNER[..RESPONSE_HEADER_LEN] {
            // Приветствие ptusa пришло перед первым ответом — дочитать и взять настоящий заголовок.
            let mut rest = [0u8; BANNER.len() - RESPONSE_HEADER_LEN];
            self.read_exact(&mut rest).await?;
            if rest[..] != BANNER[RESPONSE_HEADER_LEN..] {
                bail!("PAC: неожиданное приветствие контроллера");
            }
            self.read_exact(&mut hdr).await?;
        }
        if hdr[0] != NET_ID {
            bail!("PAC: неверный заголовок ответа {:?}", hdr);
        }
        let (status, rpidx) = (hdr[1], hdr[2]);
        let len = ((hdr[3] as usize) << 8) | hdr[4] as usize;
        let mut body = vec![0u8; len];
        self.read_exact(&mut body).await?;
        if status == STATUS_ERROR {
            bail!("PAC вернул статус ошибки на команду {cmd}");
        }
        if rpidx != self.pidx {
            bail!("PAC: рассинхрон пакета (ждали pidx={}, пришёл {rpidx})", self.pidx);
        }
        if body.is_empty() {
            return Ok(body);
        }
        inflate(&body).context("PAC: ошибка zlib-распаковки ответа")
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        timeout(self.op_timeout, self.stream.read_exact(buf))
            .await
            .context("PAC: таймаут чтения")?
            .context("PAC: соединение закрыто до получения ответа")?;
        Ok(())
    }
}

/// Lua-текст ответа. ptusa отдаёт тело как C-строку с завершающим `\0`: Lua 5.1 на нём
/// падает (`unexpected symbol`), поэтому текст берётся до первого нулевого байта.
fn lua_text(body: &[u8]) -> std::borrow::Cow<'_, str> {
    let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    String::from_utf8_lossy(&body[..end])
}

/// Фейковый PAC в процессе — повторяет поведение эмулятора ptusa (для тестов).
#[cfg(test)]
pub mod fake {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::super::protocol::*;

    // Как у ptusa: тела — C-строки с завершающим \0.
    pub const INFO: &str = "protocol_version = 104; PAC_name = \"FAKE\"; params_CRC=0;\n\0";
    pub const STATES: &str = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\t}\n\
        t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n\t{\n\tRT_PAR_F=\n\t\t{\n\t\t0, 2.5,\n\t\t},\n\t}\n\0";

    /// Снимок plc-simulator: плоская таблица по channelId, без `t`.
    pub const SIM_STATES: &str = "tags={}\ntags['9001']=7\ntags['9002']=1.5\n";

    /// Поднять фейковый PAC на свободном порту. banner=false — как plc-simulator: без приветствия
    /// и со снимком [`SIM_STATES`].
    pub async fn spawn(banner: bool) -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let seen = commands.clone();
        tokio::spawn(async move {
            let Ok((mut s, _)) = listener.accept().await else { return };
            if banner {
                s.write_all(BANNER).await.ok();
            }
            let states = if banner { STATES } else { SIM_STATES };
            loop {
                let mut hdr = [0u8; REQUEST_HEADER_LEN];
                if s.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                let mut payload = vec![0u8; ((hdr[4] as usize) << 8) | hdr[5] as usize];
                if s.read_exact(&mut payload).await.is_err() {
                    return;
                }
                let body: Vec<u8> = match payload[0] {
                    CMD_GET_INFO_ON_CONNECT => INFO.as_bytes().to_vec(),
                    CMD_GET_DEVICES_STATES => [&[0u8, 0][..], states.as_bytes()].concat(),
                    CMD_EXEC_DEVICE_COMMAND => {
                        let lua = String::from_utf8_lossy(&payload[1..]).to_string();
                        let ok = lua.starts_with("__LINE1V0:");
                        seen.lock().unwrap().push(lua);
                        if ok { vec![0, 0] } else { vec![1, 0] }
                    }
                    _ => Vec::new(),
                };
                let packed = if body.is_empty() { body } else { deflate(&body) };
                let mut frame = vec![b's', 12, hdr[3], (packed.len() >> 8) as u8, packed.len() as u8];
                frame.extend_from_slice(&packed);
                if s.write_all(&frame).await.is_err() {
                    return;
                }
            }
        });
        (port, commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_snapshot_after_banner() {
        let (port, _) = fake::spawn(true).await;
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_secs(2)).await.unwrap();
        conn.poll_states().await.unwrap();
        assert_eq!(conn.read_value("LINE1V0", "ST", "INT32"), Some(TagValue::Int(1)));
        assert_eq!(conn.read_value("OBJECT1", "RT_PAR_F[2]", "FLOAT"), Some(TagValue::F64(2.5)));
    }

    #[tokio::test]
    async fn reads_simulator_snapshot_without_banner() {
        let (port, _) = fake::spawn(false).await;
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_millis(500)).await.unwrap();
        conn.poll_states().await.unwrap();
        assert_eq!(conn.read_tag(Some(9001), Some("DEV"), Some("ST"), "INT32"), Some(TagValue::Int(7)));
        assert_eq!(conn.read_tag(Some(9002), None, None, "FLOAT"), Some(TagValue::F64(1.5)));
        assert_eq!(conn.read_tag(Some(9999), None, None, "FLOAT"), None);
    }

    #[tokio::test]
    async fn write_command_applied_and_rejected() {
        let (port, commands) = fake::spawn(true).await;
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_secs(2)).await.unwrap();
        conn.write_command("LINE1V0", "M", &TagValue::Int(1)).await.unwrap();
        assert_eq!(commands.lock().unwrap()[0], "__LINE1V0:set_cmd('M', 1, 1)");
        let err = conn.write_command("NO_SUCH", "M", &TagValue::Int(1)).await.unwrap_err();
        assert!(err.to_string().contains("код 1"), "{err}");
    }
}
