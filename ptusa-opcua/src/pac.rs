//! Клиент driver-master одной прошивки ptusa: одно TCP-соединение, запрос → ответ.
//!
//! Опрос (снимок) идёт по одному соединению и по очереди; команды клиентов OPC UA (запись, действие над аварией) — по
//! своему, второму (см. `sibling`) в отдельном рантайме: их обработчик синхронный и не должен зависеть от драйвера
//! ввода-вывода основного; список аварий — по третьему, со своим периодом. Первый запрос шлём сразу после приветствия:
//! ptusa рвёт соединение, если клиент молчит дольше ~300 мс.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::protocol::*;

/// Открытое соединение с прошивкой: сокет и номер пакета запроса.
struct Conn {
    stream: TcpStream,
    pidx: u8,
}

/// Клиент driver-master прошивки ptusa: одно соединение, операции под мьютексом.
pub struct PacClient {
    pub host: String,
    pub port: u16,
    op_timeout: Duration,
    conn: Mutex<Option<Conn>>,
    connected: AtomicBool,
    /// Ожидаемое имя прошивки (`PAC_name` из handshake); `None` — не сверяется.
    expected_name: Option<String>,
    /// Имя, названное прошивкой при последнем подключении.
    name: std::sync::Mutex<Option<String>>,
}

/// `PAC_name` из Lua-текста ответа на GET_INFO_ON_CONNECT (`PAC_name = "BN1-МСА1"; …`); `None`, если его нет.
pub fn parse_pac_name(text: &str) -> Option<String> {
    let rest = text.split("PAC_name").nth(1)?.trim_start().strip_prefix('=')?.trim_start().strip_prefix('"')?;
    let mut name = String::new();
    let mut chars = rest.chars();
    loop {
        match chars.next()? {
            '"' => return Some(name),
            '\\' => name.push(chars.next()?),
            c => name.push(c),
        }
    }
}

impl PacClient {
    /// Клиент без соединения.
    pub fn new(host: &str, port: u16, op_timeout: Duration) -> Self {
        PacClient {
            host: host.into(),
            port,
            op_timeout,
            conn: Mutex::new(None),
            connected: AtomicBool::new(false),
            expected_name: None,
            name: std::sync::Mutex::new(None),
        }
    }

    /// Сверять имя прошивки (`PAC_name`) при каждом подключении: другое имя — отказ, соединение не открывается и значения
    /// чужой прошивки наружу не отдаются.
    pub fn expecting_name(mut self, name: Option<String>) -> Self {
        self.expected_name = name.filter(|n| !n.trim().is_empty());
        self
    }

    /// Имя, которым прошивка назвала себя при последнем подключении.
    pub fn pac_name(&self) -> Option<String> {
        self.name.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Новый клиент той же прошивки со своим соединением (для команд записи: см. `server::add_write_callbacks`).
    pub fn sibling(&self) -> PacClient {
        PacClient::new(&self.host, self.port, self.op_timeout).expecting_name(self.expected_name.clone())
    }

    /// Есть ли соединение (читается без блокировки).
    pub fn connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Открыть сокет, принять приветствие PAC и сделать handshake (GET_INFO_ON_CONNECT).
    pub async fn connect(&self) -> Result<()> {
        let (host, port) = (&self.host, self.port);
        let stream = timeout(self.op_timeout, TcpStream::connect((host.as_str(), port)))
            .await
            .with_context(|| format!("PAC {host}:{port}: таймаут подключения"))?
            .with_context(|| format!("PAC {host}:{port}: нет подключения"))?;
        stream.set_nodelay(true).ok();
        let mut conn = Conn { stream, pidx: 0 };
        let mut banner = [0u8; BANNER.len()];
        timeout(self.op_timeout, conn.stream.read_exact(&mut banner)).await.context("PAC: таймаут приветствия")??;
        if banner != BANNER {
            bail!("PAC: нет приветствия 'PAC accept', пришло {:?}", String::from_utf8_lossy(&banner));
        }
        let info = self.request(&mut conn, CMD_GET_INFO_ON_CONNECT, &[]).await?;
        let name = parse_pac_name(&lua_text(&info));
        *self.name.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = name.clone();
        if let Some(expected) = &self.expected_name
            && name.as_deref().map(str::trim) != Some(expected.trim())
        {
            // Соединение не сохраняется: чужая прошивка не опрашивается и команды в неё не идут.
            bail!(
                "PAC {host}:{port}: не та прошивка — ожидали PAC_name {expected:?}, она назвала {:?}",
                name.as_deref().unwrap_or("")
            );
        }
        *self.conn.lock().await = Some(conn);
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    /// Закрыть соединение; следующий вызов переподключится.
    pub async fn close(&self) {
        self.connected.store(false, Ordering::Release);
        *self.conn.lock().await = None;
    }

    /// GET_DEVICES_STATES: Lua-текст снимка всех приборов.
    pub async fn snapshot(&self) -> Result<String> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().context("PAC: нет соединения")?;
        let body = self.request(conn, CMD_GET_DEVICES_STATES, &[]).await?;
        Ok(lua_text(body.get(LUA_START_OFFSET..).unwrap_or_default()))
    }

    /// GET_PAC_ERRORS: Lua-текст списка аварий (`alarms[ 0 ] = {…}`; в отличие от снимка — без devices_request_id).
    pub async fn alarms(&self) -> Result<String> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().context("PAC: нет соединения")?;
        let body = self.request(conn, CMD_GET_PAC_ERRORS, &[0]).await?;
        Ok(lua_text(&body))
    }

    /// Lua-команда прошивке: `cmd` — EXEC_DEVICE_COMMAND (прибору) или SET_PAC_ERROR_CMD (аварии). Код результата
    /// прошивки: 0 — применено, иначе команда не выполнилась (соединение при этом живо). Err — сбой связи.
    pub async fn command(&self, cmd: u8, text: &str) -> Result<u16> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().context("PAC: нет соединения")?;
        let body = self.request(conn, cmd, text.as_bytes()).await?;
        Ok(exec_result_code(&body))
    }

    /// Один запрос: кадр, ответ, проверка статуса и номера пакета, распаковка zlib.
    async fn request(&self, conn: &mut Conn, cmd: u8, extra: &[u8]) -> Result<Vec<u8>> {
        conn.pidx = conn.pidx.wrapping_add(1);
        let frame = build_request(conn.pidx, cmd, extra);
        timeout(self.op_timeout, conn.stream.write_all(&frame)).await.context("PAC: таймаут отправки")??;
        let mut hdr = [0u8; RESPONSE_HEADER_LEN];
        timeout(self.op_timeout, conn.stream.read_exact(&mut hdr))
            .await
            .context("PAC: таймаут чтения")?
            .context("PAC: соединение закрыто до получения ответа")?;
        if hdr[0] != NET_ID {
            bail!("PAC: неверный заголовок ответа {hdr:?}");
        }
        let len = (usize::from(hdr[3]) << 8) | usize::from(hdr[4]);
        let mut body = vec![0u8; len];
        timeout(self.op_timeout, conn.stream.read_exact(&mut body))
            .await
            .context("PAC: таймаут чтения")?
            .context("PAC: соединение закрыто до получения ответа")?;
        if hdr[1] == STATUS_ERROR {
            bail!("PAC вернул статус ошибки на команду {cmd}");
        }
        if hdr[2] != conn.pidx {
            bail!("PAC: рассинхрон пакета (ждали pidx={}, пришёл {})", conn.pidx, hdr[2]);
        }
        if body.is_empty() {
            Ok(body)
        } else {
            inflate(&body).context("PAC: ошибка zlib-распаковки ответа")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pac_name_is_taken_from_the_handshake_text() {
        let info = "protocol_version = 104; PAC_name = \"BN1-МСА1\"; is_reset_params = 0;params_CRC=32634;\n";
        assert_eq!(parse_pac_name(info).as_deref(), Some("BN1-МСА1"));
        assert_eq!(parse_pac_name(r#"PAC_name="a \"b\" \\ c""#).as_deref(), Some(r#"a "b" \ c"#));
        assert_eq!(parse_pac_name("protocol_version = 104;"), None);
        assert_eq!(parse_pac_name("PAC_name = 5;"), None, "не строка — имени нет");
        assert_eq!(parse_pac_name("PAC_name = \"без конца"), None);
    }
}
