//! Клиент driver-master одной прошивки ptusa: одно TCP-соединение, запрос → ответ.
//!
//! Опрос (снимок) идёт по одному соединению и по очереди; команды записи клиента OPC UA — по своему, второму (см. `sibling`)
//! в отдельном рантайме: их обработчик синхронный и не должен зависеть от драйвера ввода-вывода основного. Первый запрос шлём сразу после приветствия: ptusa рвёт соединение, если клиент
//! молчит дольше ~300 мс.

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
}

impl PacClient {
    /// Клиент без соединения.
    pub fn new(host: &str, port: u16, op_timeout: Duration) -> Self {
        PacClient { host: host.into(), port, op_timeout, conn: Mutex::new(None), connected: AtomicBool::new(false) }
    }

    /// Новый клиент той же прошивки со своим соединением (для команд записи: см. `server::add_write_callbacks`).
    pub fn sibling(&self) -> PacClient {
        PacClient::new(&self.host, self.port, self.op_timeout)
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
        self.request(&mut conn, CMD_GET_INFO_ON_CONNECT, &[]).await?;
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

    /// EXEC_DEVICE_COMMAND: код результата прошивки (0 — применено, иначе команда не выполнилась;
    /// соединение при этом живо). Err — сбой связи.
    pub async fn command(&self, text: &str) -> Result<u16> {
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().context("PAC: нет соединения")?;
        let body = self.request(conn, CMD_EXEC_DEVICE_COMMAND, text.as_bytes()).await?;
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
