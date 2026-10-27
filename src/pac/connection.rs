//! Одно TCP-соединение с PAC-контроллером: сокет + свой Lua-стейт.
//!
//! Цикл: [`PacConnection::connect`] (с приветствием PAC) → handshake (версия/имя) →
//! [`PacConnection::poll_states`] (снимок всех приборов) → [`PacConnection::read_value`] по
//! тегам. Запись — `exec_command`. Аварии — [`PacConnection::alarms_text`] и [`PacConnection::exec_alarm_command`]
//! (тем же соединением). Первый запрос шлём сразу: ptusa рвёт соединение, если после приветствия клиент молчит дольше
//! ~300 мс.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::warn;

use super::lua::{self, PacLua};
use super::protocol::*;
use crate::model::TagValue;

/// Ответ получен целиком, но негоден (статус ошибки, не распаковался): соединение при этом в порядке — следующий запрос
/// идёт как обычно. Остальные ошибки запроса (таймаут, обрыв, чужой номер пакета) оставляют поток в неизвестном
/// состоянии — соединение надо открыть заново.
#[derive(Debug)]
struct BadAnswer(String);

impl std::fmt::Display for BadAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BadAnswer {}

/// Ошибка запроса не нарушила соединение (ответ пришёл целиком, но негоден) — его можно использовать дальше.
pub fn connection_intact(e: &anyhow::Error) -> bool {
    e.downcast_ref::<BadAnswer>().is_some()
}

/// Ошибка запроса — обрыв сокета (закрыт, сброшен), а не таймаут или негодный ответ: о такой ошибке сообщит опрос.
pub fn link_lost(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.is::<std::io::Error>())
}

/// Соединение с PAC: сокет, Lua-стейт и счётчик пакетов запроса.
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
        // Без этого первые байты приветствия читались бы как заголовок ответа («PAC a»).
        let mut banner = [0u8; BANNER.len()];
        conn.read_exact(&mut banner).await?;
        if banner != BANNER {
            bail!("PAC: нет приветствия 'PAC accept', пришло {:?}", String::from_utf8_lossy(&banner));
        }
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

    /// Имя, которым PAC назвал себя в handshake (`PAC_name`).
    pub fn pac_name(&self) -> Option<String> {
        self.lua.pac_name()
    }

    /// Значение поля прибора из снимка `t[device][field]`.
    pub fn read_value(&self, device: &str, field: &str, data_type: &str) -> Option<TagValue> {
        self.lua.read(device, field, data_type)
    }

    /// EXEC_DEVICE_COMMAND; текст команды — [`lua::command_text`] (индекс массива отдельным аргументом, `RECMAN[n]` в
    /// своём формате). Возвращает код результата PAC (LE16): 0 — применено, иначе команда не выполнилась (соединение при
    /// этом живо). `Err` — сбой связи или значение, которое нельзя записать командой.
    pub async fn exec_command(&mut self, device: &str, field: &str, value: &TagValue) -> Result<u16> {
        let cmd = lua::command_text(device, field, value)?;
        let result = self.request(CMD_EXEC_DEVICE_COMMAND, cmd.as_bytes()).await?;
        Ok(exec_result_code(&result))
    }

    /// GET_PAC_ERRORS: Lua-текст списка аварий (тело — без смещения `devices_request_id`, в отличие от снимка).
    /// Разбор — [`super::alarms::AlarmsReader`], уже без соединения.
    pub async fn alarms_text(&mut self) -> Result<String> {
        if !self.handshaked {
            self.handshake().await?;
        }
        let body = self.request(CMD_GET_PAC_ERRORS, &[0]).await?;
        Ok(lua_text(&body).into_owned())
    }

    /// SET_PAC_ERROR_CMD; текст — [`super::alarms::command_text`] (только целые). Код результата PAC (LE16): 0 —
    /// выполнено, иначе ошибка Lua в прошивке. `Err` — сбой обмена.
    pub async fn exec_alarm_command(&mut self, text: &str) -> Result<u16> {
        let result = self.request(CMD_SET_PAC_ERROR_CMD, text.as_bytes()).await?;
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
        if hdr[0] != NET_ID {
            bail!("PAC: неверный заголовок ответа {:?}", hdr);
        }
        let (status, rpidx) = (hdr[1], hdr[2]);
        let len = ((hdr[3] as usize) << 8) | hdr[4] as usize;
        let mut body = vec![0u8; len];
        self.read_exact(&mut body).await?;
        if rpidx != self.pidx {
            bail!("PAC: рассинхрон пакета (ждали pidx={}, пришёл {rpidx})", self.pidx);
        }
        if status == STATUS_ERROR {
            return Err(BadAnswer(format!("PAC вернул статус ошибки на команду {cmd}")).into());
        }
        if body.is_empty() {
            return Ok(body);
        }
        inflate(&body).map_err(|e| BadAnswer(format!("PAC: ошибка zlib-распаковки ответа: {e}")).into())
    }

    /// Прочитать ровно `buf.len()` байт с таймаутом операции.
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
    use tokio::net::{TcpListener, TcpStream};

    use super::super::protocol::*;

    // Как у ptusa: тела — C-строки с завершающим \0.
    /// Ответ GET_INFO_ON_CONNECT фейкового PAC: версия протокола и имя (тело — C-строка с завершающим \0, как у ptusa).
    pub const INFO: &str = "protocol_version = 104; PAC_name = \"FAKE\"; params_CRC=0;\n\0";
    /// Ответ GET_DEVICES_STATES фейкового PAC: два прибора и объект с массивом параметров.
    pub const STATES: &str = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\t}\n\
        t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n\t{\n\tRT_PAR_F=\n\t\t{\n\t\t0, 2.5,\n\t\t},\n\t}\n\0";

    /// Пустой список аварий (ответ 103).
    pub const NO_ALARMS: &str = "alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {  id = 1,\n  }\n";
    /// Ответ 103 вместо списка: тело не сжато (ответ пришёл целиком, но не распаковывается).
    pub const BAD_ZLIB: &str = "<не zlib>";
    /// Ответ 103 вместо списка: статус ошибки.
    pub const STATUS_ERROR_ANSWER: &str = "<статус ошибки>";
    /// Ответ 103 вместо списка: ответа нет (соединение молчит).
    pub const SILENT: &str = "<молчание>";
    /// Ответ 103 вместо списка: PAC закрывает соединение.
    pub const CLOSED: &str = "<обрыв>";

    /// Поднять фейковый PAC на свободном порту. banner=false — без приветствия.
    pub async fn spawn(banner: bool) -> (u16, Arc<Mutex<Vec<String>>>) {
        spawn_with_alarms(banner, Arc::new(Mutex::new(NO_ALARMS.into()))).await
    }

    /// То же, ответ на 103 — текущее значение `alarms` (тест меняет его по ходу; особые ответы — [`BAD_ZLIB`],
    /// [`STATUS_ERROR_ANSWER`], [`SILENT`], [`CLOSED`]). Команды 102 и 104 копятся в возвращённом списке.
    pub async fn spawn_with_alarms(banner: bool, alarms: Arc<Mutex<String>>) -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let seen = commands.clone();
        tokio::spawn(async move {
            // Каждое соединение — своя задача: тест может переподключиться после обрыва.
            while let Ok((s, _)) = listener.accept().await {
                tokio::spawn(serve(s, banner, alarms.clone(), seen.clone()));
            }
        });
        (port, commands)
    }

    /// Одно соединение фейкового PAC: приветствие, затем «запрос — ответ» до обрыва.
    async fn serve(mut s: TcpStream, banner: bool, alarms: Arc<Mutex<String>>, seen: Arc<Mutex<Vec<String>>>) {
        if banner {
            s.write_all(BANNER).await.ok();
        } else {
            s.write_all(&[b's', 12, 1, 0, 0]).await.ok();
        }
        loop {
            let mut hdr = [0u8; REQUEST_HEADER_LEN];
            if s.read_exact(&mut hdr).await.is_err() {
                return;
            }
            let mut payload = vec![0u8; ((hdr[4] as usize) << 8) | hdr[5] as usize];
            if s.read_exact(&mut payload).await.is_err() {
                return;
            }
            let mut status = 12;
            let body: Vec<u8> = match payload[0] {
                CMD_GET_PAC_ERRORS => {
                    let text = alarms.lock().unwrap().clone();
                    match text.as_str() {
                        SILENT => std::future::pending().await,
                        CLOSED => return,
                        BAD_ZLIB => {
                            let raw = text.as_bytes();
                            let mut frame = vec![b's', 12, hdr[3], (raw.len() >> 8) as u8, raw.len() as u8];
                            frame.extend_from_slice(raw);
                            if s.write_all(&frame).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        STATUS_ERROR_ANSWER => {
                            status = STATUS_ERROR;
                            Vec::new()
                        }
                        _ => format!("{text}\0").into_bytes(),
                    }
                }
                CMD_SET_PAC_ERROR_CMD => {
                    let lua = String::from_utf8_lossy(&payload[1..]).to_string();
                    let ok = lua.starts_with("errors_manager:get_instance():set_cmd(");
                    seen.lock().unwrap().push(lua);
                    if ok { vec![0, 0] } else { vec![1, 0] }
                }
                CMD_GET_INFO_ON_CONNECT => INFO.as_bytes().to_vec(),
                CMD_GET_DEVICES_STATES => [&[0u8, 0][..], STATES.as_bytes()].concat(),
                CMD_EXEC_DEVICE_COMMAND => {
                    let lua = String::from_utf8_lossy(&payload[1..]).to_string();
                    let ok = lua.starts_with("__LINE1V0:") || lua.starts_with("__RECMAN[");
                    seen.lock().unwrap().push(lua);
                    if ok { vec![0, 0] } else { vec![1, 0] }
                }
                _ => Vec::new(),
            };
            let packed = if body.is_empty() { body } else { deflate(&body) };
            let mut frame = vec![b's', status, hdr[3], (packed.len() >> 8) as u8, packed.len() as u8];
            frame.extend_from_slice(&packed);
            if s.write_all(&frame).await.is_err() {
                return;
            }
        }
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
    async fn fails_without_banner() {
        let (port, _) = fake::spawn(false).await;
        assert!(PacConnection::connect("127.0.0.1", port, Duration::from_millis(500)).await.is_err());
    }

    /// Запись в элемент массива уходит индексом отдельным аргументом (иначе ptusa отвечает 0 и ничего не меняет), а
    /// менеджер рецептов — в своём формате с двойными кавычками.
    #[tokio::test]
    async fn array_element_and_recipe_manager_commands_have_the_wire_format_of_ptusa() {
        let (port, commands) = fake::spawn(true).await;
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_secs(2)).await.unwrap();
        conn.write_command("LINE1V0", "RT_PAR_F[62]", &TagValue::F64(7.5)).await.unwrap();
        conn.write_command("RECMAN[1]", "CMD", &TagValue::Int(1001)).await.unwrap();
        conn.write_command("RECMAN[2]", "NAME", &TagValue::Text("Сыр".into())).await.unwrap();
        let sent = commands.lock().unwrap().clone();
        assert_eq!(sent[0], "__LINE1V0:set_cmd('RT_PAR_F', 62, 7.5)");
        assert_eq!(sent[1], "__RECMAN[1]:set_cmd( \"CMD\", 1, 1001 )");
        assert_eq!(sent[2], "__RECMAN[2]:set_cmd( \"NAME\", 1, \"Сыр\" )");
        // Недопустимое значение отклоняется до отправки: ни одной лишней команды.
        assert!(conn.write_command("RECMAN[1]", "CMD", &TagValue::Int(-1)).await.is_err());
        assert_eq!(commands.lock().unwrap().len(), 3);
    }

    /// Список аварий и действие над аварией — тем же соединением, что снимок; негодный ответ 103 соединение не рвёт,
    /// молчание — рвёт (поток в неизвестном состоянии).
    #[tokio::test]
    async fn alarms_are_read_and_commanded_over_the_same_connection() {
        use std::sync::{Arc, Mutex};

        use crate::pac::alarms::{AlarmsReader, samples};

        let alarms = Arc::new(Mutex::new(samples::OBJECT.to_string()));
        let (port, commands) = fake::spawn_with_alarms(true, alarms.clone()).await;
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_millis(500)).await.unwrap();
        let reader = AlarmsReader::new().unwrap();
        let (id, list) = reader.parse(&conn.alarms_text().await.unwrap()).unwrap();
        assert_eq!((id, list.len(), list[0].object_number), (50, 1, 2));
        conn.poll_states().await.unwrap();
        assert_eq!(conn.read_value("LINE1V0", "ST", "INT32"), Some(TagValue::Int(1)), "снимок после 103 не сбит");

        let text = crate::pac::alarms::command_text(100, 102, 2, 3);
        assert_eq!(conn.exec_alarm_command(&text).await.unwrap(), 0);
        assert_eq!(conn.exec_alarm_command("set_cmd(1)").await.unwrap(), 1, "ошибка Lua в прошивке — код, не обрыв");
        assert_eq!(commands.lock().unwrap()[0], "errors_manager:get_instance():set_cmd( 100, 102, 2, 3 )");

        for bad in [fake::BAD_ZLIB, fake::STATUS_ERROR_ANSWER] {
            *alarms.lock().unwrap() = bad.into();
            let err = conn.alarms_text().await.unwrap_err();
            assert!(connection_intact(&err), "{bad}: {err:#}");
            conn.poll_states().await.unwrap();
        }
        *alarms.lock().unwrap() = fake::SILENT.into();
        let err = conn.alarms_text().await.unwrap_err();
        assert!(!connection_intact(&err), "таймаут — соединение рассинхронизировано: {err:#}");
        assert!(!link_lost(&err), "таймаут — не обрыв сокета: {err:#}");
        *alarms.lock().unwrap() = fake::CLOSED.into();
        let mut conn = PacConnection::connect("127.0.0.1", port, Duration::from_millis(500)).await.unwrap();
        let err = conn.alarms_text().await.unwrap_err();
        assert!(link_lost(&err) && !connection_intact(&err), "{err:#}");
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
