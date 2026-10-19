//! OPC UA-клиент (async-opcua): подключение, пакетное чтение, запись.
//!
//! Подключение напрямую к адресу из конфига, без discovery: у Milo Java-шлюза после
//! discovery клиент уходил на адрес, который анонсирует сервер, и с хоста не подключался
//! (`opc.tcp://simulator:4840`). Политика безопасности — None, как у стенда.
//!
//! Чтение — пачками по [`READ_CHUNK`] узлов: по умолчанию клиент декодирует массивы не
//! длиннее 1000 элементов, а у реальных серверов бывает MaxNodesPerRead.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use opcua::client::{Client, ClientBuilder, IdentityToken, Session};
use opcua::crypto::SecurityPolicy;
pub use opcua::types::ReadValueId;
use opcua::types::{
    AttributeId, DataValue, EndpointDescription, MessageSecurityMode, NodeId, NumericRange, StatusCode,
    TimestampsToReturn, UAString, UserTokenPolicy, Variant, WriteValue,
};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::info;

use crate::model::{self, OpcMode, OpcPolicy, OpcSecurity, Quality, TagValue, Timestamp};

/// Узлов в одном запросе чтения.
pub const READ_CHUNK: usize = 500;

/// Живое соединение с OPC UA-сервером: сессия и задача её цикла событий. Закрывается [`OpcConnection::close`].
pub struct OpcConnection {
    session: Arc<Session>,
    event_loop: JoinHandle<StatusCode>,
    _client: Client,
    op_timeout: Duration,
}

/// Сколько ждать TCP-подключения при выборе адреса из нескольких.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Адрес из `opc.tcp://host:port/путь`, который стоит отдать клиенту OPC UA.
///
/// Клиент async-opcua берёт ПЕРВЫЙ адрес из DNS и не пробует остальные: `localhost` → `::1` первым, а
/// сервер на `0.0.0.0` слушает только IPv4 — подключения нет, хотя `127.0.0.1` рядом. Поэтому имя с
/// несколькими адресами проверяем сами: первый доступный — как есть (имя остаётся в URL), иначе
/// подставляем IP-адрес, который ответил. Имя из одного адреса, IP-адрес и чужая схема — без изменений.
pub async fn reachable_url(url: &str, probe_timeout: Duration) -> String {
    let Some(rest) = url.strip_prefix("opc.tcp://") else { return url.to_string() };
    let (authority, path) = rest.find('/').map_or((rest, ""), |i| rest.split_at(i));
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(p) => (h, p),
            Err(_) => return url.to_string(),
        },
        None => (authority, 4840),
    };
    if host.is_empty() || host.starts_with('[') || host.parse::<IpAddr>().is_ok() {
        return url.to_string();
    }
    let Ok(addrs) = tokio::net::lookup_host((host, port)).await else { return url.to_string() };
    let addrs: Vec<SocketAddr> = addrs.collect();
    if addrs.len() < 2 {
        return url.to_string();
    }
    match pick_address(&addrs, probe_timeout).await {
        Some(addr) if addr != addrs[0] => {
            info!("OPC UA {host}: {} недоступен — подключаюсь к {addr}", addrs[0]);
            format!("opc.tcp://{addr}{path}")
        }
        _ => url.to_string(),
    }
}

/// Первый из адресов, к которому удалось открыть TCP-соединение.
async fn pick_address(addrs: &[SocketAddr], probe_timeout: Duration) -> Option<SocketAddr> {
    for &addr in addrs {
        if matches!(timeout(probe_timeout, TcpStream::connect(addr)).await, Ok(Ok(_))) {
            return Some(addr);
        }
    }
    None
}

/// Как подключаться к одному OPC UA-серверу: защита канала, пользователь, хранилище сертификатов.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Политика и режим канала, пользователь.
    pub security: OpcSecurity,
    /// Хранилище сертификатов клиента.
    pub pki_dir: std::path::PathBuf,
    /// Доверять любому сертификату сервера. Для канала без защиты не имеет значения.
    pub trust_server_certs: bool,
}

impl ConnectOptions {
    /// Канал без защиты, анонимно (стенд, тесты).
    pub fn insecure() -> Self {
        ConnectOptions {
            security: OpcSecurity::default(),
            pki_dir: std::env::temp_dir().join("scada-gateway-pki"),
            trust_server_certs: true,
        }
    }

    /// Параметры контроллера: его защита плюс общие для процесса хранилище сертификатов и доверие к серверам.
    pub fn for_controller(security: &OpcSecurity, settings: &crate::config::GatewaySettings) -> Self {
        ConnectOptions {
            security: security.clone(),
            pki_dir: settings.opcua_pki_dir.clone(),
            trust_server_certs: settings.opcua_trust_server_certs,
        }
    }
}

/// Наша политика безопасности → политика async-opcua.
fn security_policy(p: OpcPolicy) -> SecurityPolicy {
    match p {
        OpcPolicy::None => SecurityPolicy::None,
        OpcPolicy::Basic128Rsa15 => SecurityPolicy::Basic128Rsa15,
        OpcPolicy::Basic256 => SecurityPolicy::Basic256,
        OpcPolicy::Basic256Sha256 => SecurityPolicy::Basic256Sha256,
        OpcPolicy::Aes128Sha256RsaOaep => SecurityPolicy::Aes128Sha256RsaOaep,
        OpcPolicy::Aes256Sha256RsaPss => SecurityPolicy::Aes256Sha256RsaPss,
    }
}

/// Наш режим защиты → режим async-opcua.
fn message_mode(m: OpcMode) -> MessageSecurityMode {
    match m {
        OpcMode::None => MessageSecurityMode::None,
        OpcMode::Sign => MessageSecurityMode::Sign,
        OpcMode::SignAndEncrypt => MessageSecurityMode::SignAndEncrypt,
    }
}

impl OpcConnection {
    /// Без защиты и анонимно — как раньше (стенд, тесты).
    pub async fn connect(url: &str, op_timeout: Duration) -> Result<Self> {
        Self::connect_with(url, op_timeout, &ConnectOptions::insecure()).await
    }

    /// Подключиться с заданной защитой: подобрать адрес, выбрать конечную точку, дождаться сессии не дольше `op_timeout`.
    pub async fn connect_with(url: &str, op_timeout: Duration, opts: &ConnectOptions) -> Result<Self> {
        let url = reachable_url(url, op_timeout.min(PROBE_TIMEOUT)).await;
        let url = url.as_str();
        let secure = opts.security.is_secure();
        let mut client = ClientBuilder::new()
            .application_name("SCADA Gateway")
            .application_uri("urn:scada:gateway")
            .product_uri("urn:scada:gateway")
            .pki_dir(&opts.pki_dir)
            // Самоподписанный сертификат клиента (создаётся один раз в pki_dir): для политики
            // None он не нужен, но без него клиент при каждом подключении пишет ошибки в лог. При
            // защищённом канале сервер должен ему доверять (у сервера — своё хранилище).
            .create_sample_keypair(true)
            // Сертификат сервера: без защиты канала проверять нечего; с защитой — только из `trusted/`,
            // если не включено GATEWAY_OPCUA_TRUST_SERVER_CERTS.
            .trust_server_certs(opts.trust_server_certs || !secure)
            // Переподключением управляет опрос шлюза: оборвалась сессия — событие DISCONNECTED
            // и новая попытка по своему расписанию.
            .session_retry_limit(0)
            .request_timeout(op_timeout)
            .max_array_length(100_000)
            .client()
            .map_err(|e| anyhow!("конфигурация OPC UA-клиента: {e:?}"))?;
        let identity = match (&opts.security.username, &opts.security.password) {
            (Some(user), Some(pass)) => IdentityToken::new_user_name(user.clone(), pass.clone()),
            _ => IdentityToken::Anonymous,
        };
        let (session, event_loop) = if secure || opts.security.username.is_some() {
            // Защищённый канал шифрует первое сообщение открытым ключом сервера, а политика токена «логин/пароль»
            // у сервера своя (её идентификатор подставить нельзя), поэтому конечные точки запрашиваем у сервера и
            // берём подходящую по политике и режиму.
            let endpoint: EndpointDescription =
                (url, security_policy(opts.security.policy).to_str(), message_mode(opts.security.mode)).into();
            timeout(op_timeout, client.connect_to_matching_endpoint(endpoint, identity))
                .await
                .with_context(|| {
                    format!("OPC UA {url}: таймаут {} мс при запросе конечных точек", op_timeout.as_millis())
                })?
                .map_err(|e| {
                    anyhow!(
                        "OPC UA {url}: нет конечной точки {:?}/{:?} или к ней не подключиться: {e}",
                        opts.security.policy,
                        opts.security.mode
                    )
                })?
        } else {
            let endpoint: EndpointDescription =
                (url, "None", MessageSecurityMode::None, UserTokenPolicy::anonymous()).into();
            client.connect_to_endpoint_directly(endpoint, identity).map_err(|e| anyhow!("OPC UA {url}: {e}"))?
        };
        let mut event_loop = event_loop.spawn();

        tokio::select! {
            connected = session.wait_for_connection() => {
                if !connected {
                    event_loop.abort();
                    bail!("OPC UA {url}: сессия не установлена");
                }
            }
            status = &mut event_loop => bail!("OPC UA {url}: соединение не установлено ({status:?})"),
            _ = tokio::time::sleep(op_timeout) => {
                event_loop.abort();
                bail!("OPC UA {url}: таймаут {} мс (handshake завис)", op_timeout.as_millis());
            }
        }
        Ok(OpcConnection { session, event_loop, _client: client, op_timeout })
    }

    /// Сессия жива, пока крутится её event loop.
    pub fn is_alive(&self) -> bool {
        !self.event_loop.is_finished()
    }

    /// Прочитать значения узлов (порядок результатов = порядок `nodes`).
    pub async fn read(&self, nodes: &[ReadValueId]) -> Result<Vec<DataValue>> {
        let mut out = Vec::with_capacity(nodes.len());
        for chunk in nodes.chunks(READ_CHUNK) {
            let values = timeout(self.op_timeout, self.session.read(chunk, TimestampsToReturn::Both, 0.0))
                .await
                .with_context(|| format!("таймаут чтения {} мс (сервер завис)", self.op_timeout.as_millis()))?
                .map_err(|e| anyhow!("{e}"))?;
            if values.len() != chunk.len() {
                bail!("сервер вернул {} значений на {} узлов", values.len(), chunk.len());
            }
            out.extend(values);
        }
        Ok(out)
    }

    /// Записать значение в узел; StatusCode — ответ сервера.
    pub async fn write(&self, node_id: NodeId, value: Variant) -> Result<StatusCode> {
        let write = WriteValue {
            node_id,
            attribute_id: AttributeId::Value as u32,
            index_range: NumericRange::None,
            value: DataValue::value_only(value),
        };
        let statuses = timeout(self.op_timeout, self.session.write(&[write]))
            .await
            .context("таймаут записи")?
            .map_err(|e| anyhow!("{e}"))?;
        statuses.into_iter().next().context("сервер не вернул статус записи")
    }

    /// Закрыть сессию (с таймаутом) и остановить её цикл событий. Если цикл событий уже завершился (сервер пропал,
    /// транспорт закрыт), закрывать нечего: `disconnect` ждал бы ответа от мёртвого соединения до конца таймаута
    /// операции (по умолчанию 5 с), и новое подключение откладывалось бы на это время после возврата контроллера.
    pub async fn close(&self) {
        if self.is_alive() {
            let _ = timeout(self.op_timeout, self.session.disconnect()).await;
        }
        self.event_loop.abort();
    }
}

/// Узел чтения значения по строке nodeId (`ns=2;s=6`).
pub fn read_value_id(node_id: &str) -> Result<ReadValueId> {
    Ok(ReadValueId::from(parse_node_id(node_id)?))
}

/// Разобрать `ns=2;s=6` в `NodeId`.
pub fn parse_node_id(node_id: &str) -> Result<NodeId> {
    node_id.parse::<NodeId>().map_err(|_| anyhow!("некорректный nodeId: {node_id}"))
}

/// Значение, качество и момент снятия из DataValue OPC UA.
pub fn reading(dv: &DataValue) -> (Option<TagValue>, Quality, Timestamp) {
    let good = dv.status.map(|s| s.is_good()).unwrap_or(true);
    let value = dv.value.as_ref().and_then(extract_value);
    // Момент снятия значения сервером: sourceTimestamp, иначе serverTimestamp, иначе сейчас.
    let ts = [dv.source_timestamp, dv.server_timestamp]
        .into_iter()
        .flatten()
        .find(|t| !t.is_null())
        .map(|t| Timestamp(t.as_chrono()))
        .unwrap_or_else(|| Timestamp(Utc::now()));
    (value, if good { Quality::Good } else { Quality::Bad }, ts)
}

/// Значение узла → [`TagValue`]; типы, которых у тега быть не может (массивы, структуры), дают `None`.
pub fn extract_value(v: &Variant) -> Option<TagValue> {
    Some(match v {
        Variant::Boolean(b) => TagValue::Bool(*b),
        Variant::SByte(n) => TagValue::Int(*n as i64),
        Variant::Byte(n) => TagValue::Int(*n as i64),
        Variant::Int16(n) => TagValue::Int(*n as i64),
        Variant::UInt16(n) => TagValue::Int(*n as i64),
        Variant::Int32(n) => TagValue::Int(*n as i64),
        Variant::UInt32(n) => TagValue::Int(*n as i64),
        Variant::Int64(n) => TagValue::Int(*n),
        Variant::UInt64(n) => TagValue::Int(*n as i64),
        Variant::Float(f) => TagValue::F32(*f),
        Variant::Double(f) => TagValue::F64(*f),
        Variant::String(s) => TagValue::Text(s.as_ref().to_string()),
        _ => return None,
    })
}

/// Значение команды → Variant по типу тега (сервер проверяет тип строго: Float-узел не
/// примет Double).
pub fn to_variant(data_type: &str, value: &TagValue) -> Result<Variant> {
    let dt = data_type.trim().to_ascii_uppercase();
    Ok(if model::is_bool(&dt) {
        Variant::Boolean(value.as_f64().context("не число/bool")? != 0.0)
    } else if model::is_int(&dt) {
        let n = value.as_f64().context("не число")?;
        Variant::Int32(i32::try_from(n.trunc() as i64).map_err(|_| anyhow!("{n} вне диапазона Int32"))?)
    } else if dt.starts_with("FLOAT") || dt.starts_with("REAL") {
        Variant::Float(value.as_f64().context("не число")? as f32)
    } else if dt.starts_with("DOUBLE") {
        Variant::Double(value.as_f64().context("не число")?)
    } else if model::is_string(&dt) {
        Variant::String(UAString::from(match value {
            TagValue::Text(s) => s.clone(),
            other => serde_json::to_string(other).unwrap_or_default(),
        }))
    } else {
        match value {
            TagValue::Bool(b) => Variant::Boolean(*b),
            TagValue::Int(i) => Variant::Int64(*i),
            TagValue::F32(f) => Variant::Float(*f),
            TagValue::F64(f) => Variant::Double(*f),
            TagValue::Text(s) => Variant::String(UAString::from(s.clone())),
        }
    })
}

/// Исход неудачной записи по StatusCode сервера (коды — OPC UA Part 4/6).
pub fn classify_write_status(status: StatusCode) -> &'static str {
    match status.bits() & 0xFFFF_0000 {
        // Bad_NotWritable, Bad_WriteNotSupported, Bad_UserAccessDenied — последним на запись в
        // узел только для чтения отвечают многие серверы (asyncua симулятора в том числе).
        0x803B_0000 | 0x8073_0000 | 0x801F_0000 => "REJECTED_NOT_WRITABLE",
        0x8074_0000 | 0x803C_0000 => "REJECTED_TYPE_MISMATCH", // Bad_TypeMismatch, Bad_OutOfRange
        _ => "FAILED_WRITE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_by_tag_type() {
        assert_eq!(to_variant("INT32", &TagValue::F64(3.9)).unwrap(), Variant::Int32(3));
        assert_eq!(to_variant("FLOAT", &TagValue::Int(2)).unwrap(), Variant::Float(2.0));
        assert_eq!(to_variant("DOUBLE", &TagValue::Int(2)).unwrap(), Variant::Double(2.0));
        assert_eq!(to_variant("BOOLEAN", &TagValue::Int(1)).unwrap(), Variant::Boolean(true));
        assert!(to_variant("INT32", &TagValue::Text("x".into())).is_err());
        assert!(to_variant("INT32", &TagValue::F64(1e12)).is_err());
    }

    #[test]
    fn extract_like_milo() {
        assert_eq!(extract_value(&Variant::UInt32(7)), Some(TagValue::Int(7)));
        assert_eq!(extract_value(&Variant::Float(64.7)), Some(TagValue::F32(64.7)));
        assert_eq!(extract_value(&Variant::String(UAString::from("REC"))), Some(TagValue::Text("REC".into())));
    }

    #[test]
    fn write_status_classification() {
        assert_eq!(classify_write_status(StatusCode::from(0x803B_0000u32)), "REJECTED_NOT_WRITABLE");
        assert_eq!(classify_write_status(StatusCode::from(0x801F_0000u32)), "REJECTED_NOT_WRITABLE");
        assert_eq!(classify_write_status(StatusCode::from(0x8074_0000u32)), "REJECTED_TYPE_MISMATCH");
        assert_eq!(classify_write_status(StatusCode::from(0x8002_0000u32)), "FAILED_WRITE");
    }

    /// Первый адрес недоступен (на `::1` никто не слушает), второй отвечает: выбираем второй.
    #[tokio::test]
    async fn first_reachable_address_is_picked() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (dead, alive): (SocketAddr, SocketAddr) =
            (format!("[::1]:{port}").parse().unwrap(), listener.local_addr().unwrap());
        assert_eq!(pick_address(&[dead, alive], Duration::from_secs(1)).await, Some(alive));
        assert_eq!(pick_address(&[alive, dead], Duration::from_secs(1)).await, Some(alive));
        assert_eq!(pick_address(&[dead], Duration::from_millis(300)).await, None);
    }

    #[tokio::test]
    async fn urls_without_a_name_with_several_addresses_are_unchanged() {
        let t = Duration::from_millis(300);
        for url in [
            "opc.tcp://127.0.0.1:4840",
            "opc.tcp://[::1]:4840/x",
            "opc.tcp://host:notaport",
            "http://localhost:4840",
            "opc.tcp://:4840",
        ] {
            assert_eq!(reachable_url(url, t).await, url);
        }
    }

    /// `localhost` (на машине это `::1` и/или `127.0.0.1`) при слушающем IPv4-сокете даёт адрес, к
    /// которому действительно можно подключиться, с тем же портом и путём.
    #[tokio::test]
    async fn localhost_resolves_to_a_connectable_url() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = reachable_url(&format!("opc.tcp://localhost:{port}/srv"), Duration::from_secs(1)).await;
        assert!(
            url == format!("opc.tcp://localhost:{port}/srv") || url == format!("opc.tcp://127.0.0.1:{port}/srv"),
            "{url}"
        );
        let host_port = url.trim_start_matches("opc.tcp://").split('/').next().unwrap().to_string();
        assert!(TcpStream::connect(host_port).await.is_ok(), "{url}");
    }

    #[test]
    fn node_id_parsing() {
        assert!(parse_node_id("ns=2;s=6").is_ok());
        assert!(parse_node_id("pac:385").is_err());
    }
}
