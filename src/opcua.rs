//! OPC UA-клиент (async-opcua): подключение, пакетное чтение, запись.
//!
//! Подключение напрямую к адресу из конфига, без discovery: у Milo Java-шлюза после
//! discovery клиент уходил на адрес, который анонсирует сервер, и с хоста не подключался
//! (`opc.tcp://simulator:4840`). Политика безопасности — None, как у стенда.
//!
//! Чтение — пачками не больше [`READ_CHUNK`] узлов и не больше `MaxNodesPerRead` сервера: на запрос сверх своего
//! предела сервер отвечает `Bad_TooManyOperations` целиком, а клиент async-opcua предел сам не учитывает.
//!
//! После подключения [`OpcConnection::prepare`] разбирает адреса узлов (`nsu=` — по таблице пространств имён сервера) и
//! читает их тип: запись идёт в типе узла, а не в типе из `dataType` тега, — сервер проверяет тип строго.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::Utc;
use opcua::client::{Client, ClientBuilder, IdentityToken, OnSubscriptionNotification, Session};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    AttributeId, EndpointDescription, ExtensionObject, Identifier, MessageSecurityMode, MonitoredItemCreateRequest,
    MonitoringMode, MonitoringParameters, NumericRange, StatusChangeNotification, StatusCode, TimestampsToReturn,
    UAString, UserTokenPolicy, Variant, WriteValue,
};
pub use opcua::types::{DataValue, NodeId, ReadValueId, VariableId};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::model::{self, OpcMode, OpcPolicy, OpcSecurity, Quality, Tag, TagValue, Timestamp};
use crate::opcua_cert::{self, CertSettings};

/// Узлов в одном запросе чтения, если сервер не требует меньше.
pub const READ_CHUNK: usize = 500;

/// Узлов в одной подписке, если сервер не требует меньше.
const MONITORED_ITEMS_PER_SUBSCRIPTION: usize = 5000;
/// Подписка живёт 60 интервалов публикации без Publish-запроса, keep-alive — раз в 10 интервалов.
const SUBSCRIPTION_LIFETIME_COUNT: u32 = 60;
const SUBSCRIPTION_KEEP_ALIVE_COUNT: u32 = 10;
/// Очередь элемента на сервере: значения между двумя публикациями не теряются (старейшие вытесняются при переполнении).
const MONITORED_ITEM_QUEUE: u32 = 10;

/// Что присылает подписка: новое значение узла (номер узла в списке `subscribe`) или смену состояния подписки.
#[derive(Debug)]
pub enum SubscriptionEvent {
    /// Значение узла `index`.
    Data {
        /// Номер узла в списке, переданном в `subscribe`.
        index: usize,
        /// Значение с качеством и временем источника.
        value: DataValue,
    },
    /// Сервер закрыл подписку (код — причина): данные больше не придут, сессию надо пересоздать.
    Closed(StatusCode),
}

/// Созданные подписки.
pub struct Subscription {
    /// Изменения значений и закрытие подписки.
    pub events: tokio::sync::mpsc::UnboundedReceiver<SubscriptionEvent>,
    /// Узлы (номер в списке и код), которые сервер отказался взять в подписку.
    pub refused: Vec<(usize, StatusCode)>,
    /// Сколько подписок создано.
    pub subscriptions: usize,
    /// Интервал публикации, мс.
    pub publishing_ms: f64,
}

/// Передача уведомлений подписки в канал цикла опроса.
struct Forward {
    tx: tokio::sync::mpsc::UnboundedSender<SubscriptionEvent>,
}

impl OnSubscriptionNotification for Forward {
    fn on_data_value(&mut self, value: DataValue, item: &opcua::client::MonitoredItem) {
        let index = item.client_handle().saturating_sub(1) as usize;
        let _ = self.tx.send(SubscriptionEvent::Data { index, value });
    }

    fn on_subscription_status_change(&mut self, notification: StatusChangeNotification) {
        let _ = self.tx.send(SubscriptionEvent::Closed(notification.status));
    }
}

/// Живое соединение с OPC UA-сервером: сессия и задача её цикла событий. Закрывается [`OpcConnection::close`].
pub struct OpcConnection {
    session: Arc<Session>,
    event_loop: JoinHandle<StatusCode>,
    _client: Client,
    op_timeout: Duration,
    /// Адрес сервера (для журнала).
    url: String,
    /// Узлов в запросе чтения: не больше [`READ_CHUNK`] и `MaxNodesPerRead` сервера; на `Bad_TooManyOperations`
    /// уменьшается вдвое и остаётся таким до конца сессии.
    chunk: AtomicUsize,
    /// Таблица пространств имён сервера (`NamespaceArray`): по ней адрес `nsu=<URI>;…` переводится в индекс.
    namespaces: Vec<String>,
    /// Узлы тегов, разобранные в [`OpcConnection::prepare`]: адрес из конфигурации → узел и его тип.
    nodes: HashMap<String, NodeInfo>,
    /// Пределы подписок сервера (`MaxMonitoredItemsPerCall`, `MaxMonitoredItemsPerSubscription`); `None`/0 — не названы.
    max_items_per_call: Option<u32>,
    max_items_per_subscription: Option<u32>,
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
    /// Сертификат клиента и каталог PKI (там же `trusted/` и `rejected/` для сертификатов серверов).
    pub cert: CertSettings,
    /// Доверять любому сертификату сервера. Для канала без защиты не имеет значения.
    pub trust_server_certs: bool,
}

impl ConnectOptions {
    /// Канал без защиты, анонимно (стенд, тесты).
    pub fn insecure() -> Self {
        ConnectOptions {
            security: OpcSecurity::default(),
            cert: CertSettings::in_dir(std::env::temp_dir().join("scada-gateway-pki")),
            trust_server_certs: true,
        }
    }

    /// Параметры контроллера: его защита плюс общие для процесса хранилище сертификатов и доверие к серверам.
    pub fn for_controller(security: &OpcSecurity, settings: &crate::config::GatewaySettings) -> Self {
        ConnectOptions {
            security: security.clone(),
            cert: settings.opcua_cert.clone(),
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
        // Сертификат клиента: свой из настроек или самоподписанный (выпускается один раз, под блокировкой — контроллеры
        // подключаются одновременно). Для канала None он не нужен, но без него клиент на каждом подключении пишет
        // ошибки в журнал. При защищённом канале сервер должен ему доверять (у сервера — своё хранилище).
        let cert_settings = opts.cert.clone();
        let cert = match tokio::task::spawn_blocking(move || opcua_cert::ensure(&cert_settings)).await? {
            Ok(cert) => Some(cert),
            Err(e) if secure => return Err(e.context(format!("OPC UA {url}: нет сертификата клиента"))),
            Err(_) => None,
        };
        let mut builder = ClientBuilder::new()
            .application_name(opcua_cert::APPLICATION_NAME)
            .application_uri(&opts.cert.application_uri)
            .product_uri(&opts.cert.application_uri)
            .pki_dir(&opts.cert.pki_dir)
            .create_sample_keypair(false);
        if let Some(cert) = &cert {
            builder = builder.certificate_path(&cert.cert_path).private_key_path(&cert.key_path);
        }
        let mut client = builder
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
        Ok(OpcConnection {
            session,
            event_loop,
            _client: client,
            op_timeout,
            url: url.to_string(),
            chunk: AtomicUsize::new(READ_CHUNK),
            namespaces: Vec::new(),
            nodes: HashMap::new(),
            max_items_per_call: None,
            max_items_per_subscription: None,
        })
    }

    /// Сессия жива, пока крутится её event loop.
    pub fn is_alive(&self) -> bool {
        !self.event_loop.is_finished()
    }

    /// Узлов в одном запросе чтения сейчас.
    pub fn read_chunk(&self) -> usize {
        self.chunk.load(Ordering::Relaxed)
    }

    /// Прочитать атрибуты узлов (порядок результатов = порядок `nodes`) пачками не больше [`Self::read_chunk`].
    pub async fn read(&self, nodes: &[ReadValueId]) -> Result<Vec<DataValue>> {
        let mut out = Vec::with_capacity(nodes.len());
        let mut rest = nodes;
        while !rest.is_empty() {
            let size = self.read_chunk().clamp(1, rest.len());
            let chunk = &rest[..size];
            let result = timeout(self.op_timeout, self.session.read(chunk, TimestampsToReturn::Both, 0.0))
                .await
                .with_context(|| format!("таймаут чтения {} мс (сервер завис)", self.op_timeout.as_millis()))?;
            match result {
                Ok(values) => {
                    if values.len() != chunk.len() {
                        bail!("сервер вернул {} значений на {} узлов", values.len(), chunk.len());
                    }
                    out.extend(values);
                    rest = &rest[size..];
                }
                // Сервер не принимает столько узлов в одном запросе (предел не назвал или назвал больше, чем принимает):
                // пачку вдвое меньше и повторить. Новый размер остаётся и для следующих чтений этой сессии.
                Err(e) if same_status(e.status(), StatusCode::BadTooManyOperations) && size > 1 => {
                    let smaller = size / 2;
                    let before = self.chunk.fetch_min(smaller, Ordering::Relaxed);
                    if before > smaller {
                        warn!(
                            "OPC UA {}: сервер не принимает {size} узлов в одном запросе — читаю по {smaller}",
                            self.url
                        );
                    }
                }
                Err(e) => bail!("{e}"),
            }
        }
        Ok(out)
    }

    /// Разобрать узлы тегов и узнать о них то, что нужно для чтения и записи: предел узлов в запросе, таблицу
    /// пространств имён (для `nsu=`), тип значения и размерность каждого узла. Вызывается после подключения, до опроса.
    ///
    /// Ошибка — только если сервер перестал отвечать (таймаут, сессия закрыта): это обрыв, как неудачное подключение.
    /// Если сервер отказал в чтении служебных узлов или атрибутов, шлюз работает как без проверки: пачки по
    /// [`READ_CHUNK`], запись по `dataType` тега.
    pub async fn prepare(&mut self, tags: &[Arc<Tag>]) -> Result<PreparedNodes> {
        let mut report = NodeReport::default();
        let server = [
            VariableId::Server_NamespaceArray,
            VariableId::Server_ServerCapabilities_OperationLimits_MaxNodesPerRead,
            VariableId::Server_ServerCapabilities_OperationLimits_MaxMonitoredItemsPerCall,
            VariableId::Server_ServerCapabilities_MaxMonitoredItemsPerSubscription,
        ]
        .map(|id| ReadValueId::from(NodeId::from(id)));
        match self.read(&server).await {
            Ok(values) => {
                self.namespaces = string_array(values[0].value.as_ref());
                report.max_nodes_per_read = match values[1].value {
                    Some(Variant::UInt32(n)) if values[1].status().is_good() => Some(n),
                    _ => None,
                };
                let limit_of = |dv: &DataValue| match dv.value {
                    Some(Variant::UInt32(n)) if dv.status().is_good() && n > 0 => Some(n),
                    _ => None,
                };
                self.max_items_per_call = limit_of(&values[2]);
                self.max_items_per_subscription = limit_of(&values[3]);
                // 0 — сервер предела не ставит.
                if let Some(limit) = report.max_nodes_per_read.filter(|&n| n > 0) {
                    self.chunk.fetch_min(limit as usize, Ordering::Relaxed);
                }
            }
            Err(e) if self.is_fatal(&e) => return Err(e.context("чтение сведений о сервере")),
            Err(e) => warn!("OPC UA {}: сведения о сервере не прочитаны ({e:#}) — пачки по {READ_CHUNK}", self.url),
        }

        let mut prepared = PreparedNodes::default();
        let mut resolved: Vec<(Arc<Tag>, NodeId)> = Vec::with_capacity(tags.len());
        for tag in tags {
            match resolve_node_id(&tag.node_id, &self.namespaces) {
                Ok(id) => resolved.push((tag.clone(), id)),
                Err(e) => {
                    report.unresolved.push(format!("{} ({}): {e:#}", tag.name, tag.node_id));
                    prepared.unresolved.push(tag.clone());
                }
            }
        }

        let attributes: Vec<ReadValueId> = resolved
            .iter()
            .flat_map(|(_, id)| {
                [AttributeId::DataType, AttributeId::ValueRank].map(|attribute| ReadValueId::new(id.clone(), attribute))
            })
            .collect();
        let described = match self.read(&attributes).await {
            Ok(values) => Some(values),
            Err(e) if self.is_fatal(&e) => return Err(e.context("чтение типов узлов")),
            Err(e) => {
                warn!("OPC UA {}: типы узлов не прочитаны ({e:#}) — запись по dataType тегов", self.url);
                report.types_unknown = true;
                None
            }
        };
        for (k, (tag, id)) in resolved.into_iter().enumerate() {
            let info = match &described {
                Some(values) => describe(&tag, id, &values[2 * k], &values[2 * k + 1], &mut report),
                None => NodeInfo { id, kind: None, array: false },
            };
            prepared.nodes.push(ReadValueId::from(info.id.clone()));
            self.nodes.insert(tag.node_id.clone(), info);
            prepared.tags.push(tag);
        }
        report.read_chunk = self.read_chunk();
        prepared.report = report;
        Ok(prepared)
    }

    /// Подписать узлы: `sampling_ms[i]` — период опроса узла сервером. Узлы делятся по подпискам и запросам в пределах
    /// сервера (`MaxMonitoredItemsPerSubscription`, `MaxMonitoredItemsPerCall`); client handle узла — его номер в `nodes` плюс 1.
    /// Изменения (и первое значение каждого узла) приходят в [`Subscription::events`]. Ошибка — подписка не создана
    /// (шлюз тогда опрашивает контроллер запросами Read); узлы, которые сервер отказался взять, — в [`Subscription::refused`].
    pub async fn subscribe(
        &self,
        nodes: &[ReadValueId],
        sampling_ms: &[f64],
        publishing_ms: f64,
    ) -> Result<Subscription> {
        ensure!(nodes.len() == sampling_ms.len(), "периодов опроса {} на {} узлов", sampling_ms.len(), nodes.len());
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        // Размеры подписки и запроса берутся из названных сервером пределов, а если сервер их не назвал (или назвал больше, чем
        // принимает) — по отказу `BadTooManyMonitoredItems`: запрос вдвое меньше, а полная подписка закрывается и открывается следующая.
        let mut per_subscription =
            self.max_items_per_subscription.map_or(usize::MAX, |n| n as usize).min(MONITORED_ITEMS_PER_SUBSCRIPTION);
        let mut per_call =
            self.max_items_per_call.map_or(READ_CHUNK, |n| n as usize).min(READ_CHUNK).min(per_subscription);
        let mut refused = Vec::new();
        let mut subscriptions = 0usize;
        let mut current: Option<(u32, usize)> = None; // (подписка, узлов в ней)
        let mut next = 0usize;
        while next < nodes.len() {
            let (id, in_subscription) = match current {
                Some((id, n)) if n < per_subscription => (id, n),
                _ => {
                    let id = timeout(
                        self.op_timeout,
                        self.session.create_subscription(
                            Duration::from_millis(publishing_ms.max(1.0) as u64),
                            SUBSCRIPTION_LIFETIME_COUNT,
                            SUBSCRIPTION_KEEP_ALIVE_COUNT,
                            0,
                            0,
                            true,
                            Forward { tx: tx.clone() },
                        ),
                    )
                    .await
                    .context("таймаут создания подписки")?
                    .map_err(|e| anyhow!("подписка не создана: {e}"))?;
                    subscriptions += 1;
                    current = Some((id, 0));
                    (id, 0)
                }
            };
            let count = per_call.min(per_subscription - in_subscription).min(nodes.len() - next);
            let items: Vec<MonitoredItemCreateRequest> = (next..next + count)
                .map(|i| MonitoredItemCreateRequest {
                    item_to_monitor: nodes[i].clone(),
                    monitoring_mode: MonitoringMode::Reporting,
                    requested_parameters: MonitoringParameters {
                        client_handle: (i + 1) as u32,
                        sampling_interval: sampling_ms[i],
                        filter: ExtensionObject::null(),
                        queue_size: MONITORED_ITEM_QUEUE,
                        discard_oldest: true,
                    },
                })
                .collect();
            let created =
                timeout(self.op_timeout, self.session.create_monitored_items(id, TimestampsToReturn::Both, items))
                    .await
                    .context("таймаут создания элементов подписки")?;
            match created {
                Ok(created) => {
                    ensure!(created.len() == count, "сервер вернул {} результатов на {count} узлов", created.len());
                    for (k, item) in created.iter().enumerate() {
                        if item.result.status_code.is_bad() {
                            refused.push((next + k, item.result.status_code));
                        }
                    }
                    next += count;
                    current = Some((id, in_subscription + count));
                }
                Err(e) if same_status(e.status(), StatusCode::BadTooManyMonitoredItems) => {
                    if in_subscription > 0 {
                        // В этой подписке места нет: больше в неё не кладём.
                        per_subscription = in_subscription;
                        warn!(
                            "OPC UA {}: сервер принимает в подписку {in_subscription} узлов — открываю следующую",
                            self.url
                        );
                    } else if count > 1 {
                        per_call = count / 2;
                        per_subscription = per_subscription.max(per_call);
                    } else {
                        bail!("сервер не принимает узлы в подписку: {e}");
                    }
                }
                Err(e) if same_status(e.status(), StatusCode::BadTooManyOperations) && count > 1 => {
                    per_call = count / 2
                }
                Err(e) => bail!("элементы подписки не созданы: {e}"),
            }
        }
        Ok(Subscription { events, refused, subscriptions, publishing_ms })
    }

    /// Ошибка чтения означает обрыв: таймаут или закрытая сессия (а не отказ живого сервера).
    fn is_fatal(&self, e: &anyhow::Error) -> bool {
        !self.is_alive() || e.downcast_ref::<tokio::time::error::Elapsed>().is_some()
    }

    /// Узел тега для записи и чтения команды: разобранный при подключении, а если его нет (соединение без
    /// [`Self::prepare`]) — по адресу, без типа (запись по `dataType` тега).
    pub fn node(&self, node_id: &str) -> Result<NodeInfo> {
        if let Some(info) = self.nodes.get(node_id) {
            return Ok(info.clone());
        }
        Ok(NodeInfo { id: resolve_node_id(node_id, &self.namespaces)?, kind: None, array: false })
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

/// Разобрать адрес узла: `ns=<индекс>;…` (или без `ns=` — пространство 0) либо `nsu=<URI>;…` — тогда индекс берётся
/// из таблицы пространств имён сервера `namespaces`. Индекс у сервера может смениться (обновление прошивки, новый
/// модуль), URI — нет.
pub fn resolve_node_id(node_id: &str, namespaces: &[String]) -> Result<NodeId> {
    let Some(rest) = node_id.strip_prefix("nsu=") else { return parse_node_id(node_id) };
    // URI кончается на первой `;`, за которой идёт тип идентификатора: i=, s=, g=, b=.
    let split = rest.match_indices(';').map(|(i, _)| i).find(|&i| {
        let tail = rest.as_bytes();
        matches!(tail.get(i + 1), Some(b'i' | b's' | b'g' | b'b')) && tail.get(i + 2) == Some(&b'=')
    });
    let Some(i) = split else { bail!("некорректный nodeId: {node_id}") };
    let (uri, identifier) = (&rest[..i], &rest[i + 1..]);
    let Some(index) = namespaces.iter().position(|u| u == uri) else {
        bail!("пространства имён {uri} нет на сервере")
    };
    let index = u16::try_from(index).map_err(|_| anyhow!("индекс пространства имён {index} вне диапазона"))?;
    parse_node_id(&format!("ns={index};{identifier}")).map_err(|_| anyhow!("некорректный nodeId: {node_id}"))
}

/// Тот же код состояния без учёта младших битов (в них сервер кладёт подробности).
fn same_status(a: StatusCode, b: StatusCode) -> bool {
    a.bits() & 0xFFFF_0000 == b.bits() & 0xFFFF_0000
}

/// Массив строк из значения узла (`NamespaceArray`); не массив строк — пусто.
fn string_array(value: Option<&Variant>) -> Vec<String> {
    match value {
        Some(Variant::Array(array)) => array
            .values
            .iter()
            .map(|v| match v {
                Variant::String(s) => s.as_ref().to_string(),
                _ => String::new(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Тип узла по ответу сервера на чтение атрибутов `DataType` и `ValueRank`; что шлюзу с узлом не так — в `report`.
fn describe(tag: &Tag, id: NodeId, data_type: &DataValue, value_rank: &DataValue, report: &mut NodeReport) -> NodeInfo {
    let who = format!("{} ({})", tag.name, tag.node_id);
    let status = data_type.status();
    if same_status(status, StatusCode::BadNodeIdUnknown) {
        report.unknown.push(who);
        return NodeInfo { id, kind: None, array: false };
    }
    if same_status(status, StatusCode::BadAttributeIdInvalid) {
        // Атрибута DataType нет только у не-переменных: значение такого узла не прочитать.
        report.unsupported.push(format!("{who}: узел не переменная"));
        return NodeInfo { id, kind: None, array: false };
    }
    let kind = match &data_type.value {
        Some(Variant::NodeId(t)) if status.is_good() => NodeKind::from_data_type(t),
        _ => None,
    };
    // ValueRank ≥ 0 — массив (0 — одна или больше размерностей); −1 — скаляр, −2 и −3 — может быть скаляром.
    let array = matches!(value_rank.value, Some(Variant::Int32(rank)) if rank >= 0);
    if array {
        report.unsupported.push(format!("{who}: массив"));
    } else if let Some(NodeKind::Unsupported(name)) = kind {
        report.unsupported.push(format!("{who}: тип {name}"));
    } else if let Some(kind) = kind
        && type_conflict(&tag.data_type, kind)
    {
        report.mismatched.push(format!("{who}: dataType {}, узел {kind}", tag.data_type));
    }
    NodeInfo { id, kind, array }
}

/// Смысл типа: логический, целый, вещественный, строка.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sense {
    Bool,
    Integer,
    Real,
    Text,
}

/// Смысл `dataType` тега, включая принятые в ПЛК имена (`WORD`, `DINT`, `LREAL`); неизвестное имя — `None`.
fn tag_sense(data_type: &str) -> Option<Sense> {
    let t = data_type.trim().to_ascii_uppercase();
    Some(if t.starts_with("BOOL") {
        Sense::Bool
    } else if model::is_float(&t) || t == "LREAL" {
        Sense::Real
    } else if model::is_string(&t) || t == "WSTRING" {
        Sense::Text
    } else if model::is_int(&t)
        || t.starts_with("UINT")
        || matches!(
            t.as_str(),
            "SINT" | "DINT" | "LINT" | "USINT" | "UDINT" | "ULINT" | "BYTE" | "WORD" | "DWORD" | "LWORD"
        )
    {
        Sense::Integer
    } else {
        return None;
    })
}

/// `dataType` тега по смыслу расходится с типом узла: значения пойдут в типе узла, а не в объявленном.
fn type_conflict(data_type: &str, kind: NodeKind) -> bool {
    match (tag_sense(data_type), kind.sense()) {
        (Some(tag), Some(node)) => tag != node,
        _ => false,
    }
}

/// Встроенный тип значения узла (атрибут `DataType`): в нём сервер ждёт запись и проверяет тип строго.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// `Boolean`.
    Boolean,
    /// `SByte`: −128…127.
    SByte,
    /// `Byte`: 0…255.
    Byte,
    /// `Int16`.
    Int16,
    /// `UInt16`.
    UInt16,
    /// `Int32`.
    Int32,
    /// `UInt32`.
    UInt32,
    /// `Int64`.
    Int64,
    /// `UInt64`.
    UInt64,
    /// `Float`: 32 бита.
    Float,
    /// `Double`: 64 бита.
    Double,
    /// `String`.
    String,
    /// Тип, значения которого шлюз не передаёт: `DateTime`, `Guid`, `ByteString`, `LocalizedText`, структуры…
    Unsupported(&'static str),
}

impl NodeKind {
    /// Тип по атрибуту `DataType` узла. `None` — тип из пространства имён сервера (свой тип ПЛК) или абстрактный
    /// (`Number`, `BaseDataType`): тогда запись идёт по `dataType` тега, как без проверки.
    pub fn from_data_type(id: &NodeId) -> Option<Self> {
        if id.namespace != 0 {
            return None;
        }
        let Identifier::Numeric(n) = id.identifier else { return None };
        Some(match n {
            1 => NodeKind::Boolean,
            2 => NodeKind::SByte,
            3 => NodeKind::Byte,
            4 => NodeKind::Int16,
            5 => NodeKind::UInt16,
            6 => NodeKind::Int32,
            7 => NodeKind::UInt32,
            8 => NodeKind::Int64,
            9 => NodeKind::UInt64,
            10 => NodeKind::Float,
            11 => NodeKind::Double,
            12 => NodeKind::String,
            // Стандартные производные типы: на проводе — значения базового типа.
            290 => NodeKind::Double,                       // Duration
            288 | 289 => NodeKind::UInt32,                 // IntegerId, Counter
            291 | 295 => NodeKind::String,                 // NumericRange, LocaleId
            13 | 294 => NodeKind::Unsupported("DateTime"), // DateTime, UtcTime
            14 => NodeKind::Unsupported("Guid"),
            15 => NodeKind::Unsupported("ByteString"),
            16 => NodeKind::Unsupported("XmlElement"),
            17 | 18 => NodeKind::Unsupported("NodeId"),
            19 => NodeKind::Unsupported("StatusCode"),
            20 => NodeKind::Unsupported("QualifiedName"),
            21 => NodeKind::Unsupported("LocalizedText"),
            22 => NodeKind::Unsupported("Structure"),
            _ => return None,
        })
    }

    /// Смысл типа; у неподдерживаемого — нет.
    fn sense(self) -> Option<Sense> {
        Some(match self {
            NodeKind::Boolean => Sense::Bool,
            NodeKind::Float | NodeKind::Double => Sense::Real,
            NodeKind::String => Sense::Text,
            NodeKind::Unsupported(_) => return None,
            _ => Sense::Integer,
        })
    }

    /// Значение команды → Variant этого типа. Дробная часть у целых отбрасывается (как у целого тега), значение вне
    /// диапазона типа — ошибка, а не обрезка: 300 в `Byte` записалось бы как 44.
    pub fn variant(self, value: &TagValue) -> Result<Variant> {
        Ok(match self {
            NodeKind::Boolean => Variant::Boolean(match value {
                TagValue::Bool(b) => *b,
                other => other.as_f64().context("не число/bool")? != 0.0,
            }),
            NodeKind::SByte => Variant::SByte(self.integer(value, i8::MIN.into(), i8::MAX.into())?),
            NodeKind::Byte => Variant::Byte(self.integer(value, u8::MIN.into(), u8::MAX.into())?),
            NodeKind::Int16 => Variant::Int16(self.integer(value, i16::MIN.into(), i16::MAX.into())?),
            NodeKind::UInt16 => Variant::UInt16(self.integer(value, u16::MIN.into(), u16::MAX.into())?),
            NodeKind::Int32 => Variant::Int32(self.integer(value, i32::MIN.into(), i32::MAX.into())?),
            NodeKind::UInt32 => Variant::UInt32(self.integer(value, u32::MIN.into(), u32::MAX.into())?),
            NodeKind::Int64 => Variant::Int64(self.integer(value, i64::MIN.into(), i64::MAX.into())?),
            NodeKind::UInt64 => Variant::UInt64(self.integer(value, u64::MIN.into(), u64::MAX.into())?),
            NodeKind::Float => {
                let f = value.as_f64().context("не число")?;
                let narrow = f as f32;
                ensure!(narrow.is_finite() || !f.is_finite(), "{f} вне диапазона Float");
                Variant::Float(narrow)
            }
            NodeKind::Double => Variant::Double(value.as_f64().context("не число")?),
            NodeKind::String => Variant::String(UAString::from(text(value))),
            NodeKind::Unsupported(name) => bail!("узел типа {name}: запись не поддерживается"),
        })
    }

    /// Целое значение команды для узла этого типа с диапазоном `min…max`.
    fn integer<T: TryFrom<i128>>(self, value: &TagValue, min: i128, max: i128) -> Result<T> {
        let n = match value {
            TagValue::Int(i) => i128::from(*i),
            other => {
                let f = other.as_f64().context("не число")?;
                ensure!(f.is_finite(), "{f} не конечное число");
                f.trunc() as i128
            }
        };
        ensure!((min..=max).contains(&n), "{n} вне диапазона {self} ({min}…{max})");
        T::try_from(n).map_err(|_| anyhow!("{n} вне диапазона {self}"))
    }
}

impl std::fmt::Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeKind::Unsupported(name) => f.write_str(name),
            other => write!(f, "{other:?}"),
        }
    }
}

/// Узел тега после подключения: разобранный адрес и тип значения, если сервер его сообщил.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    /// Адрес узла (у `nsu=` — с индексом пространства имён этого сервера).
    pub id: NodeId,
    /// Тип значения; `None` — неизвестен, запись по `dataType` тега.
    pub kind: Option<NodeKind>,
    /// Узел — массив: шлюз передаёт только одиночные значения.
    pub array: bool,
}

impl NodeInfo {
    /// Значение команды → Variant для записи в этот узел: в типе узла, а если он неизвестен — по `dataType` тега.
    pub fn variant(&self, data_type: &str, value: &TagValue) -> Result<Variant> {
        ensure!(!self.array, "узел — массив, шлюз записывает только одиночные значения");
        match self.kind {
            Some(kind) => kind.variant(value),
            None => to_variant(data_type, value),
        }
    }
}

/// Итог [`OpcConnection::prepare`]: что читать в цикле опроса и что не так с узлами.
#[derive(Debug, Default)]
pub struct PreparedNodes {
    /// Теги, которые читаются (адрес разобран).
    pub tags: Vec<Arc<Tag>>,
    /// Узлы чтения значений — в порядке `tags`.
    pub nodes: Vec<ReadValueId>,
    /// Теги, адрес которых не разобран (или пространства имён `nsu=` нет на сервере): не читаются, всегда BAD.
    pub unresolved: Vec<Arc<Tag>>,
    /// Что выяснилось об узлах.
    pub report: NodeReport,
}

/// Что выяснилось об узлах тегов при подключении (строки — «тег (адрес): пояснение»).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeReport {
    /// Предел узлов в запросе чтения, названный сервером (`MaxNodesPerRead`; 0 — без предела).
    pub max_nodes_per_read: Option<u32>,
    /// Узлов в запросе чтения у этой сессии.
    pub read_chunk: usize,
    /// Типы узлов не прочитаны (сервер отказал): запись по `dataType` тегов.
    pub types_unknown: bool,
    /// Адрес не разобран или пространства имён `nsu=` нет на сервере: тег не читается.
    pub unresolved: Vec<String>,
    /// Узла нет на сервере (`Bad_NodeIdUnknown`): тег будет BAD.
    pub unknown: Vec<String>,
    /// Значение узла шлюз не передаёт (массив, `DateTime`, структура, не переменная): тег будет BAD.
    pub unsupported: Vec<String>,
    /// `dataType` тега по смыслу не совпадает с типом узла: значения идут в типе узла.
    pub mismatched: Vec<String>,
}

impl NodeReport {
    /// Есть ли что сообщить оператору.
    pub fn has_problems(&self) -> bool {
        !(self.unresolved.is_empty()
            && self.unknown.is_empty()
            && self.unsupported.is_empty()
            && self.mismatched.is_empty())
    }

    /// Для `/api/status` и события: числа и не больше `limit` строк каждого списка.
    pub fn summary(&self, limit: usize) -> Value {
        let list = |v: &[String]| json!({"count": v.len(), "tags": v.iter().take(limit).collect::<Vec<_>>()});
        json!({
            "maxNodesPerRead": self.max_nodes_per_read,
            "readChunk": self.read_chunk,
            "typesUnknown": self.types_unknown,
            "unresolved": list(&self.unresolved),
            "unknownNodes": list(&self.unknown),
            "unsupportedNodes": list(&self.unsupported),
            "typeMismatch": list(&self.mismatched),
        })
    }
}

/// Значение, качество и момент снятия из DataValue OPC UA. Хороший статус без значения, которое шлюз умеет передать
/// (пусто, массив, `DateTime`, структура), — BAD: иначе монитор получил бы `{"value": null, "quality": "GOOD"}`.
pub fn reading(dv: &DataValue) -> (Option<TagValue>, Quality, Timestamp) {
    let value = dv.value.as_ref().and_then(extract_value);
    let good = dv.status.map(|s| s.is_good()).unwrap_or(true) && value.is_some();
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
        // Больше i64::MAX на проводе не передать: отрицательное число было бы неправдой.
        Variant::UInt64(n) => TagValue::Int(i64::try_from(*n).ok()?),
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
        Variant::String(UAString::from(text(value)))
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

/// Значение строкой для строкового узла: строка как есть, остальное — JSON-литерал.
fn text(value: &TagValue) -> String {
    match value {
        TagValue::Text(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
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

    /// `nsu=<URI>` переводится в индекс по таблице сервера; `ns=` и адрес без пространства — как раньше.
    #[test]
    fn namespace_uri_is_resolved_by_the_server_table() {
        let table =
            ["http://opcfoundation.org/UA/", "urn:server", "http://www.siemens.com/simatic-s7-opcua"].map(String::from);
        assert_eq!(
            resolve_node_id("nsu=http://www.siemens.com/simatic-s7-opcua;s=\"DB1\".\"Temp\"", &table).unwrap(),
            NodeId::new(2, "\"DB1\".\"Temp\"")
        );
        assert_eq!(resolve_node_id("nsu=urn:server;i=42", &table).unwrap(), NodeId::new(1, 42u32));
        // `;` в строковом идентификаторе — часть идентификатора, а не конец URI.
        assert_eq!(resolve_node_id("nsu=urn:server;s=a;b", &table).unwrap(), NodeId::new(1, "a;b"));
        assert_eq!(resolve_node_id("ns=2;s=6", &table).unwrap(), NodeId::new(2, "6"));
        assert_eq!(resolve_node_id("i=2255", &[]).unwrap(), NodeId::new(0, 2255u32));
        let missing = resolve_node_id("nsu=urn:other;s=x", &table).unwrap_err().to_string();
        assert!(missing.contains("urn:other"), "{missing}");
        assert!(resolve_node_id("nsu=urn:server", &table).is_err());
        assert!(resolve_node_id("nsu=urn:server;x=1", &table).is_err());
    }

    #[test]
    fn node_kind_by_data_type() {
        let ns0 = |n: u32| NodeId::new(0, n);
        assert_eq!(NodeKind::from_data_type(&ns0(4)), Some(NodeKind::Int16));
        assert_eq!(NodeKind::from_data_type(&ns0(5)), Some(NodeKind::UInt16));
        assert_eq!(NodeKind::from_data_type(&ns0(10)), Some(NodeKind::Float));
        assert_eq!(NodeKind::from_data_type(&ns0(290)), Some(NodeKind::Double)); // Duration
        assert_eq!(NodeKind::from_data_type(&ns0(13)), Some(NodeKind::Unsupported("DateTime")));
        // Абстрактный Number и свой тип сервера — неизвестны: запись по dataType тега.
        assert_eq!(NodeKind::from_data_type(&ns0(26)), None);
        assert_eq!(NodeKind::from_data_type(&NodeId::new(3, 3003u32)), None);
        assert_eq!(NodeKind::from_data_type(&NodeId::new(0, "x")), None);
    }

    #[test]
    fn values_are_written_in_the_node_type_and_range() {
        let v = |kind: NodeKind, value: TagValue| kind.variant(&value);
        assert_eq!(v(NodeKind::Int16, TagValue::Int(-300)).unwrap(), Variant::Int16(-300));
        assert_eq!(v(NodeKind::UInt16, TagValue::F64(65535.9)).unwrap(), Variant::UInt16(65535));
        assert_eq!(v(NodeKind::Byte, TagValue::Int(255)).unwrap(), Variant::Byte(255));
        assert_eq!(v(NodeKind::SByte, TagValue::Bool(true)).unwrap(), Variant::SByte(1));
        assert_eq!(v(NodeKind::Int64, TagValue::Int(i64::MAX)).unwrap(), Variant::Int64(i64::MAX));
        assert_eq!(v(NodeKind::UInt64, TagValue::Int(7)).unwrap(), Variant::UInt64(7));
        assert_eq!(v(NodeKind::UInt32, TagValue::Int(4_000_000_000)).unwrap(), Variant::UInt32(4_000_000_000));
        assert_eq!(v(NodeKind::Boolean, TagValue::Int(2)).unwrap(), Variant::Boolean(true));
        assert_eq!(v(NodeKind::Float, TagValue::Int(2)).unwrap(), Variant::Float(2.0));
        assert_eq!(v(NodeKind::Double, TagValue::F32(0.5)).unwrap(), Variant::Double(0.5));
        assert_eq!(v(NodeKind::String, TagValue::Int(5)).unwrap(), Variant::String(UAString::from("5")));
        // Вне диапазона — ошибка, а не обрезка (300 в Byte записалось бы как 44).
        let err = v(NodeKind::Byte, TagValue::Int(300)).unwrap_err().to_string();
        assert!(err.contains("300") && err.contains("Byte") && err.contains("0…255"), "{err}");
        assert!(v(NodeKind::UInt16, TagValue::Int(-1)).is_err());
        assert!(v(NodeKind::UInt64, TagValue::Int(-1)).is_err());
        assert!(v(NodeKind::Int16, TagValue::F64(1e12)).is_err());
        assert!(v(NodeKind::Float, TagValue::F64(1e300)).is_err());
        assert!(v(NodeKind::Int32, TagValue::Text("x".into())).is_err());
        assert!(v(NodeKind::Unsupported("DateTime"), TagValue::Text("2026-10-21".into())).is_err());
    }

    /// Без типа узла — прежняя запись по dataType тега; в массив не пишем.
    #[test]
    fn unknown_node_type_falls_back_to_the_tag_type() {
        let node = |kind, array| NodeInfo { id: NodeId::new(2, "x"), kind, array };
        assert_eq!(node(None, false).variant("INT32", &TagValue::Int(5)).unwrap(), Variant::Int32(5));
        assert_eq!(node(None, false).variant("FLOAT", &TagValue::Int(5)).unwrap(), Variant::Float(5.0));
        assert_eq!(node(Some(NodeKind::Int16), false).variant("INT32", &TagValue::Int(5)).unwrap(), Variant::Int16(5));
        assert!(node(Some(NodeKind::Float), true).variant("FLOAT", &TagValue::Int(5)).is_err());
    }

    /// Типы, которые по смыслу совпадают (целое с целым), конфликтом не считаются; неизвестное имя не проверяется.
    #[test]
    fn tag_and_node_types_conflict_only_by_meaning() {
        assert!(!type_conflict("INT32", NodeKind::Int16));
        assert!(!type_conflict("WORD", NodeKind::UInt16));
        assert!(!type_conflict("FLOAT", NodeKind::Double));
        assert!(!type_conflict("BOOLEAN", NodeKind::Boolean));
        assert!(!type_conflict("STRING", NodeKind::String));
        assert!(!type_conflict("MYTYPE", NodeKind::Float));
        assert!(type_conflict("INT32", NodeKind::Float));
        assert!(type_conflict("FLOAT", NodeKind::Int32));
        assert!(type_conflict("BOOL", NodeKind::Byte));
        assert!(!type_conflict("FLOAT", NodeKind::Unsupported("DateTime")));
    }

    /// Хороший статус без значения, которое можно передать, — BAD, а не `null` + GOOD.
    #[test]
    fn good_status_without_a_value_is_bad() {
        let good =
            |value: Variant| DataValue { value: Some(value), status: Some(StatusCode::Good), ..Default::default() };
        assert_eq!(reading(&good(Variant::Float(1.5))).1, Quality::Good);
        for v in
            [Variant::Empty, Variant::DateTime(Box::default()), Variant::from(vec![1i32, 2]), Variant::UInt64(u64::MAX)]
        {
            let (value, quality, _) = reading(&good(v.clone()));
            assert_eq!((value, quality), (None, Quality::Bad), "{v:?}");
        }
        assert_eq!(reading(&DataValue::default()).1, Quality::Bad);
        assert_eq!(extract_value(&Variant::UInt64(7)), Some(TagValue::Int(7)));
    }

    #[test]
    fn report_summary_is_capped() {
        let report = NodeReport { unknown: (0..5).map(|i| format!("t{i}")).collect(), ..Default::default() };
        assert!(report.has_problems());
        let s = report.summary(2);
        assert_eq!(s["unknownNodes"]["count"], 5);
        assert_eq!(s["unknownNodes"]["tags"].as_array().unwrap().len(), 2);
        assert!(!NodeReport::default().has_problems());
    }
}
