//! OPC UA-сервер фасада и цикл опроса прошивки.
//!
//! Узлы получают значения из снимка раз в период опроса; запись клиента идёт командой прошивке из обработчика записи
//! (синхронного) — по отдельному соединению и в отдельном рантайме, см. `add_write_callbacks` (нужен многопоточный рантайм tokio).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use opcua::server::address_space::{ObjectBuilder, VariableBuilder};
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use opcua::server::{Server, ServerBuilder, ServerHandle};
use opcua::types::{DataTypeId, DataValue, DateTime, NodeId, ObjectId, StatusCode, Variant};
use tracing::{info, warn};

use crate::channel::{Channel, Converted, Kind, command_text, convert};
use crate::pac::PacClient;
use crate::security::{Security, warn_if_expiring};
use crate::snapshot::Snapshot;

/// Пространство имён узлов станции; обязано получить индекс 2 (адреса узлов в базе каналов — `ns=2;s=…`).
pub const NAMESPACE_URI: &str = "urn:savushkin:ptusa";
/// URI приложения сервера (он же первое альтернативное имя его сертификата).
pub const APPLICATION_URI: &str = "urn:savushkin:ptusa-opcua";
/// Как часто писать в журнал сводку (снимки, команды, каналы без значения).
const REPORT_EVERY: Duration = Duration::from_secs(60);

/// Узел с именем прошивки (`PAC_name`) и узел «есть связь с прошивкой» (`ns=2;s=…`).
pub const SYSTEM_PAC_NAME: &str = "SYSTEM.PAC_NAME";
/// См. [`SYSTEM_PAC_NAME`].
pub const SYSTEM_LINK_OK: &str = "SYSTEM.LINK_OK";
/// Счётчики работы фасада для сводки в журнале.
#[derive(Default)]
pub struct Stats {
    /// Снимков прошивки получено.
    pub polls: AtomicU64,
    /// Команд записи принято от клиентов.
    pub commands: AtomicU64,
    /// Из них отклонено прошивкой.
    pub rejected: AtomicU64,
}

/// Кладёт значения и статусы в узлы запущенного сервера.
pub struct Pusher {
    handle: ServerHandle,
    manager: Arc<SimpleNodeManager>,
    ns: u16,
}

impl Pusher {
    /// NodeId канала: `ns=2;s=<прибор>.<поле>`.
    fn node(&self, ch: &Channel) -> NodeId {
        NodeId::new(self.ns, ch.node_name.clone())
    }

    /// Значения каналов (None — нет значения: статус `bad`) в момент `ts`.
    pub fn push(&self, items: &[(&Channel, Option<Converted>)], bad: StatusCode, ts: DateTime) {
        let batch: Vec<(NodeId, DataValue)> = items
            .iter()
            .map(|(ch, value)| {
                let dv = match value {
                    Some(v) => DataValue {
                        value: Some(variant(v)),
                        status: Some(StatusCode::Good),
                        source_timestamp: Some(ts),
                        server_timestamp: Some(ts),
                        ..Default::default()
                    },
                    None => bad_value(bad, ts),
                };
                (self.node(ch), dv)
            })
            .collect();
        // set_values синхронно берёт блокировку адресного пространства, которую на время записи клиента держит и обработчик
        // записи (он ждёт ответа прошивки). Ждать её на рабочем потоке, пока он держит драйвер ввода-вывода, нельзя:
        // ответ прошивки некому принять, и сервер замирает. block_in_place отдаёт драйвер другому потоку.
        tokio::task::block_in_place(|| {
            let _ = self
                .manager
                .set_values(self.handle.subscriptions(), batch.iter().map(|(id, dv)| (id, None, dv.clone())));
        });
    }

    /// Узлы `SYSTEM.PAC_NAME` (имя, которым прошивка назвала себя) и `SYSTEM.LINK_OK` (есть ли связь с прошивкой): по ним
    /// шлюз убеждается, что подключился к нужной станции, и видит связь с прошивкой, а не «все узлы BAD».
    pub fn push_system(&self, name: Option<&str>, link_ok: bool) {
        let ts = DateTime::now();
        let name_value = match name {
            Some(n) => DataValue {
                value: Some(Variant::from(n)),
                status: Some(StatusCode::Good),
                source_timestamp: Some(ts),
                server_timestamp: Some(ts),
                ..Default::default()
            },
            None => bad_value(StatusCode::BadNoData, ts),
        };
        let link = DataValue {
            value: Some(Variant::Boolean(link_ok)),
            status: Some(StatusCode::Good),
            source_timestamp: Some(ts),
            server_timestamp: Some(ts),
            ..Default::default()
        };
        let (name_id, link_id) = (NodeId::new(self.ns, SYSTEM_PAC_NAME), NodeId::new(self.ns, SYSTEM_LINK_OK));
        tokio::task::block_in_place(|| {
            let _ = self.manager.set_values(
                self.handle.subscriptions(),
                [(&name_id, None, name_value), (&link_id, None, link)].into_iter(),
            );
        });
    }

    /// У всех каналов один и тот же плохой статус.
    pub fn push_all_bad(&self, channels: &[Channel], status: StatusCode) {
        let ts = DateTime::now();
        let items: Vec<(&Channel, Option<Converted>)> = channels.iter().map(|c| (c, None)).collect();
        self.push(&items, status, ts);
    }
}

/// Значение с плохим статусом. Пустое значение (`Empty`) нужно сервису чтения async-opcua: без значения
/// он статус в ответ не кладёт, и узел выглядел бы хорошим.
fn bad_value(status: StatusCode, ts: DateTime) -> DataValue {
    DataValue {
        value: Some(Variant::Empty),
        status: Some(status),
        source_timestamp: Some(ts),
        server_timestamp: Some(ts),
        ..Default::default()
    }
}

/// Значение канала → вариант узла OPC UA (тип совпадает с типом канала: сервер проверяет его строго).
fn variant(v: &Converted) -> Variant {
    match v {
        Converted::Int(i) => Variant::Int32(*i),
        Converted::Float(f) => Variant::Float(*f),
        Converted::Text(s) => Variant::from(s.as_str()),
        Converted::Bool(b) => Variant::Boolean(*b),
    }
}

/// Значение узла до первого снимка (оно помечается статусом «ждём данные»).
fn initial(kind: Kind) -> Variant {
    match kind {
        Kind::Int32 => Variant::Int32(0),
        Kind::Float => Variant::Float(0.0),
        Kind::String => Variant::from(""),
        Kind::Boolean => Variant::Boolean(false),
    }
}

/// Тип данных узла по типу канала.
fn data_type_id(kind: Kind) -> DataTypeId {
    match kind {
        Kind::Int32 => DataTypeId::Int32,
        Kind::Float => DataTypeId::Float,
        Kind::String => DataTypeId::String,
        Kind::Boolean => DataTypeId::Boolean,
    }
}

/// Значение записи клиента → скаляр для `set_cmd` (bool → 1/0, число — как есть). None — не число.
fn scalar(v: &Variant) -> Option<String> {
    Some(match v {
        Variant::Boolean(b) => u8::from(*b).to_string(),
        Variant::SByte(i) => i.to_string(),
        Variant::Byte(i) => i.to_string(),
        Variant::Int16(i) => i.to_string(),
        Variant::UInt16(i) => i.to_string(),
        Variant::Int32(i) => i.to_string(),
        Variant::UInt32(i) => i.to_string(),
        Variant::Int64(i) => i.to_string(),
        Variant::UInt64(i) => i.to_string(),
        Variant::Float(f) if f.is_finite() => f.to_string(),
        Variant::Double(f) if f.is_finite() => f.to_string(),
        _ => return None,
    })
}

/// Запись клиента → команда прошивке; её исход — статус записи. Идёт по своему соединению `cmd_pac` (оно открывается
/// при первой команде и после обрыва); `pac` — общий опрос, по нему видно, жива ли связь с прошивкой.
async fn command(ch: &Channel, value: &Variant, pac: &PacClient, cmd_pac: &PacClient, stats: &Stats) -> StatusCode {
    stats.commands.fetch_add(1, Ordering::Relaxed);
    // Строковый канал прошивке командой не пишется (set_cmd принимает только числа).
    let Some(scalar) = scalar(value).filter(|_| ch.kind != Kind::String) else {
        stats.rejected.fetch_add(1, Ordering::Relaxed);
        return StatusCode::BadTypeMismatch;
    };
    if !pac.connected() {
        return StatusCode::BadCommunicationError;
    }
    let text = command_text(ch, &scalar);
    // Соединение команд могло оборваться, пока им не пользовались (прошивку перезапускали, простой): первая же неудача —
    // повод открыть новое и повторить команду один раз. set_cmd значения идемпотентна.
    for attempt in 0..2 {
        if !cmd_pac.connected()
            && let Err(e) = cmd_pac.connect().await
        {
            warn!("команда {text}: нет связи с прошивкой: {e:#}");
            return StatusCode::BadCommunicationError;
        }
        match cmd_pac.command(&text).await {
            Ok(0) => {
                info!("✍ {text}");
                return StatusCode::Good;
            }
            Ok(code) => {
                stats.rejected.fetch_add(1, Ordering::Relaxed);
                warn!("команда {text}: прошивка вернула код {code}");
                return StatusCode::BadInvalidState;
            }
            Err(e) => {
                cmd_pac.close().await;
                if attempt == 1 {
                    warn!("команда {text}: нет связи с прошивкой: {e:#}");
                }
            }
        }
    }
    StatusCode::BadCommunicationError
}

/// Хост и порт из `host:port`.
pub fn parse_bind(bind: &str) -> Result<(String, u16)> {
    let (host, port) =
        bind.rsplit_once(':').with_context(|| format!("адрес привязки {bind:?}: ожидается host:port"))?;
    ensure!(!host.is_empty(), "адрес привязки {bind:?}: пустой хост");
    Ok((host.to_string(), port.parse().with_context(|| format!("адрес привязки {bind:?}: порт"))?))
}

/// Объект `PAC`, объект на прибор и переменная на канал; возвращает число приборов.
fn populate(manager: &SimpleNodeManager, ns: u16, channels: &[Channel]) -> usize {
    let mut devices: HashMap<&str, NodeId> = HashMap::new();
    let mut space = manager.address_space().write();
    let root = NodeId::new(ns, "PAC");
    ObjectBuilder::new(&root, "PAC", "PAC").organized_by(NodeId::from(ObjectId::ObjectsFolder)).insert(&mut *space);
    // Прибор прошивки `SYSTEM` (поле `UP_TIME`…) и служебные узлы фасада — один объект: у них один идентификатор узла.
    let system = NodeId::new(ns, "SYSTEM");
    ObjectBuilder::new(&system, "SYSTEM", "SYSTEM").organized_by(root.clone()).insert(&mut *space);
    devices.insert("SYSTEM", system.clone());
    VariableBuilder::new(&NodeId::new(ns, SYSTEM_PAC_NAME), "PAC_NAME", "PAC_NAME")
        .data_type(DataTypeId::String)
        .value(Variant::from(""))
        .organized_by(system.clone())
        .insert(&mut *space);
    VariableBuilder::new(&NodeId::new(ns, SYSTEM_LINK_OK), "LINK_OK", "LINK_OK")
        .data_type(DataTypeId::Boolean)
        .value(Variant::Boolean(false))
        .organized_by(system)
        .insert(&mut *space);
    for ch in channels {
        let parent = devices
            .entry(ch.device.as_str())
            .or_insert_with(|| {
                let id = NodeId::new(ns, ch.device.clone());
                ObjectBuilder::new(&id, ch.device.clone(), ch.device.clone())
                    .organized_by(root.clone())
                    .insert(&mut *space);
                id
            })
            .clone();
        let id = NodeId::new(ns, ch.node_name.clone());
        // Имя переменной — поле внутри прибора (`ST`, `RT_PAR_F[12]`).
        let browse = ch.node_name.split_once('.').map_or(ch.node_name.as_str(), |(_, f)| f).to_string();
        let mut builder = VariableBuilder::new(&id, browse.clone(), browse)
            .data_type(data_type_id(ch.kind))
            .value(initial(ch.kind))
            .organized_by(parent);
        if ch.writable {
            builder = builder.writable();
        }
        builder.insert(&mut *space);
    }
    devices.len()
}

/// Запись клиента: сначала команда прошивке, статус записи — её исход. Значение узла не трогаем:
/// оно придёт из следующего снимка.
///
/// Обработчик записи синхронный и вызывается под блокировкой адресного пространства. Ждать в нём ответа прошивки через
/// драйвер ввода-вывода основного рантайма нельзя: тот же драйвер держит рабочий поток, которого параллельный запрос
/// чтения останавливает на этой самой блокировке, — ответ некому принять, и сервер замирает (у шлюза — таймауты и
/// «handshake завис»). Поэтому команды идут по отдельному соединению и в отдельном рантайме со своим драйвером, а
/// обработчик ждёт исхода обычным каналом.
fn add_write_callbacks(
    manager: &SimpleNodeManager,
    ns: u16,
    channels: &[Channel],
    pac: &Arc<PacClient>,
    stats: &Arc<Stats>,
) {
    // Рантайм команд живёт до конца процесса (его нельзя уронить из асинхронного контекста).
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("pac-commands")
            .enable_all()
            .build()
            .expect("рантайм команд"),
    ));
    let cmd_pac = Arc::new(pac.sibling());
    for ch in channels.iter().filter(|c| c.writable) {
        let (ch_cb, pac, cmd_pac, stats) = (ch.clone(), pac.clone(), cmd_pac.clone(), stats.clone());
        manager.inner().add_write_callback(NodeId::new(ns, ch.node_name.clone()), move |dv: DataValue, _range| {
            let Some(value) = dv.value else { return StatusCode::BadTypeMismatch };
            let (ch, pac, cmd_pac, stats) = (ch_cb.clone(), pac.clone(), cmd_pac.clone(), stats.clone());
            let (tx, rx) = std::sync::mpsc::channel();
            rt.spawn(async move {
                let _ = tx.send(command(&ch, &value, &pac, &cmd_pac, &stats).await);
            });
            // block_in_place: рабочий поток отдаёт ядро другому, пока ждёт.
            tokio::task::block_in_place(|| rx.recv()).unwrap_or(StatusCode::BadInternalError)
        });
    }
}

/// Собирает сервер с адресным пространством из каналов. Сервер надо запустить (`run`).
///
/// `bind` — на чём слушать; `announce` — адрес, который сервер называет в FindServers (сами
/// конечные точки GetEndpoints строятся из адреса привязки: у async-opcua это один параметр).
pub fn build(
    channels: &[Channel],
    pac: Arc<PacClient>,
    stats: Arc<Stats>,
    announce: &str,
    bind: (String, u16),
    security: &Security,
) -> Result<(Server, Pusher)> {
    let mut builder = ServerBuilder::new()
        .application_name("ptusa (эмулятор мойки) — OPC UA-фасад")
        .application_uri(APPLICATION_URI)
        .product_uri(APPLICATION_URI)
        .host(bind.0)
        .port(bind.1)
        .pki_dir(&security.pki_dir)
        .trust_client_certs(security.trust_all_clients)
        .with_authenticator(security.authenticator());
    // Сертификат выпускается и для стенда без защиты: без него сервер на каждом запуске пишет в журнал ошибку.
    let cert = security.ensure_cert(APPLICATION_URI)?;
    warn_if_expiring(&cert);
    builder = builder.certificate_path(&cert.cert_path).private_key_path(&cert.key_path).create_sample_keypair(false);
    for (policy, mode) in &security.endpoints {
        let ids = security.token_ids(*policy);
        let id = format!("{policy:?}_{mode:?}").to_ascii_lowercase();
        builder =
            builder.add_endpoint(id, ("/", *policy, *mode, &ids.iter().map(String::as_str).collect::<Vec<_>>()[..]));
    }
    for (id, token) in security.user_tokens() {
        builder = builder.add_user_token(id, token);
    }
    let (server, handle) = builder
        .discovery_urls(vec![announce.to_string()])
        .with_node_manager(simple_node_manager(
            NamespaceMetadata { namespace_uri: NAMESPACE_URI.into(), ..Default::default() },
            "ptusa",
        ))
        .build()
        .map_err(|e| anyhow::anyhow!("OPC UA-сервер: {e}"))?;
    let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().context("менеджер узлов")?;
    let ns = handle.get_namespace_index(NAMESPACE_URI).context("namespace")?;
    ensure!(ns == 2, "namespace должен быть 2, а не {ns}: узлы станции — ns=2;s=<прибор>.<поле>");

    let devices = populate(&manager, ns, channels);
    add_write_callbacks(&manager, ns, channels, &pac, &stats);
    let pusher = Pusher { handle, manager, ns };
    pusher.push_all_bad(channels, StatusCode::BadWaitingForInitialData);
    pusher.push_system(None, false);
    info!(
        "адресное пространство: {} каналов ({} на запись), {devices} объектов",
        channels.len(),
        channels.iter().filter(|c| c.writable).count(),
    );
    Ok((server, pusher))
}

/// Цикл опроса: снимок прошивки → узлы; при обрыве связи все узлы `BadCommunicationError`, фасад
/// переподключается сам (пауза 1 → 10 с).
pub struct Bridge {
    pub channels: Arc<Vec<Channel>>,
    pub pac: Arc<PacClient>,
    pub pusher: Pusher,
    pub snapshot: Snapshot,
    pub poll: Duration,
    pub stats: Arc<Stats>,
}

impl Bridge {
    /// Один шаг: снимок → значения узлов. Возвращает, у скольких каналов в снимке нет значения.
    pub async fn poll_once(&self) -> Result<usize> {
        let text = self.pac.snapshot().await?;
        let ts = DateTime::now();
        self.snapshot.load(&text)?;
        let items: Vec<(&Channel, Option<Converted>)> = self
            .channels
            .iter()
            .map(|ch| (ch, self.snapshot.get(&ch.device, &ch.base, ch.index).and_then(|raw| convert(&raw, ch.kind))))
            .collect();
        let missing = items.iter().filter(|(_, v)| v.is_none()).count();
        self.pusher.push(&items, StatusCode::BadNoData, ts);
        self.stats.polls.fetch_add(1, Ordering::Relaxed);
        Ok(missing)
    }

    /// Цикл фасада: снимок прошивки → узлы с заданным периодом; обрыв — все узлы `BadCommunicationError` и повтор с паузой 1 → 10 с.
    pub async fn run(self) {
        let (mut backoff, mut last_report) = (Duration::from_secs(1), Instant::now() - REPORT_EVERY);
        loop {
            let started = Instant::now();
            let step = async {
                if !self.pac.connected() {
                    self.pac.connect().await?;
                    info!(
                        "🟢 связь с прошивкой {}:{} (PAC_name {:?})",
                        self.pac.host,
                        self.pac.port,
                        self.pac.pac_name().unwrap_or_default()
                    );
                    self.pusher.push_system(self.pac.pac_name().as_deref(), true);
                    backoff = Duration::from_secs(1);
                }
                self.poll_once().await
            };
            match step.await {
                Ok(missing) => {
                    if last_report.elapsed() >= REPORT_EVERY {
                        last_report = Instant::now();
                        info!(
                            "снимков {}, команд {} (отклонено {}), каналов без значения {missing}",
                            self.stats.polls.load(Ordering::Relaxed),
                            self.stats.commands.load(Ordering::Relaxed),
                            self.stats.rejected.load(Ordering::Relaxed)
                        );
                    }
                    tokio::time::sleep(self.poll.saturating_sub(started.elapsed())).await;
                }
                Err(e) => {
                    let was = self.pac.connected();
                    self.pac.close().await;
                    if was || backoff == Duration::from_secs(1) {
                        warn!("🔴 нет связи с прошивкой {}:{}: {e:#}", self.pac.host, self.pac.port);
                    }
                    self.pusher.push_all_bad(&self.channels, StatusCode::BadCommunicationError);
                    // Имя — то, что назвала прошивка (чужая тоже): по нему видно, к чему подключились; связи нет.
                    self.pusher.push_system(self.pac.pac_name().as_deref(), false);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_address_is_split() {
        assert_eq!(parse_bind("0.0.0.0:4840").unwrap(), ("0.0.0.0".into(), 4840));
        assert!(parse_bind("4840").is_err());
        assert!(parse_bind(":4840").is_err());
        assert!(parse_bind("host:x").is_err());
    }

    #[test]
    fn write_values_become_command_scalars() {
        assert_eq!(scalar(&Variant::Boolean(true)).as_deref(), Some("1"));
        assert_eq!(scalar(&Variant::Int32(-5)).as_deref(), Some("-5"));
        assert_eq!(scalar(&Variant::Float(7.5)).as_deref(), Some("7.5"));
        assert_eq!(scalar(&Variant::Float(0.1)).as_deref(), Some("0.1"), "f32 печатается как f32, без хвоста double");
        assert_eq!(scalar(&Variant::Double(3.0)).as_deref(), Some("3"));
        assert_eq!(scalar(&Variant::from("x")), None);
        assert_eq!(scalar(&Variant::Float(f32::NAN)), None);
    }
}
