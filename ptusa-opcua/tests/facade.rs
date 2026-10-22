//! Фасад целиком: фейковый PAC (тот же проводной формат, что у ptusa: приветствие PAC accept, кадры
//! 's',1,1,pidx,len, zlib, тела — C-строки) + настоящий OPC UA-сервер фасада + настоящий OPC UA-клиент.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opcua::client::{ClientBuilder, IdentityToken, Session};
use opcua::types::{
    AttributeId, DataValue, EndpointDescription, MessageSecurityMode, NodeId, NumericRange, ReadValueId, StatusCode,
    TimestampsToReturn, UserTokenPolicy, Variant, WriteValue,
};
use ptusa_opcua::channel::load_channels;
use ptusa_opcua::pac::PacClient;
use ptusa_opcua::server::{Bridge, Stats, build};
use ptusa_opcua::snapshot::Snapshot;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------- фейковый PAC --

/// Прошивка: приборы с полями, снимок `t=…`, `set_cmd` меняет поле; код ≠ 0 для чужих приборов.
#[derive(Default)]
struct PacState {
    devices: Vec<(String, Vec<(String, f64)>)>,
    rt_par_f: Vec<f64>,
    commands: Vec<String>,
}

impl PacState {
    fn new() -> Self {
        let f = |pairs: &[(&str, f64)]| pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect::<Vec<_>>();
        PacState {
            devices: vec![
                ("LINE1V0".into(), f(&[("M", 0.0), ("ST", 0.0)])),
                ("LINE1TE1".into(), f(&[("V", 21.5), ("P_CZ", 0.0)])),
            ],
            rt_par_f: vec![0.0, 0.0, 0.0],
            commands: Vec::new(),
        }
    }

    fn snapshot(&self) -> String {
        let mut s = String::from("t=\n{\n");
        for (name, fields) in &self.devices {
            let body: Vec<String> = fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
            s += &format!("{name}={{{}}},\n", body.join(", "));
        }
        s += "}\n";
        let arr: Vec<String> = self.rt_par_f.iter().map(f64::to_string).collect();
        s += &format!("t.OBJECT1 = t.OBJECT1 or {{}}\nt.OBJECT1=\n{{\nRT_PAR_F=\n{{\n{},\n}},\n}}\n", arr.join(", "));
        s += "t.SYSTEM =\n{\nUP_TIME=\"0 дн. 00:00:20\",\n}\n";
        s
    }

    fn apply(&mut self, cmd: &str) -> u16 {
        self.commands.push(cmd.to_string());
        let re = regex_lite(cmd);
        let Some((dev, field, idx, value)) = re else { return 1 };
        if dev == "OBJECT1" && field == "RT_PAR_F" && (1..=self.rt_par_f.len()).contains(&idx) {
            self.rt_par_f[idx - 1] = value;
            return 0;
        }
        if let Some((_, fields)) = self.devices.iter_mut().find(|(n, _)| *n == dev)
            && let Some((_, v)) = fields.iter_mut().find(|(k, _)| *k == field)
        {
            *v = value;
            return 0;
        }
        1
    }
}

/// `__DEV:set_cmd('FIELD', idx, value)` без regex-зависимости.
fn regex_lite(cmd: &str) -> Option<(String, String, usize, f64)> {
    let rest = cmd.strip_prefix("__")?;
    let (dev, rest) = rest.split_once(":set_cmd('")?;
    let (field, rest) = rest.split_once("', ")?;
    let (idx, rest) = rest.split_once(", ")?;
    let value = rest.strip_suffix(')')?;
    Some((dev.to_string(), field.to_string(), idx.parse().ok()?, value.parse().ok()?))
}

struct FakePac {
    task: Option<JoinHandle<()>>,
    clients: Arc<Mutex<Vec<JoinHandle<()>>>>,
    port: u16,
}

fn deflate(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

/// Рантайм поддельной прошивки: у настоящей это отдельный процесс, и её ответы не зависят от рантайма фасада
/// (иначе тест, намеренно загружающий рантайм фасада, останавливал бы и «прошивку»).
fn pac_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap())
}

impl FakePac {
    async fn start(port: u16, state: Arc<Mutex<PacState>>) -> FakePac {
        let std_listener = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let port = std_listener.local_addr().unwrap().port();
        let clients: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let (st, cl) = (state, clients.clone());
        let task = pac_runtime().spawn(async move {
            let listener = TcpListener::from_std(std_listener).unwrap();
            while let Ok((stream, _)) = listener.accept().await {
                let st = st.clone();
                cl.lock().unwrap().push(tokio::spawn(async move {
                    let _ = serve(stream, st).await;
                }));
            }
        });
        FakePac { task: Some(task), clients, port }
    }

    /// Прошивка пропала: слушающий сокет и все соединения закрыты.
    fn stop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        for c in self.clients.lock().unwrap().drain(..) {
            c.abort();
        }
    }
}

async fn serve(mut s: TcpStream, state: Arc<Mutex<PacState>>) -> std::io::Result<()> {
    s.write_all(b"PAC accept").await?;
    loop {
        let mut hdr = [0u8; 6];
        s.read_exact(&mut hdr).await?;
        let mut payload = vec![0u8; (usize::from(hdr[4]) << 8) | usize::from(hdr[5])];
        s.read_exact(&mut payload).await?;
        let (cmd, extra) = (payload[0], &payload[1..]);
        let body: Vec<u8> = match cmd {
            10 => b"protocol_version = 104; PAC_name = \"FAKE\";\n\0".to_vec(),
            101 => {
                let mut b = vec![0u8, 0u8];
                b.extend(state.lock().unwrap().snapshot().into_bytes());
                b.push(0);
                b
            }
            _ => state.lock().unwrap().apply(&String::from_utf8_lossy(extra)).to_le_bytes().to_vec(),
        };
        let packed = deflate(&body);
        let mut frame = vec![b's', 12, hdr[3], (packed.len() >> 8) as u8, (packed.len() & 255) as u8];
        frame.extend(packed);
        s.write_all(&frame).await?;
    }
}

// ------------------------------------------------------------------- стенд и клиент --

const CHANNELS: &[(&str, &str, &str, bool)] = &[
    ("LINE1V0", "ST", "INT32", true),
    ("LINE1V0", "M", "INT32", true),
    ("LINE1TE1", "V", "FLOAT", false),
    ("LINE1TE1", "P_CZ", "FLOAT", true),
    ("OBJECT1", "RT_PAR_F[2]", "FLOAT", true),
    ("SYSTEM", "UP_TIME", "STRING", false),
    ("LINE1V0", "NOPE", "INT32", false), // поля нет в прошивке
    ("LINE9V9", "ST", "INT32", true),    // прибора нет в прошивке
];

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Stand {
    pac: FakePac,
    pac_state: Arc<Mutex<PacState>>,
    session: Arc<Session>,
    tasks: Vec<JoinHandle<()>>,
    stats: Arc<Stats>,
}

impl Stand {
    async fn start() -> Stand {
        Stand::start_polling(Duration::from_millis(100)).await
    }

    async fn start_polling(poll: Duration) -> Stand {
        Stand::start_expecting(poll, None).await
    }

    /// Фасад со сверкой имени прошивки (`PAC_NAME`): `expected` — ожидаемое имя.
    async fn start_expecting(poll: Duration, expected: Option<&str>) -> Stand {
        let dir = std::env::temp_dir().join(format!("ptusa-opcua-it-{}-{}", std::process::id(), free_port()));
        std::fs::create_dir_all(&dir).unwrap();
        let tags: Vec<String> = CHANNELS
            .iter()
            .map(|(d, f, t, w)| {
                format!("{{deviceName: {d}, fieldName: \"{f}\", dataType: {t}, enabled: true, writable: {w}}}")
            })
            .collect();
        let cfg = dir.join("station.yaml");
        std::fs::write(&cfg, format!("opcua:\n  servers:\n    - tags: [{}]\n", tags.join(", "))).unwrap();
        let channels = Arc::new(load_channels(&cfg).unwrap());

        let pac_state = Arc::new(Mutex::new(PacState::new()));
        let pac = FakePac::start(0, pac_state.clone()).await;
        let client = Arc::new(
            PacClient::new("127.0.0.1", pac.port, Duration::from_secs(1)).expecting_name(expected.map(str::to_string)),
        );
        let stats = Arc::new(Stats::default());
        let opc_port = free_port();
        let (server, pusher) = build(
            &channels,
            client.clone(),
            stats.clone(),
            &format!("opc.tcp://127.0.0.1:{opc_port}"),
            ("127.0.0.1".into(), opc_port),
        )
        .unwrap();
        let bridge = Bridge {
            channels: channels.clone(),
            pac: client,
            pusher,
            snapshot: Snapshot::new().unwrap(),
            poll,
            stats: stats.clone(),
        };
        let tasks = vec![
            tokio::spawn(async move {
                let _ = server.run().await;
            }),
            tokio::spawn(bridge.run()),
        ];
        tokio::time::sleep(Duration::from_millis(800)).await;
        let session = connect(opc_port).await;
        Stand { pac, pac_state, session, tasks, stats }
    }

    fn node(&self, name: &str) -> NodeId {
        NodeId::new(2, name.to_string())
    }

    /// Значение и имя статуса узла (плохой статус — данные теста, а не ошибка).
    async fn read(&self, name: &str) -> (Option<Variant>, StatusCode) {
        let rv = ReadValueId::from(self.node(name));
        let dv: DataValue = self.session.read(&[rv], TimestampsToReturn::Both, 0.0).await.unwrap().remove(0);
        (dv.value, dv.status.unwrap_or(StatusCode::Good))
    }

    async fn write(&self, name: &str, value: Variant) -> StatusCode {
        let w = WriteValue {
            node_id: self.node(name),
            attribute_id: AttributeId::Value as u32,
            index_range: NumericRange::None,
            value: DataValue::value_only(value),
        };
        self.session.write(&[w]).await.unwrap().remove(0)
    }

    fn commands(&self) -> Vec<String> {
        self.pac_state.lock().unwrap().commands.clone()
    }
}

impl Drop for Stand {
    fn drop(&mut self) {
        self.pac.stop();
        for t in &self.tasks {
            t.abort();
        }
    }
}

async fn connect(port: u16) -> Arc<Session> {
    let mut client = ClientBuilder::new()
        .application_name("ptusa-opcua test")
        .application_uri("urn:test")
        .product_uri("urn:test")
        .pki_dir(std::env::temp_dir().join(format!("ptusa-opcua-test-pki-{port}")))
        .create_sample_keypair(true)
        .trust_server_certs(true)
        .session_retry_limit(0)
        .client()
        .unwrap();
    let url = format!("opc.tcp://127.0.0.1:{port}");
    let endpoint: EndpointDescription =
        (url.as_str(), "None", MessageSecurityMode::None, UserTokenPolicy::anonymous()).into();
    let (session, event_loop) = client.connect_to_endpoint_directly(endpoint, IdentityToken::Anonymous).unwrap();
    event_loop.spawn();
    assert!(tokio::time::timeout(Duration::from_secs(10), session.wait_for_connection()).await.unwrap());
    // Клиент должен жить столько же, сколько сессия.
    Box::leak(Box::new(client));
    session
}

fn int(v: &Option<Variant>) -> Option<i32> {
    match v {
        Some(Variant::Int32(i)) => Some(*i),
        _ => None,
    }
}

fn float(v: &Option<Variant>) -> Option<f32> {
    match v {
        Some(Variant::Float(f)) => Some(*f),
        _ => None,
    }
}

// ----------------------------------------------------------------------- сценарии --

/// Имя прошивки и связь с ней — узлами `SYSTEM.PAC_NAME` и `SYSTEM.LINK_OK`; тот же объект `SYSTEM`, что и у прибора прошивки.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn system_nodes_report_the_firmware_name_and_link() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let s = Stand::start_expecting(Duration::from_millis(100), Some("FAKE")).await;
        assert_eq!(s.read("SYSTEM.PAC_NAME").await, (Some(Variant::from("FAKE")), StatusCode::Good));
        assert_eq!(s.read("SYSTEM.LINK_OK").await, (Some(Variant::Boolean(true)), StatusCode::Good));
        let (v, st) = s.read("SYSTEM.UP_TIME").await;
        assert_eq!((v, st), (Some(Variant::from("0 дн. 00:00:20")), StatusCode::Good), "прибор SYSTEM не затронут");
    })
    .await
    .expect("тест завис");
}

/// Прошивка назвала себя иначе, чем ожидалось (`PAC_NAME`): значений чужой станции фасад не отдаёт, команды в неё не идут.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_firmware_is_not_served_and_gets_no_commands() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let s = Stand::start_expecting(Duration::from_millis(100), Some("BN1-МСА1")).await;
        assert_eq!(s.read("LINE1V0.ST").await.1, StatusCode::BadCommunicationError);
        assert_eq!(s.read("LINE1TE1.V").await.1, StatusCode::BadCommunicationError);
        assert_eq!(s.read("SYSTEM.LINK_OK").await, (Some(Variant::Boolean(false)), StatusCode::Good));
        // Имя — то, что назвала прошивка: по нему видно, к чему подключились.
        assert_eq!(s.read("SYSTEM.PAC_NAME").await, (Some(Variant::from("FAKE")), StatusCode::Good));
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(1)).await, StatusCode::BadCommunicationError);
        assert!(s.commands().is_empty(), "в чужую прошивку ушла команда: {:?}", s.commands());
    })
    .await
    .expect("тест завис");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_writes_and_faults() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut s = Stand::start().await;

        // --- чтение: типы, значения из снимка, массив по индексу, строка ---
        let (v, st) = s.read("LINE1V0.ST").await;
        assert_eq!((int(&v), st), (Some(0), StatusCode::Good));
        let (v, st) = s.read("LINE1TE1.V").await;
        assert_eq!((float(&v), st), (Some(21.5), StatusCode::Good));
        let (v, st) = s.read("OBJECT1.RT_PAR_F[2]").await;
        assert_eq!((float(&v), st), (Some(0.0), StatusCode::Good));
        let (v, st) = s.read("SYSTEM.UP_TIME").await;
        assert_eq!((v, st), (Some(Variant::from("0 дн. 00:00:20")), StatusCode::Good));
        // нет поля / нет прибора в прошивке → BadNoData, а не выдуманное значение
        assert_eq!(s.read("LINE1V0.NOPE").await.1, StatusCode::BadNoData);
        assert_eq!(s.read("LINE9V9.ST").await.1, StatusCode::BadNoData);

        // --- запись = команда прошивке, значение приходит из следующего снимка ---
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(1)).await, StatusCode::Good);
        assert_eq!(s.write("LINE1V0.ST", Variant::Int32(1)).await, StatusCode::Good);
        assert_eq!(s.write("OBJECT1.RT_PAR_F[2]", Variant::Float(7.5)).await, StatusCode::Good);
        assert_eq!(s.write("LINE1TE1.P_CZ", Variant::Float(0.25)).await, StatusCode::Good);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            s.commands(),
            [
                "__LINE1V0:set_cmd('M', 1, 1)",
                "__LINE1V0:set_cmd('ST', 1, 1)",
                "__OBJECT1:set_cmd('RT_PAR_F', 2, 7.5)",
                "__LINE1TE1:set_cmd('P_CZ', 1, 0.25)"
            ]
        );
        assert_eq!(int(&s.read("LINE1V0.ST").await.0), Some(1));
        assert_eq!(float(&s.read("OBJECT1.RT_PAR_F[2]").await.0), Some(7.5));
        assert_eq!(float(&s.read("LINE1TE1.P_CZ").await.0), Some(0.25));

        // --- отказы: датчик (не writable) и прибор, который прошивка не знает ---
        let denied = s.write("LINE1TE1.V", Variant::Float(5.0)).await;
        assert!(denied.is_bad(), "датчик не записывается: {denied:?}");
        let before = s.commands().len();
        assert_eq!(s.write("LINE9V9.ST", Variant::Int32(1)).await, StatusCode::BadInvalidState, "код прошивки ≠ 0");
        assert_eq!(s.commands().len(), before + 1);
        assert_eq!(int(&s.read("LINE1V0.ST").await.0), Some(1), "значение не тронуто");
        // Строка и неверный тип прошивке командой не пишутся.
        let n = s.commands().len();
        assert_eq!(s.write("LINE1V0.M", Variant::from("x")).await, StatusCode::BadTypeMismatch);
        assert_eq!(s.commands().len(), n);

        // --- обрыв связи с прошивкой: ВСЕ узлы BadCommunicationError, запись — тоже ---
        s.pac.stop();
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(s.read("LINE1V0.ST").await.1, StatusCode::BadCommunicationError);
        assert_eq!(s.read("SYSTEM.UP_TIME").await.1, StatusCode::BadCommunicationError);
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(0)).await, StatusCode::BadCommunicationError);

        // --- восстановление: прошивка вернулась на прежнем порту — фасад переподключается ---
        let (port, state) = (s.pac.port, s.pac_state.clone());
        s.pac = FakePac::start(port, state).await;
        let mut good = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if s.read("LINE1V0.ST").await.1 == StatusCode::Good {
                good = true;
                break;
            }
        }
        assert!(good, "фасад не переподключился");
        assert_eq!(int(&s.read("LINE1V0.ST").await.0), Some(1));
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(0)).await, StatusCode::Good);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(int(&s.read("LINE1V0.M").await.0), Some(0));
        assert!(s.stats.polls.load(std::sync::atomic::Ordering::Relaxed) > 5);
    })
    .await
    .expect("сценарий не уложился в 90 с");
}

/// Снимок с ядовитым Lua не роняет фасад и не даёт ему выйти из песочницы: узлы получают
/// BadCommunicationError, а после починки прошивки значения возвращаются.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_snapshot_is_contained() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let s = Stand::start().await;
        assert_eq!(s.read("LINE1V0.ST").await.1, StatusCode::Good);
        // Прошивка начала присылать снимок, который пытается выполнить код на хосте.
        s.pac_state.lock().unwrap().devices[0].0 = "LINE1V0=os.execute('touch /tmp/ptusa-opcua-pwned') or {}; X".into();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(s.read("LINE1V0.ST").await.1, StatusCode::BadCommunicationError);
        assert!(!std::path::Path::new("/tmp/ptusa-opcua-pwned").exists());
        s.pac_state.lock().unwrap().devices[0].0 = "LINE1V0".into();
        let mut good = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if s.read("LINE1V0.ST").await.1 == StatusCode::Good {
                good = true;
                break;
            }
        }
        assert!(good, "после починки снимка значения не вернулись");
    })
    .await
    .expect("сценарий не уложился в 60 с");
}

/// Запись, пока фасад часто снимает снимок и клиент читает узлы, не вешает сервер. Обратный случай: запись шла в
/// синхронной функции с ожиданием асинхронной команды, а цикл снимков синхронно ждал ту же блокировку адресного
/// пространства; поток, державший драйвер ввода-вывода, оказывался заблокирован, команда не получала ответа, и сервер
/// переставал отвечать совсем (у шлюза — таймауты чтения и записи, «handshake завис»).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_during_fast_polling_do_not_freeze_the_server() {
    let _ = tracing_subscriber::fmt().with_env_filter("ptusa_opcua=info").try_init();
    tokio::time::timeout(Duration::from_secs(120), async {
        let s = Stand::start_polling(Duration::from_millis(5)).await;
        let session = s.session.clone();
        let names = ["LINE1V0.ST", "LINE1V0.M", "LINE1TE1.V", "LINE1TE1.P_CZ", "OBJECT1.RT_PAR_F[2]", "SYSTEM.UP_TIME"];
        // Читатель: как шлюз, опрашивает все узлы без пауз.
        let reader = tokio::spawn(async move {
            loop {
                let rv: Vec<ReadValueId> =
                    names.iter().map(|n| ReadValueId::from(NodeId::new(2, n.to_string()))).collect();
                if session.read(&rv, TimestampsToReturn::Both, 0.0).await.is_err() {
                    return;
                }
            }
        });
        for i in 0..300 {
            let status = tokio::time::timeout(Duration::from_secs(5), s.write("LINE1V0.ST", Variant::Int32(i % 2)))
                .await
                .unwrap_or_else(|_| panic!("запись {i} не вернулась за 5 с: сервер завис"));
            assert_eq!(status, StatusCode::Good, "запись {i}");
        }
        reader.abort();
    })
    .await
    .expect("тест завис");
}
