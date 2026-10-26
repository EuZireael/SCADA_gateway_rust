//! Фасад целиком: фейковый PAC (тот же проводной формат, что у ptusa: приветствие PAC accept, кадры
//! 's',1,1,pidx,len, zlib, тела — C-строки) + настоящий OPC UA-сервер фасада + настоящий OPC UA-клиент.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opcua::client::{ClientBuilder, IdentityToken, Session};
use opcua::types::{
    AttributeId, DataValue, DateTime, EndpointDescription, MessageSecurityMode, NodeId, NumericRange, ReadValueId,
    StatusCode, TimestampsToReturn, UserTokenPolicy, Variant, WriteValue,
};
use ptusa_opcua::alarms::AlarmsReader;
use ptusa_opcua::channel::load_channels;
use ptusa_opcua::pac::PacClient;
use ptusa_opcua::security::{Security, hash_password, parse_users};
use ptusa_opcua::server::{AlarmBridge, Bridge, Stats, build};
use ptusa_opcua::snapshot::Snapshot;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------- фейковый PAC --

/// Прошивка: приборы с полями, снимок `t=…`, `set_cmd` меняет поле; код ≠ 0 для чужих приборов. Аварии — сообщения
/// объектов (`id_type`, `id_n`, номер, текст): любая команда над сообщением удаляет его, как у ptusa; тип 999 — ошибка Lua.
#[derive(Default)]
struct PacState {
    devices: Vec<(String, Vec<(String, f64)>)>,
    rt_par_f: Vec<f64>,
    commands: Vec<String>,
    alarms: Vec<(i32, i32, i32, String)>,
    alarms_id: u32,
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
            alarms: vec![(102, 2, 3, "'Танк 2' - не включена операция 1 'Мойка'".into())],
            alarms_id: 50,
        }
    }

    /// Ответ 103 в виде ptusa: таблица присваивается дважды, описание не экранируется.
    fn alarms_text(&self) -> String {
        let mut s = String::from("alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {");
        for (t, n, a, text) in &self.alarms {
            s += &format!(
                "\t{{\n\tdescription=\"{text}\",\n\tgroup=\"ответ\",\npriority=750,state=1,\ntype=4,\nid_n={n},\n\
                 id_object_alarm_number={a},\nid_type={t},\nsuppress=false\n}},\n"
            );
        }
        s + &format!("  id = {},\n  }}\n", self.alarms_id)
    }

    /// Команда 104: `errors_manager:get_instance():set_cmd( cmd, type, n, alarm )`.
    fn alarm_cmd(&mut self, cmd: &str) -> u16 {
        self.commands.push(cmd.to_string());
        let Some(args) = cmd.strip_prefix("errors_manager:get_instance():set_cmd( ").and_then(|r| r.strip_suffix(" )"))
        else {
            return 1;
        };
        let args: Vec<i32> = args.split(", ").filter_map(|a| a.parse().ok()).collect();
        let [_, t, n, a] = args[..] else { return 1 };
        if t == 999 {
            return 1;
        }
        let before = self.alarms.len();
        self.alarms.retain(|(at, an, aa, _)| (*at, *an, *aa) != (t, n, a));
        if self.alarms.len() != before {
            self.alarms_id += 1;
        }
        0
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
            103 => {
                let mut b = state.lock().unwrap().alarms_text().into_bytes();
                b.push(0);
                b
            }
            104 => state.lock().unwrap().alarm_cmd(&String::from_utf8_lossy(extra)).to_le_bytes().to_vec(),
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
    port: u16,
    pki: std::path::PathBuf,
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
        let pki = std::env::temp_dir().join(format!("ptusa-opcua-it-pki-{}-{}", std::process::id(), free_port()));
        Stand::start_with(poll, expected, Security::open(pki), None).await
    }

    /// Фасад с заданной защитой; `login` — как подключается сессия стенда (`None` — анонимно без защиты канала).
    async fn start_with(
        poll: Duration,
        expected: Option<&str>,
        security: Security,
        login: Option<(&str, MessageSecurityMode, IdentityToken)>,
    ) -> Stand {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
            .with_test_writer()
            .try_init();
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
            &security,
        )
        .unwrap();
        let alarm_bridge = AlarmBridge {
            pac: client.sibling(),
            pusher: pusher.clone(),
            reader: AlarmsReader::new().unwrap(),
            poll: Duration::from_millis(100),
            stats: stats.clone(),
        };
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
            tokio::spawn(alarm_bridge.run()),
        ];
        tokio::time::sleep(Duration::from_millis(800)).await;
        let session = match login {
            None => connect(opc_port).await,
            Some((policy, mode, identity)) => {
                try_connect(opc_port, policy, mode, identity, Some(&security.pki_dir)).await.expect("вход стенда")
            }
        };
        Stand { pac, pac_state, session, port: opc_port, pki: security.pki_dir.clone(), tasks, stats }
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
        self.write_on(&self.session, name, value).await
    }

    async fn write_on(&self, session: &Session, name: &str, value: Variant) -> StatusCode {
        let w = WriteValue {
            node_id: self.node(name),
            attribute_id: AttributeId::Value as u32,
            index_range: NumericRange::None,
            value: DataValue::value_only(value),
        };
        session.write(&[w]).await.unwrap().remove(0)
    }

    /// Узел `SYSTEM.ALARMS`: разобранный JSON, статус и метка времени источника.
    async fn alarms(&self) -> (Option<serde_json::Value>, StatusCode, Option<DateTime>) {
        let rv = ReadValueId::from(self.node("SYSTEM.ALARMS"));
        let dv: DataValue = self.session.read(&[rv], TimestampsToReturn::Both, 0.0).await.unwrap().remove(0);
        let json = match dv.value {
            Some(Variant::String(s)) => Some(serde_json::from_str(s.as_ref()).unwrap()),
            _ => None,
        };
        (json, dv.status.unwrap_or(StatusCode::Good), dv.source_timestamp)
    }

    /// Ждать, пока статус узла `SYSTEM.ALARMS` станет `want`.
    async fn wait_alarms_status(&self, want: StatusCode) {
        for _ in 0..60 {
            if self.alarms().await.1 == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("статус SYSTEM.ALARMS не стал {want:?}: {:?}", self.alarms().await);
    }

    /// Вызов метода `SYSTEM.ALARM_CMD`; статус вызова.
    async fn alarm_cmd_on(&self, session: &Session, args: Vec<Variant>) -> StatusCode {
        let call = (self.node("SYSTEM"), self.node("SYSTEM.ALARM_CMD"), Some(args));
        session.call_one(call).await.unwrap().status_code
    }

    async fn alarm_cmd(&self, action: &str, object_type: i32, object_number: i32, alarm: i32) -> StatusCode {
        let args = vec![
            Variant::from(action),
            Variant::Int32(object_type),
            Variant::Int32(object_number),
            Variant::Int32(alarm),
        ];
        self.alarm_cmd_on(&self.session, args).await
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

/// Подключение с политикой, режимом и пользователем; `trust_in` — каталог PKI сервера, куда положить сертификат клиента
/// как доверенный (`None` — клиент сервером не доверен). `None` в ответе — сервер отказал.
async fn try_connect(
    port: u16,
    policy: &str,
    mode: MessageSecurityMode,
    identity: IdentityToken,
    trust_in: Option<&std::path::Path>,
) -> Option<Arc<Session>> {
    // Выпуск ключа клиента в отладочной сборке занимает секунды: доверенные подключения делят одну пару, «чужому» клиенту
    // нужна своя.
    static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = if trust_in.is_some() { 0 } else { 1 + N.fetch_add(1, std::sync::atomic::Ordering::SeqCst) };
    let pki = std::env::temp_dir().join(format!("ptusa-opcua-client-pki-{}-{id}", std::process::id()));
    let mut client = ClientBuilder::new()
        .application_name("ptusa-opcua test")
        .application_uri("urn:test")
        .product_uri("urn:test")
        .pki_dir(&pki)
        .create_sample_keypair(true)
        .trust_server_certs(true)
        .session_retry_limit(0)
        .client()
        .unwrap();
    if let Some(server_pki) = trust_in {
        let trusted = server_pki.join("trusted");
        std::fs::create_dir_all(&trusted).unwrap();
        // Сервер ищет доверенный сертификат по имени файла, которое строит сам (как в `rejected/` при первой попытке).
        let cert = opcua::crypto::CertificateStore::read_cert(&pki.join("own").join("cert.der")).unwrap();
        std::fs::copy(
            pki.join("own").join("cert.der"),
            trusted.join(opcua::crypto::CertificateStore::cert_file_name(&cert)),
        )
        .unwrap();
    }
    // Конечные точки запрашиваем у сервера: политика токена «логин/пароль» у него своя.
    let url = format!("opc.tcp://127.0.0.1:{port}");
    let endpoint: EndpointDescription = (url.as_str(), policy, mode).into();
    let opened =
        tokio::time::timeout(Duration::from_secs(10), client.connect_to_matching_endpoint(endpoint, identity)).await;
    let Ok(Ok((session, event_loop))) = opened else {
        eprintln!("подключение {policy}/{mode:?}: {:?}", opened.as_ref().map(|r| r.as_ref().err()));
        Box::leak(Box::new(client));
        return None;
    };
    event_loop.spawn();
    let connected = tokio::time::timeout(Duration::from_secs(10), session.wait_for_connection()).await.unwrap_or(false);
    Box::leak(Box::new(client));
    connected.then_some(session)
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

/// Аварии прошивки: список — JSON в узле `SYSTEM.ALARMS` (меняется только вместе со списком), действие — метод
/// `SYSTEM.ALARM_CMD` командой 104; неразобранный ответ и обрыв — плохой статус узла, а не старый список.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alarms_are_read_and_commanded() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut s = Stand::start().await;
        s.wait_alarms_status(StatusCode::Good).await;
        let (json, _, ts) = s.alarms().await;
        assert_eq!(
            json.unwrap(),
            serde_json::json!({"id": 50, "alarms": [{
                "objectType": 102, "objectNumber": 2, "alarmNumber": 3, "critical": false,
                "description": "'Танк 2' - не включена операция 1 'Мойка'", "group": "ответ", "priority": 750,
                "state": "ALARM", "suppressed": false
            }]})
        );
        // Список не менялся — узел не переписывается (подписка не получает одно и то же).
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(s.alarms().await.2, ts, "узел переписан без изменения списка");

        // Квитирование: команда 104 только из чисел, сообщение уходит из списка.
        assert_eq!(s.alarm_cmd("ACCEPT", 102, 2, 3).await, StatusCode::Good);
        assert_eq!(s.commands(), ["errors_manager:get_instance():set_cmd( 100, 102, 2, 3 )"]);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(s.alarms().await.0.unwrap(), serde_json::json!({"id": 51, "alarms": []}));
        // Повтор — прошивка молча ничего не делает; ошибка Lua — BadInvalidState; неизвестное действие в прошивку не идёт.
        assert_eq!(s.alarm_cmd("SUPPRESS", 102, 2, 3).await, StatusCode::Good);
        assert_eq!(s.alarm_cmd("ACCEPT", 999, 1, 1).await, StatusCode::BadInvalidState);
        assert_eq!(s.alarm_cmd("DELETE", 102, 2, 3).await, StatusCode::BadInvalidArgument);
        let wrong_type = vec![Variant::from("ACCEPT"), Variant::from("102"), Variant::Int32(2), Variant::Int32(3)];
        assert_eq!(s.alarm_cmd_on(&s.session, wrong_type).await, StatusCode::BadInvalidArgument);
        assert_eq!(s.commands().len(), 3, "{:?}", s.commands());

        // Кавычка в описании (прошивка её не экранирует) — ответ не разобран: плохой статус, а не прежний список.
        s.pac_state.lock().unwrap().alarms.push((102, 1, 4, "нет сигнала \"Линия свободна\"".into()));
        s.wait_alarms_status(StatusCode::BadDecodingError).await;
        s.pac_state.lock().unwrap().alarms[0].3 = "нет сигнала 'Линия свободна'".into();
        s.wait_alarms_status(StatusCode::Good).await;
        assert_eq!(s.alarms().await.0.unwrap()["alarms"][0]["alarmNumber"], 4);

        // Обрыв связи — BadCommunicationError, метод тоже; прошивка вернулась — список снова читается.
        s.pac.stop();
        s.wait_alarms_status(StatusCode::BadCommunicationError).await;
        assert_eq!(s.alarm_cmd("ACCEPT", 102, 1, 4).await, StatusCode::BadCommunicationError);
        let (port, state) = (s.pac.port, s.pac_state.clone());
        s.pac = FakePac::start(port, state).await;
        s.wait_alarms_status(StatusCode::Good).await;
        assert!(s.stats.alarm_polls.load(std::sync::atomic::Ordering::Relaxed) > 5);
    })
    .await
    .expect("сценарий не уложился в 90 с");
}

// ------------------------------------------------------------------------ защита --

const SIGN_ENCRYPT: MessageSecurityMode = MessageSecurityMode::SignAndEncrypt;
const POLICY: &str = "Basic256Sha256";

/// Защита из двух пользователей: `anna` (запись) и `boris` (только чтение); анонимный вход выключен, клиенты — только из `trusted/`.
fn secured(name: &str) -> Security {
    let pki = std::env::temp_dir().join(format!("ptusa-opcua-it-pki-{name}-{}-{}", std::process::id(), free_port()));
    let users = parse_users(&format!(
        "anna {} write\nboris {} read\n",
        hash_password("anna-pass").unwrap(),
        hash_password("boris-pass").unwrap()
    ))
    .unwrap();
    Security {
        endpoints: vec![(opcua::crypto::SecurityPolicy::Basic256Sha256, SIGN_ENCRYPT)],
        allow_anonymous: false,
        users,
        pki_dir: pki,
        trust_all_clients: false,
        cert_days: 30,
        cert_hosts: vec!["127.0.0.1".into()],
    }
}

fn user(name: &str, pass: &str) -> IdentityToken {
    IdentityToken::new_user_name(name.to_string(), pass.to_string())
}

/// Защищённое подключение с паролем работает; анонимный вход, чужой пароль и неизвестный сертификат клиента отклоняются;
/// запись разрешена только роли `write`, чтение — обеим; в прошивку уходит одна команда — от `write`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secured_facade_requires_a_password_a_trusted_certificate_and_the_write_role() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let security = secured("roles");
        let s = Stand::start_with(
            Duration::from_millis(100),
            None,
            security,
            Some((POLICY, SIGN_ENCRYPT, user("anna", "anna-pass"))),
        )
        .await;

        // Чтение и запись роли write.
        assert_eq!(int(&s.read("LINE1V0.ST").await.0), Some(0));
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(1)).await, StatusCode::Good);

        // Роль read: подключается, читает, писать не может.
        let boris = try_connect(s.port, POLICY, SIGN_ENCRYPT, user("boris", "boris-pass"), Some(&s.pki))
            .await
            .expect("boris подключается");
        let rv = ReadValueId::from(s.node("LINE1V0.ST"));
        let dv = boris.read(&[rv], TimestampsToReturn::Both, 0.0).await.unwrap().remove(0);
        assert_eq!(dv.status.unwrap_or(StatusCode::Good), StatusCode::Good);
        assert_eq!(s.write_on(&boris, "LINE1V0.M", Variant::Int32(0)).await, StatusCode::BadUserAccessDenied);
        // Действие над аварией — тоже команда: роли read нельзя.
        let accept = vec![Variant::from("ACCEPT"), Variant::Int32(102), Variant::Int32(2), Variant::Int32(3)];
        assert_eq!(s.alarm_cmd_on(&boris, accept).await, StatusCode::BadUserAccessDenied);

        // Анонимный вход, неверный пароль и неизвестный пользователь отклонены.
        assert!(try_connect(s.port, POLICY, SIGN_ENCRYPT, IdentityToken::Anonymous, Some(&s.pki)).await.is_none());
        assert!(try_connect(s.port, POLICY, SIGN_ENCRYPT, user("anna", "wrong"), Some(&s.pki)).await.is_none());
        assert!(try_connect(s.port, POLICY, SIGN_ENCRYPT, user("nobody", "anna-pass"), Some(&s.pki)).await.is_none());
        // Канала без защиты у фасада нет.
        assert!(try_connect(s.port, "None", MessageSecurityMode::None, IdentityToken::Anonymous, None).await.is_none());
        // Чужой сертификат клиента (его нет в trusted/) отклонён, даже с верным паролем.
        assert!(try_connect(s.port, POLICY, SIGN_ENCRYPT, user("anna", "anna-pass"), None).await.is_none());

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(s.commands(), ["__LINE1V0:set_cmd('M', 1, 1)"], "в прошивку ушла только команда роли write");
        assert_eq!(s.alarm_cmd("ACCEPT", 102, 2, 3).await, StatusCode::Good, "роли write — можно");
    })
    .await
    .expect("тест завис");
}

/// Стенд с явно разрешённым анонимным входом: анонимный клиент по защищённому каналу подключается; пользователей нет.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anonymous_login_works_only_when_it_is_enabled() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut security = secured("anon");
        security.users.clear();
        security.allow_anonymous = true;
        security.trust_all_clients = true;
        let s = Stand::start_with(
            Duration::from_millis(100),
            None,
            security,
            Some((POLICY, SIGN_ENCRYPT, IdentityToken::Anonymous)),
        )
        .await;
        assert_eq!(int(&s.read("LINE1V0.ST").await.0), Some(0));
        assert_eq!(s.write("LINE1V0.M", Variant::Int32(1)).await, StatusCode::Good);
    })
    .await
    .expect("тест завис");
}
