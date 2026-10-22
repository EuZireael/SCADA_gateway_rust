//! Клиент OPC UA шлюза против сервера в процессе теста (async-opcua): типы узлов, предел узлов в запросе, неизвестные
//! и неподдерживаемые узлы, адрес `nsu=`. Симулятор и Kafka не нужны.
//!
//! Сервер async-opcua сам тип записи в скалярный узел не проверяет (примет `Int32` в узел `Int16`), поэтому симулятор и
//! фасад, построенные на нём, ошибку типа не показывали. Настоящие ПЛК по стандарту такую запись отвергают
//! (`Bad_TypeMismatch`); тестовый сервер делает так же — обработчиком записи у каждого скалярного узла.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ::opcua::crypto::SecurityPolicy;
use ::opcua::server::address_space::VariableBuilder;
use ::opcua::server::diagnostics::NamespaceMetadata;
use ::opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use ::opcua::server::{ServerBuilder, ServerHandle};
use ::opcua::types::{DataTypeId, DataValue, DateTime, MessageSecurityMode, NodeId, ObjectId, StatusCode, Variant};
use scada_gateway::config;
use scada_gateway::model::{Controller, OpcSecurity, Quality, Tag, TagValue};
use scada_gateway::opcua::{self, ConnectOptions, NodeKind, OpcConnection};
use scada_gateway::opcua_cert::{self, CertSettings};
use serde_json::Value;

use common::gateway::Gateway;

/// Пространство имён узлов тестового сервера.
const NS_URI: &str = "urn:scada-gateway:test-server";
const OP_TIMEOUT: Duration = Duration::from_secs(5);

/// Скалярные узлы всех типов, которые шлюз пишет: имя узла, его тип, начальное значение, `dataType` тега (как его
/// называют в ПЛК) и значение для записи.
fn scalars() -> Vec<(&'static str, DataTypeId, Variant, &'static str, TagValue)> {
    vec![
        ("Boolean", DataTypeId::Boolean, Variant::Boolean(false), "BOOL", TagValue::Bool(true)),
        ("SByte", DataTypeId::SByte, Variant::SByte(0), "SINT", TagValue::Int(-100)),
        ("Byte", DataTypeId::Byte, Variant::Byte(0), "BYTE", TagValue::Int(200)),
        ("Int16", DataTypeId::Int16, Variant::Int16(0), "INT16", TagValue::Int(-30000)),
        ("UInt16", DataTypeId::UInt16, Variant::UInt16(0), "WORD", TagValue::Int(60000)),
        ("Int32", DataTypeId::Int32, Variant::Int32(0), "INT32", TagValue::Int(-2_000_000_000)),
        ("UInt32", DataTypeId::UInt32, Variant::UInt32(0), "UDINT", TagValue::Int(4_000_000_000)),
        ("Int64", DataTypeId::Int64, Variant::Int64(0), "LINT", TagValue::Int(-9_000_000_000_000)),
        ("UInt64", DataTypeId::UInt64, Variant::UInt64(0), "ULINT", TagValue::Int(9_000_000_000_000)),
        ("Float", DataTypeId::Float, Variant::Float(0.0), "FLOAT", TagValue::F64(64.5)),
        ("Double", DataTypeId::Double, Variant::Double(0.0), "LREAL", TagValue::F64(1.0e10)),
        ("String", DataTypeId::String, Variant::from(""), "STRING", TagValue::Text("РЕЦЕПТ 1".into())),
    ]
}

/// Сервер в процессе теста; останавливается при выходе из области видимости.
struct TestServer {
    url: String,
    /// Индекс пространства имён [`NS_URI`] на этом сервере.
    ns: u16,
    handle: ServerHandle,
    /// Принятые записи в скалярные узлы: имя узла → значение, как оно пришло.
    written: Arc<Mutex<HashMap<String, Variant>>>,
}

/// Встроенный тип значения записи (для строгой проверки на сервере).
fn variant_type(v: &Variant) -> Option<DataTypeId> {
    Some(match v {
        Variant::Boolean(_) => DataTypeId::Boolean,
        Variant::SByte(_) => DataTypeId::SByte,
        Variant::Byte(_) => DataTypeId::Byte,
        Variant::Int16(_) => DataTypeId::Int16,
        Variant::UInt16(_) => DataTypeId::UInt16,
        Variant::Int32(_) => DataTypeId::Int32,
        Variant::UInt32(_) => DataTypeId::UInt32,
        Variant::Int64(_) => DataTypeId::Int64,
        Variant::UInt64(_) => DataTypeId::UInt64,
        Variant::Float(_) => DataTypeId::Float,
        Variant::Double(_) => DataTypeId::Double,
        Variant::String(_) => DataTypeId::String,
        _ => return None,
    })
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}

impl TestServer {
    /// Сервер с узлами [`scalars`], массивом `FloatArray`, узлом `Time` типа DateTime и `floats` узлами `F0…`
    /// типа Float; в запросе чтения — не больше `max_nodes_per_read` узлов.
    async fn start(max_nodes_per_read: usize, floats: usize) -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let pki = std::env::temp_dir().join(format!("test-opcua-server-pki-{}-{port}", std::process::id()));
        let mut builder = ServerBuilder::new_anonymous("test server")
            .application_uri("urn:scada-gateway:test")
            .product_uri("urn:scada-gateway:test")
            .host("127.0.0.1")
            .port(port)
            .pki_dir(pki)
            .create_sample_keypair(true)
            .trust_client_certs(true)
            .add_endpoint("none", ("/", SecurityPolicy::None, MessageSecurityMode::None, &["ANONYMOUS"] as &[&str]))
            .with_node_manager(simple_node_manager(
                NamespaceMetadata { namespace_uri: NS_URI.into(), ..Default::default() },
                "test",
            ));
        builder.limits_mut().operational.max_nodes_per_read = max_nodes_per_read;
        let (server, handle) = builder.build().expect("сервер");
        let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().expect("менеджер узлов");
        let ns = handle.get_namespace_index(NS_URI).expect("пространство имён");
        {
            let mut space = manager.address_space().write();
            let folder = NodeId::from(ObjectId::ObjectsFolder);
            let mut add = |name: String, data_type: DataTypeId, value: Variant, rank: i32| {
                VariableBuilder::new(&NodeId::new(ns, name.clone()), name.clone(), name)
                    .data_type(data_type)
                    .value_rank(rank)
                    .value(value)
                    .writable()
                    .organized_by(folder.clone())
                    .insert(&mut *space);
            };
            for (name, data_type, value, _, _) in scalars() {
                add(name.into(), data_type, value, -1);
            }
            add("FloatArray".into(), DataTypeId::Float, Variant::from(vec![1.0f32, 2.0]), 1);
            add("Time".into(), DataTypeId::DateTime, Variant::from(DateTime::now()), -1);
            for i in 0..floats {
                add(format!("F{i}"), DataTypeId::Float, Variant::Float(i as f32), -1);
            }
        }
        // Строгая запись: значение не того типа — Bad_TypeMismatch, как у настоящего ПЛК; принятое запоминается.
        let written = Arc::new(Mutex::new(HashMap::new()));
        for (name, data_type, _, _, _) in scalars() {
            let written = written.clone();
            manager.inner().add_write_callback(NodeId::new(ns, name), move |dv: DataValue, _range| match dv.value {
                Some(v) if variant_type(&v) == Some(data_type) => {
                    written.lock().unwrap().insert(name.to_string(), v);
                    StatusCode::Good
                }
                _ => StatusCode::BadTypeMismatch,
            });
        }
        tokio::spawn(server.run());
        let deadline = Instant::now() + Duration::from_secs(10);
        while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            assert!(Instant::now() < deadline, "тестовый сервер не запустился");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        TestServer { url: format!("opc.tcp://127.0.0.1:{port}"), ns, handle, written }
    }

    /// Адрес узла по имени: `ns=<индекс>;s=<имя>`.
    fn node(&self, name: &str) -> String {
        format!("ns={};s={name}", self.ns)
    }
}

/// YAML станции с одним OPC UA-контроллером: теги (имя, nodeId, dataType).
fn station_yaml(url: &str, tags: &[(String, String, String)]) -> String {
    let lines: Vec<String> = tags
        .iter()
        .map(|(name, node, data_type)| {
            format!(
                "        - {{name: \"{name}\", nodeId: '{node}', dataType: {data_type}, pollingRate: 500, enabled: true, writable: true}}"
            )
        })
        .collect();
    format!(
        "opcua:\n  servers:\n    - id: test\n      name: \"Тестовый сервер\"\n      endpoint: \"{url}\"\n      enabled: true\n      tags:\n{}\n",
        lines.join("\n")
    )
}

/// Теги контроллера из YAML станции.
fn tags_of(url: &str, tags: &[(String, String, String)]) -> Vec<Arc<Tag>> {
    let servers = config::parse_controllers(&station_yaml(url, tags)).expect("станция");
    Controller::from_config(&servers[0]).expect("контроллер").tags.clone()
}

fn tag(name: &str, node: String, data_type: &str) -> (String, String, String) {
    (name.to_string(), node, data_type.to_string())
}

/// Шлюз узнаёт тип каждого узла и пишет в нём; прежняя запись по dataType тега (`Int32` в узел `Int16`) сервером
/// отвергается — это и была ошибка. Значение вне диапазона типа узла в ПЛК не уходит.
#[tokio::test]
async fn writes_go_in_the_node_type() {
    let server = TestServer::start(10_000, 0).await;
    let defs: Vec<_> = scalars().iter().map(|(name, _, _, dt, _)| tag(name, server.node(name), dt)).collect();
    let tags = tags_of(&server.url, &defs);
    let mut conn = OpcConnection::connect(&server.url, OP_TIMEOUT).await.expect("подключение");
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    assert!(!prepared.report.has_problems(), "{:?}", prepared.report);
    assert_eq!(prepared.tags.len(), scalars().len());

    for ((name, _, _, data_type, value), tag) in scalars().into_iter().zip(&tags) {
        let node = conn.node(&tag.node_id).unwrap();
        assert!(node.kind.is_some_and(|k| k.to_string() == name), "{name}: тип узла {:?}", node.kind);
        let variant = node.variant(data_type, &value).unwrap_or_else(|e| panic!("{name}: {e:#}"));
        let status = conn.write(node.id.clone(), variant).await.unwrap();
        assert!(status.is_good(), "{name}: сервер отклонил запись: {status}");
        // Сервер получил значение того же типа, что узел, и то самое число.
        let got = server.written.lock().unwrap()[name].clone();
        let got = opcua::extract_value(&got).unwrap_or_else(|| panic!("{name}: {got:?}"));
        match (&value, &got) {
            (TagValue::F64(want), got) => assert_eq!(got.as_f64(), Some(*want), "{name}"),
            (want, got) => assert_eq!(got, want, "{name}"),
        }
    }

    // Прежняя запись по dataType тега: INT16 → Int32 — строгий сервер отвечает Bad_TypeMismatch.
    let int16 = conn.node(&server.node("Int16")).unwrap();
    let old = opcua::to_variant("INT16", &TagValue::Int(5)).unwrap();
    let status = conn.write(int16.id.clone(), old).await.unwrap();
    assert_eq!(opcua::classify_write_status(status), "REJECTED_TYPE_MISMATCH", "{status}");

    // 300 в Byte не обрезается до 44, а отклоняется до записи.
    let byte = conn.node(&server.node("Byte")).unwrap();
    assert_eq!(byte.kind, Some(NodeKind::Byte));
    assert!(byte.variant("BYTE", &TagValue::Int(300)).is_err());
    conn.close().await;
}

/// Сервер принимает не больше 100 узлов в запросе: шлюз читает его предел при подключении, а без него (или если
/// предел назван неверно) уменьшает пачку, получив `Bad_TooManyOperations`.
#[tokio::test]
async fn reads_respect_the_server_limit() {
    let server = TestServer::start(100, 1000).await;
    let defs: Vec<_> = (0..1000).map(|i| tag(&format!("F{i}"), server.node(&format!("F{i}")), "FLOAT")).collect();
    let tags = tags_of(&server.url, &defs);

    // Без разбора узлов: первая пачка в 500 узлов отклоняется, дальше — по 250, 125, 62.
    let conn = OpcConnection::connect(&server.url, OP_TIMEOUT).await.expect("подключение");
    let nodes: Vec<_> = tags.iter().map(|t| opcua::read_value_id(&t.node_id).unwrap()).collect();
    let values = conn.read(&nodes).await.expect("чтение 1000 узлов");
    assert_eq!(values.len(), 1000);
    assert!(values.iter().all(|dv| opcua::reading(dv).1 == Quality::Good));
    assert!(conn.read_chunk() <= 100, "пачка {}", conn.read_chunk());
    conn.close().await;

    // С разбором: предел известен заранее, отказов нет.
    let mut conn = OpcConnection::connect(&server.url, OP_TIMEOUT).await.expect("подключение");
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    assert_eq!(prepared.report.max_nodes_per_read, Some(100));
    assert_eq!(prepared.report.read_chunk, 100);
    assert!(!prepared.report.has_problems(), "{:?}", prepared.report);
    let values = conn.read(&prepared.nodes).await.expect("чтение 1000 узлов");
    assert!(values.iter().all(|dv| opcua::reading(dv).1 == Quality::Good));
    assert_eq!(conn.read_chunk(), 100);
    conn.close().await;
}

/// Что не так с узлами, видно при подключении: узла нет, массив, DateTime, тип тега по смыслу не тот, адрес не
/// разобран. `nsu=<URI>` переводится в индекс по таблице сервера.
#[tokio::test]
async fn node_problems_are_reported_and_namespace_uris_are_resolved() {
    let server = TestServer::start(10_000, 0).await;
    let defs = vec![
        tag("missing", server.node("Missing"), "FLOAT"),
        tag("array", server.node("FloatArray"), "FLOAT"),
        tag("time", server.node("Time"), "STRING"),
        tag("mismatch", server.node("Int32"), "FLOAT"),
        tag("by-uri", format!("nsu={NS_URI};s=Float"), "FLOAT"),
        tag("foreign-uri", "nsu=urn:nowhere;s=Float".into(), "FLOAT"),
        tag("malformed", "pac:1".into(), "FLOAT"),
    ];
    let tags = tags_of(&server.url, &defs);
    let mut conn = OpcConnection::connect(&server.url, OP_TIMEOUT).await.expect("подключение");
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    let report = &prepared.report;
    let has = |list: &[String], name: &str| list.iter().any(|s| s.starts_with(&format!("{name} (")));
    assert!(has(&report.unknown, "missing"), "{report:?}");
    assert!(has(&report.unsupported, "array") && has(&report.unsupported, "time"), "{report:?}");
    assert!(has(&report.mismatched, "mismatch"), "{report:?}");
    assert!(has(&report.unresolved, "foreign-uri") && has(&report.unresolved, "malformed"), "{report:?}");
    assert_eq!(report.unknown.len() + report.unsupported.len() + report.mismatched.len(), 4, "{report:?}");
    let unresolved: Vec<_> = prepared.unresolved.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(unresolved, ["foreign-uri", "malformed"]);

    let values = conn.read(&prepared.nodes).await.expect("чтение");
    let quality: std::collections::HashMap<_, _> =
        prepared.tags.iter().zip(&values).map(|(t, dv)| (t.name.as_str(), opcua::reading(dv).1)).collect();
    // DateTime и массив — BAD, а не `null` + GOOD; узел по URI читается.
    assert_eq!(quality["time"], Quality::Bad);
    assert_eq!(quality["array"], Quality::Bad);
    assert_eq!(quality["missing"], Quality::Bad);
    assert_eq!(quality["by-uri"], Quality::Good);
    assert_eq!(conn.node(&format!("nsu={NS_URI};s=Float")).unwrap().id, NodeId::new(server.ns, "Float"));
    // В массив и неподдерживаемый тип команда не пишется.
    assert!(conn.node(&server.node("FloatArray")).unwrap().variant("FLOAT", &TagValue::F64(1.0)).is_err());
    assert!(conn.node(&server.node("Time")).unwrap().variant("STRING", &TagValue::Text("x".into())).is_err());
    conn.close().await;
}

/// Шлюз целиком: читает сервер с пределом 100 узлов, сообщает о проблемах узлов в журнале и в `/api/status`.
#[tokio::test(flavor = "multi_thread")]
async fn gateway_reads_a_limited_server_and_reports_nodes_in_status() {
    let server = TestServer::start(100, 150).await;
    let mut defs: Vec<_> = (0..150).map(|i| tag(&format!("F{i}"), server.node(&format!("F{i}")), "FLOAT")).collect();
    defs.push(tag("missing", server.node("Missing"), "FLOAT"));
    defs.push(tag("mismatch", server.node("Int32"), "FLOAT"));
    let dir = tempfile::tempdir().unwrap();
    let yaml = dir.path().join("controllers.yaml");
    std::fs::write(&yaml, station_yaml(&server.url, &defs)).unwrap();

    let gw = Gateway::start_with(Path::new(&yaml), &[("KAFKA_ENABLED", "false")]);
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    let report = loop {
        let body = gw.get("/api/status").map(|(_, b)| b).unwrap_or_default();
        let status: Value = serde_json::from_str(&body).unwrap_or_default();
        let report = status["controllers"][0]["opcua"].clone();
        if !report.is_null() {
            break report;
        }
        assert!(Instant::now() < deadline, "нет проверки узлов в /api/status: {body}\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(report["maxNodesPerRead"], 100, "{report}");
    assert_eq!(report["readChunk"], 100, "{report}");
    assert_eq!(report["unknownNodes"]["count"], 1, "{report}");
    assert_eq!(report["typeMismatch"]["count"], 1, "{report}");
    let log = gw.log();
    assert!(log.contains("узлов нет на сервере, теги будут BAD (1): missing"), "{log}");
    assert!(!log.contains("сервер не принимает"), "предел сервера должен быть известен заранее: {log}");
}

/// Сервер с одной конечной точкой Basic256Sha256/SignAndEncrypt, который сертификатам клиентов не доверяет, пока их
/// нет в `trusted/` (как настоящий ПЛК); один узел `Float`.
async fn secure_server(pki: &Path) -> (String, ServerHandle) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let endpoint =
        ("/", SecurityPolicy::Basic256Sha256, MessageSecurityMode::SignAndEncrypt, &["ANONYMOUS"] as &[&str]);
    let (server, handle) = ServerBuilder::new_anonymous("secure test server")
        .application_uri("urn:scada-gateway:secure-test")
        .product_uri("urn:scada-gateway:secure-test")
        .host("127.0.0.1")
        .port(port)
        .pki_dir(pki)
        .create_sample_keypair(true)
        .trust_client_certs(false)
        .add_endpoint("secure", endpoint)
        .with_node_manager(simple_node_manager(
            NamespaceMetadata { namespace_uri: NS_URI.into(), ..Default::default() },
            "test",
        ))
        .build()
        .expect("сервер");
    let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().expect("менеджер узлов");
    let ns = handle.get_namespace_index(NS_URI).expect("пространство имён");
    VariableBuilder::new(&NodeId::new(ns, "Float"), "Float", "Float")
        .data_type(DataTypeId::Float)
        .value(Variant::Float(1.5))
        .organized_by(NodeId::from(ObjectId::ObjectsFolder))
        .insert(&mut *manager.address_space().write());
    tokio::spawn(server.run());
    let deadline = Instant::now() + Duration::from_secs(10);
    while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        assert!(Instant::now() < deadline, "тестовый сервер не запустился");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (format!("opc.tcp://127.0.0.1:{port}"), handle)
}

/// Свой сертификат клиента из настроек (как от УЦ предприятия, со своим URI приложения) идёт в защищённый канал:
/// сервер, который клиенту не доверяет, отвергает его и кладёт в `rejected/` именно этот сертификат; после переноса в
/// `trusted/` шлюз подключается и читает.
#[tokio::test]
async fn configured_client_certificate_is_presented_on_the_secure_channel() {
    let root = tempfile::tempdir().unwrap();
    let (server_pki, issuer, gateway_pki) =
        (root.path().join("server"), root.path().join("ca"), root.path().join("gw"));
    let (url, handle) = secure_server(&server_pki).await;

    // «Выданный» сертификат: пара в своих файлах и свой URI приложения.
    let uri = "urn:plant:scada-gw-1";
    let issued =
        opcua_cert::ensure(&CertSettings { application_uri: uri.into(), ..CertSettings::in_dir(&issuer) }).unwrap();
    let opts = ConnectOptions {
        security: OpcSecurity::parse(Some("Basic256Sha256"), None, None).unwrap(),
        cert: CertSettings {
            own: Some((issued.cert_path.clone(), issued.key_path.clone())),
            application_uri: uri.into(),
            ..CertSettings::in_dir(&gateway_pki)
        },
        trust_server_certs: true,
    };

    assert!(OpcConnection::connect_with(&url, OP_TIMEOUT, &opts).await.is_err(), "недоверенный клиент принят");
    let rejected: Vec<_> = std::fs::read_dir(server_pki.join("rejected")).expect("rejected/").flatten().collect();
    assert_eq!(rejected.len(), 1, "сервер не отложил сертификат клиента");
    assert!(
        rejected[0].file_name().to_string_lossy().contains(&issued.thumbprint),
        "сервер получил не тот сертификат: {:?}, ждали отпечаток {}",
        rejected[0].file_name(),
        issued.thumbprint
    );
    assert!(!gateway_pki.join("own/cert.der").exists(), "при своём сертификате самоподписанный не выпускается");

    std::fs::create_dir_all(server_pki.join("trusted")).unwrap();
    std::fs::rename(rejected[0].path(), server_pki.join("trusted").join(rejected[0].file_name())).unwrap();
    let conn = OpcConnection::connect_with(&url, OP_TIMEOUT, &opts).await.unwrap_or_else(|e| panic!("{e:#}"));
    let ns = handle.get_namespace_index(NS_URI).unwrap();
    let values = conn.read(&[opcua::read_value_id(&format!("ns={ns};s=Float")).unwrap()]).await.unwrap();
    let (value, quality, _) = opcua::reading(&values[0]);
    assert_eq!((value, quality), (Some(TagValue::F32(1.5)), Quality::Good));
    conn.close().await;
    handle.cancel();
}
