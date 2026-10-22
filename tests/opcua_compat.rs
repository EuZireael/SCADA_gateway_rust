//! Совместимость клиента OPC UA шлюза с независимым сервером — OPC PLC (стек OPC Foundation .NET), а не async-opcua,
//! на котором построены симулятор и фасад `ptusa-opcua`: ошибки, общие для клиента и сервера одной библиотеки, и
//! особенности других серверов (строгость типов, пределы, коды ошибок, защита) видны только так.
//!
//! Сервер — контейнер `mcr.microsoft.com/iotedge/opc-plc` с узлами из `tests/compat/opc_plc_nodes.json` (в CI —
//! задание `compat`; как запустить у себя — `docs/DEVELOPMENT.md`, «Совместимость с независимым сервером OPC UA»):
//!
//!   COMPAT_OPCUA_URL=opc.tcp://127.0.0.1:50000 cargo test --test opcua_compat -- --ignored
//!
//! Переменные: `COMPAT_OPCUA_URL` — адрес сервера (не задан — тесты пропускаются: общее задание интеграционных тестов
//! сервер не поднимает); `COMPAT_OPCUA_USER` / `COMPAT_OPCUA_PASSWORD` — как `--du`/`--dc` у сервера (по умолчанию
//! operator / operator-pass); `COMPAT_OPCUA_CONTAINER` — имя контейнера для теста обрыва (не задано — он пропускается).

mod common;

use std::sync::Arc;
use std::time::Duration;

use scada_gateway::config;
use scada_gateway::model::{Controller, OpcSecurity, Quality, Tag, TagValue};
use scada_gateway::opcua::{self, ConnectOptions, OpcConnection, ReadValueId};
use scada_gateway::opcua_cert::CertSettings;

use common::gateway::Gateway;

const OP_TIMEOUT: Duration = Duration::from_secs(10);
/// Тест обрыва останавливает контейнер сервера — остальные тесты в это время не идут (он берёт запись, они — чтение).
static SERVER: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());
/// Пространство имён узлов OPC PLC, в том числе из файла узлов (номер `ns` зависит от версии сервера — адреса `nsu=`).
const NS: &str = "http://microsoft.com/Opc/OpcPlc/";

/// Адрес сервера; `None` — тест пропускается.
fn url() -> Option<String> {
    let url = std::env::var("COMPAT_OPCUA_URL").ok();
    if url.is_none() {
        eprintln!("COMPAT_OPCUA_URL не задан — проверка совместимости с OPC PLC пропущена");
    }
    url
}

fn creds() -> (String, String) {
    (
        std::env::var("COMPAT_OPCUA_USER").unwrap_or_else(|_| "operator".into()),
        std::env::var("COMPAT_OPCUA_PASSWORD").unwrap_or_else(|_| "operator-pass".into()),
    )
}

/// Скалярные узлы файла узлов: тип узла, `dataType` тега, значение для записи (у края диапазона типа).
fn scalars() -> Vec<(&'static str, &'static str, TagValue)> {
    vec![
        ("Boolean", "BOOL", TagValue::Bool(true)),
        ("SByte", "SINT", TagValue::Int(-100)),
        ("Byte", "BYTE", TagValue::Int(200)),
        ("Int16", "INT16", TagValue::Int(-30000)),
        ("UInt16", "WORD", TagValue::Int(60000)),
        ("Int32", "INT32", TagValue::Int(-2_000_000_000)),
        ("UInt32", "UDINT", TagValue::Int(4_000_000_000)),
        ("Int64", "LINT", TagValue::Int(-9_000_000_000_000)),
        ("UInt64", "ULINT", TagValue::Int(9_000_000_000_000)),
        ("Float", "FLOAT", TagValue::F64(64.5)),
        ("Double", "LREAL", TagValue::F64(1.0e10)),
        ("String", "STRING", TagValue::Text("РЕЦЕПТ 1".into())),
    ]
}

/// Адрес узла из файла узлов.
fn node(name: &str) -> String {
    format!("nsu={NS};s=Gw.{name}")
}

/// Тег станции: имя, nodeId, dataType.
type TagDef = (String, String, String);

fn def(name: &str, node: String, dt: &str) -> TagDef {
    (name.to_string(), node, dt.to_string())
}

/// YAML станции из одного OPC UA-контроллера с этими тегами.
fn station_yaml(url: &str, defs: &[TagDef], polling_ms: u32) -> String {
    let lines: Vec<String> = defs
        .iter()
        .map(|(name, node, dt)| {
            format!(
                "        - {{name: \"{name}\", nodeId: '{node}', dataType: {dt}, pollingRate: {polling_ms}, enabled: true, writable: true}}"
            )
        })
        .collect();
    format!(
        "opcua:\n  servers:\n    - id: compat\n      name: OPC PLC\n      endpoint: \"{url}\"\n      enabled: true\n      tags:\n{}\n",
        lines.join("\n")
    )
}

/// Теги из YAML станции — так же, как их видит шлюз.
fn tags(url: &str, defs: &[TagDef]) -> Vec<Arc<Tag>> {
    let servers = config::parse_controllers(&station_yaml(url, defs, 1000)).expect("станция");
    Controller::from_config(&servers[0]).expect("контроллер").tags.clone()
}

async fn connect(url: &str) -> OpcConnection {
    OpcConnection::connect(url, OP_TIMEOUT).await.unwrap_or_else(|e| panic!("OPC PLC {url} недоступен: {e:#}"))
}

/// Чтение и запись во всех скалярных типах: шлюз узнаёт тип узла и пишет в нём, сервер другого стека принимает,
/// прочитанное совпадает с записанным. Адреса — `nsu=`.
#[tokio::test]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL): cargo test --test opcua_compat -- --ignored"]
async fn every_scalar_type_is_read_and_written_in_the_node_type() {
    let Some(url) = url() else { return };
    let _shared = SERVER.read().await;
    let defs: Vec<_> = scalars().iter().map(|(t, dt, _)| def(t, node(t), dt)).collect();
    let tags = tags(&url, &defs);
    let mut conn = connect(&url).await;
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    assert!(!prepared.report.has_problems(), "{:?}", prepared.report);
    for ((type_name, data_type, value), tag) in scalars().into_iter().zip(&tags) {
        let info = conn.node(&tag.node_id).unwrap();
        assert_eq!(info.kind.map(|k| k.to_string()).as_deref(), Some(type_name), "{type_name}: тип узла");
        let variant = info.variant(data_type, &value).unwrap_or_else(|e| panic!("{type_name}: {e:#}"));
        let status = conn.write(info.id.clone(), variant).await.unwrap();
        assert!(status.is_good(), "{type_name}: сервер отклонил запись: {status}");
        let read = conn.read(&[ReadValueId::from(&info.id)]).await.unwrap();
        let (got, quality, _) = opcua::reading(&read[0]);
        assert_eq!(quality, Quality::Good, "{type_name}");
        let got = got.unwrap_or_else(|| panic!("{type_name}: прочитано пустое значение"));
        let same = match (&got, &value) {
            (TagValue::Text(a), TagValue::Text(b)) => a == b,
            (TagValue::Bool(a), TagValue::Bool(b)) => a == b,
            (a, b) => a.as_f64() == b.as_f64(),
        };
        assert!(same, "{type_name}: записано {value}, прочитано {got}");
    }
    conn.close().await;
}

/// Сервер другого стека тип записи проверяет строго: запись по `dataType` тега, как было до #97 (`Int32` в узел
/// `Int16`), отвергается. Значение вне диапазона типа узла шлюз не отправляет вовсе.
#[tokio::test]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL): cargo test --test opcua_compat -- --ignored"]
async fn the_independent_server_rejects_a_value_of_another_type() {
    let Some(url) = url() else { return };
    let _shared = SERVER.read().await;
    let tags = tags(&url, &[def("i16", node("Int16"), "INT16"), def("b", node("Byte"), "BYTE")]);
    let mut conn = connect(&url).await;
    conn.prepare(&tags).await.expect("разбор узлов");
    let info = conn.node(&tags[0].node_id).unwrap();
    let old_way = opcua::to_variant("INT16", &TagValue::Int(5)).unwrap();
    let status = conn.write(info.id.clone(), old_way).await.unwrap();
    assert!(!status.is_good(), "сервер принял Int32 в узел Int16: {status}");
    let byte = conn.node(&tags[1].node_id).unwrap();
    assert!(byte.variant("BYTE", &TagValue::Int(300)).is_err(), "300 не помещается в Byte");
    conn.close().await;
}

/// Проблемы с узлами видны при подключении и на сервере другого стека: узла нет, массив, `DateTime`; остальные теги
/// читаются.
#[tokio::test]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL): cargo test --test opcua_compat -- --ignored"]
async fn node_problems_are_reported() {
    let Some(url) = url() else { return };
    let _shared = SERVER.read().await;
    let tags = tags(
        &url,
        &[
            def("missing", node("NoSuchNode"), "FLOAT"),
            def("array", node("FloatArray"), "FLOAT"),
            def("time", node("Time"), "STRING"),
            def("ok", node("Float"), "FLOAT"),
        ],
    );
    let mut conn = connect(&url).await;
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    let r = &prepared.report;
    let has = |list: &[String], name: &str| list.iter().any(|s| s.starts_with(&format!("{name} (")));
    assert!(has(&r.unknown, "missing"), "{r:?}");
    assert!(has(&r.unsupported, "array"), "{r:?}");
    assert!(has(&r.unsupported, "time"), "{r:?}");
    assert_eq!(r.unresolved.len() + r.unknown.len() + r.unsupported.len() + r.mismatched.len(), 3, "{r:?}");
    let values = conn.read(&prepared.nodes).await.unwrap();
    let quality: Vec<_> = values.iter().map(|dv| opcua::reading(dv).1).collect();
    assert_eq!(quality, [Quality::Bad, Quality::Bad, Quality::Bad, Quality::Good]);
    conn.close().await;
}

/// 1200 узлов (`--sn=1200` у сервера): шлюз читает пачками не больше предела сервера (`MaxNodesPerRead`), все значения
/// на месте.
#[tokio::test]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL): cargo test --test opcua_compat -- --ignored"]
async fn many_nodes_are_read_within_the_server_limit() {
    let Some(url) = url() else { return };
    let _shared = SERVER.read().await;
    let defs: Vec<_> =
        (1..=1200).map(|i| def(&format!("slow{i}"), format!("nsu={NS};s=SlowUInt{i}"), "UDINT")).collect();
    let tags = tags(&url, &defs);
    let mut conn = connect(&url).await;
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    assert!(!prepared.report.has_problems(), "{:?}", prepared.report);
    if let Some(limit) = prepared.report.max_nodes_per_read.filter(|&n| n > 0) {
        assert!(conn.read_chunk() <= limit as usize, "пачка {} больше предела {limit}", conn.read_chunk());
    }
    let values = conn.read(&prepared.nodes).await.expect("чтение 1200 узлов");
    assert_eq!(values.len(), 1200);
    assert!(values.iter().all(|dv| opcua::reading(dv).1 == Quality::Good));
    conn.close().await;
}

/// Защищённый канал Basic256Sha256/SignAndEncrypt с пользователем: сертификат сервера, которого нет в `trusted/`,
/// не принимается и откладывается в `rejected/`; после переноса в `trusted/` подключение проходит, узлы читаются.
/// Неверный пароль отвергается.
#[tokio::test]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL): cargo test --test opcua_compat -- --ignored"]
async fn secure_channel_with_user_and_server_trust() {
    let Some(url) = url() else { return };
    let _shared = SERVER.read().await;
    let pki = tempfile::tempdir().unwrap();
    let (user, pass) = creds();
    let opts = |password: &str| ConnectOptions {
        security: OpcSecurity::parse(Some("Basic256Sha256"), Some(&user), Some(password)).unwrap(),
        cert: CertSettings::in_dir(pki.path()),
        trust_server_certs: false,
    };
    assert!(
        OpcConnection::connect_with(&url, OP_TIMEOUT, &opts(&pass)).await.is_err(),
        "неизвестный сертификат сервера принят"
    );
    let rejected: Vec<_> = std::fs::read_dir(pki.path().join("rejected")).expect("rejected/").flatten().collect();
    assert!(!rejected.is_empty(), "сертификат сервера не отложен в rejected/");
    std::fs::create_dir_all(pki.path().join("trusted")).unwrap();
    for cert in rejected {
        std::fs::rename(cert.path(), pki.path().join("trusted").join(cert.file_name())).unwrap();
    }

    let mut conn = OpcConnection::connect_with(&url, OP_TIMEOUT, &opts(&pass))
        .await
        .unwrap_or_else(|e| panic!("доверенный сервер, верный пароль: {e:#}"));
    let tags = tags(&url, &[def("f", node("Float"), "FLOAT")]);
    let prepared = conn.prepare(&tags).await.expect("разбор узлов");
    let values = conn.read(&prepared.nodes).await.unwrap();
    assert_eq!(opcua::reading(&values[0]).1, Quality::Good);
    conn.close().await;

    assert!(
        OpcConnection::connect_with(&url, OP_TIMEOUT, &opts("не тот пароль")).await.is_err(),
        "неверный пароль принят"
    );
}

/// Обрыв: контейнер сервера остановлен — шлюз (процессом) видит потерю связи; запущен снова — шлюз сам восстанавливает
/// связь, без перезапуска.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужен OPC PLC (COMPAT_OPCUA_URL, COMPAT_OPCUA_CONTAINER): cargo test --test opcua_compat -- --ignored"]
async fn the_gateway_reconnects_after_the_server_restarts() {
    let Some(url) = url() else { return };
    let _exclusive = SERVER.write().await;
    let Ok(container) = std::env::var("COMPAT_OPCUA_CONTAINER") else {
        eprintln!("COMPAT_OPCUA_CONTAINER не задан — проверка обрыва пропущена");
        return;
    };
    let docker = |action: &str| {
        let status = std::process::Command::new("docker").args([action, &container]).status().unwrap();
        assert!(status.success(), "docker {action} {container}");
    };
    let dir = tempfile::tempdir().unwrap();
    let yaml = dir.path().join("controllers.yaml");
    std::fs::write(&yaml, station_yaml(&url, &[def("f", node("Float"), "FLOAT")], 500)).unwrap();
    let gw = Gateway::start_with(&yaml, &[("KAFKA_ENABLED", "false"), ("DB_ENABLED", "false")]);
    gw.wait_links("UP", Duration::from_secs(30)).await;

    docker("stop");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let lost = loop {
        let links = gw.links();
        if !links.is_empty() && links.values().all(|s| s == "DOWN") {
            break true;
        }
        if std::time::Instant::now() > deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    // Сервер — обратно в любом случае: следующий тест и повторный запуск ждут его.
    docker("start");
    assert!(lost, "остановку сервера шлюз не заметил\n{}", gw.log_tail());
    gw.wait_links("UP", Duration::from_secs(60)).await;
    assert!(gw.log().contains("восстановлена"), "{}", gw.log_tail());
}
