//! Проверка, что шлюз подключился к нужному контроллеру (`identity` в `controllers.yaml`), на процессе шлюза против
//! симулятора и Kafka:
//!   * имя совпало (PAC — `PAC_name` из handshake; OPC UA — значение узла `SYSTEM.PAC_NAME` и `ApplicationUri` сервера) —
//!     контроллер на связи, команды исполняются;
//!   * имя другое — контроллер `DOWN`, ни одна команда в него не уходит, событие `CONTROLLER_IDENTITY_MISMATCH`, в
//!     `/api/status` причина; поправили `identity` — связь поднимается сама.
//!
//!   cargo test --test identity -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config};

/// Имя, которым симулятор называет себя и по PAC (`PAC_name`), и по OPC UA (`SYSTEM.PAC_NAME`): имя ПЛК из его конфигурации.
const PLC_NAME: &str = "BN1_MCA1 (Барановичи-1) Two-Protocol";
const APPLICATION_URI: &str = "urn:BN1-MCA1:simulator";
const OPC_TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M3.M";
const PAC_TAG: &str = "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M";

/// controllers.yaml тестов с `identity` у OPC UA- и PAC-контроллера (`None` — без `identity`); Modbus отключён: он здесь ни при чём.
fn station(dir: &std::path::Path, opc: Option<&str>, pac: Option<&str>) -> std::path::PathBuf {
    let yaml = std::fs::read_to_string(common::controllers_path()).unwrap();
    let mut out = yaml.clone();
    for (endpoint, identity) in [
        ("      endpoint: \"opc.tcp://${SIM_HOST:127.0.0.1}:4840\"\n", opc),
        ("      endpoint: \"pac://${SIM_HOST:127.0.0.1}:${SIM_PAC_PORT:10000}\"\n", pac),
    ] {
        assert_eq!(yaml.matches(endpoint).count(), 1, "в controllers.yaml нет {endpoint}");
        if let Some(identity) = identity {
            out = out.replace(endpoint, &format!("{endpoint}      identity: {identity}\n"));
        }
    }
    let path = dir.join("controllers.yaml");
    std::fs::write(&path, out).unwrap();
    path
}

fn name_node() -> String {
    format!("{{pacName: \"{PLC_NAME}\", nodeId: \"ns=2;s=SYSTEM.PAC_NAME\"}}")
}

/// Состояние сверки контроллера из `/api/status`: (статус, причина).
fn identity_of(gw: &Gateway, endpoint_part: &str) -> Option<(String, String)> {
    let (code, body) = gw.get("/api/status")?; // шлюз ещё запускается
    assert_eq!(code, 200);
    let status: Value = serde_json::from_str(&body).unwrap();
    let c = status["controllers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["endpoint"].as_str().unwrap_or_default().contains(endpoint_part))
        .unwrap_or_else(|| panic!("нет контроллера {endpoint_part}: {status}"));
    Some((
        c["identity"]["status"].as_str().unwrap_or_default().to_string(),
        c["identity"]["reason"].as_str().unwrap_or_default().to_string(),
    ))
}

async fn wait_identity(gw: &Gateway, endpoint_part: &str, expected: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let state = identity_of(gw, endpoint_part);
        if let Some((status, reason)) = &state
            && status == expected
        {
            return reason.clone();
        }
        assert!(
            Instant::now() < deadline,
            "{endpoint_part}: сверка {:?}, ждали {expected:?}\n{}",
            state.map(|(status, _)| status),
            gw.log_tail()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Команда с повтором, пока не придёт результат (консьюмер команд мог ещё не назначиться): статус результата.
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, tag: &str, value: Value) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "it"}).to_string();
    for _ in 0..5 {
        producer
            .send(FutureRecord::to(&gw.topic("commands")).key(tag).payload(&body), Duration::from_secs(5))
            .await
            .expect("команда не отправлена");
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Ok(Ok(m)) =
            tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), results.recv()).await
        {
            let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
            if r["commandId"] == id {
                return r["status"].as_str().unwrap_or_default().to_string();
            }
        }
    }
    panic!("нет результата команды {tag} = {value}\n{}", gw.log_tail());
}

/// События типа `CONTROLLER_IDENTITY_MISMATCH` из топика событий.
async fn mismatch_events(events: &StreamConsumer) -> Vec<Value> {
    let mut found = Vec::new();
    while let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(1500), events.recv()).await {
        let e: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap_or_default();
        if e["eventType"] == "CONTROLLER_IDENTITY_MISMATCH" {
            found.push(e);
        }
    }
    found
}

/// Совпало имя — контроллеры на связи и команды исполняются; без `identity` статус `NOT_CONFIGURED`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn a_matching_name_keeps_the_controller_up() {
    let dir = tempfile::tempdir().unwrap();
    let both = station(dir.path(), Some(&name_node()), Some(&format!("{{pacName: \"{PLC_NAME}\"}}")));
    let gw = Gateway::start_with(&both, &[]);
    gw.wait_links("UP", Duration::from_secs(40)).await;
    assert_eq!(wait_identity(&gw, "opc.tcp", "VERIFIED").await, "");
    assert_eq!(wait_identity(&gw, "pac://", "VERIFIED").await, "");
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    for tag in [OPC_TAG, PAC_TAG] {
        assert_eq!(command(&gw, &producer, &results, tag, json!(0)).await, "APPLIED", "{tag}");
    }
    gw.delete_topics().await;

    // ApplicationUri сервера вместо узла — тоже сверка.
    let by_uri = station(dir.path(), Some(&format!("{{applicationUri: \"{APPLICATION_URI}\"}}")), None);
    let gw = Gateway::start_with(&by_uri, &[]);
    assert_eq!(wait_identity(&gw, "opc.tcp", "VERIFIED").await, "");
    assert_eq!(wait_identity(&gw, "pac://", "NOT_CONFIGURED").await, "");
    assert!(
        gw.log().contains("Имя контроллера не проверяется"),
        "предупреждение о PAC без identity\n{}",
        gw.log_tail()
    );
    gw.delete_topics().await;
}

/// Чужое имя: контроллер `DOWN`, команда в него не уходит (`FAILED_*`, а не `APPLIED`), событие и причина в `/api/status`.
/// Контроллер с верным именем рядом работает. Имя поправили — связь поднялась сама.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn another_controllers_name_is_refused_and_no_command_goes_there() {
    let dir = tempfile::tempdir().unwrap();
    // OPC UA — верное имя узла, PAC — чужое (латинские «MCA» вместо русских «МСА» — типичная опечатка).
    let wrong_pac = "{pacName: \"BN1-MCA1\"}";
    let wrong = station(dir.path(), Some(&name_node()), Some(wrong_pac));
    let gw = Gateway::start_with(&wrong, &[]);
    let reason = wait_identity(&gw, "pac://", "MISMATCH").await;
    assert!(reason.contains("BN1-MCA1") && reason.contains(PLC_NAME), "ожидали и полученное имя в причине: {reason}");
    assert!(reason.contains(r"\u{41"), "кириллица в причине экранирована: {reason}");
    assert_eq!(wait_identity(&gw, "opc.tcp", "VERIFIED").await, "");

    let links = gw.links();
    assert!(links.iter().any(|(_, s)| s == "DOWN") && links.iter().any(|(_, s)| s == "UP"), "{links:?}");
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    let events = consumer_from_beginning(&gw.topic("events")).await;
    assert_eq!(command(&gw, &producer, &results, OPC_TAG, json!(0)).await, "APPLIED", "верный контроллер работает");
    let refused = command(&gw, &producer, &results, PAC_TAG, json!(1)).await;
    assert!(refused.starts_with("FAILED"), "команда в чужой контроллер не должна выполняться: {refused}");
    let found = mismatch_events(&events).await;
    assert_eq!(found.len(), 1, "одно событие за минуту, а не по числу попыток: {found:?}\n{}", gw.log_tail());
    assert!(gw.log().contains("подключились не к тому контроллеру"), "{}", gw.log_tail());
    gw.delete_topics().await;

    // OPC UA с чужим значением узла и чужим ApplicationUri — то же.
    let wrong_opc = station(
        dir.path(),
        Some("{pacName: \"другой ПЛК\", nodeId: \"ns=2;s=SYSTEM.PAC_NAME\"}"),
        Some(&format!("{{pacName: \"{PLC_NAME}\"}}")),
    );
    let gw = Gateway::start_with(&wrong_opc, &[]);
    assert!(wait_identity(&gw, "opc.tcp", "MISMATCH").await.contains("другой ПЛК"));
    assert_eq!(wait_identity(&gw, "pac://", "VERIFIED").await, "");
    drop(gw);
    let wrong_uri = station(dir.path(), Some("{applicationUri: \"urn:другой:сервер\"}"), None);
    let gw = Gateway::start_with(&wrong_uri, &[]);
    assert!(wait_identity(&gw, "opc.tcp", "MISMATCH").await.contains("urn:другой:сервер"));
    gw.delete_topics().await;
}
