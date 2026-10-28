//! Подписки OPC UA (`acquisition: subscription`) на процессе шлюза против симулятора и Kafka:
//!   * значения тегов приходят в Kafka по подписке (в журнале — подписки, а не опрос), изменение узла (команда монитора)
//!     доходит до телеметрии быстрее периода опроса по умолчанию;
//!   * обрыв связи и её возврат (через TCP-прокси): тег `BAD`, контроллер `DOWN`, потом снова `GOOD` и `UP` — подписки
//!     пересоздаются на новой сессии.
//!
//!   cargo test --test subscription -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config, recv_until};
use common::proxy::{Mode, Proxy};
use common::{controller, sim_host};
use scada_gateway::model::{ControllerKind, Protocol};

const FAST: [(&str, &str); 5] = [
    ("GATEWAY_OPCUA_OP_TIMEOUT_MS", "2000"),
    ("GATEWAY_MODBUS_OP_TIMEOUT_MS", "1000"),
    ("GATEWAY_PAC_OP_TIMEOUT_MS", "1000"),
    ("GATEWAY_STALE_AFTER_MS", "3000"),
    ("GATEWAY_SUPERVISE_INTERVAL_MS", "1000"),
];
const OPC_TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M3.M";

/// Станция из одного OPC UA-контроллера симулятора (через `endpoint`) с заданным способом получения значений; сам тег
/// `OPC_TAG` записываемый, остальные — читаются.
fn station(dir: &std::path::Path, endpoint: &str, acquisition: &str) -> std::path::PathBuf {
    let ctrl = controller(ControllerKind::OpcUa);
    let tags: Vec<String> = ctrl
        .tags
        .iter()
        .filter(|t| t.protocol == Protocol::OpcUa)
        .filter(|t| t.name == OPC_TAG)
        .chain(ctrl.tags.iter().filter(|t| t.protocol == Protocol::OpcUa && t.name != OPC_TAG).take(5))
        .map(|t| {
            format!(
                "        - {{name: \"{}\", nodeId: \"{}\", dataType: {}, pollingRate: 200, enabled: true, writable: {}}}",
                t.name,
                t.node_id,
                t.data_type,
                t.name == OPC_TAG
            )
        })
        .collect();
    assert!(tags.iter().any(|t| t.contains("writable: true")), "в конфигурации нет тега {OPC_TAG}");
    let path = dir.join("controllers.yaml");
    std::fs::write(
        &path,
        format!(
            "opcua:\n  servers:\n    - name: Sub\n      endpoint: \"{endpoint}\"\n      acquisition: {acquisition}\n      enabled: true\n      tags:\n{}\n",
            tags.join("\n")
        ),
    )
    .unwrap();
    path
}

/// Последнее значение тега в телеметрии после `since`, ждём изменения на `want` не дольше `within`.
async fn wait_value(telemetry: &StreamConsumer, tag: &str, want: &Value, within: Duration) -> Option<Duration> {
    let started = Instant::now();
    let deadline = started + within;
    while let Some(m) = recv_until(telemetry, deadline).await {
        if m.key() != Some(tag.as_bytes()) {
            continue;
        }
        let body: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        if body["quality"] == "GOOD" && &body["value"] == want {
            return Some(started.elapsed());
        }
    }
    None
}

async fn send_command(gw: &Gateway, producer: &FutureProducer, value: i64) {
    let body =
        json!({"commandId": uuid::Uuid::new_v4().to_string(), "tagName": OPC_TAG, "value": value, "requestedBy": "it"})
            .to_string();
    producer
        .send(FutureRecord::to(&gw.topic("commands")).key(OPC_TAG).payload(&body), Duration::from_secs(5))
        .await
        .expect("команда не отправлена");
}

/// Подписки: значения идут в Kafka, изменение узла доходит быстро, шлюз пишет в журнал про подписки.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn changes_arrive_by_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = controller(ControllerKind::OpcUa).endpoint;
    let gw = Gateway::start_with(&station(dir.path(), &endpoint, "subscription"), &FAST);
    gw.wait_links("UP", Duration::from_secs(30)).await;
    assert!(gw.log().contains("подписки OPC UA"), "в журнале нет подписок\n{}", gw.log_tail());
    assert!(!gw.log().contains("подписки не созданы"), "{}", gw.log_tail());
    let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    // Два изменения подряд: туда и обратно — оба доходят.
    for value in [1, 0] {
        send_command(&gw, &producer, value).await;
        let took = wait_value(&telemetry, OPC_TAG, &json!(value), Duration::from_secs(10)).await;
        let took = took.unwrap_or_else(|| panic!("значение {value} не пришло в телеметрию\n{}", gw.log_tail()));
        eprintln!("изменение на {value} дошло до Kafka за {} мс", took.as_millis());
    }
    gw.delete_topics().await;
}

/// Обрыв связи и возврат: теги BAD и контроллер DOWN, затем подписки на новой сессии и снова GOOD / UP.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn subscriptions_survive_a_link_loss() {
    let dir = tempfile::tempdir().unwrap();
    let upstream = format!("{}:4840", sim_host());
    let proxy = Proxy::start(upstream).await;
    let gw = Gateway::start_with(
        &station(dir.path(), &format!("opc.tcp://127.0.0.1:{}", proxy.port), "subscription"),
        &FAST,
    );
    gw.wait_links("UP", Duration::from_secs(30)).await;
    for mode in [Mode::Reset, Mode::Hang] {
        proxy.set(mode);
        gw.wait_links("DOWN", Duration::from_secs(30)).await;
        proxy.set(Mode::Pass);
        gw.wait_links("UP", Duration::from_secs(40)).await;
        // На новой сессии подписки созданы заново, и изменения идут.
        let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
        let producer: FutureProducer = kafka_config().create().unwrap();
        for value in [1, 0] {
            send_command(&gw, &producer, value).await;
            let seen = wait_value(&telemetry, OPC_TAG, &json!(value), Duration::from_secs(15)).await;
            assert!(seen.is_some(), "после {mode:?} значение {value} не пришло\n{}", gw.log_tail());
        }
    }
    assert!(
        gw.log().matches("подписки OPC UA").count() >= 3,
        "подписки пересоздаются на каждой сессии\n{}",
        gw.log_tail()
    );
    gw.delete_topics().await;
}
