//! Консьюмер команд не теряет команды (#135):
//!   * один из адресов в `SPRING_KAFKA_BOOTSTRAP_SERVERS` недоступен (кластер из нескольких брокеров, один упал; или
//!     `localhost`, который разрешается и в IPv6, где брокер не слушает) — librdkafka сообщает о неудачном соединении
//!     ошибкой приёма и переподключается сама; шлюз не пересоздаёт из-за этого консьюмер;
//!   * новая станция (у группы позиций ещё нет): команда, пришедшая, пока шлюз перезапускался, исполняется — позиция
//!     сохраняется при первом же запуске, и новый консьюмер не начинает с конца топика.
//!
//!   cargo test --test command_consumer -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config, new_prefix, recv_until};

/// Режим `M` клапана — PAC-контроллер симулятора.
const TAG: &str = "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M";
/// Адреса, где брокера нет: соединение сразу отклоняется. Их несколько: librdkafka выбирает адрес для запроса
/// случайно, и с одним недоступным из двух ошибка соединения случается не в каждом запуске.
const DEAD_BROKERS: &str = "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3";
const CONSUMING: &str = "Приём команд включён";

/// Отправить команду один раз (без повтора); вернуть её commandId.
async fn send(producer: &FutureProducer, topic: &str, value: i64) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": TAG, "value": value, "requestedBy": "it"}).to_string();
    producer.send(FutureRecord::to(topic).key(TAG).payload(&body), Duration::from_secs(5)).await.expect("команда");
    id
}

/// Статусы результатов команд `ids`, пришедших за `within`.
async fn statuses(results: &StreamConsumer, ids: &[String], within: Duration) -> HashMap<String, String> {
    let mut got = HashMap::new();
    let deadline = Instant::now() + within;
    while got.len() < ids.len() {
        let Some(m) = recv_until(results, deadline).await else { break };
        let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        let id = r["commandId"].as_str().unwrap_or_default().to_string();
        if ids.contains(&id) {
            got.insert(id, r["status"].as_str().unwrap_or_default().to_string());
        }
    }
    got
}

/// Дождаться, пока шлюз начнёт читать команды.
async fn wait_consuming(gw: &Gateway) {
    let since = Instant::now();
    while !gw.log().contains(CONSUMING) {
        assert!(since.elapsed() < Duration::from_secs(30), "приём команд не включился\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn an_unreachable_broker_in_the_list_does_not_stop_commands() {
    let bootstrap = format!("{DEAD_BROKERS},{}", common::kafka_bootstrap());
    let gw = Gateway::start_with(&common::controllers_path(), &[("SPRING_KAFKA_BOOTSTRAP_SERVERS", &bootstrap)]);
    gw.wait_links("UP", Duration::from_secs(40)).await;
    wait_consuming(&gw).await;
    // Время на ошибки соединения с недоступным адресом.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    let mut ids = Vec::new();
    for value in [1, 0, 1, 0, 1, 0] {
        ids.push(send(&producer, &gw.topic("commands"), value).await);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let got = statuses(&results, &ids, Duration::from_secs(30)).await;
    let restarts = gw.log().matches(CONSUMING).count();
    println!("результатов {} из {}, включений приёма команд {restarts}", got.len(), ids.len());
    assert_eq!(got.len(), ids.len(), "потеряны команды: {got:?}\n{}", gw.log_tail());
    assert!(got.values().all(|s| s == "APPLIED"), "{got:?}");
    assert_eq!(restarts, 1, "консьюмер команд пересоздавался из-за недоступного адреса\n{}", gw.log_tail());
    gw.delete_topics().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn a_command_sent_while_a_new_station_restarts_is_not_skipped() {
    // Свои топики — у группы позиций в них ещё нет, как на новой станции.
    let prefix = new_prefix();
    let mut first = Gateway::start_on(&prefix, &common::controllers_path(), &[]);
    first.wait_links("UP", Duration::from_secs(40)).await;
    wait_consuming(&first).await;
    first.terminate();

    let producer: FutureProducer = kafka_config().create().unwrap();
    let id = send(&producer, &format!("{prefix}.commands"), 1).await;
    let results = consumer_from_beginning(&format!("{prefix}.results")).await;

    let second = Gateway::start_on(&prefix, &common::controllers_path(), &[]);
    second.wait_links("UP", Duration::from_secs(40)).await;
    let got = statuses(&results, std::slice::from_ref(&id), Duration::from_secs(20)).await;
    assert_eq!(
        got.get(&id).map(String::as_str),
        Some("APPLIED"),
        "команда, пришедшая во время перезапуска, пропущена\n{}",
        second.log_tail()
    );
    second.delete_topics().await;
}
