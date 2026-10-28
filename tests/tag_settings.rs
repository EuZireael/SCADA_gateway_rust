//! Настройки тегов из топика `channel.tag-settings`: шлюз как отдельный процесс против PLC-симулятора и Kafka.
//! Тег с настройкой «период 3 с» публикуется не чаще раза в 3 с, хотя его значение меняется каждую секунду;
//! после tombstone (`null`) он снова публикуется каждый цикл. Топик настроек пишет тест (в работе его ведёт `channel`).
//!
//!   cargo test --test tag_settings -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::controllers_path;
use common::gateway::{Gateway, consumer_from_beginning, kafka_config, new_prefix, recv_until};

/// Записываемый дискретный тег OPC UA (его значение переключаем командами).
const TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M3.M";

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Отправить команду записи, не дожидаясь результата.
async fn write(gw: &Gateway, producer: &FutureProducer, value: i64) {
    let body =
        json!({"commandId": uuid::Uuid::new_v4().to_string(), "tagName": TAG, "value": value, "requestedBy": "it"})
            .to_string();
    producer
        .send(FutureRecord::to(&gw.topic("commands")).key(TAG).payload(&body), Duration::from_secs(5))
        .await
        .expect("команда не отправлена");
}

/// Сколько секунд переключать значение тега каждую секунду; потом — моменты (мс Kafka) сообщений о теге за это время.
async fn toggle_and_collect(
    gw: &Gateway,
    producer: &FutureProducer,
    telemetry: &StreamConsumer,
    seconds: u64,
) -> Vec<i64> {
    let start = now_ms();
    for i in 0..seconds {
        write(gw, producer, (i % 2) as i64).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Дочитать то, что уже лежит в топике телеметрии.
    let mut times = Vec::new();
    while let Some(m) = recv_until(telemetry, Instant::now() + Duration::from_millis(1500)).await {
        let ts = m.timestamp().to_millis().unwrap_or(0);
        if m.key() == Some(TAG.as_bytes()) && ts >= start {
            times.push(ts);
        }
    }
    times
}

async fn put_setting(producer: &FutureProducer, topic: &str, payload: Option<&Value>) {
    let body = payload.map(|v| v.to_string());
    let mut record = FutureRecord::to(topic).key(TAG);
    if let Some(b) = &body {
        record = record.payload(b);
    }
    producer.send(record, Duration::from_secs(5)).await.expect("настройка не отправлена");
}

async fn wait_metric(gw: &Gateway, name: &str, expected: f64, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        if gw.metric(name) == Some(expected) {
            return;
        }
        assert!(Instant::now() < deadline, "метрика {name} не стала {expected} за {within:?}\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn tag_settings_slow_the_poll_and_a_tombstone_restores_it() {
    let prefix = new_prefix();
    let topic = format!("{prefix}.tag-settings");
    // Топик настроек в работе создаёт `channel`; в тесте — мы, до шлюза, одной партицией.
    let admin: AdminClient<DefaultClientContext> = kafka_config().create().unwrap();
    admin
        .create_topics(&[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))], &AdminOptions::new())
        .await
        .expect("создание топика настроек");
    let gw = Gateway::start_on(&prefix, &controllers_path(), &[("CHANNEL_TAG_SETTINGS_TOPIC", topic.as_str())]);
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
    // Первая команда может прийти раньше, чем шлюз назначит консьюмер команд: повторяем, пока тег не заменит значение.
    for _ in 0..10 {
        write(&gw, &producer, 1).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    wait_metric(&gw, "scada_tag_settings_tags", 0.0, Duration::from_secs(5)).await;

    // 1. Без настроек: значение меняется каждую секунду, и каждое изменение публикуется.
    let free = toggle_and_collect(&gw, &producer, &telemetry, 8).await;
    assert!(free.len() >= 6, "без настроек ждали ≥ 6 публикаций за 8 с, получили {}\n{}", free.len(), gw.log_tail());

    // 2. Период 3 с: публикаций не чаще раза в ~3 с, хотя значение по-прежнему меняется каждую секунду.
    put_setting(
        &producer,
        &topic,
        Some(&json!({"log": true, "deadband": 0.5, "poll": {"mode": "period", "periodMs": 3000}})),
    )
    .await;
    wait_metric(&gw, "scada_tag_settings_tags", 1.0, Duration::from_secs(30)).await;
    let slow = toggle_and_collect(&gw, &producer, &telemetry, 12).await;
    assert!(
        (2..=5).contains(&slow.len()),
        "с периодом 3 с за 12 с ждали 2–5 публикаций, получили {}: {slow:?}\n{}",
        slow.len(),
        gw.log_tail()
    );
    for pair in slow.windows(2) {
        assert!(pair[1] - pair[0] >= 2_400, "публикации чаще периода: {slow:?}");
    }

    // 3. Tombstone: настройки сняты, тег снова читается каждый цикл.
    put_setting(&producer, &topic, None).await;
    wait_metric(&gw, "scada_tag_settings_tags", 0.0, Duration::from_secs(30)).await;
    let again = toggle_and_collect(&gw, &producer, &telemetry, 8).await;
    assert!(
        again.len() >= 6,
        "после tombstone ждали ≥ 6 публикаций за 8 с, получили {}\n{}",
        again.len(),
        gw.log_tail()
    );
}
