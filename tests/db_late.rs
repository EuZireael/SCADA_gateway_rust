//! Шлюз запускается без БД (#105): PostgreSQL недоступна при запуске (TCP-прокси рвёт соединения) — телеметрия и
//! команды по имени работают сразу, health — `db: DOWN` с причиной; БД появилась — миграции и синхронизация в фоне,
//! у тегов и контроллеров появляются номера, событие `SYSTEM` пишется в журнал.
//!
//!   cargo test --test db_late -- --ignored      (симулятор + Kafka + PostgreSQL IT_DATABASE_URL, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config};
use common::proxy::{Mode, Proxy};
use scada_gateway::config::parse_jdbc_url;

/// Режим `M` мотора — OPC UA-контроллер симулятора, симулятор его не перетирает.
const TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M3.M";

/// Отправить команду по имени тега и вернуть статус результата (с повтором: консьюмер команд мог ещё не назначиться).
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, value: Value) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": TAG, "value": value, "requestedBy": "it"}).to_string();
    for _ in 0..5 {
        producer
            .send(FutureRecord::to(&gw.topic("commands")).key(TAG).payload(&body), Duration::from_secs(5))
            .await
            .expect("команда не отправлена");
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Ok(Ok(m)) =
            tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), results.recv()).await
        {
            let result: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
            if result["commandId"] == id {
                return result["status"].as_str().unwrap_or_default().to_string();
            }
        }
    }
    panic!("нет результата команды {TAG} = {value}\n{}", gw.log_tail());
}

/// JSON ответа шлюза на GET `path`.
fn get_json(gw: &Gateway, path: &str) -> Value {
    let (code, body) = gw.get(path).unwrap_or_else(|| panic!("{path}: шлюз не отвечает\n{}", gw.log_tail()));
    assert_eq!(code, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

/// Номера контроллеров из `/api/status`.
fn controller_ids(gw: &Gateway) -> Vec<i64> {
    let status = get_json(gw, "/api/status");
    status["controllers"].as_array().unwrap().iter().map(|c| c["id"].as_i64().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор, Kafka и PostgreSQL (IT_DATABASE_URL): cargo test -- --ignored"]
async fn gateway_starts_without_db_and_prepares_it_when_it_appears() {
    let url = std::env::var("IT_DATABASE_URL").expect("IT_DATABASE_URL не задан");
    let db = parse_jdbc_url(&url).expect("IT_DATABASE_URL");
    let proxy = Proxy::start(format!("{}:{}", db.host, db.port)).await;
    proxy.set(Mode::Reset);
    let through_proxy = format!("jdbc:postgresql://127.0.0.1:{}/{}", proxy.port, db.database);
    let since = chrono::Utc::now();
    let gw = Gateway::start_with(&common::controllers_path(), &[("SPRING_DATASOURCE_URL", through_proxy.as_str())]);

    // БД нет, а шлюз работает: связь с контроллерами, телеметрия в Kafka, команда по имени.
    gw.wait_links("UP", Duration::from_secs(40)).await;
    assert!(gw.log().contains("недоступна при запуске"), "{}", gw.log_tail());
    let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
    let frame = tokio::time::timeout(Duration::from_secs(15), telemetry.recv()).await;
    assert!(matches!(frame, Ok(Ok(_))), "нет телеметрии без БД\n{}", gw.log_tail());
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    assert_eq!(command(&gw, &producer, &results, json!(1)).await, "APPLIED", "{}", gw.log_tail());
    let health = get_json(&gw, "/actuator/health");
    assert_eq!(health["components"]["db"]["status"], "DOWN", "{health}");
    let reason = health["components"]["db"]["details"]["reason"].as_str().unwrap_or_default();
    assert!(reason.contains("с запуска"), "причина в health: {health}");
    assert!(controller_ids(&gw).iter().all(|id| *id == 0), "номеров из БД ещё нет");

    // БД появилась: шлюз сам применяет миграции и синхронизирует теги (повтор раз в 5 с).
    proxy.set(Mode::Pass);
    let deadline = Instant::now() + Duration::from_secs(30);
    while controller_ids(&gw).iter().any(|id| *id <= 0) {
        assert!(Instant::now() < deadline, "номера контроллеров не появились\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let health = get_json(&gw, "/actuator/health");
    assert_eq!(health["components"]["db"]["status"], "UP", "{health}");

    // Событие о подключении — в журнале БД (оно же доказывает, что журнал снова пишется).
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let events = get_json(&gw, "/api/events/type/SYSTEM?limit=50");
        let found = events.as_array().unwrap().iter().any(|e| {
            e["message"].as_str().is_some_and(|m| m.starts_with("БД подключена через"))
                && e["eventTime"].as_str().and_then(|t| t.parse::<chrono::DateTime<chrono::Utc>>().ok()) > Some(since)
        });
        if found {
            break;
        }
        assert!(Instant::now() < deadline, "нет события SYSTEM о подключении БД: {events}\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Команды по-прежнему работают; значение тега — как до теста.
    assert_eq!(command(&gw, &producer, &results, json!(0)).await, "APPLIED", "{}", gw.log_tail());
    gw.delete_topics().await;
}
