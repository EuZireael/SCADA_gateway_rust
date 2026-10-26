//! Аварии ПЛК (#139): OPC UA-сервер симулятора отдаёт список аварий узлом `SYSTEM.ALARMS` и исполняет действие методом
//! `SYSTEM.ALARM_CMD`, как фасад ptusa-opcua. Шлюз публикует снимок списка в compact-топик аварий (ключ — id контроллера),
//! исполняет команду `PAC_ALARM` и при обрыве связи публикует снимок `online: false` с последним списком.
//!
//!   cargo test --test pac_alarms -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config};
use common::proxy::{Mode, Proxy};
use common::{OP_TIMEOUT, controller};
use scada_gateway::model::ControllerKind;
use scada_gateway::opcua::{self, OpcConnection};
use scada_gateway::pac_alarms;

/// Тип объекта сообщений симулятора (танк у ptusa).
const OBJECT_TYPE: i64 = 102;
const FAST_TIMEOUTS: [(&str, &str); 3] = [
    ("GATEWAY_OPCUA_OP_TIMEOUT_MS", "2000"),
    ("GATEWAY_STALE_AFTER_MS", "3000"),
    ("GATEWAY_SUPERVISE_INTERVAL_MS", "1000"),
];

/// Завести у симулятора сообщение объекта `object` (запись в служебный узел).
async fn raise(endpoint: &str, object: i32) {
    let conn = OpcConnection::connect(endpoint, OP_TIMEOUT).await.expect("нужен симулятор");
    let node = opcua::parse_node_id("ns=2;s=SYSTEM.ALARM_RAISE").unwrap();
    let status = conn.write(node, ::opcua::types::Variant::Int32(object)).await.unwrap();
    assert!(status.is_good(), "симулятор без аварий (пересоберите образ): {status}");
    conn.close().await;
}

/// Первый снимок контроллера `id`, для которого `pred` истинно (удаления — сообщения без тела — пропускаются).
async fn snapshot(gw: &Gateway, alarms: &StreamConsumer, id: &str, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        // Ошибка приёма (брокер только что поднялся) — не конец: консьюмер переподключается сам.
        let m = match tokio::time::timeout(left, alarms.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => continue,
            Err(_) => panic!("нет снимка аварий: {what}\n{}", gw.log_tail()),
        };
        if m.key() != Some(id.as_bytes()) {
            continue;
        }
        let Some(payload) = m.payload() else { continue };
        let s: Value = serde_json::from_slice(payload).unwrap();
        if pred(&s) {
            return s;
        }
    }
}

/// Аварии объекта `object` в снимке.
fn of_object(s: &Value, object: i64) -> Vec<Value> {
    s["alarms"].as_array().unwrap().iter().filter(|a| a["objectNumber"] == object).cloned().collect()
}

/// Отправить команду (ключ — путь проекта, как у монитора) и дождаться её результата.
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, key: &str, body: Value) -> Value {
    let id = uuid::Uuid::new_v4().to_string();
    let mut body = body;
    body["commandId"] = json!(id);
    body["requestedBy"] = json!("it");
    let payload = body.to_string();
    producer
        .send(FutureRecord::to(&gw.topic("commands")).key(key).payload(&payload), Duration::from_secs(5))
        .await
        .expect("команда");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let m = match tokio::time::timeout(left, results.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => continue,
            Err(_) => panic!("нет результата команды {body}\n{}", gw.log_tail()),
        };
        let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        if r["commandId"] == id {
            return r;
        }
    }
}

/// Политика очистки топика.
async fn cleanup_policy(topic: &str) -> Option<String> {
    let admin: AdminClient<DefaultClientContext> = kafka_config().create().unwrap();
    let described = admin.describe_configs([&ResourceSpecifier::Topic(topic)], &AdminOptions::new()).await.ok()?;
    let config = described.into_iter().next()?.ok()?;
    config.get("cleanup.policy").and_then(|e| e.value.clone())
}

/// Сообщение появляется в снимке, квитирование (`APPLIED`) его убирает, неверные команды отклоняются, обрыв связи
/// публикуется снимком `online: false` с последним списком, возврат — `online: true`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn alarms_are_published_accepted_and_marked_offline_on_link_loss() {
    let ctrl = controller(ControllerKind::OpcUa);
    let id = pac_alarms::controller_id(&ctrl).to_string();
    let path = pac_alarms::path(&ctrl).expect("у тегов OPC UA-контроллера есть общее начало имён");
    // Свой номер объекта: симулятор общий для тестов, его сообщения остаются между запусками.
    let object = 10_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000) as i32;

    // Шлюз ходит к OPC UA-серверу через прокси: связь можно оборвать.
    let proxy = Proxy::start(format!("{}:4840", common::sim_host())).await;
    let yaml = std::fs::read_to_string(common::controllers_path()).unwrap();
    let from = "${SIM_HOST:127.0.0.1}:4840\"";
    assert_eq!(yaml.matches(from).count(), 1, "в controllers.yaml нет endpoint'а …{from}");
    let dir = tempfile::tempdir().unwrap();
    let yaml_path = dir.path().join("controllers.yaml");
    std::fs::write(&yaml_path, yaml.replace(from, &format!("127.0.0.1:{}\"", proxy.port))).unwrap();

    let gw = Gateway::start_with(&yaml_path, &FAST_TIMEOUTS);
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let alarms = consumer_from_beginning(&gw.topic("pac-alarms")).await;
    let results = consumer_from_beginning(&gw.topic("results")).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    assert_eq!(cleanup_policy(&gw.topic("pac-alarms")).await.as_deref(), Some("compact"));

    // Первый снимок — сразу после подключения, какой бы ни был список.
    let first = snapshot(&gw, &alarms, &id, "первый", |s| s["online"] == true).await;
    assert_eq!(first["controllerId"], id.as_str());
    assert_eq!(first["controllerName"], ctrl.name.as_str());
    assert_eq!(first["path"], path.as_str());
    assert!(first["pacVersion"].is_i64() && first["polledAt"].is_string(), "{first}");

    // Сообщение объекта появляется в снимке.
    raise(&ctrl.endpoint, object).await;
    let with = snapshot(&gw, &alarms, &id, "с сообщением", |s| !of_object(s, object.into()).is_empty()).await;
    let alarm = of_object(&with, object.into()).remove(0);
    let number = alarm["alarmNumber"].as_i64().unwrap();
    assert_eq!(alarm["key"], format!("{OBJECT_TYPE}:{object}:{number}"));
    assert_eq!(alarm["objectType"], OBJECT_TYPE);
    assert_eq!((alarm["state"].as_str(), alarm["group"].as_str()), (Some("ALARM"), Some("ответ")), "{alarm}");
    assert_eq!(alarm["suppressed"], false);

    // Квитирование: результат с путём проекта, сообщение уходит из следующего снимка.
    let accept = json!({"type": "PAC_ALARM", "controllerId": id, "action": "ACCEPT", "objectType": OBJECT_TYPE,
                        "objectNumber": object, "alarmNumber": number});
    let r = command(&gw, &producer, &results, &path, accept.clone()).await;
    assert_eq!((r["status"].as_str(), r["success"].as_bool()), (Some("APPLIED"), Some(true)), "{r}");
    assert_eq!(r["tagName"], path.as_str());
    assert_eq!(r["appliedValue"], Value::Null);
    assert!(r["message"].as_str().unwrap().contains(&format!("{OBJECT_TYPE}:{object}:{number}")), "{r}");
    snapshot(&gw, &alarms, &id, "без сообщения", |s| {
        s["online"] == true && of_object(s, object.into()).is_empty()
    })
    .await;

    // Неверные команды до ПЛК не доходят.
    for (field, value, status) in [
        ("alarmNumber", Value::Null, "REJECTED_INVALID"),
        ("objectNumber", json!(1.5), "REJECTED_INVALID"),
        ("action", json!("DELETE"), "REJECTED_INVALID"),
        ("type", json!("RESET"), "REJECTED_INVALID"),
        ("controllerId", json!("нет-такого-контроллера"), "REJECTED_UNKNOWN_TAG"),
    ] {
        let mut bad = accept.clone();
        bad[field] = value;
        let r = command(&gw, &producer, &results, &path, bad).await;
        assert_eq!(r["status"], status, "{field}: {r}");
    }

    // Обрыв связи: один снимок «нет связи» с последним списком; связь вернулась — снова «на связи».
    raise(&ctrl.endpoint, object).await;
    let listed =
        snapshot(&gw, &alarms, &id, "второе сообщение", |s| !of_object(s, object.into()).is_empty()).await;
    proxy.set(Mode::Reset);
    let offline = snapshot(&gw, &alarms, &id, "нет связи", |s| s["online"] == false).await;
    assert_eq!(offline["alarms"], listed["alarms"], "при обрыве — последний известный список");
    assert_eq!(offline["polledAt"], listed["polledAt"], "время последнего удачного чтения");
    proxy.set(Mode::Pass);
    let back = snapshot(&gw, &alarms, &id, "связь вернулась", |s| s["online"] == true).await;
    let left = of_object(&back, object.into()).remove(0);

    // Уборка: своё сообщение квитируем, чтобы не копить их в общем симуляторе.
    let mut cleanup = accept;
    cleanup["alarmNumber"] = left["alarmNumber"].clone();
    let r = command(&gw, &producer, &results, &path, cleanup).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    gw.delete_topics().await;
}
