//! Очереди команд по контроллерам на процессе шлюза (симулятор + Kafka):
//!   * OPC UA-контроллер «завис» (TCP-прокси держит соединение и молчит) — команда PAC-контроллеру исполняется сразу,
//!     а не после таймаута записи OPC UA;
//!   * 20 команд одному контроллеру с проверкой эффекта исполняются по одной и в порядке отправки.
//!
//!   cargo test --test command_queue -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config};
use common::proxy::{Mode, Proxy};

/// Режим `M` мотора — OPC UA-контроллер симулятора («чистый» актуатор: симулятор его не перетирает).
const OPC_TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M3.M";
/// Режим `M` клапана — PAC-контроллер симулятора.
const PAC_TAG: &str = "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M";
/// Режим `M` другого мотора (тоже без источника данных в симуляторе): у теста порядка свой тег — тесты файла идут
/// параллельно, и запись одного не должна попадать в проверку эффекта другого.
const ORDER_TAG: &str = "Барановичи-1.BN1_MCA1.M_M.M103.M";

/// Отправить команду, не дожидаясь результата; вернуть её commandId.
async fn send(gw: &Gateway, producer: &FutureProducer, tag: &str, value: Value) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "it"}).to_string();
    producer
        .send(FutureRecord::to(&gw.topic("commands")).key(tag).payload(&body), Duration::from_secs(5))
        .await
        .expect("команда не отправлена");
    id
}

/// Результаты команд `ids` в порядке прихода: commandId, статус, когда пришёл (от `since`).
async fn collect(
    results: &StreamConsumer,
    ids: &[String],
    since: Instant,
    within: Duration,
) -> Vec<(String, String, Duration)> {
    let mut got = Vec::new();
    let deadline = Instant::now() + within;
    while got.len() < ids.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(Ok(m)) = tokio::time::timeout(left, results.recv()).await else { break };
        let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        let id = r["commandId"].as_str().unwrap_or_default().to_string();
        if ids.contains(&id) {
            got.push((id, r["status"].as_str().unwrap_or_default().to_string(), since.elapsed()));
        }
    }
    got
}

/// Команда с повтором, пока не придёт результат: консьюмер команд шлюза мог ещё не назначиться.
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, tag: &str, value: Value) -> String {
    for _ in 0..5 {
        let id = send(gw, producer, tag, value.clone()).await;
        if let Some((_, status, _)) = collect(results, &[id], Instant::now(), Duration::from_secs(3)).await.pop() {
            return status;
        }
    }
    panic!("нет результата команды {tag} = {value}\n{}", gw.log_tail());
}

/// controllers.yaml тестов, в котором OPC UA-контроллер ходит к симулятору через прокси.
async fn opcua_through_proxy(dir: &std::path::Path) -> (Proxy, std::path::PathBuf) {
    let proxy = Proxy::start(format!("{}:4840", common::sim_host())).await;
    let yaml = std::fs::read_to_string(common::controllers_path()).unwrap();
    let from = "${SIM_HOST:127.0.0.1}:4840\"";
    assert_eq!(yaml.matches(from).count(), 1, "в controllers.yaml нет endpoint'а OPC UA …{from}");
    let path = dir.join("controllers.yaml");
    std::fs::write(&path, yaml.replace(from, &format!("127.0.0.1:{}\"", proxy.port))).unwrap();
    (proxy, path)
}

/// Запись в «зависший» OPC UA-контроллер ждёт таймаута операции (5 с), а команда PAC-контроллеру за ней в очереди не стоит.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn hung_controller_does_not_delay_commands_to_others() {
    let dir = tempfile::tempdir().unwrap();
    let (proxy, yaml) = opcua_through_proxy(dir.path()).await;
    let gw = Gateway::start_with(&yaml, &[("GATEWAY_OPCUA_OP_TIMEOUT_MS", "5000")]);
    gw.wait_links("UP", Duration::from_secs(40)).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    for tag in [OPC_TAG, PAC_TAG] {
        assert_eq!(command(&gw, &producer, &results, tag, json!(0)).await, "APPLIED", "{tag}");
    }

    // OPC UA «завис»: соединение открыто, ответов нет. Сначала команда ему, сразу за ней — PAC.
    proxy.set(Mode::Hang);
    let since = Instant::now();
    let opc = send(&gw, &producer, OPC_TAG, json!(1)).await;
    let pac = send(&gw, &producer, PAC_TAG, json!(1)).await;
    let got = collect(&results, &[opc.clone(), pac.clone()], since, Duration::from_secs(30)).await;
    let by_id: HashMap<&str, (&str, Duration)> =
        got.iter().map(|(id, status, at)| (id.as_str(), (status.as_str(), *at))).collect();
    println!("очереди: {got:?}");
    let (pac_status, pac_at) = by_id.get(pac.as_str()).copied().expect("нет результата PAC");
    let (opc_status, opc_at) = by_id.get(opc.as_str()).copied().expect("нет результата OPC UA");
    assert_eq!(pac_status, "APPLIED", "{}", gw.log_tail());
    assert_ne!(opc_status, "APPLIED", "запись через молчащий прокси не могла пройти");
    assert!(pac_at < Duration::from_secs(3), "команда PAC ждала зависший OPC UA: {pac_at:?}");
    assert!(pac_at < opc_at, "PAC {pac_at:?} должен ответить раньше OPC UA {opc_at:?}");

    proxy.set(Mode::Pass);
    gw.wait_links("UP", Duration::from_secs(40)).await;
    for tag in [OPC_TAG, PAC_TAG] {
        assert_eq!(command(&gw, &producer, &results, tag, json!(0)).await, "APPLIED", "{tag}");
    }
    gw.delete_topics().await;
}

/// 20 команд одному контроллеру с проверкой эффекта: исполняются по одной, результаты — в порядке отправки, все
/// применены (каждая меняет значение, и проверка эффекта это видит).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn commands_to_one_controller_keep_their_order() {
    let gw = Gateway::start_with(&common::controllers_path(), &[("GATEWAY_COMMANDS_VERIFY_MS", "300")]);
    gw.wait_links("UP", Duration::from_secs(40)).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    assert_eq!(command(&gw, &producer, &results, ORDER_TAG, json!(0)).await, "APPLIED");

    let since = Instant::now();
    let mut ids = Vec::new();
    for i in 0..20 {
        ids.push(send(&gw, &producer, ORDER_TAG, json!((i + 1) % 2)).await); // 1, 0, 1, … — последняя 0
    }
    let got = collect(&results, &ids, since, Duration::from_secs(60)).await;
    let order: Vec<&String> = got.iter().map(|(id, _, _)| id).collect();
    assert_eq!(order, ids.iter().collect::<Vec<_>>(), "порядок результатов\n{}", gw.log_tail());
    for (id, status, _) in &got {
        assert_eq!(status, "APPLIED", "{id}\n{}", gw.log_tail());
    }
    // Каждая следующая ждёт проверки эффекта предыдущей (300 мс): по одной, а не все сразу.
    let took = got.last().unwrap().2;
    println!("20 команд с проверкой эффекта: {} мс", took.as_millis());
    assert!(took >= Duration::from_millis(20 * 300), "{took:?}");
    gw.delete_topics().await;
}
