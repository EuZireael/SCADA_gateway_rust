//! Обрыв и восстановление связи с контроллерами. Шлюз-процесс ходит к симулятору через
//! TCP-прокси (tests/common/proxy.rs); тест рвёт, а потом «вешает» связь со всеми тремя
//! контроллерами (OPC UA, Modbus, PAC) и смотрит глазами монитора:
//!   * /actuator/health — контроллеры DOWN, после восстановления снова UP;
//!   * телеметрия — кадры BAD (value=null) по тегам каждого контроллера, затем снова GOOD;
//!   * события — CONNECTION DISCONNECTED и CONNECTED («link restored») по каждому;
//!   * процесс шлюза переживает всё это.
//!
//!   cargo test --test faults -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use serde_json::Value;

use common::gateway::{Gateway, consumer_from_beginning, recv_until};
use common::proxy::{Mode, Proxy};

/// Таймауты шлюза — короткие, чтобы обрыв замечался за секунды.
const FAST_TIMEOUTS: [(&str, &str); 5] = [
    ("GATEWAY_OPCUA_OP_TIMEOUT_MS", "2000"),
    ("GATEWAY_MODBUS_OP_TIMEOUT_MS", "1000"),
    ("GATEWAY_PAC_OP_TIMEOUT_MS", "1000"),
    ("GATEWAY_STALE_AFTER_MS", "3000"),
    ("GATEWAY_SUPERVISE_INTERVAL_MS", "1000"),
];
/// Тегов каждого контроллера под наблюдением.
const SAMPLE_PER_CONTROLLER: usize = 3;

/// Текущее время, мс от начала эпохи.
fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Дождаться по каждому тегу `tags` (имя → контроллер) кадра качества `quality`, записанного
/// в Kafka не раньше `since_ms`. BAD — только с value=null, GOOD — только со значением.
/// Возвращает теги, по которым не дождались.
async fn await_frames(
    telemetry: &StreamConsumer,
    tags: &HashMap<String, String>,
    quality: &str,
    since_ms: i64,
    within: Duration,
) -> Vec<String> {
    let mut pending: HashSet<&String> = tags.keys().collect();
    let deadline = Instant::now() + within;
    while !pending.is_empty() {
        let Some(m) = recv_until(telemetry, deadline).await else { break };
        if m.timestamp().to_millis().unwrap_or(0) < since_ms {
            continue;
        }
        let key = String::from_utf8_lossy(m.key().unwrap_or_default()).to_string();
        if !pending.contains(&key) {
            continue;
        }
        let body: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        if body["quality"] == quality && body["value"].is_null() == (quality == "BAD") {
            pending.remove(&key);
        }
    }
    pending.into_iter().map(|k| format!("{k} ({})", tags[k])).collect()
}

/// Дождаться события CONNECTION с `details.state == state` (и `details.note == note`, если
/// задано) по каждому контроллеру `names`, не раньше `since_ms`. Возвращает недождавшихся.
async fn await_link_events(
    events: &StreamConsumer,
    names: &[String],
    state: &str,
    note: Option<&str>,
    since_ms: i64,
    within: Duration,
) -> Vec<String> {
    let mut pending: HashSet<&String> = names.iter().collect();
    let deadline = Instant::now() + within;
    while !pending.is_empty() {
        let Some(m) = recv_until(events, deadline).await else { break };
        if m.timestamp().to_millis().unwrap_or(0) < since_ms {
            continue;
        }
        let e: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        let details = &e["details"];
        if e["eventType"] == "CONNECTION"
            && details["state"] == state
            && note.is_none_or(|n| details["note"] == n)
            && let Some(name) = details["controller"].as_str()
        {
            pending.remove(&name.to_string());
        }
    }
    pending.into_iter().cloned().collect()
}

/// Связь с каждым из трёх контроллеров рвётся через TCP-прокси (обрыв и «зависание» без ответа): шлюз жив, значения идут как `BAD`, после возврата связь и данные восстанавливаются сами.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn link_loss_and_recovery() {
    // --- Прокси на каждый порт симулятора и controllers.yaml с endpoint'ами на них ---
    let host = common::sim_host();
    let mut proxies = Vec::new();
    let mut yaml = std::fs::read_to_string(common::controllers_path()).unwrap();
    // Порт PAC настраивается (`SIM_PAC_PORT`, если 10000 занят настоящим ptusa_main): и в YAML, и у прокси — тот же.
    let pac_port = std::env::var("SIM_PAC_PORT").unwrap_or_else(|_| "10000".into());
    for (port, written) in [("4840", "4840"), ("5020", "5020"), (pac_port.as_str(), "${SIM_PAC_PORT:10000}")] {
        let proxy = Proxy::start(format!("{host}:{port}")).await;
        let from = format!("${{SIM_HOST:127.0.0.1}}:{written}\"");
        assert_eq!(yaml.matches(&from).count(), 1, "в controllers.yaml нет endpoint'а …{from}");
        yaml = yaml.replace(&from, &format!("127.0.0.1:{}\"", proxy.port));
        proxies.push(proxy);
    }
    let dir = tempfile::tempdir().unwrap();
    let yaml_path = dir.path().join("controllers.yaml");
    std::fs::write(&yaml_path, yaml).unwrap();
    let set_all = |mode: Mode| proxies.iter().for_each(|p| p.set(mode));

    let controllers = common::controllers();
    let names: Vec<String> = controllers.iter().map(|c| c.name.clone()).collect();
    let sample: HashMap<String, String> = controllers
        .iter()
        .flat_map(|c| c.tags.iter().take(SAMPLE_PER_CONTROLLER).map(|t| (t.name.clone(), c.name.clone())))
        .collect();

    // --- Шлюз через прокси: всё на связи ---
    let mut gw = Gateway::start_with(&yaml_path, &FAST_TIMEOUTS);
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
    let events = consumer_from_beginning(&gw.topic("events")).await;
    let missing = await_frames(&telemetry, &sample, "GOOD", 0, Duration::from_secs(20)).await;
    assert!(missing.is_empty(), "до обрыва нет GOOD: {missing:?}\n{}", gw.log_tail());

    for mode in [Mode::Reset, Mode::Hang] {
        // --- Обрыв ---
        let since = now_ms();
        set_all(mode);
        gw.wait_links("DOWN", Duration::from_secs(20)).await;
        let missing = await_frames(&telemetry, &sample, "BAD", since, Duration::from_secs(20)).await;
        assert!(missing.is_empty(), "{mode:?}: нет кадров BAD: {missing:?}\n{}", gw.log_tail());
        let missing = await_link_events(&events, &names, "DISCONNECTED", None, since, Duration::from_secs(10)).await;
        assert!(missing.is_empty(), "{mode:?}: нет события DISCONNECTED: {missing:?}");

        // --- Восстановление ---
        let since = now_ms();
        set_all(Mode::Pass);
        gw.wait_links("UP", Duration::from_secs(30)).await;
        let missing = await_frames(&telemetry, &sample, "GOOD", since, Duration::from_secs(20)).await;
        assert!(missing.is_empty(), "после {mode:?}: нет GOOD: {missing:?}\n{}", gw.log_tail());
        let missing =
            await_link_events(&events, &names, "CONNECTED", Some("link restored"), since, Duration::from_secs(10))
                .await;
        assert!(missing.is_empty(), "после {mode:?}: нет события о восстановлении: {missing:?}");
    }

    assert!(gw.child.try_wait().unwrap().is_none(), "шлюз упал\n{}", gw.log_tail());
    gw.delete_topics().await;
}
