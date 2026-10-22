//! Горячее резервирование на двух настоящих процессах шлюза: общие топики, одна группа выборов,
//! настоящий Kafka и симулятор. Проверяется то, что видит монитор:
//!   * активный один, резервный держит связь с ПЛК, но наружу молчит;
//!   * `kill -9` активного — резерв подхватывает за session-timeout, заново отправляет ВСЕ теги
//!     (прежний мог упасть, потребитель значений не знает) и принимает команды сразу;
//!   * вернувшийся экземпляр лидерство не отбирает;
//!   * штатная остановка активного — резерв подхватывает за доли секунды;
//!   * две станции (каждая — пара) на одном Kafka со своими топиками команд не мешают друг другу, а экземпляр
//!     другой станции в группу выборов не вступает.
//!
//! Брокеру нужен `group.min.session.timeout.ms ≤ 1000` (docker-compose.yml и CI это задают).
//!
//!   cargo test --test ha -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config, new_prefix};
use common::{controllers, controllers_path};

/// Сессия выборов в тестах (`IT_HA_SESSION_MS`): брокеру CI нужно не меньше 6000.
fn session_ms() -> u64 {
    std::env::var("IT_HA_SESSION_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000)
}

/// Окружение экземпляра пары: общая группа и топики, своё имя.
fn ha_env(prefix: &str, instance: &str) -> Vec<(String, String)> {
    vec![
        ("GATEWAY_HA_ENABLED".into(), "true".into()),
        ("GATEWAY_HA_INSTANCE_ID".into(), instance.into()),
        ("GATEWAY_HA_TOPIC".into(), format!("{prefix}.ha")),
        ("GATEWAY_HA_GROUP_ID".into(), format!("{prefix}.group")),
        ("GATEWAY_HA_SESSION_TIMEOUT_MS".into(), session_ms().to_string()),
        ("GATEWAY_HA_HEARTBEAT_INTERVAL_MS".into(), (session_ms() / 4).to_string()),
    ]
}

/// Запуск экземпляра пары.
fn start(prefix: &str, instance: &str) -> Gateway {
    start_with(prefix, instance, &[])
}

/// Запуск экземпляра пары с дополнительными переменными.
fn start_with(prefix: &str, instance: &str, extra: &[(&str, &str)]) -> Gateway {
    let env = ha_env(prefix, instance);
    let mut refs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    refs.extend_from_slice(extra);
    Gateway::start_on(prefix, &controllers_path(), &refs)
}

/// Теги, записанные в Kafka не раньше `since_ms` (по метке записи), пока не наберутся все `expected`.
async fn tags_since(
    consumer: &StreamConsumer,
    expected: &HashSet<String>,
    since_ms: i64,
    within: Duration,
) -> HashSet<String> {
    let mut seen = HashSet::new();
    let deadline = Instant::now() + within;
    while seen.len() < expected.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(Ok(m)) = tokio::time::timeout(left, consumer.recv()).await else { break };
        if m.timestamp().to_millis().unwrap_or(0) >= since_ms {
            let key = String::from_utf8_lossy(m.key().unwrap_or_default()).to_string();
            if expected.contains(&key) {
                seen.insert(key);
            }
        }
    }
    seen
}

/// Отправить команду записи через Kafka и дождаться результата.
async fn command(
    producer: &FutureProducer,
    results: &StreamConsumer,
    prefix: &str,
    tag: &str,
    value: Value,
) -> (Value, Duration) {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "ha-it"}).to_string();
    let sent = Instant::now();
    producer
        .send(FutureRecord::to(&format!("{prefix}.commands")).key(tag).payload(&body), Duration::from_secs(5))
        .await
        .expect("команда не отправлена");
    while let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(15), results.recv()).await {
        let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        if r["commandId"] == id {
            return (r, sent.elapsed());
        }
    }
    panic!("нет результата команды {tag} за 15 с");
}

/// Пара: активный публикует, резерв держит связь с ПЛК и молчит. `kill -9` активного — резерв становится активным за время сессии,
/// заново шлёт все теги и сразу принимает команды; вернувшийся экземпляр лидерство не отбирает; штатная остановка передаёт
/// роль партнёру без ожидания сессии.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn hot_standby_failover() {
    let session = Duration::from_millis(session_ms());
    let prefix = new_prefix();
    let expected: HashSet<String> = controllers().iter().flat_map(|c| c.tags.iter().map(|t| t.name.clone())).collect();
    // Записываемый тег: по умолчанию — OPC UA-канал симулятора; на стенде станции — IT_RW_TAG.
    let rw_tag = std::env::var("IT_RW_TAG").unwrap_or_else(|_| "Барановичи-1.BN1_MCA1.M_M.M3.M".into());
    let rw_tag = rw_tag.as_str();

    // --- Два экземпляра: активный один, резервный держит связь с ПЛК ---
    let mut a = start(&prefix, "a");
    a.wait_role("ACTIVE", Duration::from_secs(40)).await;
    let mut b = start(&prefix, "b");
    b.wait_role("STANDBY", Duration::from_secs(40)).await;
    a.wait_links("UP", Duration::from_secs(40)).await;
    b.wait_links("UP", Duration::from_secs(40)).await; // резерв — горячий: контроллеры опрашивает
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(a.metric("scada_telemetry_sent_total").unwrap_or(0.0) > 0.0, "активный публикует");
    assert_eq!(b.metric("scada_telemetry_sent_total"), Some(0.0), "резервный наружу молчит\n{}", b.log_tail());

    let telemetry = consumer_from_beginning(&a.topic("tags")).await;
    let results = consumer_from_beginning(&a.topic("results")).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let (r, _) = command(&producer, &results, &prefix, rw_tag, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");

    // --- kill -9 активного ---
    let killed_at_ms = chrono::Utc::now().timestamp_millis();
    let t0 = Instant::now();
    a.child.kill().unwrap();
    let _ = a.child.wait();
    let takeover = b.wait_role("ACTIVE", session + Duration::from_secs(6)).await;
    println!("HA: kill -9 активного → резерв активен через {} мс", t0.elapsed().as_millis());
    assert!(takeover < session + Duration::from_secs(3), "переключение {takeover:?} при session-timeout {session:?}");

    // Новый активный заново отправляет значения ВСЕХ тегов: прежний мог упасть, потребитель их не знает.
    let seen = tags_since(&telemetry, &expected, killed_at_ms, Duration::from_secs(20)).await;
    assert_eq!(seen.len(), expected.len(), "после переключения пришло {}/{} тегов", seen.len(), expected.len());
    println!("HA: полная отправка после переключения — {} тегов, {} мс от kill", seen.len(), t0.elapsed().as_millis());

    // Команда принимается сразу: консьюмер команд не ждёт, пока брокер вычеркнет мёртвого члена группы.
    let (r, took) = command(&producer, &results, &prefix, rw_tag, json!(1)).await;
    assert_eq!(r["status"], "APPLIED", "{r}\n{}", b.log_tail());
    println!("HA: команда после переключения → {} за {} мс", r["status"], took.as_millis());
    assert!(took < Duration::from_secs(8));

    // --- Вернувшийся экземпляр лидерство не отбирает ---
    let a2 = start(&prefix, "a2");
    a2.wait_role("STANDBY", Duration::from_secs(40)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(b.role().as_deref(), Some("ACTIVE"), "перезапущенный резерв не отбирает лидерство");
    assert_eq!(a2.role().as_deref(), Some("STANDBY"));

    // --- Штатная остановка активного: резерв подхватывает за доли секунды ---
    let t1 = Instant::now();
    let stopper = std::thread::spawn(move || {
        b.terminate();
        b
    });
    let graceful = a2.wait_role("ACTIVE", Duration::from_secs(10)).await;
    println!("HA: штатная остановка активного → резерв активен через {} мс", t1.elapsed().as_millis());
    assert!(graceful < session, "штатная остановка не должна ждать session-timeout: {graceful:?}");
    let _b = stopper.join().unwrap();

    let (r, _) = command(&producer, &results, &prefix, rw_tag, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    a2.delete_topics().await;
}

/// Активный, потерявший связь со ВСЕМИ контроллерами, передаёт лидерство партнёру, который их видит
/// (у него другой сетевой путь к ПЛК), и лидерство потом не отбирает.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn blind_active_yields_leadership_to_a_peer_that_sees_the_plcs() {
    let prefix = new_prefix();
    let expected: HashSet<String> = controllers().iter().flat_map(|c| c.tags.iter().map(|t| t.name.clone())).collect();

    // A «слепой»: SIM_HOST ведёт на недостижимый адрес (0.0.0.1 — connect() отказывает сразу).
    let a = start_with(
        &prefix,
        "blind",
        &[("SIM_HOST", "0.0.0.1"), ("GATEWAY_HA_YIELD_AFTER_MS", "3000"), ("GATEWAY_STALE_AFTER_MS", "3000")],
    );
    a.wait_role("ACTIVE", Duration::from_secs(40)).await;
    let b = start_with(&prefix, "sighted", &[("GATEWAY_HA_YIELD_AFTER_MS", "3000")]);
    b.wait_role("STANDBY", Duration::from_secs(40)).await;
    b.wait_links("UP", Duration::from_secs(40)).await;
    a.wait_links("DOWN", Duration::from_secs(40)).await;

    // Связь с OPC UA считается потерянной после stale-after (3 с), слепота ≥ 3 с, затем проверка партнёра (раз в 5 с) → передача лидерства.
    let handover_from = chrono::Utc::now().timestamp_millis();
    let t0 = Instant::now();
    b.wait_role("ACTIVE", Duration::from_secs(30)).await;
    println!("HA: слепой активный передал лидерство за {} мс", t0.elapsed().as_millis());
    assert_eq!(a.role().as_deref(), Some("STANDBY"), "слепой ушёл в резерв\n{}", a.log_tail());

    // Зрячий активный заново отправил все теги.
    let telemetry = consumer_from_beginning(&b.topic("tags")).await;
    let seen = tags_since(&telemetry, &expected, handover_from, Duration::from_secs(20)).await;
    assert_eq!(seen.len(), expected.len(), "после передачи пришло {}/{} тегов", seen.len(), expected.len());

    // Слепой вернулся в группу (пауза ≥ 5 с), но лидерство не отбирает и сам не «качается».
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(b.role().as_deref(), Some("ACTIVE"), "{}", b.log_tail());
    assert_eq!(a.role().as_deref(), Some("STANDBY"), "{}", a.log_tail());
    b.delete_topics().await;
}

/// Без партнёра слепой активный остаётся активным: BAD-кадры монитору нужны, отдавать лидерство некому.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn blind_active_without_a_peer_stays_active() {
    let prefix = new_prefix();
    let a = start_with(
        &prefix,
        "alone",
        &[("SIM_HOST", "0.0.0.1"), ("GATEWAY_HA_YIELD_AFTER_MS", "2000"), ("GATEWAY_STALE_AFTER_MS", "3000")],
    );
    a.wait_role("ACTIVE", Duration::from_secs(40)).await;
    a.wait_links("DOWN", Duration::from_secs(40)).await;
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(a.role().as_deref(), Some("ACTIVE"), "{}", a.log_tail());
    a.delete_topics().await;
}

/// Отдельная «станция» для тестов двух станций: controllers.yaml тестов, в котором остался один контроллер `keep`.
fn station_yaml(dir: &Path, keep: &str) -> PathBuf {
    let raw = std::fs::read_to_string(controllers_path()).unwrap();
    let (head, servers) = raw.split_once("\n  servers:\n").expect("секция servers");
    let block = format!("\n{servers}")
        .split("\n    - ")
        .skip(1)
        .find(|b| b.contains(&format!("name: \"{keep}\"")))
        .unwrap_or_else(|| panic!("в {} нет контроллера {keep}", controllers_path().display()))
        .to_string();
    let path = dir.join(format!("{}.yaml", keep.replace(' ', "_")));
    std::fs::write(&path, format!("{head}\n  servers:\n    - {block}\n")).unwrap();
    path
}

/// Экземпляр станции с настройками резерва по умолчанию: группа выборов — от топика команд, общий служебный топик.
fn start_station(prefix: &str, yaml: &Path, instance: &str, ha_topic: &str) -> Gateway {
    let session = session_ms().to_string();
    let heartbeat = (session_ms() / 4).to_string();
    Gateway::start_on(
        prefix,
        yaml,
        &[
            ("GATEWAY_HA_ENABLED", "true"),
            ("GATEWAY_HA_INSTANCE_ID", instance),
            ("GATEWAY_HA_TOPIC", ha_topic),
            ("GATEWAY_HA_SESSION_TIMEOUT_MS", &session),
            ("GATEWAY_HA_HEARTBEAT_INTERVAL_MS", &heartbeat),
        ],
    )
}

/// Экземпляры другой станции в группе выборов по `/api/ha`.
fn foreign_members(gw: &Gateway) -> Vec<String> {
    let (_, body) = gw.get("/api/ha").expect("/api/ha");
    let ha: Value = serde_json::from_str(&body).unwrap();
    ha["foreignMembers"].as_array().unwrap().iter().map(|m| m.as_str().unwrap().to_string()).collect()
}

/// Две станции (по паре на каждую) на одном Kafka со своими топиками команд и настройками резерва по умолчанию: у каждой
/// свой активный, телеметрия и команды каждой идут своим путём, группа позиций команд по умолчанию общая и не мешает.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn two_stations_share_one_kafka_without_interfering() {
    let dir = tempfile::tempdir().unwrap();
    let (opc_station, pac_station) =
        (station_yaml(dir.path(), "Phoenix Contact"), station_yaml(dir.path(), "PAC Savushkin"));
    let ha_topic = format!("{}.ha", new_prefix()); // общий служебный топик, как `scada-gateway-ha` по умолчанию
    let (pa, pb) = (new_prefix(), new_prefix()); // свои топики, в том числе команд
    let a1 = start_station(&pa, &opc_station, "opc-1", &ha_topic);
    let b1 = start_station(&pb, &pac_station, "pac-1", &ha_topic);
    a1.wait_role("ACTIVE", Duration::from_secs(40)).await;
    b1.wait_role("ACTIVE", Duration::from_secs(40)).await;
    let a2 = start_station(&pa, &opc_station, "opc-2", &ha_topic);
    let b2 = start_station(&pb, &pac_station, "pac-2", &ha_topic);
    for gw in [&a1, &a2, &b1, &b2] {
        gw.wait_links("UP", Duration::from_secs(40)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    // У каждой станции ровно один активный.
    assert_eq!((a1.role().as_deref(), a2.role().as_deref()), (Some("ACTIVE"), Some("STANDBY")), "{}", a2.log_tail());
    assert_eq!((b1.role().as_deref(), b2.role().as_deref()), (Some("ACTIVE"), Some("STANDBY")), "{}", b2.log_tail());
    for gw in [&a1, &a2, &b1, &b2] {
        assert!(foreign_members(gw).is_empty(), "{}", gw.log_tail());
        assert_eq!(gw.metric("scada_ha_foreign_members"), Some(0.0));
    }

    // Команда каждой станции — в её топик; результат — один, от её активного.
    let producer: FutureProducer = kafka_config().create().unwrap();
    for (prefix, gw, tag) in
        [(&pa, &a1, "Барановичи-1.BN1_MCA1.M_M.M3.M"), (&pb, &b1, "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M")]
    {
        let results = consumer_from_beginning(&gw.topic("results")).await;
        let (r, _) = command(&producer, &results, prefix, tag, json!(1)).await;
        assert_eq!(r["status"], "APPLIED", "{tag}: {r}\n{}", gw.log_tail());
        let (r, _) = command(&producer, &results, prefix, tag, json!(0)).await;
        assert_eq!(r["status"], "APPLIED", "{tag}: {r}");
    }
    // Телеметрия каждой станции — в её топик и только её теги.
    for (gw, own) in [(&a1, &opc_station), (&b1, &pac_station)] {
        let raw = std::fs::read_to_string(own).unwrap();
        let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
        let m = tokio::time::timeout(Duration::from_secs(15), telemetry.recv()).await.expect("телеметрия").unwrap();
        let key = String::from_utf8_lossy(m.key().unwrap_or_default()).to_string();
        assert!(raw.contains(&format!("\"{key}\"")), "{key} не из своей станции");
    }
    a1.delete_topics().await;
    b1.delete_topics().await;
}

/// Две станции на настройках по умолчанию — один топик команд и, значит, одна группа выборов. Экземпляр второй станции в
/// группу не вступает (первая остаётся активной, а не уходит в резерв по жребию), смешение видно в журнале, `/api/ha` и
/// метрике; когда первая станция ушла из группы, вторая вступает сама.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn instance_of_another_station_does_not_join_its_election_group() {
    let dir = tempfile::tempdir().unwrap();
    let (opc_station, pac_station) =
        (station_yaml(dir.path(), "Phoenix Contact"), station_yaml(dir.path(), "PAC Savushkin"));
    let prefix = new_prefix(); // общие топики, в том числе команд, — ошибка настройки
    let ha_topic = format!("{prefix}.ha");
    let mut a = start_station(&prefix, &opc_station, "opc-1", &ha_topic);
    a.wait_role("ACTIVE", Duration::from_secs(40)).await;
    let b = start_station(&prefix, &pac_station, "pac-1", &ha_topic);

    let deadline = Instant::now() + Duration::from_secs(30);
    while !b.log().contains("экземпляры другой станции") {
        assert!(Instant::now() < deadline, "смешение не замечено\n{}", b.log_tail());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let found = foreign_members(&b);
    assert!(found.len() == 1 && found[0].starts_with("opc-1@"), "{found:?}");
    assert_eq!(b.metric("scada_ha_foreign_members"), Some(1.0));
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(a.role().as_deref(), Some("ACTIVE"), "работающая станция не отдаёт лидерство\n{}", a.log_tail());
    assert_eq!(b.role().as_deref(), Some("STANDBY"), "{}", b.log_tail());
    assert!(foreign_members(&a).is_empty(), "вторая станция в группу не вступала");

    // Первая станция ушла — вторая вступает при следующей проверке (раз в 30 с).
    a.terminate();
    b.wait_role("ACTIVE", Duration::from_secs(45)).await;
    assert!(foreign_members(&b).is_empty());
    assert_eq!(b.metric("scada_ha_foreign_members"), Some(0.0));
    b.delete_topics().await;
}
