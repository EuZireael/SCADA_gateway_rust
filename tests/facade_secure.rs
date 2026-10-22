//! Шлюз целиком против защищённого OPC UA-фасада ptusa-opcua: политика Basic256Sha256 (SignAndEncrypt) и пароль.
//!   * пользователь с ролью `write` — связь `UP`, команда исполняется (`APPLIED`);
//!   * пользователь с ролью `read` — связь `UP`, значения читаются, а команда не исполняется (`REJECTED_*`/`FAILED_*`);
//!   * неверный пароль — связь не поднимается.
//!
//! Нужны фасад и Kafka. Фасад запускают с `OPCUA_USERS_FILE` (два пользователя), `OPCUA_TRUST_ALL_CLIENTS=true` (сертификат
//! шлюза в его `trusted/` кладёт администратор; здесь доверие любому) и каналами `LINE1V0.ST`, `LINE1V0.M` (запись — `M`).
//! Параметры: IT_FACADE_URL (напр. opc.tcp://127.0.0.1:4870), IT_FACADE_WRITER и IT_FACADE_READER — `имя:пароль`.
//! Без IT_FACADE_URL тесты пропускаются (CI поднимает фасад сам, см. .github/workflows/ci.yml).

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config};

const TAG: &str = "Facade.LINE1V0.M";

fn facade() -> Option<(String, String, String)> {
    let url = std::env::var("IT_FACADE_URL").ok().filter(|v| !v.is_empty());
    if url.is_none() {
        eprintln!("IT_FACADE_URL не задан — пропущено");
    }
    url.map(|u| {
        (
            u,
            std::env::var("IT_FACADE_WRITER").unwrap_or_default(),
            std::env::var("IT_FACADE_READER").unwrap_or_default(),
        )
    })
}

fn split(credentials: &str) -> (&str, &str) {
    credentials.split_once(':').unwrap_or_else(|| panic!("ожидается имя:пароль"))
}

/// Станция из одного OPC UA-контроллера — фасада — с защитой канала и пользователем.
fn station(dir: &std::path::Path, url: &str, credentials: &str) -> std::path::PathBuf {
    let (user, password) = split(credentials);
    let tags = [("ST", "false"), ("M", "true")]
        .iter()
        .map(|(f, w)| {
            format!(
                "        - {{name: \"Facade.LINE1V0.{f}\", nodeId: \"ns=2;s=LINE1V0.{f}\", dataType: INT32, pollingRate: 1000, enabled: true, writable: {w}}}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let path = dir.join(format!("controllers-{user}.yaml"));
    std::fs::write(
        &path,
        format!(
            "opcua:\n  servers:\n    - name: Facade\n      endpoint: \"{url}\"\n      security: Basic256Sha256\n      username: \"{user}\"\n      password: \"{password}\"\n      enabled: true\n      tags:\n{tags}\n"
        ),
    )
    .unwrap();
    path
}

fn gateway(dir: &std::path::Path, url: &str, credentials: &str) -> Gateway {
    let pki = dir.join(format!("pki-{}", split(credentials).0));
    let yaml = station(dir, url, credentials);
    Gateway::start_with(
        &yaml,
        &[("GATEWAY_OPCUA_PKI_DIR", pki.to_str().unwrap()), ("GATEWAY_OPCUA_TRUST_SERVER_CERTS", "true")],
    )
}

async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, value: i64) -> String {
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
            let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
            if r["commandId"] == id {
                return r["status"].as_str().unwrap_or_default().to_string();
            }
        }
    }
    panic!("нет результата команды\n{}", gw.log_tail());
}

/// Роль `write`: команда исполняется. Роль `read`: подключается и читает, команда отклоняется фасадом.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны защищённый фасад (IT_FACADE_URL) и Kafka"]
async fn the_write_role_commands_and_the_read_role_cannot() {
    let Some((url, writer, reader)) = facade() else { return };
    let dir = tempfile::tempdir().unwrap();
    let producer: FutureProducer = kafka_config().create().unwrap();

    let gw = gateway(dir.path(), &url, &writer);
    gw.wait_links("UP", Duration::from_secs(60)).await;
    let results = consumer_from_beginning(&gw.topic("results")).await;
    assert_eq!(command(&gw, &producer, &results, 1).await, "APPLIED", "{}", gw.log_tail());
    gw.delete_topics().await;

    let gw = gateway(dir.path(), &url, &reader);
    gw.wait_links("UP", Duration::from_secs(60)).await;
    let results = consumer_from_beginning(&gw.topic("results")).await;
    let status = command(&gw, &producer, &results, 0).await;
    // Шлюз видит права пользователя на узел (UserAccessLevel без записи) и отклоняет команду сам; запись, дошедшая до
    // фасада, тоже отклонена (BadUserAccessDenied, см. тесты фасада).
    assert!(
        status.starts_with("REJECTED") || status.starts_with("FAILED"),
        "роль read: команда не должна исполняться: {status}\n{}",
        gw.log_tail()
    );
    gw.delete_topics().await;
}

/// Неверный пароль: связь с фасадом не поднимается, а в журнале фасада вход отклонён (проверяется администратором по его журналу).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны защищённый фасад (IT_FACADE_URL) и Kafka"]
async fn a_wrong_password_does_not_connect() {
    let Some((url, writer, _)) = facade() else { return };
    let dir = tempfile::tempdir().unwrap();
    let wrong = format!("{}:not-the-password", split(&writer).0);
    let gw = gateway(dir.path(), &url, &wrong);
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        assert!(
            gw.links().values().all(|s| s != "UP"),
            "с неверным паролем связь не должна подниматься\n{}",
            gw.log_tail()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    gw.delete_topics().await;
}
