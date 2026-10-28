//! Проверка эффекта записи (`GATEWAY_COMMANDS_VERIFY_MS`): прошивка принимает команду на клапан линии с кодом 0,
//! но значением управляет программа — пока клапан не переведён в ручной режим (M=1), он остаётся прежним.
//! Шлюз с включённой проверкой отвечает `FAILED_NOT_APPLIED`; в ручном режиме — `APPLIED`. Проверка одна и та же для
//! OPC UA и PAC («EasyDriver»): код 0 PAC говорит только «команду приняли».
//!
//!   cargo test --test write_effect -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config, recv_until};
use common::{OP_TIMEOUT, controller, controllers_path, toggled};
use scada_gateway::model::ControllerKind;
use scada_gateway::opcua::{self, OpcConnection};
use scada_gateway::pac::{self, PacConnection};

const VALVE_ST: &str = "Барановичи-1.BN1_MCA1.V_ST_2.LINE2V0.ST";
const VALVE_M: &str = "Барановичи-1.BN1_MCA1.V_M_2.LINE2V0.M";
/// Тот же главный клапан линии 1, но по PAC (LINE1V0).
const PAC_VALVE_ST: &str = "Барановичи-1.BN1_MCA1.V_ST_1.LINE1V0.ST";
const PAC_VALVE_M: &str = "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M";

/// Отправить команду через Kafka и вернуть JSON результата.
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, tag: &str, value: Value) -> Value {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "it"}).to_string();
    for _ in 0..5 {
        producer
            .send(FutureRecord::to(&gw.topic("commands")).key(tag).payload(&body), Duration::from_secs(5))
            .await
            .expect("команда не отправлена");
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Some(m) = recv_until(results, deadline).await {
            let result: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
            if result["commandId"] == id {
                return result;
            }
        }
    }
    panic!("нет результата команды {tag} = {value}\n{}", gw.log_tail());
}

/// Клапан, которым владеет программа ПЛК: в автоматическом режиме запись принята, но не действует — `FAILED_NOT_APPLIED`; после перевода в ручной режим (`M=1`) — `APPLIED`. Режим возвращается в исходное состояние.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn write_to_a_program_owned_valve_is_reported_as_not_applied_until_manual_mode() {
    // Текущее значение клапана — прямым чтением из симулятора.
    let ctrl = controller(ControllerKind::OpcUa);
    let conn = OpcConnection::connect(&ctrl.endpoint, OP_TIMEOUT).await.expect("нужен симулятор");
    let st = ctrl.tags.iter().find(|t| t.name == VALVE_ST).expect("тег клапана");
    let current =
        opcua::reading(&conn.read(&[opcua::read_value_id(&st.node_id).unwrap()]).await.unwrap()[0]).0.unwrap();
    conn.close().await;
    let opposite = json!(toggled(&current).as_f64().unwrap() as i64);

    let gw = Gateway::start_with(&controllers_path(), &[("GATEWAY_COMMANDS_VERIFY_MS", "1500")]);
    // Команда до подключения контроллера честно получает FAILED_NO_CONNECTION: приём команд включается раньше.
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;

    // Автоматический режим: команда принята ПЛК, значение осталось за программой.
    let r = command(&gw, &producer, &results, VALVE_ST, opposite.clone()).await;
    assert_eq!(r["status"], "FAILED_NOT_APPLIED", "{r}");
    assert_eq!(r["success"], false);

    // Ручной режим (M=1): запись действует.
    let r = command(&gw, &producer, &results, VALVE_M, json!(1)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    let r = command(&gw, &producer, &results, VALVE_ST, opposite).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    assert_eq!(r["success"], true);

    // Вернуть автоматику.
    let r = command(&gw, &producer, &results, VALVE_M, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
}

/// То же по PAC («EasyDriver»): прошивка принимает команду с кодом 0, а значением владеет программа. Без проверки шлюз
/// сказал бы `APPLIED`; с `GATEWAY_COMMANDS_VERIFY_MS` — `FAILED_NOT_APPLIED`, а в ручном режиме `APPLIED`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn pac_write_to_a_program_owned_valve_is_reported_as_not_applied_until_manual_mode() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect("нужен симулятор");
    conn.poll_states().await.unwrap();
    let current = conn.read_value("LINE1V0", "ST", "INT32").expect("LINE1V0.ST в снимке");
    drop(conn);
    let opposite = json!(toggled(&current).as_f64().unwrap() as i64);

    let gw = Gateway::start_with(&controllers_path(), &[("GATEWAY_COMMANDS_VERIFY_MS", "1500")]);
    // Команда до подключения контроллера честно получает FAILED_NO_CONNECTION: приём команд включается раньше.
    gw.wait_links("UP", Duration::from_secs(30)).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;

    // Автоматический режим: команда принята, значение осталось за программой.
    let r = command(&gw, &producer, &results, PAC_VALVE_ST, opposite.clone()).await;
    assert_eq!(r["status"], "FAILED_NOT_APPLIED", "{r}");
    assert_eq!(r["success"], false);

    // Ручной режим (M=1): запись действует.
    let r = command(&gw, &producer, &results, PAC_VALVE_M, json!(1)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    let r = command(&gw, &producer, &results, PAC_VALVE_ST, opposite).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    assert_eq!(r["success"], true);

    // Вернуть автоматику.
    let r = command(&gw, &producer, &results, PAC_VALVE_M, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
}
