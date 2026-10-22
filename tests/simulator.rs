//! Клиенты протоколов против PLC-симулятора (`simulator/`): каждый тег
//! controllers.yaml читается своим протоколом и приходит с верным типом; запись команд
//! применяется и откатывается; запрещённое отклоняется.
//!
//!   cargo test --test simulator -- --ignored      (симулятор на SIM_HOST, см. tests/common)

mod common;

use std::time::Duration;

use common::{OP_TIMEOUT, controller, sim_host, toggled, type_matches};
use scada_gateway::modbus::{self, ModbusClient};
use scada_gateway::model::OpcSecurity;
use scada_gateway::model::{ControllerKind, Protocol, Quality, TagValue};
use scada_gateway::opcua::{self, ConnectOptions, OpcConnection};
use scada_gateway::opcua_cert::CertSettings;
use scada_gateway::pac::{self, PacConnection};

const NEED_SIM: &str = "нужен симулятор: SIM_HOST=… cargo test -- --ignored";

// ---------------------------------------------------------------------------- OPC UA --

/// Клиент async-opcua берёт первый адрес из DNS: `localhost` → `::1`, а симулятор слушает только IPv4.
/// Шлюз сам выбирает доступный адрес (`opcua::reachable_url`), поэтому с `localhost` подключение есть.
#[tokio::test]
#[ignore = "нужен симулятор на этой машине: SIM_HOST=127.0.0.1 cargo test -- --ignored"]
async fn opcua_connects_through_localhost_name() {
    if !matches!(sim_host().as_str(), "127.0.0.1" | "localhost") {
        eprintln!("симулятор не на этой машине — проверка имени localhost пропущена");
        return;
    }
    let ctrl = controller(ControllerKind::OpcUa);
    let url = ctrl.endpoint.replace("127.0.0.1", "localhost");
    let tag = ctrl.tags.iter().find(|t| t.protocol == Protocol::OpcUa).expect("OPC UA-тег");
    let conn = OpcConnection::connect(&url, OP_TIMEOUT).await.unwrap_or_else(|e| panic!("{url}: {e:#}"));
    let values = conn.read(&[opcua::read_value_id(&tag.node_id).unwrap()]).await.unwrap();
    assert!(opcua::reading(&values[0]).1 == Quality::Good, "{url}: {:?}", values[0]);
    conn.close().await;
}

/// Каждый тег OPC UA из `controllers.yaml` читается и имеет тип, заявленный в конфигурации.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn opcua_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::OpcUa);
    let tags: Vec<_> = ctrl.tags.iter().filter(|t| t.protocol == Protocol::OpcUa).collect();
    let nodes: Vec<_> = tags.iter().map(|t| opcua::read_value_id(&t.node_id).unwrap()).collect();
    let conn = OpcConnection::connect(&ctrl.endpoint, OP_TIMEOUT).await.expect(NEED_SIM);

    let values = conn.read(&nodes).await.unwrap();
    assert_eq!(values.len(), tags.len());
    let problems: Vec<String> = tags
        .iter()
        .zip(&values)
        .filter_map(|(tag, dv)| {
            let (value, quality, _) = opcua::reading(dv);
            match value {
                Some(v) if quality == Quality::Good && type_matches(&tag.data_type, &v) => None,
                other => Some(format!("{} ({}): {other:?} {quality:?}", tag.name, tag.data_type)),
            }
        })
        .collect();
    conn.close().await;
    assert!(
        problems.is_empty(),
        "{} из {} тегов не прочитаны как надо: {:?}",
        problems.len(),
        tags.len(),
        &problems[..problems.len().min(10)]
    );
}

/// Запись в записываемый узел применяется и откатывается обратно; узел только для чтения отклоняется.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn opcua_write_applies_and_readonly_is_rejected() {
    let ctrl = controller(ControllerKind::OpcUa);
    let conn = OpcConnection::connect(&ctrl.endpoint, OP_TIMEOUT).await.expect(NEED_SIM);

    // M3.M — чистый актуатор (RW, без источника данных): симулятор его не перетирает.
    let rw = ctrl.tags.iter().find(|t| t.name.ends_with(".M_M.M3.M")).expect("тег M3.M");
    assert!(rw.writable);
    let node = opcua::parse_node_id(&rw.node_id).unwrap();
    let read = || async {
        opcua::reading(&conn.read(&[opcua::read_value_id(&rw.node_id).unwrap()]).await.unwrap()[0]).0.unwrap()
    };
    let before = read().await;
    let target = toggled(&before);
    let status = conn.write(node.clone(), opcua::to_variant(&rw.data_type, &target).unwrap()).await.unwrap();
    assert!(status.is_good(), "запись в RW-узел: {status}");
    // Симулятор кладёт записанное в узел следующим шагом цикла (миллисекунды, на медленной машине — дольше), а
    // не до ответа на запись: ждём значение, а не читаем сразу.
    let settled = |want: TagValue| {
        let read = &read;
        async move {
            let mut last = None;
            for _ in 0..40 {
                last = Some(read().await);
                if last.as_ref() == Some(&want) {
                    return last;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            last
        }
    };
    assert_eq!(settled(target.clone()).await, Some(target.clone()), "узел должен вернуть записанное значение");
    let status = conn.write(node, opcua::to_variant(&rw.data_type, &before).unwrap()).await.unwrap();
    assert!(status.is_good());
    assert_eq!(settled(before.clone()).await, Some(before.clone()));

    // Показание датчика: узел только на чтение — сервер отклоняет запись.
    let ro = ctrl
        .tags
        .iter()
        .find(|t| !t.writable && t.protocol == Protocol::OpcUa && t.data_type == "FLOAT")
        .expect("RO-тег");
    let status = conn
        .write(
            opcua::parse_node_id(&ro.node_id).unwrap(),
            opcua::to_variant(&ro.data_type, &TagValue::F64(1.0)).unwrap(),
        )
        .await
        .unwrap();
    assert!(!status.is_good(), "запись в RO-узел {} должна быть отклонена", ro.name);
    assert_eq!(opcua::classify_write_status(status), "REJECTED_NOT_WRITABLE", "статус {status}");
    conn.close().await;
}

// ---------------------------------------------------------------------------- Modbus --

/// Каждый тег Modbus читается блоками и декодируется в тип из конфигурации.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn modbus_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::Modbus);
    let tags: Vec<_> = ctrl.tags.iter().filter(|t| t.protocol == Protocol::Modbus).cloned().collect();
    let mut blocks = modbus::plan_blocks(&tags);
    let (host, port) = modbus::endpoint(&ctrl.endpoint);
    let mut client = ModbusClient::new(host, port, tags[0].modbus_unit_id, OP_TIMEOUT);

    let values = client.read(&mut blocks).await.expect(NEED_SIM);
    assert_eq!(values.len(), tags.len(), "каждый тег попал в план чтения");
    let problems: Vec<String> = values
        .iter()
        .filter(|(tag, v)| !v.as_ref().is_some_and(|v| type_matches(&tag.data_type, v)))
        .map(|(tag, v)| format!("{} ({}): {v:?}", tag.name, tag.data_type))
        .collect();
    assert!(problems.is_empty(), "не прочитаны: {problems:?}");
    assert!(blocks.len() < tags.len() / 10, "чтение блоками: {} запросов на {} тегов", blocks.len(), tags.len());
}

// ------------------------------------------------------------------------------- PAC --

/// Каждый PAC-тег читается из снимка прошивки и приводится к типу из конфигурации.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn pac_reads_every_configured_tag() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect(NEED_SIM);
    conn.poll_states().await.unwrap();

    let problems: Vec<String> = ctrl
        .tags
        .iter()
        .filter(|t| t.protocol == Protocol::Pac)
        .filter_map(|t| {
            let v = conn.read_value(t.device_name.as_deref()?, t.field_name.as_deref()?, &t.data_type);
            match v {
                Some(v) if type_matches(&t.data_type, &v) => None,
                other => Some(format!("{} ({}): {other:?}", t.name, t.data_type)),
            }
        })
        .collect();
    assert!(problems.is_empty(), "нет в снимке t[прибор][поле]: {problems:?}");
}

/// PAC-команда на известный прибор применяется (и откатывается), на несуществующий — отклоняется. Свой прибор (`LINE1V2`): тесты файла идут параллельно и не должны трогать один прибор.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn pac_write_applies_and_unknown_device_is_rejected() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect(NEED_SIM);

    // LINE1V2.M — чистый актуатор: пишем противоположное и возвращаем как было. Не LINE1V0: тесты файла идут
    // параллельно, и LINE1V0 занят проверкой клапана под управлением программы.
    conn.poll_states().await.unwrap();
    let before = conn.read_value("LINE1V2", "M", "INT32").expect("LINE1V2.M в снимке");
    let target = toggled(&before);
    assert_eq!(conn.exec_command("LINE1V2", "M", &target).await.unwrap(), 0, "PAC применяет set_cmd");
    // Снимок симулятор обновляет раз в update_rate (~0,5 с).
    let mut seen = None;
    for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        conn.poll_states().await.unwrap();
        seen = conn.read_value("LINE1V2", "M", "INT32");
        if seen.as_ref() == Some(&target) {
            break;
        }
    }
    assert_eq!(seen, Some(target), "после записи M должен смениться");
    assert_eq!(conn.exec_command("LINE1V2", "M", &before).await.unwrap(), 0);

    assert_ne!(
        conn.exec_command("NO_SUCH_DEVICE", "M", &TagValue::Int(1)).await.unwrap(),
        0,
        "несуществующий прибор — код ошибки"
    );
}

// ---------------------------------------------------------- защита канала и пользователь --
//
// Симулятор для этих проверок запускают с SIM_OPCUA_USER и SIM_OPCUA_PASSWORD (CI делает это сам);
// IT_OPCUA_USER / IT_OPCUA_PASSWORD — те же значения для тестов (по умолчанию operator / operator-pass).

/// Логин и пароль защищённой точки симулятора.
fn creds() -> (String, String) {
    (
        std::env::var("IT_OPCUA_USER").unwrap_or_else(|_| "operator".into()),
        std::env::var("IT_OPCUA_PASSWORD").unwrap_or_else(|_| "operator-pass".into()),
    )
}

/// Параметры защищённого подключения: политика, пользователь, доверие к серверу, хранилище сертификатов.
fn secure_opts(security: &str, user: Option<(&str, &str)>, trust_all: bool, pki: &std::path::Path) -> ConnectOptions {
    ConnectOptions {
        security: OpcSecurity::parse(Some(security), user.map(|u| u.0), user.map(|u| u.1)).unwrap(),
        cert: CertSettings::in_dir(pki),
        trust_server_certs: trust_all,
    }
}

/// Пустое хранилище сертификатов: тест начинается с нуля доверия.
fn fresh_pki(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("scada-it-pki-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Качество первого узла: подтверждает, что соединение читает значения.
async fn read_first_node(conn: &OpcConnection) -> Quality {
    let ctrl = controller(ControllerKind::OpcUa);
    let tag = ctrl.tags.iter().find(|t| t.protocol == Protocol::OpcUa).expect("OPC UA-тег");
    let values = conn.read(&[opcua::read_value_id(&tag.node_id).unwrap()]).await.unwrap();
    opcua::reading(&values[0]).1
}

/// Канал Basic256Sha256 SignAndEncrypt с пользователем читает значения.
#[tokio::test]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn secure_channel_with_user_reads_values() {
    let ctrl = controller(ControllerKind::OpcUa);
    let (user, pass) = creds();
    for (security, name) in [("Basic256Sha256", "enc"), ("Basic256Sha256_Sign", "sign")] {
        let opts = secure_opts(security, Some((&user, &pass)), true, &fresh_pki(name));
        let conn = OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &opts)
            .await
            .unwrap_or_else(|e| panic!("{security}: {e:#}"));
        assert_eq!(read_first_node(&conn).await, Quality::Good, "{security}");
        conn.close().await;
    }
}

/// На защищённой точке неверный пароль и анонимный вход отвергаются.
#[tokio::test]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn wrong_password_and_anonymous_are_refused_on_the_secure_endpoint() {
    let ctrl = controller(ControllerKind::OpcUa);
    let (user, _) = creds();
    let wrong = secure_opts("Basic256Sha256", Some((&user, "не тот пароль")), true, &fresh_pki("wrong"));
    assert!(OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &wrong).await.is_err(), "неверный пароль принят");
    let anonymous = secure_opts("Basic256Sha256", None, true, &fresh_pki("anon"));
    assert!(
        OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &anonymous).await.is_err(),
        "анонимный вход принят на защищённой точке"
    );
}

/// Защищённый канал без `GATEWAY_OPCUA_TRUST_SERVER_CERTS`: сертификат сервера, которого нет в `trusted/`, не
/// принимается; после того как оператор положил его в `trusted/` (как делают в OPC UA), подключение проходит.
#[tokio::test]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn untrusted_server_certificate_is_rejected_until_the_operator_trusts_it() {
    let ctrl = controller(ControllerKind::OpcUa);
    let (user, pass) = creds();
    let pki = fresh_pki("trust");
    let opts = secure_opts("Basic256Sha256", Some((&user, &pass)), false, &pki);
    assert!(
        OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &opts).await.is_err(),
        "неизвестный сертификат сервера принят"
    );

    // Первая попытка сложила сертификат сервера в rejected/ — оператор ПЕРЕНОСИТ его в trusted/ (пока файл
    // лежит и в rejected/, сертификат считается отвергнутым).
    let rejected: Vec<_> = std::fs::read_dir(pki.join("rejected")).expect("rejected/").flatten().collect();
    assert!(!rejected.is_empty(), "отклонённый сертификат сервера не сохранён в {}/rejected", pki.display());
    std::fs::create_dir_all(pki.join("trusted")).unwrap();
    for cert in rejected {
        std::fs::rename(cert.path(), pki.join("trusted").join(cert.file_name())).unwrap();
    }
    let conn = OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &opts).await.unwrap_or_else(|e| panic!("{e:#}"));
    assert_eq!(read_first_node(&conn).await, Quality::Good);
    conn.close().await;
}

/// Пользователь на канале без защиты работает, если сервер это разрешает (в журнале шлюза при этом предупреждение).
#[tokio::test]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn user_over_an_unprotected_channel_still_works_when_the_server_allows_it() {
    let ctrl = controller(ControllerKind::OpcUa);
    let (user, pass) = creds();
    let opts = secure_opts("None", Some((&user, &pass)), true, &fresh_pki("none"));
    let conn = OpcConnection::connect_with(&ctrl.endpoint, OP_TIMEOUT, &opts).await.unwrap_or_else(|e| panic!("{e:#}"));
    assert_eq!(read_first_node(&conn).await, Quality::Good);
    conn.close().await;
}

/// Условная логика прошивки (снята с эмулятора мойки): главный клапан линии `LINE1V0` в автоматическом режиме
/// команду на открытие принимает (код 0 — для шлюза это `APPLIED`), но остаётся закрытым; в ручном режиме (`M=1`)
/// открывается и держит; вернули автоматику — закрывается. Обычный клапан держит команду в любом режиме.
#[tokio::test]
#[ignore = "нужен симулятор: SIM_HOST=… cargo test -- --ignored"]
async fn pac_program_owned_valve_accepts_the_command_but_opens_only_in_manual_mode() {
    let ctrl = controller(ControllerKind::Pac);
    let (host, port) = pac::endpoint(&ctrl.endpoint);
    let mut conn = PacConnection::connect(&host, port, OP_TIMEOUT).await.expect(NEED_SIM);
    let (on, off) = (TagValue::Int(1), TagValue::Int(0));

    /// Значение состояния прибора PAC из свежего снимка.
    async fn state(conn: &mut PacConnection, device: &str) -> Option<TagValue> {
        // Снимок симулятор обновляет раз в update_rate (~0,5 с).
        tokio::time::sleep(Duration::from_millis(1200)).await;
        conn.poll_states().await.unwrap();
        conn.read_value(device, "ST", "INT32")
    }

    // Ожидаемое значение появляется в снимке не мгновенно: на медленной машине цикл симулятора отстаёт, поэтому
    // ждём его (до 10 с), а не читаем один раз; возвращается последнее увиденное.
    /// Дождаться, пока прибор примет ожидаемое значение: прошивка применяет запись на следующем цикле, а не мгновенно.
    async fn settled(conn: &mut PacConnection, device: &str, want: &TagValue) -> Option<TagValue> {
        let mut last = None;
        for _ in 0..20 {
            last = state(conn, device).await;
            if last.as_ref() == Some(want) {
                break;
            }
        }
        last
    }

    // Значение, которое считает программа (у симулятора — архив): его запись в автоматическом режиме не меняет.
    assert_eq!(conn.exec_command("LINE1V0", "M", &off).await.unwrap(), 0);
    let program = state(&mut conn, "LINE1V0").await.expect("LINE1V0.ST в снимке");
    let opposite = toggled(&program);
    assert_eq!(conn.exec_command("LINE1V0", "ST", &opposite).await.unwrap(), 0, "команда принята");
    assert_eq!(
        state(&mut conn, "LINE1V0").await,
        Some(program.clone()),
        "в автоматическом режиме значение осталось за программой"
    );

    assert_eq!(conn.exec_command("LINE1V0", "M", &on).await.unwrap(), 0);
    assert_eq!(conn.exec_command("LINE1V0", "ST", &opposite).await.unwrap(), 0);
    assert_eq!(
        settled(&mut conn, "LINE1V0", &opposite).await,
        Some(opposite.clone()),
        "в ручном режиме команда действует и держится"
    );

    assert_eq!(conn.exec_command("LINE1V0", "M", &off).await.unwrap(), 0);
    assert_eq!(
        settled(&mut conn, "LINE1V0", &program).await,
        Some(program),
        "вернули автоматику — значение снова за программой"
    );

    // Обычный клапан: команда держится без ручного режима.
    conn.poll_states().await.unwrap();
    if conn.read_value("LINE1V1", "ST", "INT32").is_some() {
        assert_eq!(conn.exec_command("LINE1V1", "ST", &on).await.unwrap(), 0);
        assert_eq!(settled(&mut conn, "LINE1V1", &on).await, Some(on.clone()));
        assert_eq!(conn.exec_command("LINE1V1", "ST", &off).await.unwrap(), 0);
    }

    // Неизвестное поле известного прибора прошивка принимает (код 0), значение не меняется; не число — код 1.
    assert_eq!(conn.exec_command("LINE1V0", "NO_SUCH_FIELD", &on).await.unwrap(), 0);
}
