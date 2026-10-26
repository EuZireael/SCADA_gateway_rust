//! SCADA Gateway на Rust: опрос контроллеров (OPC UA, Modbus TCP, PAC driver-master) →
//! Kafka для монитора; приём команд записи; журнал событий в PostgreSQL.
//!
//! Контракты — как у Java-шлюза (controllers.yaml, топики и формат Kafka, статусы команд,
//! env-переменные, /actuator/health и метрики), поэтому встаёт на его место без правок.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;
use tracing::info;

use scada_gateway::app::{App, AppDeps};
use scada_gateway::config::Settings;
use scada_gateway::events::{self, Event};
use scada_gateway::ha;
use scada_gateway::metrics::Metrics;
use scada_gateway::script::Scripts;
use scada_gateway::startup::{self, Tasks};

/// Точка входа: `healthcheck` для проверки контейнера или многопоточный tokio-рантайм с [`run`].
fn main() -> Result<()> {
    // `scada-gateway healthcheck` — проверка для HEALTHCHECK контейнера без curl в образе.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        std::process::exit(if healthcheck() { 0 } else { 1 });
    }
    // `scada-gateway opcua-cert [renew]` — сертификат OPC UA-клиента: показать или перевыпустить.
    if std::env::args().nth(1).as_deref() == Some("opcua-cert") {
        std::process::exit(opcua_cert(std::env::args().nth(2).as_deref()));
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run())
}

/// GET /actuator/health на своём порту: 200 и status UP.
fn healthcheck() -> bool {
    use std::io::{Read, Write};
    let port: u16 = std::env::var("SERVER_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8888);
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(3)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
    if s.write_all(b"GET /actuator/health HTTP/1.0\r\nHost: localhost\r\n\r\n").is_err() {
        return false;
    }
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    resp.split_whitespace().nth(1) == Some("200") && resp.contains(r#""status":"UP""#)
}

/// Сертификат OPC UA-клиента из тех же настроек окружения, что у шлюза: без аргумента — показать, `renew` —
/// перевыпустить самоподписанный (прежняя пара остаётся рядом с суффиксом `.bak`).
fn opcua_cert(action: Option<&str>) -> i32 {
    use scada_gateway::opcua_cert as cert;
    let run = || -> Result<()> {
        let settings = scada_gateway::config::opcua_cert_from_env()?;
        let now = chrono::Utc::now();
        match action {
            None | Some("show") => match cert::current(&settings)? {
                Some(c) => println!("{}", cert::describe(&c, now)),
                None => println!(
                    "Сертификата OPC UA-клиента в {} ещё нет: шлюз выпустит его при запуске (или `opcua-cert renew`)",
                    settings.pki_dir.display()
                ),
            },
            Some("renew") => {
                let c = cert::renew(&settings)?;
                println!("{}", cert::describe(&c, now));
                println!(
                    "Внесите новый сертификат в доверенные на каждом ПЛК (файл выше). Шлюз берёт его при следующем \
                     подключении; чтобы перейти сразу — перезапустите шлюз."
                );
            }
            Some(other) => anyhow::bail!("неизвестное действие «{other}»: opcua-cert [show|renew]"),
        }
        Ok(())
    };
    match run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("opcua-cert: {e:#}");
            1
        }
    }
}

/// Запуск шлюза по шагам и ожидание сигнала остановки; порядок шагов — в `docs/ARCHITECTURE.md`.
async fn run() -> Result<()> {
    startup::init_tracing();
    let settings = Settings::from_env()?;
    info!("SCADA Gateway (Rust) {} стартует", env!("CARGO_PKG_VERSION"));

    let station = startup::load_station(&settings).await?;
    let metrics = Arc::new(Metrics::new());
    let leadership = startup::build_leadership(&settings, &metrics)?;
    let kafka = startup::build_kafka(&settings, &metrics, &leadership).await?;

    let (events, events_rx) = events::channel(metrics.clone());
    let mut tasks = Tasks::new(metrics.clone(), events.clone());
    tasks.spawn(events::run_writer(
        events_rx,
        station.db.clone(),
        kafka.clone(),
        leadership.clone(),
        metrics.clone(),
        tasks.cancel.clone(),
    ));

    startup::spawn_opcua_cert(&settings, &station.controllers, &metrics, &events, &mut tasks).await?;

    // Пользовательские скрипты: ошибка при старте — ошибка старта шлюза.
    let tag_names: Vec<String> =
        station.controllers.iter().flat_map(|c| c.tags.iter().map(|t| t.name.clone())).collect();
    let scripts = Scripts::load(
        settings.gateway.scripts.clone(),
        tag_names,
        metrics.script_errors.clone(),
        metrics.scripts_bound_tags.clone(),
        events.clone(),
    )?;
    let (reload, cancel) = (scripts.clone(), tasks.cancel.clone());
    tasks.supervise("scripts-reload", move || reload.clone().run_reload(cancel.clone()));

    let telemetry = startup::spawn_telemetry_writer(&settings, station.db.as_ref(), &metrics, &mut tasks);
    info!(
        "⚙ alarms={} persist-telemetry={} send-bad-frames={}",
        settings.gateway.alarms_enabled,
        telemetry.is_some(),
        settings.gateway.send_bad_frames
    );

    let (ha_settings, kafka_settings) = (settings.gateway.ha.clone(), settings.kafka.clone());
    let app = Arc::new(App::new(
        settings,
        AppDeps {
            metrics,
            kafka: kafka.clone(),
            events: events.clone(),
            telemetry,
            db: station.db,
            leadership: leadership.clone(),
            scripts,
        },
        station.controllers,
    ));
    events.emit(
        Event::new("SYSTEM", "Gateway", "INFO", "SCADA Gateway starting up")
            .details(json!({"controllers": app.controllers.len(), "tags": app.tag_count(), "runtime": "rust"})),
    );

    // Горячее резервирование: выборы и журнал смены роли.
    let blind_app = app.clone();
    // Отметка станции — по её контроллерам: экземпляр другой станции в той же группе выборов виден и в неё не вступает.
    let station_check = ha::StationCheck::new(
        ha::StationMark::of(app.controllers.iter().map(|c| c.ctrl.name.as_str())),
        events.clone(),
        app.metrics.ha_foreign_members.clone(),
    );
    let elector = ha::spawn(
        ha_settings,
        kafka_settings,
        leadership.clone(),
        Arc::new(move || blind_app.all_links_down()),
        station_check,
        tasks.cancel.clone(),
    );
    if leadership.is_ha_enabled() {
        // Без резервирования задачи нет (она сразу выходит) — под надзор её не берём: выход считался бы сбоем.
        let (leadership, events, cancel) = (leadership.clone(), events.clone(), tasks.cancel.clone());
        tasks.supervise("ha-events", move || ha::record_events(leadership.clone(), events.clone(), cancel.clone()));
    }

    if let Some(pending) = station.pending_db {
        startup::spawn_db_sync(&app, pending, &mut tasks);
    }
    startup::spawn_pollers(&app, &mut tasks);
    startup::spawn_commands(&app, &leadership, &mut tasks);
    startup::spawn_tag_settings(&app, &mut tasks);
    startup::spawn_pac_alarms(&app, station.disabled_alarm_ids, &mut tasks);
    startup::spawn_http(app, &mut tasks).await?;

    startup::shutdown_signal().await;
    info!("Остановка...");
    events.emit(Event::new("SYSTEM", "Gateway", "INFO", "SCADA Gateway shutting down"));
    tasks.stop().await;
    if let Some(handle) = elector {
        // Поток выборов сам переведёт роль в резерв и выйдет из группы — резерв подхватит сразу.
        let _ = tokio::task::spawn_blocking(move || handle.join()).await;
    }
    if let Some(k) = &kafka {
        k.flush(Duration::from_secs(3));
    }
    info!("Остановлено");
    Ok(())
}
