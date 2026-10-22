//! OPC UA-фасад эмулятора мойки (см. документацию библиотеки).
//!
//!   ptusa-opcua              запуск
//!   ptusa-opcua healthcheck  проверка живости для контейнера: порт OPC UA открыт и прошивка отвечает (при PAC_NAME — своё имя)
//!
//! Окружение:
//!   PAC_HOST, PAC_PORT   прошивка ptusa (driver-master), по умолчанию ptusa:10000
//!   PAC_NAME             имя прошивки (`PAC_name`): задано — фасад не открывает соединение с прошивкой, назвавшей себя иначе,
//!                        и значений чужой станции не отдаёт (узлы `BadCommunicationError`, `SYSTEM.LINK_OK` = false);
//!                        не задано — имя не сверяется. Узел `SYSTEM.PAC_NAME` всегда отдаёт имя прошивки
//!   STATION_CONFIG       конфигурация станции, по умолчанию /config/station.yaml
//!   OPCUA_BIND           на чём слушать, по умолчанию 0.0.0.0:4840
//!   OPCUA_ENDPOINT       адрес, который сервер называет в FindServers (по умолчанию opc.tcp://localhost:4840);
//!                        конечные точки GetEndpoints строятся из OPCUA_BIND — клиент (шлюз) подключается
//!                        по адресу, который знает сам
//!   POLL_MS              период снимка, по умолчанию 500
//!   RUST_LOG             уровни журнала

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ptusa_opcua::channel::load_channels;
use ptusa_opcua::pac::PacClient;
use ptusa_opcua::server::{Bridge, Stats, build, parse_bind};
use ptusa_opcua::snapshot::Snapshot;
use tracing::{info, warn};

/// Переменная окружения или значение по умолчанию (пустое считается незаданным).
fn env(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.to_string())
}

/// Клиент прошивки из `PAC_HOST`/`PAC_PORT`; с `PAC_NAME` — со сверкой её имени при подключении.
fn pac_client() -> Result<PacClient> {
    let port: u16 = env("PAC_PORT", "10000").parse().context("PAC_PORT")?;
    let expected = Some(env("PAC_NAME", "")).filter(|n| !n.is_empty());
    Ok(PacClient::new(&env("PAC_HOST", "ptusa"), port, Duration::from_secs(3)).expecting_name(expected))
}

/// Запуск фасада: каналы из конфигурации станции, клиент PAC, OPC UA-сервер и цикл снимков.
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,opcua=warn".into()),
        )
        .init();
    let bind = parse_bind(&env("OPCUA_BIND", "0.0.0.0:4840"))?;
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let port = bind.1;
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .with_context(|| format!("порт {port}: таймаут"))?
            .with_context(|| format!("порт {port}"))?;
        // Порт открыт, но фасад без прошивки бесполезен: проверяем и связь с ней (и её имя, если оно задано).
        let pac = pac_client()?;
        pac.connect().await?;
        return Ok(());
    }

    let channels = Arc::new(load_channels(&PathBuf::from(env("STATION_CONFIG", "/config/station.yaml")))?);
    let poll_ms: u64 = env("POLL_MS", "500").parse().context("POLL_MS")?;
    let pac = Arc::new(pac_client()?);
    let stats = Arc::new(Stats::default());
    let announce = env("OPCUA_ENDPOINT", "opc.tcp://localhost:4840");
    let (server, pusher) = build(&channels, pac.clone(), stats.clone(), &announce, bind.clone())?;
    info!("OPC UA на {}:{}, прошивка {}:{}, снимок раз в {poll_ms} мс", bind.0, bind.1, pac.host, pac.port);

    let bridge =
        Bridge { channels, pac, pusher, snapshot: Snapshot::new()?, poll: Duration::from_millis(poll_ms), stats };
    let server_task = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            warn!("OPC UA-сервер остановлен: {e}");
        }
    });
    let bridge_task = tokio::spawn(bridge.run());
    shutdown_signal().await;
    info!("Останавливаю фасад");
    bridge_task.abort();
    server_task.abort();
    Ok(())
}

/// Ждать SIGTERM или Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
