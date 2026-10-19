//! PLC-симулятор для стенда шлюза: OPC UA, Modbus TCP и PAC (driver-master) в одном процессе;
//! значения тегов проигрываются из 5-суточного архива станции BN1_MCA1. Поднимаются только те протоколы,
//! теги которых есть в конфигурации: станционная конфигурация по умолчанию целиком на OPC UA.
//!
//!   simulator \[config/station_opcua.yaml\]     запуск (все каналы базы каналов — OPC UA)
//!   simulator healthcheck \[config\]             проверка живости (порты настроенных протоколов) для контейнера
//!   simulator station-config \[opcua|pac\] \[станция\] \[прежняя\] \[выход\]  собрать конфигурацию из базы каналов станции
//!   simulator conformance \[host\] \[port\] \[config\]  проверка (по умолчанию replay_config.yaml): PAC ведёт себя при записи как описано в конфигурации
//!   simulator probe-pac \[host\] \[port\]         снимок любого PAC (симулятор, прошивка ptusa): по умолчанию localhost:10000
//!
//! Окружение: OPCUA_ENDPOINT (напр. opc.tcp://simulator:4840 — адрес привязки и анонса вместо
//! конфигурации), MODBUS_PORT, PAC_PORT, RUST_LOG; SIM_OPCUA_USER и SIM_OPCUA_PASSWORD — включить
//! защищённые конечные точки (Basic256Sha256) для этого пользователя.

mod config;
mod conformance;
mod luatab;
mod modbus;
mod opcua;
mod pac;
mod pac_client;
mod plc;
mod replay;
mod station;
mod tag;
mod value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::pac::{PacDevice, PacField, PacModel};
use crate::plc::Plc;
use crate::replay::{Archive, Replay};
use crate::tag::Protocol;

/// Порт из переменной окружения или значение по умолчанию.
fn env_port(name: &str, default: u16) -> Result<u16> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.trim().parse().with_context(|| format!("{name}={v}")),
        _ => Ok(default),
    }
}

/// Путь к архиву: относительный ищется от текущей папки, затем от папки симулятора (родитель config/).
fn resolve_data_path(data_path: &str, config_path: &Path) -> PathBuf {
    let p = PathBuf::from(data_path);
    if p.is_absolute() || p.exists() {
        return p;
    }
    config_path.parent().and_then(Path::parent).map(|base| base.join(&p)).unwrap_or(p)
}

/// Приборы PAC в порядке появления в конфигурации.
fn pac_devices(plc: &Plc) -> Vec<PacDevice> {
    let mut devices: Vec<PacDevice> = Vec::new();
    for tag in plc.tags().iter().filter(|t| t.protocol == Protocol::Pac) {
        let name = tag.device.clone().unwrap_or_else(|| tag.name.clone());
        let field =
            PacField { field: tag.field.clone().unwrap_or_else(|| tag.name.clone()), address: tag.address.clone() };
        match devices.iter_mut().find(|d| d.device == name) {
            Some(d) => d.fields.push(field),
            None => devices.push(PacDevice { device: name, fields: vec![field] }),
        }
    }
    devices
}

/// Сколько тегов в конфигурации на каждом протоколе: (OPC UA, Modbus, PAC).
fn protocol_tags(cfg: &config::Config) -> (usize, usize, usize) {
    let count = |p: &str| {
        cfg.plc
            .data_blocks
            .iter()
            .flat_map(|b| &b.tags)
            .filter(|t| t.protocol.as_deref().unwrap_or("opcua") == p)
            .count()
    };
    (count("opcua"), count("modbus"), count("pac"))
}

/// `simulator healthcheck`: порты тех протоколов, теги которых есть в конфигурации, принимают соединения (проверка
/// контейнера). Modbus и PAC слушают на всех адресах, а OPC UA —
/// на адресе из `OPCUA_ENDPOINT` (в Docker это имя сервиса, а не loopback), поэтому ему loopback не подходит.
async fn healthcheck() -> Result<()> {
    let cfg_path = arg(2).unwrap_or_else(|| DEFAULT_CONFIG.into());
    let cfg = config::load(Path::new(&cfg_path))?;
    let endpoint = std::env::var("OPCUA_ENDPOINT").ok().filter(|v| !v.is_empty()).unwrap_or(cfg.plc.endpoint.clone());
    let (opcua_host, opcua_port) = opcua::parse_endpoint(&endpoint)?;
    let opcua_host =
        if matches!(opcua_host.as_str(), "0.0.0.0" | "::" | "[::]") { "127.0.0.1".into() } else { opcua_host };
    let (opcua_tags, modbus, pac) = protocol_tags(&cfg);
    let mut targets = Vec::new();
    if opcua_tags > 0 {
        targets.push((opcua_host, opcua_port));
    }
    if modbus > 0 {
        targets.push(("127.0.0.1".into(), env_port("MODBUS_PORT", cfg.modbus_port)?));
    }
    if pac > 0 {
        targets.push(("127.0.0.1".into(), env_port("PAC_PORT", cfg.pac_port)?));
    }
    for (host, port) in targets {
        tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect((host.as_str(), port)))
            .await
            .with_context(|| format!("{host}:{port}: таймаут"))?
            .with_context(|| format!("{host}:{port}"))?;
    }
    Ok(())
}

/// Конфигурация по умолчанию: все каналы базы каналов станции, только OPC UA.
const DEFAULT_CONFIG: &str = "config/station_opcua.yaml";

/// Конфигурация «EasyDriver»: все каналы станции только по PAC (driver-master).
const PAC_CONFIG: &str = "config/station_pac.yaml";

/// Конфигурация трёх протоколов (OPC UA, Modbus, PAC): тесты шлюза и проверка соответствия прошивке PAC.
const MULTIPROTOCOL_CONFIG: &str = "config/replay_config.yaml";

/// База каналов станции, из которой собирается [`DEFAULT_CONFIG`] (путь от папки симулятора).
const STATION_DATABASE: &str = "../config/stations/BN1_MCA1.yaml";

/// Аргумент командной строки по номеру (после имени подкоманды).
fn arg(n: usize) -> Option<String> {
    std::env::args().nth(n)
}

/// Порт из аргумента командной строки (по умолчанию 10000).
fn port_arg(n: usize) -> Result<u16> {
    Ok(arg(n).map(|p| p.parse()).transpose().context("порт")?.unwrap_or(10000))
}

/// `simulator station-config [opcua|pac] [станция] [прежняя] [выход]`: конфигурация из базы каналов станции.
fn station_config() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(2).collect();
    let protocol =
        if matches!(args.first().map(String::as_str), Some("opcua" | "pac")) { args.remove(0) } else { "opcua".into() };
    let default_out = if protocol == "pac" { PAC_CONFIG } else { DEFAULT_CONFIG };
    let mut it = args.into_iter();
    let station = it.next().unwrap_or_else(|| STATION_DATABASE.into());
    let legacy = it.next().unwrap_or_else(|| MULTIPROTOCOL_CONFIG.into());
    let out = it.next().unwrap_or_else(|| default_out.into());
    station::run(&protocol, Path::new(&station), Path::new(&legacy), Path::new(&out))
}

/// Служебные подкоманды (`healthcheck`, `conformance`, `probe-pac`); `None` — обычный запуск симулятора.
async fn subcommand() -> Option<Result<()>> {
    match arg(1).as_deref() {
        Some("healthcheck") => Some(healthcheck().await),
        Some("conformance") => Some(
            async {
                let host = arg(2).unwrap_or_else(|| "127.0.0.1".into());
                let cfg = arg(4).unwrap_or_else(|| MULTIPROTOCOL_CONFIG.into());
                conformance::run(&host, port_arg(3)?, Path::new(&cfg)).await
            }
            .await,
        ),
        Some("station-config") => Some(station_config()),
        Some("probe-pac") => {
            Some(async { pac::probe(&arg(2).unwrap_or_else(|| "localhost".into()), port_arg(3)?).await }.await)
        }
        _ => None,
    }
}

/// Загрузить архив, если реплей включён в конфигурации.
fn load_replay(cfg: &config::Config, config_path: &Path) -> Result<Option<Replay>> {
    if !cfg.replay.enabled {
        return Ok(None);
    }
    let archive = Archive::load(&resolve_data_path(&cfg.replay.data_path, config_path))?;
    let replay = Replay::new(archive, cfg.replay.speed, cfg.replay.looped);
    info!(
        "Загружен архив replay: {} тегов, длительность {:.2} сут, speed={}, loop={}",
        replay.series_count(),
        replay.duration / 86400.0,
        replay.speed,
        replay.looped
    );
    Ok(Some(replay))
}

/// PAC-сервер (если в конфигурации есть теги PAC): запись клиента идёт в состояние контроллера.
async fn start_pac(plc: &Arc<Plc>, cfg: &config::Config) -> Result<Option<Arc<PacModel>>> {
    if plc.stats().pac == 0 {
        return Ok(None);
    }
    let writer = plc.clone();
    let model = Arc::new(PacModel::new(
        &plc.name,
        pac_devices(plc),
        Box::new(move |device, field, index, value| writer.pac_write(device, field, index, value)),
    ));
    let listener = TcpListener::bind(("0.0.0.0", env_port("PAC_PORT", cfg.pac_port)?)).await.context("порт PAC")?;
    tokio::spawn(pac::serve(listener, model.clone()));
    Ok(Some(model))
}

/// OPC UA-сервер; с `SIM_OPCUA_USER` / `SIM_OPCUA_PASSWORD` — ещё и защищённые точки для проверки шлюза.
fn start_opcua(plc: &Arc<Plc>, cfg: &config::Config) -> Result<(tokio::task::JoinHandle<()>, opcua::Pusher)> {
    let env = |name| std::env::var(name).ok().filter(|v| !v.is_empty());
    let endpoint = env("OPCUA_ENDPOINT").unwrap_or_else(|| cfg.plc.endpoint.clone());
    let user = env("SIM_OPCUA_USER").zip(env("SIM_OPCUA_PASSWORD"));
    let (server, pusher) = opcua::build(plc, &endpoint, user.as_ref().map(|(u, p)| (u.as_str(), p.as_str())))?;
    if user.is_some() {
        info!("OPC UA: включены защищённые конечные точки Basic256Sha256 (пользователь из SIM_OPCUA_USER)");
    }
    let task = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            warn!("OPC UA-сервер остановлен: {e}");
        }
    });
    Ok((task, pusher))
}

/// Цикл обновления: значения из архива → регистры, узлы OPC UA, снимок PAC.
fn spawn_cycle(
    plc: Arc<Plc>,
    regs: Arc<modbus::Registers>,
    pusher: Option<opcua::Pusher>,
    pac: Option<Arc<PacModel>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(plc.update_rate);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = plc.wake.notified() => {}
            }
            let out = plc.cycle(&regs);
            if let Some(pusher) = &pusher {
                pusher.push(&out.opcua);
            }
            if let Some(pac) = &pac {
                let snapshot: HashMap<String, value::Value> = out.pac;
                pac.update_snapshot(snapshot);
            }
        }
    })
}

/// Запуск: подкоманда или полный симулятор (ПЛК, Modbus, PAC, OPC UA, цикл обновления).
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,opcua=warn".into()),
        )
        .init();
    if let Some(result) = subcommand().await {
        return result;
    }
    let config_path = PathBuf::from(arg(1).unwrap_or_else(|| DEFAULT_CONFIG.into()));
    let cfg = config::load(&config_path)?;

    let plc = Arc::new(Plc::new(&cfg, load_replay(&cfg, &config_path)?)?);
    let st = plc.stats();
    info!(
        "PLC {} ({}): {} тегов — OPC UA {}, Modbus {}, PAC {}; шаг {:?}",
        plc.name, plc.id, st.total, st.opcua, st.modbus, st.pac, plc.update_rate
    );

    let regs = Arc::new(modbus::Registers::new());
    // Modbus-сервер — только если в конфигурации есть его теги: станция целиком на OPC UA порт не занимает.
    if st.modbus > 0 {
        let listener =
            TcpListener::bind(("0.0.0.0", env_port("MODBUS_PORT", cfg.modbus_port)?)).await.context("порт Modbus")?;
        tokio::spawn(modbus::serve(listener, regs.clone()));
    }
    let pac = start_pac(&plc, &cfg).await?;
    // OPC UA-сервер — тоже только если есть его теги: станция целиком на PAC («EasyDriver») порт 4840 не занимает.
    let (server_task, pusher) = if st.opcua > 0 {
        let (task, pusher) = start_opcua(&plc, &cfg)?;
        (Some(task), Some(pusher))
    } else {
        (None, None)
    };
    let cycle = spawn_cycle(plc, regs, pusher, pac);

    info!("Симулятор запущен, остановка — Ctrl+C / SIGTERM");
    shutdown_signal().await;
    info!("Останавливаю симулятор");
    cycle.abort();
    if let Some(task) = server_task {
        task.abort();
    }
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
