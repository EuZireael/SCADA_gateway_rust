//! Нагрузочный источник для шлюза: N OPC UA-серверов (ПЛК) по M узлов; часть узлов меняется каждую
//! секунду. Масштаб из контракта «телеметрия по исключению»: 20 контроллеров по 2740 тегов
//! (54 800 тегов), треть тегов меняется каждую секунду (≈18 000 изменений/с).
//!
//!   loadsim run     [--servers 20] [--tags 2740] [--base-port 4900] [--first 0] [--fraction 0.3333] [--period-ms 1000]
//!   loadsim config  [--servers 20] [--tags 2740] [--base-port 4900] [--first 0] [--poll-ms 2000] > controllers.yaml
//!
//! `--first` — номер первого сервера: порт `base-port + номер`, имя `LOAD-<номер>`. Нужен, чтобы источник из сотен
//! серверов разнести по нескольким процессам (`loadsim run --first 0 --servers 50`, `loadsim run --first 50 …`):
//! один процесс упирается в свой процессор раньше, чем шлюз.
//!
//! Узел `ns=2;s=<j>`, тип — Float (2/3) или Int32 (1/3), как на станции; имя тега в конфиге —
//! `LOAD-<i>.T<j>.V`. Значение меняют по кругу окном в `fraction` узлов — за 1/fraction периодов
//! меняется каждый узел.

use std::sync::Arc;
use std::time::Duration;

use opcua::server::address_space::Variable;
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use opcua::server::{ServerBuilder, ServerHandle};
use opcua::types::{DataValue, MessageSecurityMode, NodeId, Variant};

/// Параметры запуска из командной строки: `--servers`, `--tags`, `--base-port`, `--first`, `--fraction`, `--period-ms`,
/// `--poll-ms`.
struct Args {
    /// Сколько OPC UA-серверов поднять (по одному на порт).
    servers: usize,
    /// Сколько узлов в каждом сервере.
    tags: usize,
    /// Порт сервера номер 0; сервер номер `n` слушает `base_port + n`.
    base_port: u16,
    /// Номер первого сервера этого процесса.
    first: usize,
    /// Доля узлов, меняющихся каждый период.
    fraction: f64,
    /// Период изменения значений, мс.
    period_ms: u64,
    /// `pollingRate` в сгенерированной конфигурации шлюза, мс.
    poll_ms: u64,
}

/// Команда (`run` по умолчанию или `config`) и параметры; неверный флаг — паника с его именем (служебный инструмент).
fn args() -> (String, Args) {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| "run".into());
    let mut a = Args { servers: 20, tags: 2740, base_port: 4900, first: 0, fraction: 1.0 / 3.0, period_ms: 1000, poll_ms: 2000 };
    while let Some(flag) = it.next() {
        let value = it.next().unwrap_or_default();
        match flag.as_str() {
            "--servers" => a.servers = value.parse().expect("--servers"),
            "--tags" => a.tags = value.parse().expect("--tags"),
            "--base-port" => a.base_port = value.parse().expect("--base-port"),
            "--first" => a.first = value.parse().expect("--first"),
            "--fraction" => a.fraction = value.parse().expect("--fraction"),
            "--period-ms" => a.period_ms = value.parse().expect("--period-ms"),
            "--poll-ms" => a.poll_ms = value.parse().expect("--poll-ms"),
            other => panic!("неизвестный флаг {other}"),
        }
    }
    (cmd, a)
}

/// Тип узла по номеру: каждый третий целый, остальные вещественные.
fn is_float(j: usize) -> bool {
    !j.is_multiple_of(3)
}

/// Печатает `controllers.yaml` для шлюза: N контроллеров по M тегов (`loadsim config`).
fn config(a: &Args) {
    println!("opcua:\n  servers:");
    for i in a.first..a.first + a.servers {
        println!("    - id: load-{i}\n      name: \"Load-{i}\"");
        println!("      endpoint: \"opc.tcp://${{SIM_HOST:127.0.0.1}}:{}\"\n      enabled: true\n      tags:", a.base_port as usize + i);
        for j in 0..a.tags {
            println!(
                "        - {{name: \"LOAD-{i}.T{j}.V\", nodeId: \"ns=2;s={j}\", dataType: {}, pollingRate: {}, enabled: true, writable: false}}",
                if is_float(j) { "FLOAT" } else { "INT32" },
                a.poll_ms
            );
        }
    }
}

/// Сервер одного «ПЛК»: запускает и возвращает обработчик с менеджером узлов.
async fn start_server(i: usize, a: &Args) -> anyhow::Result<(ServerHandle, Arc<SimpleNodeManager>, Vec<NodeId>)> {
    let ns_uri = format!("urn:loadsim:{i}");
    let port = a.base_port + i as u16;
    let (server, handle) = ServerBuilder::new_anonymous(format!("loadsim-{i}"))
        .application_uri(format!("urn:loadsim:app:{i}"))
        .host("0.0.0.0")
        .port(port)
        .pki_dir(std::env::temp_dir().join(format!("loadsim-pki-{i}")))
        .create_sample_keypair(true)
        .trust_client_certs(true)
        .add_endpoint("none", ("/", opcua::crypto::SecurityPolicy::None, MessageSecurityMode::None, &["ANONYMOUS"] as &[&str]))
        .with_node_manager(simple_node_manager(NamespaceMetadata { namespace_uri: ns_uri.clone(), ..Default::default() }, "load"))
        .max_array_length(100_000)
        .build()
        .map_err(|e| anyhow::anyhow!("сервер {i}: {e}"))?;
    let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().expect("менеджер узлов");
    let ns = handle.get_namespace_index(&ns_uri).expect("namespace");
    anyhow::ensure!(ns == 2, "namespace должен быть 2, а не {ns}: адрес узлов в конфиге — ns=2;s=<j>");
    let mut ids = Vec::with_capacity(a.tags);
    {
        let mut space = manager.address_space().write();
        let folder = NodeId::new(ns, "plc");
        space.add_folder(&folder, "plc", "plc", &NodeId::objects_folder_id());
        let vars: Vec<Variable> = (0..a.tags)
            .map(|j| {
                let id = NodeId::new(ns, j.to_string());
                ids.push(id.clone());
                if is_float(j) {
                    Variable::new(&id, j.to_string(), j.to_string(), j as f32)
                } else {
                    Variable::new(&id, j.to_string(), j.to_string(), j as i32)
                }
            })
            .collect();
        space.add_variables(vars, &folder);
    }
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("сервер {i} остановлен: {e}");
        }
    });
    Ok((handle, manager, ids))
}

/// Запуск источника нагрузки: N OPC UA-серверов по M узлов, доля узлов меняется каждый период.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cmd, a) = args();
    if cmd == "config" {
        config(&a);
        return Ok(());
    }
    let mut servers = Vec::new();
    for i in a.first..a.first + a.servers {
        servers.push(start_server(i, &a).await?);
    }
    let window = ((a.tags as f64 * a.fraction).ceil() as usize).max(1);
    eprintln!(
        "loadsim: {} серверов :{}…:{}, по {} узлов, меняется {} узлов/с на сервер ({} изменений/с всего)",
        a.servers,
        a.base_port as usize + a.first,
        a.base_port as usize + a.first + a.servers - 1,
        a.tags,
        window as f64 * 1000.0 / a.period_ms as f64,
        (window * a.servers) as f64 * 1000.0 / a.period_ms as f64
    );
    let mut tick = tokio::time::interval(Duration::from_millis(a.period_ms));
    let (mut round, mut offset) = (0u64, 0usize);
    loop {
        tick.tick().await;
        round += 1;
        for (handle, manager, ids) in &servers {
            let mut batch = Vec::with_capacity(window);
            for k in 0..window {
                let j = (offset + k) % a.tags;
                let variant = if is_float(j) {
                    Variant::from(j as f32 + (round % 1000) as f32 * 0.37)
                } else {
                    Variant::from((j as i32).wrapping_add(round as i32))
                };
                batch.push((&ids[j], DataValue::new_now(variant)));
            }
            let _ = manager.set_values(handle.subscriptions(), batch.iter().map(|(id, dv)| (*id, None, dv.clone())));
        }
        offset = (offset + window) % a.tags;
    }
}
