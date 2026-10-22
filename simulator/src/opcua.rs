//! OPC UA-сервер симулятора (async-opcua).
//!
//! Адресное пространство — как у настоящего ПЛК: на прибор объект с переменными-полями (LINE1V0 →
//! {ST, M}). NodeId поля — `ns=2;s=<address тега>`, поэтому привязка шлюза к базе каналов не
//! зависит от иерархии. Состояние тегов ведёт [`Plc`]; узлы получают только изменившиеся значения.
//! Запись клиента приходит в [`Plc::opcua_write`] (RO-узлы не записываемы вовсе).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use opcua::crypto::SecurityPolicy;
use opcua::server::address_space::{ObjectBuilder, VariableBuilder};
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use opcua::server::{Server, ServerBuilder, ServerHandle, ServerUserToken};
use opcua::types::{DataTypeId, DataValue, MessageSecurityMode, NodeId, ObjectId, StatusCode, Variant};
use tracing::info;

use crate::plc::{Plc, WriteError};
use crate::tag::Protocol;
use crate::value::{DataType, Value};

/// Адрес и порт из `opc.tcp://host:port[/path]`.
pub fn parse_endpoint(endpoint: &str) -> Result<(String, u16)> {
    let rest = endpoint
        .strip_prefix("opc.tcp://")
        .with_context(|| format!("endpoint {endpoint:?}: ожидается opc.tcp://host:port"))?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse().with_context(|| format!("endpoint {endpoint:?}: порт"))?),
        None => (authority, 4840),
    };
    ensure!(!host.is_empty(), "endpoint {endpoint:?}: пустой адрес");
    Ok((host.to_string(), port))
}

fn data_type_id(t: DataType) -> DataTypeId {
    match t {
        DataType::Bool => DataTypeId::Boolean,
        DataType::Int => DataTypeId::Int32,
        DataType::Float => DataTypeId::Float,
        DataType::Byte => DataTypeId::Byte,
        DataType::String => DataTypeId::String,
    }
}

/// Значение тега → вариант узла с типом из базы каналов (Float — 32 бита, Int — Int32).
pub fn to_variant(value: &Value, t: DataType) -> Variant {
    match (t, value) {
        (DataType::Bool, v) => Variant::from(v.as_f64().is_some_and(|n| n != 0.0)),
        (DataType::Int, v) => Variant::from(v.as_f64().unwrap_or(0.0).trunc() as i32),
        (DataType::Float, v) => Variant::from(v.as_f64().unwrap_or(0.0) as f32),
        (DataType::Byte, v) => Variant::from((v.as_f64().unwrap_or(0.0).trunc() as i64 & 0xFF) as u8),
        (DataType::String, Value::Text(s)) => Variant::from(s.as_str()),
        (DataType::String, other) => Variant::from(format!("{other:?}")),
    }
}

/// Вариант записи клиента → значение (None — тип не из числа/строки/bool).
fn from_variant(v: &Variant) -> Option<Value> {
    Some(match v {
        Variant::Boolean(b) => Value::Bool(*b),
        Variant::SByte(i) => Value::Int(i64::from(*i)),
        Variant::Byte(i) => Value::Int(i64::from(*i)),
        Variant::Int16(i) => Value::Int(i64::from(*i)),
        Variant::UInt16(i) => Value::Int(i64::from(*i)),
        Variant::Int32(i) => Value::Int(i64::from(*i)),
        Variant::UInt32(i) => Value::Int(i64::from(*i)),
        Variant::Int64(i) => Value::Int(*i),
        Variant::UInt64(i) => Value::Int(i64::try_from(*i).ok()?),
        Variant::Float(f) => Value::Float(f64::from(*f)),
        Variant::Double(f) => Value::Float(*f),
        Variant::String(s) => Value::Text(s.as_ref().to_string()),
        _ => return None,
    })
}

/// Кладёт значения в узлы запущенного сервера.
pub struct Pusher {
    handle: ServerHandle,
    manager: Arc<SimpleNodeManager>,
    ns: u16,
    /// address → тип тега (для приведения значения узла).
    types: HashMap<String, DataType>,
}

/// Сервер без узлов: адрес, PKI и конечные точки. С `user` (логин, пароль) сервер дополнительно предлагает
/// защищённые точки (Basic256Sha256: Sign и SignAndEncrypt), доступные только этому пользователю, а на открытой
/// принимает и его. Нужно для проверки защищённого подключения шлюза.
fn server_builder(plc: &Plc, host: String, port: u16, user: Option<(&str, &str)>) -> ServerBuilder {
    let pki = std::env::temp_dir().join(format!("sim-pki-{}-{port}", std::process::id()));
    let builder = ServerBuilder::new_anonymous(plc.name.clone())
        .application_uri(format!("urn:{}:simulator", plc.id))
        .product_uri("urn:scada-rust:simulator")
        .host(host)
        .port(port)
        .pki_dir(pki)
        .create_sample_keypair(true)
        .trust_client_certs(true);
    match user {
        None => builder
            .add_endpoint("none", ("/", SecurityPolicy::None, MessageSecurityMode::None, &["ANONYMOUS"] as &[&str])),
        Some((name, pass)) => builder
            .add_user_token("operator", ServerUserToken::user_pass(name, pass))
            .add_endpoint(
                "none",
                ("/", SecurityPolicy::None, MessageSecurityMode::None, &["ANONYMOUS", "operator"] as &[&str]),
            )
            .add_endpoint(
                "basic256sha256_sign",
                ("/", SecurityPolicy::Basic256Sha256, MessageSecurityMode::Sign, &["operator"] as &[&str]),
            )
            .add_endpoint(
                "basic256sha256_signandencrypt",
                ("/", SecurityPolicy::Basic256Sha256, MessageSecurityMode::SignAndEncrypt, &["operator"] as &[&str]),
            ),
    }
}

/// Что положено в адресное пространство.
struct Populated {
    /// address → тип тега (для приведения значения узла).
    types: HashMap<String, DataType>,
    variables: usize,
    devices: usize,
}

/// Объект на ПЛК и на каждый прибор, переменная на каждый OPC UA-тег (прибор = `<тип>_<имя>`, поле — внутри).
fn populate(plc: &Plc, manager: &SimpleNodeManager, ns: u16) -> Populated {
    let mut types = HashMap::new();
    let mut devices = 0usize;
    let mut space = manager.address_space().write();
    let objects = NodeId::from(ObjectId::ObjectsFolder);
    let plc_node = NodeId::new(ns, format!("plc:{}", plc.id));
    ObjectBuilder::new(&plc_node, plc.id.clone(), plc.id.clone()).organized_by(objects).insert(&mut *space);
    let db_node = NodeId::new(ns, "db:main");
    ObjectBuilder::new(&db_node, "DB1", "DB1").organized_by(plc_node.clone()).insert(&mut *space);
    let mut device_nodes: HashMap<String, NodeId> = HashMap::new();
    for tag in plc.tags().iter().filter(|t| t.protocol == Protocol::OpcUa) {
        let parent = match &tag.device {
            Some(device) => {
                let key = format!("{}_{device}", tag.dev_type.as_deref().filter(|t| !t.is_empty()).unwrap_or("DEV"));
                device_nodes
                    .entry(key.clone())
                    .or_insert_with(|| {
                        let id = NodeId::new(ns, format!("dev:{key}"));
                        ObjectBuilder::new(&id, key.clone(), key.clone())
                            .organized_by(db_node.clone())
                            .insert(&mut *space);
                        devices += 1;
                        id
                    })
                    .clone()
            }
            None => db_node.clone(),
        };
        let id = NodeId::new(ns, tag.address.clone());
        let browse = tag.field.clone().unwrap_or_else(|| tag.name.clone());
        let display = if tag.unit.is_empty() { tag.name.clone() } else { format!("{} [{}]", tag.name, tag.unit) };
        let mut builder = VariableBuilder::new(&id, browse, display)
            .data_type(data_type_id(tag.data_type))
            .value(to_variant(&tag.value, tag.data_type))
            .organized_by(parent);
        if !tag.unit.is_empty() {
            builder = builder.description(format!("Unit: {}", tag.unit));
        }
        if tag.writable {
            builder = builder.writable();
        }
        builder.insert(&mut *space);
        types.insert(tag.address.clone(), tag.data_type);
    }
    // Как у фасада ptusa-opcua: имя контроллера и признак связи — по ним шлюз убеждается, что подключился к нужному ПЛК
    // (`identity` в controllers.yaml).
    let system = NodeId::new(ns, "SYSTEM");
    ObjectBuilder::new(&system, "SYSTEM", "SYSTEM").organized_by(plc_node.clone()).insert(&mut *space);
    VariableBuilder::new(&NodeId::new(ns, "SYSTEM.PAC_NAME"), "PAC_NAME", "PAC_NAME")
        .data_type(DataTypeId::String)
        .value(Variant::from(plc.name.as_str()))
        .organized_by(system.clone())
        .insert(&mut *space);
    VariableBuilder::new(&NodeId::new(ns, "SYSTEM.LINK_OK"), "LINK_OK", "LINK_OK")
        .data_type(DataTypeId::Boolean)
        .value(Variant::Boolean(true))
        .organized_by(system)
        .insert(&mut *space);
    Populated { variables: types.len(), types, devices }
}

/// Запись клиента — в состояние контроллера; узел обновит следующий шаг цикла.
fn add_write_callbacks(plc: &Arc<Plc>, manager: &SimpleNodeManager, ns: u16) {
    for tag in plc.tags().iter().filter(|t| t.protocol == Protocol::OpcUa && t.writable) {
        let (plc, address) = (plc.clone(), tag.address.clone());
        manager.inner().add_write_callback(NodeId::new(ns, tag.address.clone()), move |dv: DataValue, _range| {
            let Some(value) = dv.value.as_ref().and_then(from_variant) else { return StatusCode::BadTypeMismatch };
            match plc.opcua_write(&address, &value) {
                Ok(()) => StatusCode::Good,
                Err(WriteError::NotWritable) => StatusCode::BadNotWritable,
                Err(WriteError::TypeMismatch) => StatusCode::BadTypeMismatch,
                Err(WriteError::UnknownTag) => StatusCode::BadNodeIdUnknown,
            }
        });
    }
}

/// Собирает сервер с адресным пространством из тегов контроллера. Сервер надо запустить (`run`).
pub fn build(plc: &Arc<Plc>, endpoint: &str, user: Option<(&str, &str)>) -> Result<(Server, Pusher)> {
    let (host, port) = parse_endpoint(endpoint)?;
    let ns_uri = format!("http://{}", plc.id);
    let (server, handle) = server_builder(plc, host.clone(), port, user)
        .with_node_manager(simple_node_manager(
            NamespaceMetadata { namespace_uri: ns_uri.clone(), ..Default::default() },
            "sim",
        ))
        .build()
        .map_err(|e| anyhow::anyhow!("OPC UA-сервер: {e}"))?;
    let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().context("менеджер узлов")?;
    let ns = handle.get_namespace_index(&ns_uri).context("namespace")?;
    ensure!(ns == 2, "namespace должен быть 2, а не {ns}: адрес узлов в базе каналов — ns=2;s=<address>");

    let populated = populate(plc, &manager, ns);
    add_write_callbacks(plc, &manager, ns);
    info!("OPC UA: {} переменных в {} приборах, {host}:{port}", populated.variables, populated.devices);
    Ok((server, Pusher { handle, manager, ns, types: populated.types }))
}

impl Pusher {
    /// Положить в узлы изменившиеся значения.
    pub fn push(&self, changes: &[(String, Value)]) {
        if changes.is_empty() {
            return;
        }
        let batch: Vec<(NodeId, DataValue)> = changes
            .iter()
            .map(|(address, value)| {
                let t = self.types.get(address).copied().unwrap_or(DataType::Float);
                (NodeId::new(self.ns, address.clone()), DataValue::new_now(to_variant(value, t)))
            })
            .collect();
        let _ =
            self.manager.set_values(self.handle.subscriptions(), batch.iter().map(|(id, dv)| (id, None, dv.clone())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_is_split_into_host_and_port() {
        assert_eq!(parse_endpoint("opc.tcp://simulator:4840").unwrap(), ("simulator".into(), 4840));
        assert_eq!(parse_endpoint("opc.tcp://0.0.0.0:4900/x").unwrap(), ("0.0.0.0".into(), 4900));
        assert_eq!(parse_endpoint("opc.tcp://host").unwrap(), ("host".into(), 4840));
        assert!(parse_endpoint("http://x:1").is_err());
        assert!(parse_endpoint("opc.tcp://:4840").is_err());
    }

    #[test]
    fn variants_follow_the_channel_type() {
        assert!(matches!(to_variant(&Value::Float(1.5), DataType::Float), Variant::Float(f) if f == 1.5));
        assert!(matches!(to_variant(&Value::Int(7), DataType::Int), Variant::Int32(7)));
        assert!(matches!(to_variant(&Value::Bool(true), DataType::Bool), Variant::Boolean(true)));
        assert!(matches!(to_variant(&Value::Int(257), DataType::Byte), Variant::Byte(1)));
        assert!(matches!(to_variant(&Value::Text("a".into()), DataType::String), Variant::String(_)));
    }

    #[test]
    fn client_variants_become_values() {
        assert_eq!(from_variant(&Variant::Int32(5)), Some(Value::Int(5)));
        assert_eq!(from_variant(&Variant::Float(0.5)), Some(Value::Float(0.5)));
        assert_eq!(from_variant(&Variant::Boolean(true)), Some(Value::Bool(true)));
        assert_eq!(from_variant(&Variant::Empty), None);
    }
}
