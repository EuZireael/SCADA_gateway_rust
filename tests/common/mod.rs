//! Общее для интеграционных тестов: где симулятор, какой controllers.yaml, проверка типов.
//!
//! Окружение (всё с умолчаниями под стенд репозитория `docker compose up -d`):
//!   SIM_HOST          — хост симулятора (127.0.0.1)
//!   CONTROLLERS_YAML  — конфиг шлюза (config/controllers.yaml репозитория)
//!   KAFKA_BOOTSTRAP   — брокер для сквозных тестов (127.0.0.1:9094 — внешний listener стенда)
//!   IT_DATABASE_URL   — если задан (jdbc:postgresql://…), сквозной тест гоняет шлюз с БД
#![allow(dead_code)]

pub mod gateway;
pub mod proxy;

use std::path::PathBuf;
use std::time::Duration;

use scada_gateway::config;
use scada_gateway::model::{self, Controller, ControllerKind, TagValue};

pub const OP_TIMEOUT: Duration = Duration::from_secs(5);

/// Хост симулятора (`SIM_HOST`).
pub fn sim_host() -> String {
    std::env::var("SIM_HOST").unwrap_or_else(|_| "127.0.0.1".into())
}

/// Брокер для тестов (`KAFKA_BOOTSTRAP`).
pub fn kafka_bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9094".into())
}

/// Файл станции для тестов (`CONTROLLERS_YAML`).
pub fn controllers_path() -> PathBuf {
    std::env::var("CONTROLLERS_YAML")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/controllers.yaml"))
}

/// Контроллеры из YAML с endpoint'ами на симулятор (только включённые теги).
pub fn controllers() -> Vec<Controller> {
    let path = controllers_path();
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let host = sim_host();
    let yaml =
        config::expand_placeholders(&raw, |k| if k == "SIM_HOST" { Some(host.clone()) } else { std::env::var(k).ok() })
            .expect("плейсхолдеры controllers.yaml");
    config::parse_controllers(&yaml)
        .expect("controllers.yaml")
        .iter()
        .filter(|s| s.enabled)
        .filter_map(Controller::from_config)
        .collect()
}

/// Первый контроллер заданного протокола из станции.
pub fn controller(kind: ControllerKind) -> Controller {
    controllers().into_iter().find(|c| c.kind == kind).unwrap_or_else(|| panic!("в конфиге нет контроллера {kind:?}"))
}

/// Значение соответствует объявленному типу тега.
pub fn type_matches(data_type: &str, value: &TagValue) -> bool {
    if model::is_int(data_type) {
        matches!(value, TagValue::Int(_))
    } else if model::is_float(data_type) {
        matches!(value, TagValue::F32(_) | TagValue::F64(_))
    } else if model::is_string(data_type) {
        matches!(value, TagValue::Text(_))
    } else if model::is_bool(data_type) {
        matches!(value, TagValue::Bool(_))
    } else {
        true
    }
}

/// То же для JSON-значения с провода Kafka.
pub fn json_type_matches(data_type: &str, value: &serde_json::Value) -> bool {
    if model::is_int(data_type) {
        value.is_i64()
    } else if model::is_float(data_type) {
        value.is_number()
    } else if model::is_string(data_type) {
        value.is_string()
    } else if model::is_bool(data_type) {
        value.is_boolean()
    } else {
        true
    }
}

/// Противоположное значение для дискретной команды (0 ↔ 1).
pub fn toggled(v: &TagValue) -> TagValue {
    match v {
        TagValue::Int(0) => TagValue::Int(1),
        TagValue::Int(_) => TagValue::Int(0),
        TagValue::F64(f) if *f == 0.0 => TagValue::F64(1.0),
        TagValue::F64(_) => TagValue::F64(0.0),
        other => panic!("не дискретное значение {other:?}"),
    }
}
