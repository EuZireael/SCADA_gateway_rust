//! PAC (протокол driver-master, Savushkin/ptusa): протокол, Lua-снимок, соединение.

pub mod alarms;
pub mod connection;
pub mod lua;
pub mod protocol;

pub use connection::{PacConnection, connection_intact, link_lost};

/// Хост и порт из `pac://host:port` (порт по умолчанию 10000).
pub fn endpoint(endpoint: &str) -> (String, u16) {
    crate::modbus::split_host_port(endpoint.trim_start_matches("pac://"), 10000)
}
