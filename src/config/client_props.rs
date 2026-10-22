//! Свойства безопасности клиентов Kafka (TLS, SASL) из переменных окружения.

/// Дополнительные свойства librdkafka (`security.protocol`, `sasl.*`, `ssl.*`…). В `Debug` значения
/// паролей и ключей скрыты, чтобы они не попали в журнал.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ClientProperties(pub Vec<(String, String)>);

impl std::fmt::Debug for ClientProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.0.iter().map(|(k, v)| (k, if is_secret(k) { "***" } else { v.as_str() }))).finish()
    }
}

/// Свойства, которые шлюз задаёт сам и не отдаёт `KAFKA_CLIENT_*`: от них зависят порядок и однозначность доставки (идемпотентный
/// продюсер), позиции коммитов и состав группы выборов резерва. Значение из окружения для них игнорируется — об этом
/// предупреждение при запуске.
pub const FIXED_CLIENT_PROPERTIES: &[&str] = &[
    "enable.idempotence",
    "group.id",
    "enable.auto.commit",
    "auto.offset.reset",
    "allow.auto.create.topics",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "max.poll.interval.ms",
    "partition.assignment.strategy",
];

impl ClientProperties {
    /// Заданные в окружении свойства из [`FIXED_CLIENT_PROPERTIES`] — шлюз их значения не применит.
    pub fn ignored(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k.as_str()).filter(|k| FIXED_CLIENT_PROPERTIES.contains(k)).collect()
    }

    /// Сводка для журнала: свойства без секретов.
    pub fn summary(&self) -> String {
        self.0.iter().filter(|(k, _)| !is_secret(k)).map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(", ")
    }
}

/// Содержит ли имя свойства секрет: его значение скрывается в `Debug` и журнале.
fn is_secret(key: &str) -> bool {
    key.contains("password") || key.contains("secret") || key.contains("key.pem")
}

/// Свойства Kafka-клиентов из окружения.
///
/// Именованные: `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`,
/// `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_CA_LOCATION`, `KAFKA_SSL_CERTIFICATE_LOCATION`,
/// `KAFKA_SSL_KEY_LOCATION`, `KAFKA_SSL_KEY_PASSWORD`. Любое другое свойство librdkafka —
/// `KAFKA_CLIENT_<ИМЯ>` (`KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=none` →
/// `ssl.endpoint.identification.algorithm=none`); оно перекрывает именованное. Для совместимости с
/// Spring понимаются `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL`, `…_SASL_MECHANISM` и
/// `…_SASL_JAAS_CONFIG` (имя и пароль берутся из `username="…" password="…"`).
pub fn client_properties(vars: impl IntoIterator<Item = (String, String)>) -> ClientProperties {
    let vars: Vec<(String, String)> = vars.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    let get = |name: &str| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    let mut props: Vec<(String, String)> = Vec::new();
    let mut set = |key: &str, value: String| {
        props.retain(|(k, _)| k != key);
        props.push((key.to_string(), value));
    };
    for (key, names) in [
        ("security.protocol", &["KAFKA_SECURITY_PROTOCOL", "SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL"][..]),
        ("sasl.mechanism", &["KAFKA_SASL_MECHANISM", "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM"][..]),
        ("sasl.username", &["KAFKA_SASL_USERNAME"][..]),
        ("sasl.password", &["KAFKA_SASL_PASSWORD"][..]),
        ("ssl.ca.location", &["KAFKA_SSL_CA_LOCATION"][..]),
        ("ssl.certificate.location", &["KAFKA_SSL_CERTIFICATE_LOCATION"][..]),
        ("ssl.key.location", &["KAFKA_SSL_KEY_LOCATION"][..]),
        ("ssl.key.password", &["KAFKA_SSL_KEY_PASSWORD"][..]),
    ] {
        if let Some(v) = names.iter().find_map(|n| get(n)) {
            set(key, v.trim().to_string());
        }
    }
    if let Some(jaas) = get("SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG") {
        for (key, field) in [("sasl.username", "username"), ("sasl.password", "password")] {
            if let Some(v) = jaas_field(&jaas, field) {
                set(key, v);
            }
        }
    }
    for (name, value) in &vars {
        if let Some(rest) = name.strip_prefix("KAFKA_CLIENT_") {
            set(&rest.to_ascii_lowercase().replace('_', "."), value.trim().to_string());
        }
    }
    ClientProperties(props)
}

/// `username="alice" password="s3"` → значение поля в кавычках (или без них, до пробела/`;`).
fn jaas_field(jaas: &str, field: &str) -> Option<String> {
    let rest = &jaas[jaas.find(&format!("{field}="))? + field.len() + 1..];
    Some(match rest.strip_prefix('"') {
        Some(quoted) => quoted[..quoted.find('"')?].to_string(),
        None => rest.split(|c: char| c.is_whitespace() || c == ';').next()?.to_string(),
    })
}
