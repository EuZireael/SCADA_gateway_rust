//! Настройки шлюза и конфигурация контроллеров.
//!
//! Настройки берутся из env с теми же именами, что у Java-шлюза (Spring relaxed binding):
//! `SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS_TELEMETRY`,
//! `GATEWAY_SEND_BAD_FRAMES`… — поэтому Rust-шлюз встаёт в те же docker-compose без правок.
//! Контроллеры и теги — тот же `controllers.yaml`, с подстановкой `${VAR:default}`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

/// Все настройки процесса.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Порт HTTP (`SERVER_PORT`): `/actuator/*` и `/api/*`.
    pub http_port: u16,
    /// Адрес привязки HTTP (`GATEWAY_HTTP_BIND`, по умолчанию все интерфейсы).
    pub http_bind: String,
    /// Токен доступа к `/api/*` (`GATEWAY_API_TOKEN` или файл `GATEWAY_API_TOKEN_FILE`); `None` — без проверки.
    /// `/actuator/health` и `/actuator/prometheus` токеном не закрываются (проверка контейнера, сбор метрик).
    pub api_token: Option<Secret>,
    /// Путь к файлу станции (`CONTROLLERS_YAML`).
    pub controllers_path: PathBuf,
    /// `None` — работа без БД (журнал и история не пишутся, команды только по имени тега).
    pub db: Option<DbSettings>,
    /// Kafka: брокеры, топики, безопасность.
    pub kafka: KafkaSettings,
    /// Поведение шлюза: опрос, публикация, команды, резервирование, скрипты.
    pub gateway: GatewaySettings,
}

/// Настройки Kafka (`KAFKA_*`, `SPRING_KAFKA_*`).
#[derive(Debug, Clone)]
pub struct KafkaSettings {
    /// `false` — без Kafka: опрос идёт, наружу ничего не уходит.
    pub enabled: bool,
    /// Адреса брокеров через запятую (`SPRING_KAFKA_BOOTSTRAP_SERVERS`).
    pub bootstrap_servers: String,
    /// Публиковать ли события журнала в топик событий.
    pub publish_events: bool,
    /// Публиковать ли алармы в топик алармов.
    pub publish_alarms: bool,
    /// Фактор репликации создаваемых шлюзом топиков (на стенде 1, на кластере — по числу брокеров).
    pub replication: i32,
    /// Свойства librdkafka для всех клиентов шлюза (безопасность: TLS, SASL) — см. [`client_properties`].
    pub client: ClientProperties,
    /// Имена топиков.
    pub topics: Topics,
}

impl KafkaSettings {
    /// Заготовка конфигурации клиента: брокеры и свойства безопасности. Остальное клиент задаёт сам.
    pub fn client_config(&self) -> rdkafka::ClientConfig {
        let mut c = rdkafka::ClientConfig::new();
        c.set("bootstrap.servers", &self.bootstrap_servers);
        for (k, v) in &self.client.0 {
            c.set(k, v);
        }
        c
    }
}

mod client_props;
mod db;
mod env;
mod station;

pub use client_props::{ClientProperties, client_properties};
pub use db::{DbSettings, DbSsl, JdbcUrl, parse_jdbc_url};
pub use env::Secret;
use env::{api_token, default_instance_id, env_any, env_bool, env_ms, env_or, env_parse};
pub use station::*;

/// Имена топиков Kafka.
#[derive(Debug, Clone)]
pub struct Topics {
    /// Телеметрия: ключ — имя тега, тело — `{value, quality, timestamp}`.
    pub telemetry: String,
    /// Алармы: аларм и его снятие (`cleared`).
    pub alarms: String,
    /// События журнала (подключения, команды, смена роли…).
    pub events: String,
    /// Команды записи от монитора (шлюз читает).
    pub commands: String,
    /// Результаты команд (шлюз пишет).
    pub command_results: String,
}

/// Флаги горячего пути (секция `gateway.*` Java-шлюза).
#[derive(Debug, Clone)]
pub struct GatewaySettings {
    /// Алармы по уставкам minValue/maxValue. По умолчанию их считает монитор.
    pub alarms_enabled: bool,
    /// Писать каждую точку в таблицу telemetry. По умолчанию выкл: ~15 ГБ/сут на станцию.
    pub persist_telemetry: bool,
    /// Сколько хранить историю telemetry (если она пишется).
    pub telemetry_retention: Duration,
    /// Кадр value=null/quality=BAD при обрыве. Монитор (runtime) к нему готов.
    pub send_bad_frames: bool,
    /// Отправка в Kafka по исключению (контракт: docs/TELEMETRY_BY_EXCEPTION_CONTRACT.md).
    pub publish: PublishSettings,
    /// Какие точки попадают в локальную историю (если она включена).
    pub history: HistorySettings,
    /// Команды старше — не исполняются (REJECTED_EXPIRED).
    pub command_max_age: Duration,
    /// Проверка эффекта записи OPC UA: через столько мс узел читается снова; 0 — выключено.
    pub command_verify: Duration,
    /// Пользовательские Lua-скрипты каналов.
    pub scripts: ScriptSettings,
    /// Горячее резервирование.
    pub ha: HaSettings,
    /// Таймаут одной операции OPC UA (подключение, пачка чтения, запись).
    pub opcua_op_timeout: Duration,
    /// Каталог сертификатов OPC UA-клиента (`own/`, `private/`, `trusted/`, `rejected/`): свой
    /// самоподписанный сертификат создаётся здесь, сертификаты серверов, которым шлюз доверяет, лежат в `trusted/`.
    pub opcua_pki_dir: PathBuf,
    /// Доверять любому сертификату сервера (только для стенда: без проверки сервера защищённый канал
    /// не защищает от подмены). По умолчанию — нет: сертификат сервера кладут в `trusted/`.
    pub opcua_trust_server_certs: bool,
    /// Таймаут одной операции Modbus.
    pub modbus_op_timeout: Duration,
    /// Таймаут одной операции PAC.
    pub pac_op_timeout: Duration,
    /// Нет удачных чтений дольше — связь считается мёртвой, сессия пересоздаётся.
    pub stale_after: Duration,
    /// Пауза между попытками подключения.
    pub reconnect_interval: Duration,
    /// Как часто писать в журнал сводку «кто на связи».
    pub health_log_interval: Duration,
    /// Как часто слать событие `HEARTBEAT` — признак «шлюз жив» для монитора.
    pub heartbeat_interval: Duration,
    /// Внеочередной опрос контроллера после удачной записи, через эту задержку (дать ПЛК применить
    /// команду). Без него записанное значение ждало бы обычного цикла — в среднем полпериода опроса,
    /// оператор видел результат нажатия через ~1 с вместо десятков мс. `None` — выключено.
    pub poll_after_write: Option<Duration>,
}

/// `gateway.publish.*`: значение уходит в Kafka при первом появлении, смене качества, изменении
/// за зону нечувствительности и раз в `full_resend` неизменившимся («полная отправка»).
#[derive(Debug, Clone)]
pub struct PublishSettings {
    /// false — прежнее поведение: каждый тег каждый цикл.
    pub enabled: bool,
    /// Абсолютная зона нечувствительности: меньшее изменение не публикуется.
    pub deadband: f64,
    /// Относительная зона, % от последнего опубликованного значения; действует большая из двух зон.
    pub deadband_percent: f64,
    /// Не чаще одной публикации тега за этот интервал (антидребезг); 0 — выключено.
    pub min_interval: Duration,
    /// Полная отправка: неизменившееся значение повторяется раз в этот интервал; 0 — выключена.
    pub full_resend: Duration,
}

/// `gateway.history.*`: фильтр локальной истории; тег переопределяет поля блоком `history:` в YAML.
#[derive(Debug, Clone)]
pub struct HistorySettings {
    /// Абсолютная зона нечувствительности истории.
    pub deadband: f64,
    /// Относительная зона истории, %.
    pub deadband_percent: f64,
    /// Не чаще одной точки истории за этот интервал.
    pub min_interval: Duration,
    /// «Пульс»: точка раз в столько, даже если значение стоит. 0 — выключен.
    pub max_interval: Duration,
}

/// `gateway.scripts.*`: пользовательские Lua-скрипты обработки значений.
#[derive(Debug, Clone)]
pub struct ScriptSettings {
    /// Папка со `scripts.yaml` и `.lua`-файлами.
    pub dir: PathBuf,
    /// Лимит времени на один вызов скрипта.
    pub timeout: Duration,
    /// Как часто проверять папку на изменения.
    pub reload_interval: Duration,
}

/// `gateway.ha.*`: горячее резервирование (выборы активного экземпляра через группу Kafka).
#[derive(Debug, Clone)]
pub struct HaSettings {
    /// Включено ли резервирование (`GATEWAY_HA_ENABLED`); без него экземпляр активен всегда.
    pub enabled: bool,
    /// Имя экземпляра в журнале и событиях (по умолчанию `<HOSTNAME>-<pid>`).
    pub instance_id: String,
    /// Служебный топик выборов (одна партиция).
    pub topic: String,
    /// Группа выборов Kafka; у пары одинаковая.
    pub group_id: String,
    /// Через сколько брокер считает экземпляр упавшим и отдаёт партнёру лидерство.
    pub session_timeout_ms: u32,
    /// Период heartbeat группы выборов.
    pub heartbeat_interval_ms: u32,
    /// Активный экземпляр без связи ни с одним контроллером столько времени передаёт лидерство
    /// партнёру (если тот в группе). 0 — не передавать.
    pub yield_after: Duration,
}

impl Settings {
    /// Настройки из окружения; значения по умолчанию — как в application.yaml Java-шлюза.
    pub fn from_env() -> Result<Self> {
        let db = if env_bool(&["DB_ENABLED"], true) { Some(DbSettings::from_env()?) } else { None };
        let kafka = KafkaSettings::from_env()?;
        let gateway = GatewaySettings::from_env(&kafka.topics.commands)?;
        Ok(Settings {
            http_port: env_parse(&["SERVER_PORT"], 8888)?,
            http_bind: env_or(&["GATEWAY_HTTP_BIND", "SERVER_ADDRESS"], "0.0.0.0"),
            api_token: api_token()?,
            // CONTROLLERS_CONFIG — имя и форма (`file:/путь`) из Java-шлюза: его compose работает без правок.
            controllers_path: PathBuf::from(
                env_any(&["CONTROLLERS_YAML", "CONTROLLERS_CONFIG"])
                    .map(|p| p.strip_prefix("file:").map(str::to_string).unwrap_or(p))
                    .unwrap_or_else(|| "config/controllers.yaml".into()),
            ),
            db,
            kafka,
            gateway,
        })
    }
}

impl KafkaSettings {
    /// Kafka из окружения; имена как у Java-шлюза.
    fn from_env() -> Result<Self> {
        Ok(KafkaSettings {
            enabled: env_bool(&["KAFKA_ENABLED"], true),
            bootstrap_servers: env_any(&["SPRING_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"])
                .unwrap_or_else(|| "localhost:9092".into()),
            publish_events: env_bool(&["KAFKA_PUBLISH_EVENTS"], true),
            publish_alarms: env_bool(&["KAFKA_PUBLISH_ALARMS"], true),
            replication: env_parse(&["KAFKA_TOPICS_REPLICATION"], 1)?,
            client: client_properties(std::env::vars()),
            topics: Topics {
                telemetry: env_or(&["KAFKA_TOPICS_TELEMETRY"], "scada.tags"),
                alarms: env_or(&["KAFKA_TOPICS_ALARMS"], "scada-alarms"),
                events: env_or(&["KAFKA_TOPICS_EVENTS"], "scada-events"),
                commands: env_or(&["KAFKA_TOPICS_COMMANDS"], "scada-commands"),
                command_results: env_or(
                    &["KAFKA_TOPICS_COMMAND_RESULTS", "KAFKA_TOPICS_COMMANDRESULTS"],
                    "scada-command-results",
                ),
            },
        })
    }
}

impl GatewaySettings {
    /// `commands_topic` — от него по умолчанию зависит группа выборов резервирования.
    fn from_env(commands_topic: &str) -> Result<Self> {
        Ok(GatewaySettings {
            alarms_enabled: env_bool(&["GATEWAY_ALARMS_ENABLED"], false),
            persist_telemetry: env_bool(&["GATEWAY_PERSIST_TELEMETRY", "GATEWAY_PERSISTTELEMETRY"], false),
            telemetry_retention: Duration::from_secs(
                env_parse::<u64>(&["GATEWAY_TELEMETRY_RETENTION_HOURS"], 72)? * 3600,
            ),
            send_bad_frames: env_bool(&["GATEWAY_SEND_BAD_FRAMES", "GATEWAY_SENDBADFRAMES"], true),
            publish: PublishSettings::from_env()?,
            history: HistorySettings::from_env()?,
            command_max_age: env_ms(&["GATEWAY_COMMANDS_MAX_AGE_MS", "GATEWAY_COMMANDS_MAXAGEMS"], 30_000)?,
            command_verify: env_ms(&["GATEWAY_COMMANDS_VERIFY_MS"], 0)?,
            scripts: ScriptSettings::from_env()?,
            ha: HaSettings::from_env(commands_topic)?,
            opcua_op_timeout: env_ms(&["GATEWAY_OPCUA_OP_TIMEOUT_MS", "GATEWAY_OPCUAOPTIMEOUTMS"], 5000)?,
            opcua_pki_dir: env_any(&["GATEWAY_OPCUA_PKI_DIR"])
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("scada-gateway-pki")),
            opcua_trust_server_certs: env_bool(&["GATEWAY_OPCUA_TRUST_SERVER_CERTS"], false),
            modbus_op_timeout: env_ms(&["GATEWAY_MODBUS_OP_TIMEOUT_MS", "GATEWAY_MODBUSOPTIMEOUTMS"], 3000)?,
            pac_op_timeout: env_ms(&["GATEWAY_PAC_OP_TIMEOUT_MS", "GATEWAY_PACOPTIMEOUTMS"], 3000)?,
            stale_after: env_ms(&["GATEWAY_STALE_AFTER_MS"], 30_000)?,
            reconnect_interval: env_ms(&["GATEWAY_SUPERVISE_INTERVAL_MS", "GATEWAY_SUPERVISEINTERVALMS"], 10_000)?,
            health_log_interval: env_ms(&["GATEWAY_HEALTH_LOG_INTERVAL_MS", "GATEWAY_HEALTHLOGINTERVALMS"], 60_000)?,
            heartbeat_interval: env_ms(&["GATEWAY_HEARTBEAT_INTERVAL_MS", "GATEWAY_HEARTBEATINTERVALMS"], 30_000)?,
            poll_after_write: env_bool(&["GATEWAY_POLL_AFTER_WRITE_ENABLED"], true)
                .then(|| env_ms(&["GATEWAY_POLL_AFTER_WRITE_DELAY_MS"], 50))
                .transpose()?,
        })
    }
}

impl PublishSettings {
    /// Публикация «по исключению» из окружения (`GATEWAY_PUBLISH_*`).
    fn from_env() -> Result<Self> {
        Ok(PublishSettings {
            enabled: env_bool(&["GATEWAY_PUBLISH_ENABLED"], true),
            deadband: env_parse(&["GATEWAY_PUBLISH_DEADBAND"], 0.0)?,
            deadband_percent: env_parse(&["GATEWAY_PUBLISH_DEADBAND_PERCENT", "GATEWAY_PUBLISH_DEADBANDPERCENT"], 0.0)?,
            min_interval: env_ms(&["GATEWAY_PUBLISH_MIN_INTERVAL_MS", "GATEWAY_PUBLISH_MININTERVALMS"], 0)?,
            full_resend: env_ms(&["GATEWAY_PUBLISH_FULL_RESEND_MS", "GATEWAY_PUBLISH_FULLRESENDMS"], 30_000)?,
        })
    }
}

impl HistorySettings {
    /// Фильтр истории из окружения (`GATEWAY_HISTORY_*`).
    fn from_env() -> Result<Self> {
        Ok(HistorySettings {
            deadband: env_parse(&["GATEWAY_HISTORY_DEADBAND"], 0.0)?,
            deadband_percent: env_parse(&["GATEWAY_HISTORY_DEADBAND_PERCENT", "GATEWAY_HISTORY_DEADBANDPERCENT"], 0.0)?,
            min_interval: env_ms(&["GATEWAY_HISTORY_MIN_INTERVAL_MS", "GATEWAY_HISTORY_MININTERVALMS"], 0)?,
            max_interval: env_ms(&["GATEWAY_HISTORY_MAX_INTERVAL_MS", "GATEWAY_HISTORY_MAXINTERVALMS"], 600_000)?,
        })
    }
}

impl ScriptSettings {
    /// Скрипты из окружения (`GATEWAY_SCRIPTS_*`).
    fn from_env() -> Result<Self> {
        Ok(ScriptSettings {
            dir: PathBuf::from(env_or(&["GATEWAY_SCRIPTS_DIR"], "scripts")),
            timeout: env_ms(&["GATEWAY_SCRIPTS_TIMEOUT_MS", "GATEWAY_SCRIPTS_TIMEOUTMS"], 50)?,
            reload_interval: env_ms(&["GATEWAY_SCRIPTS_RELOAD_INTERVAL_MS", "GATEWAY_SCRIPTS_RELOADINTERVALMS"], 5000)?,
        })
    }
}

impl HaSettings {
    /// Резервирование из окружения (`GATEWAY_HA_*`); группа выборов по умолчанию зависит от топика команд.
    fn from_env(commands_topic: &str) -> Result<Self> {
        Ok(HaSettings {
            enabled: env_bool(&["GATEWAY_HA_ENABLED"], false),
            instance_id: env_any(&["GATEWAY_HA_INSTANCE_ID", "GATEWAY_HA_INSTANCEID"])
                .unwrap_or_else(default_instance_id),
            topic: env_or(&["GATEWAY_HA_TOPIC"], "scada-gateway-ha"),
            group_id: env_any(&["GATEWAY_HA_GROUP_ID", "GATEWAY_HA_GROUPID"])
                .unwrap_or_else(|| format!("scada-gateway-ha.{commands_topic}")),
            session_timeout_ms: env_parse(&["GATEWAY_HA_SESSION_TIMEOUT_MS", "GATEWAY_HA_SESSIONTIMEOUTMS"], 6000)?,
            heartbeat_interval_ms: env_parse(
                &["GATEWAY_HA_HEARTBEAT_INTERVAL_MS", "GATEWAY_HA_HEARTBEATINTERVALMS"],
                1000,
            )?,
            yield_after: env_ms(&["GATEWAY_HA_YIELD_AFTER_MS", "GATEWAY_HA_YIELDAFTERMS"], 30_000)?,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn kafka_client_properties_from_named_and_generic_vars() {
        let vars =
            |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
        let p = super::client_properties(vars(&[
            ("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            ("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            ("KAFKA_SASL_USERNAME", "gw"),
            ("KAFKA_SASL_PASSWORD", "p@ss"),
            ("KAFKA_SSL_CA_LOCATION", "/certs/ca.pem"),
            ("KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "none"),
            ("KAFKA_CLIENT_SASL_USERNAME", "override"),
            ("KAFKA_TOPICS_TELEMETRY", "scada.tags"),
            ("KAFKA_SSL_KEY_PASSWORD", ""),
        ]));
        let get = |k: &str| p.0.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(get("sasl.username"), Some("override"), "KAFKA_CLIENT_* перекрывает именованную");
        assert_eq!(get("sasl.password"), Some("p@ss"));
        assert_eq!(get("ssl.endpoint.identification.algorithm"), Some("none"));
        assert_eq!(get("ssl.key.password"), None, "пустое значение — не задано");
        assert_eq!(get("topics.telemetry"), None, "чужие KAFKA_* не попадают в клиент");
        let shown = format!("{p:?}");
        assert!(!shown.contains("p@ss"), "пароль не должен печататься: {shown}");
        assert!(!p.summary().contains("p@ss"));
        assert!(p.summary().contains("security.protocol=SASL_SSL"));
    }

    #[test]
    fn kafka_client_properties_from_spring_jaas() {
        let p = super::client_properties([
            ("SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL".to_string(), "SASL_PLAINTEXT".to_string()),
            ("SPRING_KAFKA_PROPERTIES_SASL_MECHANISM".to_string(), "PLAIN".to_string()),
            (
                "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG".to_string(),
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"s3\";"
                    .to_string(),
            ),
        ]);
        let get = |k: &str| p.0.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(get("sasl.username"), Some("alice"));
        assert_eq!(get("sasl.password"), Some("s3"));
    }

    #[test]
    fn no_security_vars_means_no_properties() {
        assert!(super::client_properties([("KAFKA_ENABLED".to_string(), "true".to_string())]).0.is_empty());
    }

    use super::*;

    #[test]
    fn placeholders_use_env_or_default() {
        let env = |k: &str| (k == "SIM_HOST").then(|| "simulator".to_string());
        assert_eq!(
            expand_placeholders("opc.tcp://${SIM_HOST:127.0.0.1}:4840 ${NOPE:x} [${EMPTY:}]", env).unwrap(),
            "opc.tcp://simulator:4840 x []"
        );
    }

    #[test]
    fn required_placeholder_without_value_is_an_error() {
        let err = expand_placeholders("opc.tcp://${PLC_HOST}:${PLC_PORT}", |_| None).unwrap_err().to_string();
        assert!(err.contains("PLC_HOST") && err.contains("PLC_PORT"), "{err}");
        assert_eq!(expand_placeholders("${PLC_HOST}", |_| Some("10.0.0.1".into())).unwrap(), "10.0.0.1");
    }

    #[test]
    fn station_validation_rejects_duplicates_and_unknown_protocols() {
        // Тег под протокол контроллера: адрес и поля, которых требует его протокол.
        let tag = |name: &str, endpoint: &str| match endpoint.split("://").next().unwrap() {
            "modbus" => format!(
                "{{name: \"{name}\", nodeId: \"m:{name}\", protocol: modbus, modbusAddress: 40001, dataType: INT32, enabled: true}}"
            ),
            "pac" => format!(
                "{{name: \"{name}\", nodeId: \"p:{name}\", protocol: pac, deviceName: LINE1V0, fieldName: ST, dataType: INT32, enabled: true}}"
            ),
            _ => format!("{{name: \"{name}\", nodeId: \"ns=2;s={name}\", dataType: INT32, enabled: true}}"),
        };
        let server = |name: &str, endpoint: &str, tags: &[&str]| {
            format!(
                "    - {{name: \"{name}\", endpoint: \"{endpoint}\", enabled: true, tags: [{}]}}\n",
                tags.iter().map(|t| tag(t, endpoint)).collect::<Vec<_>>().join(", ")
            )
        };
        let yaml = |servers: String| format!("opcua:\n  servers:\n{servers}");
        assert!(
            parse_controllers(&yaml(
                server("A", "opc.tcp://h:1", &["x", "y"]) + &server("B", "modbus://h:502", &["z"])
            ))
            .is_ok()
        );
        let dup_in = parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x", "x"]))).unwrap_err();
        assert!(format!("{dup_in:#}").contains("повторяются 1: x"), "{dup_in:#}");
        let dup_across =
            parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x"]) + &server("B", "pac://h:2", &["x"])))
                .unwrap_err();
        assert!(format!("{dup_across:#}").contains("(A и B)"), "{dup_across:#}");
        let same_name =
            parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x"]) + &server("A", "opc.tcp://h:2", &["y"])))
                .unwrap_err();
        assert!(format!("{same_name:#}").contains("описан дважды"), "{same_name:#}");
        let proto = parse_controllers(&yaml(server("A", "http://h:1", &["x"]))).unwrap_err();
        assert!(format!("{proto:#}").contains("неизвестный протокол"), "{proto:#}");
        // Выключенное в расчёт не идёт: дубль имени в выключенном контроллере и выключенный тег не мешают.
        let off = "    - {name: \"A\", endpoint: \"opc.tcp://h:1\", enabled: true, tags: [{name: \"x\", nodeId: \"n\", dataType: INT32, enabled: true}, {name: \"x\", nodeId: \"n\", dataType: INT32, enabled: false}]}\n    - {name: \"B\", endpoint: \"junk\", enabled: false, tags: [{name: \"x\", nodeId: \"n\", dataType: INT32, enabled: true}]}\n";
        assert!(parse_controllers(&yaml(off.to_string())).is_ok());
    }

    /// Конфигурации из репозитория проходят проверку целиком (имена уникальны, протоколы известны).
    #[test]
    fn repository_configs_pass_station_validation() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for (file, tags) in [
            ("config/controllers.yaml", 2517usize),
            ("config/stations/BN1_MCA1.yaml", 1834),
            ("config/stations/BN1_MCA1_easydriver.yaml", 1834),
        ] {
            let raw = std::fs::read_to_string(root.join(file)).unwrap();
            let yaml = expand_placeholders(&raw, |_| Some("host".to_string())).unwrap();
            let servers = parse_controllers(&yaml).unwrap_or_else(|e| panic!("{file}: {e:#}"));
            let enabled: usize =
                servers.iter().filter(|s| s.enabled).flat_map(|s| &s.tags).filter(|t| t.enabled).count();
            assert_eq!(enabled, tags, "{file}");
        }
    }

    /// Станция «EasyDriver» (PAC) — та же станция, что для OPC UA: те же каналы, имена, типы, доступ на запись и
    /// выключенные каналы; различаются только протокол и адрес. Иначе сравнение двух режимов было бы нечестным.
    #[test]
    fn easydriver_station_has_the_same_channels_as_the_opcua_one() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let load = |file: &str| {
            let raw = std::fs::read_to_string(root.join(file)).unwrap();
            let yaml = expand_placeholders(&raw, |_| Some("host".to_string())).unwrap();
            let mut servers = parse_controllers(&yaml).unwrap();
            assert_eq!(servers.len(), 1, "{file}: один контроллер");
            servers.remove(0)
        };
        let (opc, pac) = (load("config/stations/BN1_MCA1.yaml"), load("config/stations/BN1_MCA1_easydriver.yaml"));
        assert!(opc.endpoint.starts_with("opc.tcp://") && pac.endpoint.starts_with("pac://"));
        assert_eq!((opc.name.as_str(), opc.enabled), (pac.name.as_str(), pac.enabled));
        assert_eq!(opc.tags.len(), pac.tags.len());
        // Поле PAC — адрес в ПЛК: пробелы в индексе (`ST_CH[ 1 ]`) убраны, в имени канала они остаются.
        let no_spaces = |f: &Option<String>| f.as_deref().map(|f| f.replace(' ', ""));
        for (a, b) in opc.tags.iter().zip(&pac.tags) {
            assert_eq!(
                (
                    &a.name,
                    &a.device_name,
                    no_spaces(&a.field_name),
                    &a.device_type,
                    &a.data_type,
                    a.enabled,
                    a.writable,
                    a.polling_rate
                ),
                (
                    &b.name,
                    &b.device_name,
                    no_spaces(&b.field_name),
                    &b.device_type,
                    &b.data_type,
                    b.enabled,
                    b.writable,
                    b.polling_rate
                ),
                "{}",
                a.name
            );
            assert_eq!(b.field_name.as_deref(), no_spaces(&b.field_name).as_deref(), "{}: пробел в поле PAC", b.name);
            assert_eq!(a.protocol.as_deref(), Some("opcua"));
            assert_eq!(b.protocol.as_deref(), Some("pac"));
        }
    }

    #[test]
    fn opcua_security_and_credentials_are_parsed_and_validated() {
        let yaml = |extra: &str, endpoint: &str| {
            format!(
                "opcua:\n  servers:\n    - {{name: C, endpoint: \"{endpoint}\", enabled: true, {extra}, tags: []}}\n"
            )
        };
        let ok =
            parse_controllers(&yaml("security: Basic256Sha256_Sign, username: op, password: pw", "opc.tcp://h:4840"))
                .unwrap();
        let sec = ok[0].opc_security().unwrap();
        assert_eq!((sec.policy, sec.mode), (crate::model::OpcPolicy::Basic256Sha256, crate::model::OpcMode::Sign));
        assert_eq!(sec.username.as_deref(), Some("op"));

        // Молча проигнорированная защита — дыра: ошибка в security и логин без пароля останавливают запуск.
        let err = parse_controllers(&yaml("security: Basic999", "opc.tcp://h:4840")).unwrap_err();
        assert!(format!("{err:#}").contains("Basic999"), "{err:#}");
        assert!(parse_controllers(&yaml("username: op", "opc.tcp://h:4840")).is_err(), "логин без пароля");
        // Modbus и PAC защиты на уровне протокола не имеют: настройка не должна создавать иллюзию защиты.
        for endpoint in ["modbus://h:502", "pac://h:10000"] {
            let err = parse_controllers(&yaml("security: Basic256Sha256", endpoint)).unwrap_err();
            assert!(format!("{err:#}").contains("только для OPC UA"), "{err:#}");
            assert!(parse_controllers(&yaml("username: u, password: p", endpoint)).is_err());
        }
        // Выключенный контроллер с ошибкой не мешает запуску остальных.
        let disabled = "opcua:\n  servers:\n    - {name: D, endpoint: \"opc.tcp://h:4840\", enabled: false, security: Nonsense, tags: []}\n";
        assert!(parse_controllers(disabled).is_ok());
    }

    #[test]
    fn tag_history_override_is_parsed() {
        let yaml = r#"
opcua:
  servers:
    - name: c
      endpoint: "opc.tcp://h:4840"
      enabled: true
      tags:
        - {name: a, nodeId: "ns=2;s=1", dataType: FLOAT, pollingRate: 1000, enabled: true, history: {deadband: 0.5, maxIntervalMs: 60000}}
        - {name: b, nodeId: "ns=2;s=2", dataType: FLOAT, pollingRate: 1000, enabled: true}
"#;
        let servers = parse_controllers(yaml).unwrap();
        assert_eq!(
            servers[0].tags[0].history,
            Some(HistoryConfig { deadband: Some(0.5), max_interval_ms: Some(60000), ..Default::default() })
        );
        assert_eq!(servers[0].tags[1].history, None);
    }

    /// Одна строка YAML с серверами → ошибка проверки (текст) или `None`, если принято.
    fn rejected(servers: &str) -> Option<String> {
        parse_controllers(&format!("opcua:\n  servers:\n{servers}")).err().map(|e| format!("{e:#}"))
    }

    #[test]
    fn tags_that_would_silently_not_work_are_rejected_at_start() {
        let server = |endpoint: &str, tags: &str| {
            format!("    - {{name: C, endpoint: \"{endpoint}\", enabled: true, tags: [{tags}]}}\n")
        };
        let pac = |device: &str, field: &str| {
            format!(
                "{{name: t, nodeId: n, protocol: pac, deviceName: \"{device}\", fieldName: \"{field}\", dataType: INT32, enabled: true}}"
            )
        };
        let modbus = |name: &str, addr: i64, unit: u32| {
            format!(
                "{{name: {name}, nodeId: m{addr}, protocol: modbus, modbusAddress: {addr}, modbusUnitId: {unit}, dataType: INT32, enabled: true}}"
            )
        };
        // PAC: адрес поля попадает в Lua-текст команды — кавычки и пробелы недопустимы, а без device/field тег всегда BAD.
        assert_eq!(rejected(&server("pac://h:1", &pac("LINE1V0", "RT_PAR_F[12]"))), None);
        for (device, field) in [("LINE1V0", "ST', 1, 0); os.exit() --"), ("LINE 1", "ST"), ("", "ST")] {
            let err = rejected(&server("pac://h:1", &pac(device, field))).expect("отклонён");
            assert!(err.contains("недопустимые символы") || err.contains("не задан"), "{device:?} {field:?}: {err}");
        }
        // Протокол тега совпадает с контроллером: чужой тег никто не опрашивал бы.
        let err = rejected(&server("opc.tcp://h:1", &modbus("m", 40001, 1))).expect("отклонён");
        assert!(err.contains("не совпадает с протоколом контроллера"), "{err}");
        // Два тега с одним адресом получили бы общий id в БД и общую историю.
        let two = format!("{}, {}", modbus("a", 40001, 1), modbus("b", 40001, 1));
        let err = rejected(&server("modbus://h:502", &two)).expect("отклонён");
        assert!(err.contains("уже занят другим тегом"), "{err}");
        // Modbus: адрес за пределами 16 бит молча обрезался бы до чужого регистра; unit id один на контроллер.
        assert_eq!(rejected(&server("modbus://h:502", &modbus("a", 105536, 1))), None);
        for addr in [140001, 40000] {
            let err = rejected(&server("modbus://h:502", &modbus("a", addr, 1))).expect("отклонён");
            assert!(err.contains("вне диапазона"), "{addr}: {err}");
        }
        let units = format!("{}, {}", modbus("a", 40001, 1), modbus("b", 40002, 2));
        let err = rejected(&server("modbus://h:502", &units)).expect("отклонён");
        assert!(err.contains("разные modbusUnitId"), "{err}");
    }

    /// Примеры `controllers.yaml` в документации не устаревают: каждый блок ```yaml, начинающийся с `opcua:`, проходит
    /// ту же подстановку и проверку, что и настоящий файл станции.
    #[test]
    fn controllers_examples_in_the_docs_are_valid() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut checked = 0;
        for doc in ["README.md", "docs/CONFIGURATION.md"] {
            let text = std::fs::read_to_string(root.join(doc)).unwrap();
            for block in text.split("```yaml\n").skip(1) {
                let yaml = block.split("```").next().unwrap();
                if !yaml.starts_with("opcua:") {
                    continue;
                }
                let expanded = expand_placeholders(yaml, |name| Some(format!("{name}-value"))).unwrap();
                parse_controllers(&expanded)
                    .unwrap_or_else(|e| panic!("{doc}: пример не проходит проверку: {e:#}\n{yaml}"));
                checked += 1;
            }
        }
        assert!(checked >= 2, "примеры controllers.yaml в документации не найдены");
    }

    #[test]
    fn endpoint_kind_follows_the_scheme_not_the_host_name() {
        use crate::model::ControllerKind;
        assert_eq!(ControllerKind::from_endpoint("pac://modbus-gw:502"), Some(ControllerKind::Pac));
        assert_eq!(ControllerKind::from_endpoint("opc.tcp://modbus-bridge:4840"), Some(ControllerKind::OpcUa));
        assert_eq!(ControllerKind::from_endpoint("MODBUS://h"), Some(ControllerKind::Modbus));
        assert_eq!(ControllerKind::from_endpoint("http://opc.tcp"), None);
    }

    #[test]
    fn controllers_yaml_flow_mapping() {
        let yaml = r#"
opcua:
  servers:
    - id: pac-demo-001
      name: "PAC Savushkin"
      endpoint: "pac://127.0.0.1:10000"
      enabled: true
      tags:
        - {name: "A.B.LINE1V0.ST", nodeId: "pac:385", channelId: 385, deviceName: "LINE1V0", fieldName: "ST", deviceType: "V", protocol: pac, dataType: INT32, pollingRate: 2000, enabled: true, writable: true}
"#;
        let servers = parse_controllers(yaml).unwrap();
        assert_eq!(servers.len(), 1);
        let tag = &servers[0].tags[0];
        assert_eq!(tag.channel_id, Some(385));
        assert_eq!(tag.protocol.as_deref(), Some("pac"));
        assert!(tag.writable && tag.enabled);
        assert_eq!(tag.polling_rate, 2000);
    }
}
