//! Собранный шлюз как отдельный процесс + Kafka-хелперы для сквозных тестов.
//! Топики — уникальные `it-<id>.*`, чтобы не мешать стенду и другим тестам.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::Value;

/// Шлюз-процесс; убивается при выходе из теста (в том числе по панике).
pub struct Gateway {
    pub child: Child,
    pub port: u16,
    prefix: String,
    log: PathBuf,
}

impl Gateway {
    /// Шлюз на controllers.yaml тестов (см. [`super::controllers_path`]).
    pub fn start() -> Self {
        Self::start_with(&super::controllers_path(), &[])
    }

    /// Шлюз на своём YAML и с дополнительными переменными окружения.
    pub fn start_with(controllers_yaml: &Path, env: &[(&str, &str)]) -> Self {
        Self::start_on(&new_prefix(), controllers_yaml, env)
    }

    /// Шлюз на топиках с заданным префиксом: два экземпляра пары резервирования делят топики.
    pub fn start_on(prefix: &str, controllers_yaml: &Path, env: &[(&str, &str)]) -> Self {
        let prefix = prefix.to_string();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let log = std::env::temp_dir().join(format!("scada-gateway-{prefix}-{port}.log"));
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_scada-gateway"));
        cmd.env("CONTROLLERS_YAML", controllers_yaml)
            .env("SIM_HOST", super::sim_host())
            .env("SPRING_KAFKA_BOOTSTRAP_SERVERS", super::kafka_bootstrap())
            .env("SERVER_PORT", port.to_string())
            .env("GATEWAY_HEARTBEAT_INTERVAL_MS", "2000");
        for (var, suffix) in [
            ("KAFKA_TOPICS_TELEMETRY", "tags"),
            ("KAFKA_TOPICS_COMMANDS", "commands"),
            ("KAFKA_TOPICS_COMMAND_RESULTS", "results"),
            ("KAFKA_TOPICS_EVENTS", "events"),
            ("KAFKA_TOPICS_ALARMS", "alarms"),
            ("KAFKA_TOPICS_PAC_ALARMS", "pac-alarms"),
        ] {
            cmd.env(var, format!("{prefix}.{suffix}"));
        }
        match std::env::var("IT_DATABASE_URL") {
            Ok(url) => {
                cmd.env("SPRING_DATASOURCE_URL", url)
                    .env(
                        "SPRING_DATASOURCE_USERNAME",
                        std::env::var("IT_DATABASE_USERNAME").unwrap_or("scada_user".into()),
                    )
                    .env(
                        "SPRING_DATASOURCE_PASSWORD",
                        std::env::var("IT_DATABASE_PASSWORD").unwrap_or("scada_password".into()),
                    );
            }
            Err(_) => {
                cmd.env("DB_ENABLED", "false");
            }
        }
        cmd.envs(env.iter().copied());
        let out = std::fs::File::create(&log).unwrap();
        let child =
            cmd.stdout(Stdio::from(out.try_clone().unwrap())).stderr(Stdio::from(out)).spawn().expect("запуск шлюза");
        Gateway { child, port, prefix, log }
    }

    /// Роль экземпляра из `/api/ha` ("ACTIVE" / "STANDBY"); None — шлюз не отвечает.
    pub fn role(&self) -> Option<String> {
        let (200, body) = self.get("/api/ha")? else { return None };
        serde_json::from_str::<Value>(&body).ok()?["role"].as_str().map(str::to_string)
    }

    /// Значение счётчика из `/actuator/prometheus` (без учёта меток).
    pub fn metric(&self, name: &str) -> Option<f64> {
        let (_, body) = self.get("/actuator/prometheus")?;
        body.lines().find(|l| l.starts_with(name)).and_then(|l| l.rsplit(' ').next()?.parse().ok())
    }

    /// Дождаться роли; вернуть, сколько ждали.
    pub async fn wait_role(&self, role: &str, within: Duration) -> Duration {
        let started = Instant::now();
        loop {
            if self.role().as_deref() == Some(role) {
                return started.elapsed();
            }
            assert!(
                started.elapsed() < within,
                "роль {role} не наступила за {within:?} (сейчас {:?})\n{}",
                self.role(),
                self.log_tail()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Штатная остановка (SIGTERM) и ожидание выхода.
    pub fn terminate(&mut self) {
        let _ = Command::new("kill").args(["-TERM", &self.child.id().to_string()]).status();
        let _ = self.child.wait();
    }

    /// Имя тестового топика с префиксом этого запуска.
    pub fn topic(&self, suffix: &str) -> String {
        format!("{}.{suffix}", self.prefix)
    }

    /// GET без токена: статус и тело ответа.
    pub fn get(&self, path: &str) -> Option<(u16, String)> {
        self.get_with(path, None)
    }

    /// GET с заголовком `Authorization: Bearer <token>`.
    pub fn get_with(&self, path: &str, bearer: Option<&str>) -> Option<(u16, String)> {
        self.request("GET", path, bearer)
    }

    /// POST без тела.
    pub fn post(&self, path: &str) -> Option<(u16, String)> {
        self.request("POST", path, None)
    }

    /// HTTP/1.0-запрос к шлюзу по простому сокету (чтобы не тянуть HTTP-клиент в тесты).
    fn request(&self, method: &str, path: &str, bearer: Option<&str>) -> Option<(u16, String)> {
        let mut s = TcpStream::connect_timeout(&([127, 0, 0, 1], self.port).into(), Duration::from_secs(2)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        let auth = bearer.map(|t| format!("Authorization: Bearer {t}\r\n")).unwrap_or_default();
        write!(s, "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Length: 0\r\n{auth}\r\n").ok()?;
        let mut resp = String::new();
        s.read_to_string(&mut resp).ok()?;
        let status = resp.split_whitespace().nth(1)?.parse().ok()?;
        Some((status, resp.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default()))
    }

    /// Связь с контроллерами из /actuator/health: имя → "UP"/"DOWN".
    pub fn links(&self) -> HashMap<String, String> {
        let Some((200, body)) = self.get("/actuator/health") else { return HashMap::new() };
        let health: Value = serde_json::from_str(&body).unwrap_or_default();
        health["components"]["controllers"]
            .as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
            .unwrap_or_default()
    }

    /// Дождаться, что все контроллеры в состоянии `state` ("UP"/"DOWN").
    pub async fn wait_links(&self, state: &str, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            let links = self.links();
            if !links.is_empty() && links.values().all(|s| s == state) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "связь не перешла в {state} за {within:?}: {links:?}\n{}",
                self.log_tail()
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Журнал шлюза целиком.
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Последние строки журнала шлюза — для сообщений об ошибках.
    pub fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text.lines().rev().take(30).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n")
    }

    /// Уборка тестовых топиков (не критично при неудаче).
    pub async fn delete_topics(&self) {
        let admin: AdminClient<DefaultClientContext> = kafka_config().create().unwrap();
        let topics: Vec<String> =
            ["tags", "commands", "results", "events", "alarms", "pac-alarms"].iter().map(|s| self.topic(s)).collect();
        let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        let _ = admin.delete_topics(&refs, &AdminOptions::new()).await;
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Уникальный префикс топиков запуска (`it-<id>`): параллельные тесты не мешают друг другу.
pub fn new_prefix() -> String {
    format!("it-{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

/// Клиент Kafka на брокер тестов.
pub fn kafka_config() -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", super::kafka_bootstrap());
    c
}

/// Следующее сообщение консьюмера до срока `deadline`; `None` — срок вышел.
///
/// Ошибка приёма — не конец ожидания: librdkafka переподключается сам. Так бывает, когда брокер только что поднялся
/// или когда `localhost` сначала разрешается в `::1`, а порт проброшен только на IPv4 (#142).
pub async fn recv_until(consumer: &StreamConsumer, deadline: Instant) -> Option<BorrowedMessage<'_>> {
    loop {
        match tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), consumer.recv()).await {
            Ok(Ok(m)) => return Some(m),
            Ok(Err(_)) => continue,
            Err(_) => return None,
        }
    }
}

/// Консьюмер всех партиций топика с начала (топик создаёт шлюз при старте — ждём его).
pub async fn consumer_from_beginning(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer =
        kafka_config().set("group.id", "scada-gateway-it").set("enable.auto.commit", "false").create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(5)).unwrap();
        let parts: Vec<i32> = md
            .topics()
            .iter()
            .filter(|t| t.name() == topic)
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if !parts.is_empty() {
            let mut tpl = TopicPartitionList::new();
            for p in parts {
                tpl.add_partition_offset(topic, p, Offset::Beginning).unwrap();
            }
            consumer.assign(&tpl).unwrap();
            return consumer;
        }
        assert!(Instant::now() < deadline, "топик {topic} не появился за 30 с");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
