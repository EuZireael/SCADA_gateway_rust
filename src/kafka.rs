//! Kafka: продюсер телеметрии/событий/алармов/результатов и консьюмер команд.
//!
//! Продюсер — ThreadedProducer librdkafka: send() только ставит сообщение в очередь и
//! не ждёт брокер, поэтому обрыв Kafka не морозит опрос ПЛК. Ошибки доставки считаются
//! метрикой в колбэке. Идемпотентность + acks=all держат порядок значений тега.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{DeliveryResult, Message};
use rdkafka::producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::Serialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{KafkaSettings, Topics};
use crate::leadership::{Leadership, RoleChange};
use crate::messages::{AlarmMessage, CommandMessage, CommandResultMessage, EventMessage, TelemetryMessage};
use crate::metrics::Metrics;

struct DeliveryCounter {
    metrics: Arc<Metrics>,
}

impl ClientContext for DeliveryCounter {}

impl ProducerContext for DeliveryCounter {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((err, msg)) = result {
            self.metrics.kafka_send_errors.inc();
            warn!("Kafka: сообщение в {} не доставлено: {err}", msg.topic());
        }
    }
}

/// Исходящая сторона Kafka.
pub struct KafkaOut {
    producer: ThreadedProducer<DeliveryCounter>,
    topics: Topics,
    publish_events: bool,
    publish_alarms: bool,
    metrics: Arc<Metrics>,
    /// Горячий резерв: события и алармы в Kafka шлёт только активный экземпляр.
    leadership: Arc<Leadership>,
    /// Только для юнит-тестов (`cfg!(test)`): вместо отправки сообщения (топик, ключ, тело) копятся здесь.
    pub captured: std::sync::Mutex<Vec<(String, String, String)>>,
}

impl KafkaOut {
    pub fn new(settings: &KafkaSettings, metrics: Arc<Metrics>, leadership: Arc<Leadership>) -> Result<Self> {
        let producer: ThreadedProducer<DeliveryCounter> = ClientConfig::new()
            .set("bootstrap.servers", &settings.bootstrap_servers)
            .set("client.id", "scada-gateway-rs")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "snappy")
            .set("linger.ms", "5")
            .set("batch.size", "32768")
            // Недоставленное за 30 с считается ошибкой: свежий цикл опроса пришлёт новое значение.
            .set("message.timeout.ms", "30000")
            .create_with_context(DeliveryCounter { metrics: metrics.clone() })
            .context("не создан Kafka-продюсер")?;
        Ok(KafkaOut {
            producer,
            topics: settings.topics.clone(),
            publish_events: settings.publish_events,
            publish_alarms: settings.publish_alarms,
            metrics,
            leadership,
            captured: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn send<T: Serialize>(&self, topic: &str, key: &str, message: &T) {
        let payload = match serde_json::to_vec(message) {
            Ok(p) => p,
            Err(e) => {
                error!("Kafka: сообщение для {topic} не сериализовано: {e}");
                return;
            }
        };
        if cfg!(test) {
            self.captured.lock().expect("mutex").push((
                topic.into(),
                key.into(),
                String::from_utf8_lossy(&payload).into(),
            ));
            return;
        }
        if let Err((e, _)) = self.producer.send(BaseRecord::to(topic).key(key).payload(&payload)) {
            self.metrics.kafka_send_errors.inc();
            warn!("Kafka: {topic} не принял сообщение {key}: {e}");
        }
    }

    pub fn send_telemetry(&self, tag_name: &str, message: &TelemetryMessage) {
        self.send(&self.topics.telemetry, tag_name, message);
    }

    pub fn send_event(&self, message: &EventMessage) {
        if self.publish_events && self.leadership.is_active() {
            self.send(&self.topics.events, &message.event_type, message);
        }
    }

    pub fn send_alarm(&self, message: &AlarmMessage) {
        if self.publish_alarms && self.leadership.is_active() {
            self.send(&self.topics.alarms, &message.tag_name, message);
        }
    }

    pub fn send_result(&self, message: &CommandResultMessage) {
        let key = message.tag_name.as_deref().or(message.command_id.as_deref()).unwrap_or("");
        self.send(&self.topics.command_results, key, message);
    }

    /// Дослать очередь при остановке.
    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            warn!("Kafka: очередь не дослана при остановке: {e}");
        }
    }
}

/// Создать топики шлюза, если их нет (партиции — как у Java-шлюза). Уже существующие не трогаем;
/// недоступный брокер — не повод не стартовать (брокер может подняться позже).
pub async fn ensure_topics(settings: &KafkaSettings) {
    let t = &settings.topics;
    let specs = [(&t.telemetry, 3), (&t.alarms, 2), (&t.events, 1), (&t.commands, 1), (&t.command_results, 1)];
    create_topics(settings, &specs.iter().map(|(n, p)| (n.as_str(), *p)).collect::<Vec<_>>()).await;
}

/// Один топик (служебный топик выборов).
pub async fn ensure_topic(settings: &KafkaSettings, name: &str, partitions: i32) {
    create_topics(settings, &[(name, partitions)]).await;
}

async fn create_topics(settings: &KafkaSettings, specs: &[(&str, i32)]) {
    let admin: AdminClient<DefaultClientContext> =
        match ClientConfig::new().set("bootstrap.servers", &settings.bootstrap_servers).create() {
            Ok(a) => a,
            Err(e) => {
                warn!("Kafka admin не создан: {e}");
                return;
            }
        };
    let topics: Vec<NewTopic> = specs
        .iter()
        .map(|(name, parts)| NewTopic::new(name, *parts, TopicReplication::Fixed(settings.replication)))
        .collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    match tokio::time::timeout(Duration::from_secs(15), admin.create_topics(topics.iter(), &opts)).await {
        Ok(Ok(results)) => {
            for r in results {
                match r {
                    Ok(name) => info!("Kafka: создан топик {name}"),
                    Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                    Err((name, code)) => warn!("Kafka: топик {name} не создан: {code}"),
                }
            }
        }
        Ok(Err(e)) => warn!("Kafka: создание топиков не удалось: {e}"),
        Err(_) => warn!("Kafka: брокер не ответил на создание топиков за 15 с"),
    }
}

/// Консьюмер команд: работает, только пока экземпляр активный (резервный команд не принимает).
///
/// Партиции назначаются вручную, без членства в группе: после падения активного новому не надо ждать,
/// пока брокер вычеркнет мёртвого члена группы (до `session.timeout.ms` — десятки секунд), — команды
/// принимаются сразу после получения лидерства. Позиция хранится коммитом в группе
/// `scada-gateway-group`: новый активный продолжает с позиции прежнего, и команды, пришедшие за время
/// переключения, не теряются. Первый запуск группы (коммитов нет) — с конца топика. Команды,
/// пролежавшие в топике дольше `gateway.commands.max-age-ms` (шлюзы стояли оба), отсекаются по
/// возрасту — [`crate::command::handle`].
///
/// Позиция фиксируется **до** исполнения (не более одного раза): повторное исполнение управляющей
/// команды после сбоя опаснее потерянной — монитор получит `NO_CONFIRMATION` («результат неизвестен»).
pub async fn consume_commands<F, Fut>(
    settings: KafkaSettings,
    leadership: Arc<Leadership>,
    cancel: CancellationToken,
    handler: F,
) where
    F: Fn(CommandMessage, Option<i64>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let topic = settings.topics.commands.clone();
    let mut role = leadership.subscribe();
    while !cancel.is_cancelled() {
        // Ждём активную роль.
        loop {
            if role.borrow_and_update().active {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                changed = role.changed() => if changed.is_err() { return },
            }
        }
        match assigned_consumer(&settings, &topic) {
            Ok(consumer) => {
                info!("▶ Приём команд включён: {topic}");
                consume_while_active(&consumer, &leadership, &mut role, &cancel, &handler).await;
                // drop консьюмера ждёт его закрытия — не на потоке рантайма.
                let _ = tokio::task::spawn_blocking(move || drop(consumer)).await;
                if !cancel.is_cancelled() {
                    info!("⏸ Приём команд выключен (резерв или ошибка)");
                }
            }
            Err(e) => warn!("Kafka: консьюмер команд не поднят: {e:#}"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn consume_while_active<F, Fut>(
    consumer: &StreamConsumer,
    leadership: &Leadership,
    role: &mut watch::Receiver<RoleChange>,
    cancel: &CancellationToken,
    handler: &F,
) where
    F: Fn(CommandMessage, Option<i64>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return,
            changed = role.changed() => {
                if changed.is_err() || !role.borrow_and_update().active { return }
                continue;
            }
            m = consumer.recv() => m,
        };
        match msg {
            Ok(m) => {
                // Роль могла смениться, пока сообщение ехало: не коммитим и не исполняем — его прочтёт
                // новый активный с позиции прежнего.
                if !leadership.is_active() {
                    return;
                }
                if let Err(e) = consumer.commit_message(&m, CommitMode::Sync) {
                    warn!("Kafka: позиция команд не зафиксирована ({e}) — команда не исполняется");
                    continue;
                }
                let Some(payload) = m.payload() else { continue };
                match serde_json::from_slice::<CommandMessage>(payload) {
                    Ok(cmd) => handler(cmd, m.timestamp().to_millis()).await,
                    Err(e) => warn!("Команда не разобрана ({e}): {}", String::from_utf8_lossy(payload)),
                }
            }
            Err(e) => {
                warn!("Kafka: приём команд прерван: {e}");
                return;
            }
        }
    }
}

fn assigned_consumer(settings: &KafkaSettings, topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &settings.bootstrap_servers)
        .set("group.id", &settings.consumer_group_id)
        .set("enable.auto.commit", "false")
        .create()?;
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .filter(|t| t.name() == topic)
        .flat_map(|t| t.partitions().iter().map(|p| p.id()))
        .collect();
    anyhow::ensure!(!partitions.is_empty(), "топик {topic} без партиций");
    // Закоммиченная позиция группы, а если её нет (первый запуск) — конец топика.
    let mut query = TopicPartitionList::new();
    for p in &partitions {
        query.add_partition(topic, *p);
    }
    let committed = consumer.committed_offsets(query, Duration::from_secs(10))?;
    let mut tpl = TopicPartitionList::new();
    for p in partitions {
        let offset = match committed.find_partition(topic, p).map(|e| e.offset()) {
            Some(Offset::Offset(n)) => Offset::Offset(n),
            _ => Offset::End,
        };
        tpl.add_partition_offset(topic, p, offset)?;
    }
    consumer.assign(&tpl)?;
    Ok(consumer)
}
