//! Kafka: продюсер телеметрии/событий/алармов/результатов и консьюмер команд.
//!
//! Продюсер — ThreadedProducer librdkafka: send() только ставит сообщение в очередь и
//! не ждёт брокер, поэтому обрыв Kafka не морозит опрос ПЛК. Ошибки доставки считаются
//! метрикой в колбэке. Идемпотентность + acks=all держат порядок значений тега.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
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

/// Как часто повторять предупреждение о недоставленных сообщениях (при обрыве брокера их тысячи).
const WARN_EVERY: Duration = Duration::from_secs(30);
/// Не чаще раза в это время восстановление доставки запускает полную отправку всех тегов: при «мигающем» брокере
/// каждое восстановление иначе повторяло бы залп из всех тегов всех контроллеров (при 54 800 тегах — 54 800 сообщений).
/// Недоставленное в паузе уходит с обычной полной отправкой (`GATEWAY_PUBLISH_FULL_RESEND_MS`, 30 с).
const MIN_RESYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Что произошло, когда доставка после сбоя снова пошла.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// Сбоя не было.
    None,
    /// Доставка восстановилась, все теги будут отправлены заново.
    Resync,
    /// Доставка восстановилась, но полная отправка была совсем недавно — новой не будет.
    Throttled,
}

/// Состояние доставки: недоставленное запоминает фильтр публикации («по исключению») как
/// отправленное, и потребитель не узнал бы значение до полной отправки. Поэтому, как только доставка
/// после сбоя снова пошла, обработчики сбрасывают фильтры и отправляют все теги заново.
#[derive(Default)]
pub struct DeliveryState {
    failing: AtomicBool,
    /// Растёт при восстановлении доставки после сбоя, но не чаще `MIN_RESYNC_INTERVAL`.
    resync_epoch: AtomicU64,
    /// Момент последнего ресинка (мс от `epoch_ms`); 0 — ещё не было.
    last_resync_ms: AtomicI64,
    /// Момент последнего предупреждения (мс от `epoch_ms`), чтобы не засорять журнал.
    last_warn_ms: AtomicI64,
    suppressed: AtomicU64,
}

impl DeliveryState {
    /// Сбой: true (и число подавленных до этого предупреждений), если пора написать в журнал.
    pub(crate) fn record_failure(&self, now_ms: i64) -> Option<u64> {
        self.failing.store(true, Ordering::Release);
        let last = self.last_warn_ms.load(Ordering::Relaxed);
        if last == 0 || now_ms - last >= WARN_EVERY.as_millis() as i64 {
            self.last_warn_ms.store(now_ms, Ordering::Relaxed);
            return Some(self.suppressed.swap(0, Ordering::Relaxed));
        }
        self.suppressed.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Удачная доставка: было ли восстановление после сбоя и нужна ли теперь полная отправка. Флаг сбоя снимается всегда;
    /// «эпоха» растёт, только если с прошлого ресинка прошло не меньше `MIN_RESYNC_INTERVAL`.
    pub(crate) fn record_success(&self, now_ms: i64) -> Recovery {
        if !self.failing.swap(false, Ordering::AcqRel) {
            return Recovery::None;
        }
        let last = self.last_resync_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms - last < MIN_RESYNC_INTERVAL.as_millis() as i64 {
            return Recovery::Throttled;
        }
        self.last_resync_ms.store(now_ms, Ordering::Relaxed);
        self.resync_epoch.fetch_add(1, Ordering::AcqRel);
        Recovery::Resync
    }

    /// «Эпоха» восстановления доставки: растёт при восстановлении после сбоя (не чаще раза в 30 с); обработчики сравнивают её со своей и отправляют все теги заново.
    pub fn resync_epoch(&self) -> u64 {
        self.resync_epoch.load(Ordering::Acquire)
    }

    /// Доставка идёт (последняя попытка удалась или сбоев ещё не было).
    pub fn healthy(&self) -> bool {
        !self.failing.load(Ordering::Acquire)
    }
}

/// Колбэк доставки продюсера: считает ошибки и ведёт [`DeliveryState`].
struct DeliveryCounter {
    metrics: Arc<Metrics>,
    state: Arc<DeliveryState>,
}

impl ClientContext for DeliveryCounter {}

impl ProducerContext for DeliveryCounter {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        match result {
            Err((err, msg)) => {
                self.metrics.kafka_send_errors.inc();
                if let Some(suppressed) = self.state.record_failure(chrono::Utc::now().timestamp_millis()) {
                    warn!(
                        "Kafka: сообщение в {} не доставлено: {err}{}",
                        msg.topic(),
                        if suppressed > 0 {
                            format!(" (ещё {suppressed} с прошлого предупреждения)")
                        } else {
                            String::new()
                        }
                    );
                }
            }
            Ok(_) => match self.state.record_success(chrono::Utc::now().timestamp_millis()) {
                Recovery::Resync => {
                    self.metrics.kafka_resyncs.inc();
                    info!("Kafka: доставка восстановлена — все теги будут отправлены заново");
                }
                Recovery::Throttled => info!(
                    "Kafka: доставка восстановлена; полная отправка была меньше {} с назад — недоставленное уйдёт с ближайшей плановой",
                    MIN_RESYNC_INTERVAL.as_secs()
                ),
                Recovery::None => {}
            },
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
    delivery: Arc<DeliveryState>,
    /// Только для юнит-тестов (`cfg!(test)`): вместо отправки сообщения (топик, ключ, тело) копятся здесь.
    pub captured: std::sync::Mutex<Vec<(String, String, String)>>,
    /// Только для юнит-тестов: сколько следующих сообщений «очередь продюсера не приняла» (имитация переполнения).
    pub reject_next: std::sync::atomic::AtomicUsize,
    /// Предупреждение о переполнении очереди — не чаще раза в [`WARN_EVERY`].
    queue_full_warn: WarnThrottle,
}

/// Ограничитель частоты предупреждений: пропускает одно в [`WARN_EVERY`], остальные считает.
#[derive(Default)]
struct WarnThrottle {
    last_ms: AtomicI64,
    suppressed: AtomicU64,
}

impl WarnThrottle {
    /// `Some(число подавленных с прошлого раза)`, если пора писать в журнал.
    fn record(&self, now_ms: i64) -> Option<u64> {
        let last = self.last_ms.load(Ordering::Relaxed);
        if last == 0 || now_ms - last >= WARN_EVERY.as_millis() as i64 {
            self.last_ms.store(now_ms, Ordering::Relaxed);
            return Some(self.suppressed.swap(0, Ordering::Relaxed));
        }
        self.suppressed.fetch_add(1, Ordering::Relaxed);
        None
    }
}

impl KafkaOut {
    /// Создать продюсера (идемпотентность, `acks=all`, snappy, пачки по 5 мс); неверное свойство безопасности — ошибка.
    pub fn new(settings: &KafkaSettings, metrics: Arc<Metrics>, leadership: Arc<Leadership>) -> Result<Self> {
        let delivery = Arc::new(DeliveryState::default());
        let producer: ThreadedProducer<DeliveryCounter> = settings
            .client_config()
            .set("client.id", "scada-gateway-rs")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "snappy")
            .set("linger.ms", "5")
            .set("batch.size", "32768")
            // Недоставленное за 30 с считается ошибкой: свежий цикл опроса пришлёт новое значение.
            .set("message.timeout.ms", "30000")
            .create_with_context(DeliveryCounter { metrics: metrics.clone(), state: delivery.clone() })
            .context("не создан Kafka-продюсер")?;
        Ok(KafkaOut {
            producer,
            topics: settings.topics.clone(),
            publish_events: settings.publish_events,
            publish_alarms: settings.publish_alarms,
            metrics,
            leadership,
            delivery,
            captured: std::sync::Mutex::new(Vec::new()),
            reject_next: std::sync::atomic::AtomicUsize::new(0),
            queue_full_warn: WarnThrottle::default(),
        })
    }

    /// Сериализовать сообщение в JSON и отправить; ошибка сериализации — в журнал, сообщение теряется.
    fn send<T: Serialize>(&self, topic: &str, key: &str, message: &T) {
        let payload = match serde_json::to_vec(message) {
            Ok(p) => p,
            Err(e) => {
                error!("Kafka: сообщение для {topic} не сериализовано: {e}");
                return;
            }
        };
        self.send_raw(topic, key, &payload);
    }

    /// Отправить готовое тело (JSON). `false` — очередь продюсера переполнена и сообщение не принято: это не сбой
    /// доставки (брокер жив, просто не успевает), поэтому повторную отправку всех тегов он не запускает; вызывающий
    /// сам повторит сообщение в следующем цикле. Прочие ошибки постановки считаются сбоем доставки.
    fn send_raw(&self, topic: &str, key: &str, payload: &[u8]) -> bool {
        if cfg!(test) {
            // Тестовый хук (один поток): отказать следующему сообщению, пока счётчик не обнулится.
            if self.reject_next.load(Ordering::Acquire) > 0 {
                self.reject_next.fetch_sub(1, Ordering::AcqRel);
                self.metrics.kafka_queue_full.inc();
                return false;
            }
            self.captured.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push((
                topic.into(),
                key.into(),
                String::from_utf8_lossy(payload).into(),
            ));
            return true;
        }
        match self.producer.send(BaseRecord::to(topic).key(key).payload(payload)) {
            Ok(()) => true,
            Err((rdkafka::error::KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                self.metrics.kafka_queue_full.inc();
                if let Some(suppressed) = self.queue_full_warn.record(chrono::Utc::now().timestamp_millis()) {
                    warn!(
                        "Kafka: очередь продюсера переполнена — брокер не успевает; теги уйдут следующим циклом \
                         (ещё {suppressed} с прошлого предупреждения)"
                    );
                }
                false
            }
            Err((e, _)) => {
                self.metrics.kafka_send_errors.inc();
                if let Some(suppressed) = self.delivery.record_failure(chrono::Utc::now().timestamp_millis()) {
                    warn!("Kafka: {topic} не принял сообщение {key}: {e} (ещё {suppressed} с прошлого предупреждения)");
                }
                true
            }
        }
    }

    /// Телеметрия тега в топик телеметрии (ключ — имя тега). Только ставит в очередь librdkafka, сеть не ждёт.
    /// `false` — очередь переполнена, сообщение не принято: тег надо отправить заново (это не сбой доставки и повторную отправку всех тегов не запускает).
    pub fn send_telemetry(&self, tag_name: &str, message: &TelemetryMessage) -> bool {
        thread_local! {
            /// Буфер тела телеметрии переиспользуется: сообщение копируется в очередь librdkafka при отправке.
            static BODY: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(128));
        }
        BODY.with(|cell| {
            let mut body = cell.borrow_mut();
            body.clear();
            message.write_json(&mut body);
            self.send_raw(&self.topics.telemetry, tag_name, &body)
        })
    }

    /// Событие в топик событий (ключ — тип): только активный экземпляр и только если публикация включена.
    pub fn send_event(&self, message: &EventMessage) {
        if self.publish_events && self.leadership.is_active() {
            self.send(&self.topics.events, &message.event_type, message);
        }
    }

    /// Аларм в топик алармов (ключ — имя тега): только активный экземпляр и только если публикация включена.
    pub fn send_alarm(&self, message: &AlarmMessage) {
        if self.publish_alarms && self.leadership.is_active() {
            self.send(&self.topics.alarms, &message.tag_name, message);
        }
    }

    /// Результат команды (ключ — имя тега или `commandId`). Всегда: команды принимает только активный экземпляр.
    pub fn send_result(&self, message: &CommandResultMessage) {
        let key = message.tag_name.as_deref().or(message.command_id.as_deref()).unwrap_or("");
        self.send(&self.topics.command_results, key, message);
    }

    /// Состояние доставки: эпоха повторной отправки и признак «доставка идёт».
    pub fn delivery(&self) -> &DeliveryState {
        &self.delivery
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

/// Создать топики, которых нет; недоступный брокер или отказ — предупреждение, а не ошибка запуска.
async fn create_topics(settings: &KafkaSettings, specs: &[(&str, i32)]) {
    let admin: AdminClient<DefaultClientContext> = match settings.client_config().create() {
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
/// возрасту — [`crate::command::process`].
///
/// Позиция фиксируется **до** исполнения (не более одного раза): повторное исполнение управляющей
/// команды после сбоя опаснее потерянной — монитор получит `NO_CONFIRMATION` («результат неизвестен»).
/// `handler` раскладывает команды по очередям контроллеров ([`crate::command::submit`]) и записи не ждёт:
/// позиция фиксируется в момент постановки в очередь.
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
        // Создание консьюмера синхронно (метаданные и позиции — до 10 с каждое, пока брокер недоступен): на потоке
        // рантайма оно держало бы рабочий поток и останавливало соседние задачи, в том числе чтение ПЛК (ложная «потеря связи»).
        let (cfg, topic_name) = (settings.clone(), topic.clone());
        let created = tokio::task::spawn_blocking(move || assigned_consumer(&cfg, &topic_name))
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("задача создания консьюмера: {e}")));
        match created {
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

/// Читать команды, пока экземпляр активен; позиция коммитится до исполнения. Возврат — роль сменилась, остановка или ошибка консьюмера.
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
                    Err(e) => warn!("Команда не разобрана ({e}): {}", preview(payload)),
                }
            }
            Err(e) => {
                warn!("Kafka: приём команд прерван: {e}");
                return;
            }
        }
    }
}

/// Начало чужого сообщения для журнала: в топик может прийти что угодно и сколько угодно.
fn preview(payload: &[u8]) -> String {
    const MAX_CHARS: usize = 300;
    let text = String::from_utf8_lossy(payload);
    match text.char_indices().nth(MAX_CHARS) {
        Some((end, _)) => format!("{}… (всего {} байт)", text[..end].escape_debug(), payload.len()),
        None => text.escape_debug().to_string(),
    }
}

/// Консьюмер с ручным назначением всех партиций топика команд: с закоммиченной позиции группы, а при первом запуске — с конца.
fn assigned_consumer(settings: &KafkaSettings, topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer =
        settings.client_config().set("group.id", "scada-gateway-group").set("enable.auto.commit", "false").create()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientProperties, Settings};
    use crate::metrics::Metrics;

    fn settings_with(props: &[(&str, &str)]) -> KafkaSettings {
        let mut s = Settings::from_env().expect("настройки по умолчанию").kafka;
        s.client = ClientProperties(props.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect());
        s
    }

    fn producer(s: &KafkaSettings) -> Result<KafkaOut> {
        let metrics = Arc::new(Metrics::new());
        KafkaOut::new(s, metrics.clone(), Leadership::single("test".into(), metrics.ha_gauge("test")))
    }

    /// librdkafka собран с OpenSSL и SASL: клиенты с TLS и SCRAM/PLAIN создаются (подключения нет).
    #[test]
    fn producer_accepts_tls_and_sasl_properties() {
        let tls = settings_with(&[("security.protocol", "ssl"), ("ssl.endpoint.identification.algorithm", "none")]);
        producer(&tls).expect("TLS");
        let scram = settings_with(&[
            ("security.protocol", "sasl_ssl"),
            ("sasl.mechanism", "SCRAM-SHA-512"),
            ("sasl.username", "gw"),
            ("sasl.password", "secret"),
        ]);
        producer(&scram).expect("SASL_SSL + SCRAM");
        let plain = settings_with(&[
            ("security.protocol", "sasl_plaintext"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.username", "gw"),
            ("sasl.password", "secret"),
        ]);
        producer(&plain).expect("SASL_PLAINTEXT + PLAIN");
    }

    #[test]
    fn delivery_recovery_after_failures_bumps_the_resync_epoch_once() {
        let d = DeliveryState::default();
        assert!(d.healthy());
        assert_eq!(d.record_success(500), Recovery::None, "успех без сбоя ничего не меняет");
        assert_eq!(d.resync_epoch(), 0);
        d.record_failure(1_000);
        d.record_failure(2_000);
        assert!(!d.healthy());
        assert_eq!(d.record_success(3_000), Recovery::Resync, "первый успех после сбоя — восстановление");
        assert_eq!(d.record_success(3_100), Recovery::None);
        assert!(d.healthy());
        assert_eq!(d.resync_epoch(), 1);
    }

    #[test]
    fn flapping_broker_causes_one_resync_per_interval() {
        let d = DeliveryState::default();
        d.record_failure(1_000);
        assert_eq!(d.record_success(2_000), Recovery::Resync);
        // «Мигание»: сбой и восстановление ещё дважды в пределах 30 с — флаг сбоя снят, ресинка нет.
        d.record_failure(5_000);
        assert_eq!(d.record_success(6_000), Recovery::Throttled);
        assert!(d.healthy(), "флаг сбоя при этом снимается");
        d.record_failure(20_000);
        assert_eq!(d.record_success(21_000), Recovery::Throttled);
        assert_eq!(d.resync_epoch(), 1);
        // Через 30 с после прошлого ресинка — снова можно.
        d.record_failure(31_900);
        assert_eq!(d.record_success(32_000), Recovery::Resync);
        assert_eq!(d.resync_epoch(), 2);
    }

    #[test]
    fn failure_warnings_are_throttled_and_count_the_skipped() {
        let d = DeliveryState::default();
        assert_eq!(d.record_failure(10_000), Some(0), "первое предупреждение — сразу");
        for t in [10_100, 12_000, 39_000] {
            assert_eq!(d.record_failure(t), None);
        }
        assert_eq!(d.record_failure(40_001), Some(3), "через 30 с — с числом подавленных");
    }

    #[test]
    fn foreign_payload_is_cut_and_escaped_in_the_log() {
        assert_eq!(preview(b"{\"a\":1}"), "{\\\"a\\\":1}");
        let huge = vec![b'x'; 100_000];
        let shown = preview(&huge);
        assert!(shown.chars().count() < 400 && shown.contains("всего 100000 байт"), "{shown}");
        assert!(!preview(b"a\nb\x1b[31m").contains('\n'), "переводы строк не ломают журнал");
    }

    #[test]
    fn unknown_property_is_an_error_not_silently_ignored() {
        assert!(producer(&settings_with(&[("no.such.property", "1")])).is_err());
    }
}
