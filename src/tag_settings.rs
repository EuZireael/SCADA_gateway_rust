//! Настройки тегов из топика `channel.tag-settings` (контракт бэкенда: `channel` публикует, шлюз читает).
//!
//! Топик сжатый (compact), одна партиция. Ключ — имя тега (Kafka-key телеметрии), значение —
//! `{"log": true, "deadband": 0.2, "poll": {"mode": "period", "periodMs": 3000}}`; `null` (tombstone) — настроек
//! больше нет. Шлюз читает топик целиком с начала (каждому экземпляру нужна полная копия, группа не нужна) и
//! дальше следит за изменениями — перезапуск при правке не требуется.
//!
//! Что применяется:
//! * `poll.mode = "period"` → тег опрашивается не чаще `periodMs` ([`TagTuning::poll_period`], расписание —
//!   `schedule.rs`); `"cycle"` — значение из YAML не трогаем: «каждый цикл» и есть базовый период опроса. Цикл
//!   контроллера по-прежнему определяется YAML: тег с периодом 3 с при цикле 1 с читается каждый третий цикл;
//! * `deadband` → зона нечувствительности **локальной истории** тега ([`TagTuning::deadband`] подменяет
//!   `history.deadband`), а не фильтр публикации в Kafka: он общий для всех тегов и должен таким остаться;
//! * `log` шлюзу не нужен — архив ведёт `runtime`.
//!
//! Теги без записи работают по YAML; удаление записи (tombstone) опрос тега не выключает — какие теги опрашивать,
//! по-прежнему решает YAML.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use prometheus::IntGauge;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::app::ControllerHandle;
use crate::config::KafkaSettings;
use crate::model::Tag;

/// «Зоны нет»: битовое представление NaN, которое настоящая зона принять не может.
const NO_DEADBAND: u64 = u64::MAX;
/// Пауза перед повторной попыткой создать консьюмер (топика может ещё не быть: его создаёт `channel`).
const RETRY_PAUSE: Duration = Duration::from_secs(10);
/// Таймаут запросов метаданных и позиций.
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// Настройки тега, применённые на лету. Поля меняются из задачи-консьюмера, читаются опросом и обработчиком.
#[derive(Debug)]
pub struct TagTuning {
    /// Период опроса, мс; 0 — настройки нет (период цикла контроллера).
    poll_period_ms: AtomicU64,
    /// Зона нечувствительности истории (биты `f64`); [`NO_DEADBAND`] — настройки нет.
    deadband_bits: AtomicU64,
}

impl Default for TagTuning {
    fn default() -> Self {
        TagTuning { poll_period_ms: AtomicU64::new(0), deadband_bits: AtomicU64::new(NO_DEADBAND) }
    }
}

impl TagTuning {
    /// Период опроса из настроек: `None` — тег читается каждый цикл.
    pub fn poll_period(&self) -> Option<Duration> {
        match self.poll_period_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// Зона нечувствительности истории из настроек: `None` — действует `history.deadband` тега из YAML и умолчания.
    pub fn deadband(&self) -> Option<f64> {
        match self.deadband_bits.load(Ordering::Relaxed) {
            NO_DEADBAND => None,
            bits => Some(f64::from_bits(bits)),
        }
    }

    /// Есть ли у тега применённые настройки.
    pub fn is_set(&self) -> bool {
        self.deadband().is_some() || self.poll_period().is_some()
    }

    /// Применить настройки или сбросить (`None` — вернуться к YAML).
    pub(crate) fn apply(&self, settings: Option<&TagSettings>) {
        match settings {
            Some(s) => {
                self.poll_period_ms.store(s.poll_period_ms.unwrap_or(0), Ordering::Relaxed);
                self.deadband_bits.store(s.deadband.to_bits(), Ordering::Relaxed);
            }
            None => {
                self.poll_period_ms.store(0, Ordering::Relaxed);
                self.deadband_bits.store(NO_DEADBAND, Ordering::Relaxed);
            }
        }
    }
}

/// Разобранная запись топика.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TagSettings {
    /// Зона нечувствительности, ≥ 0; `0` — любое изменение значимо.
    pub deadband: f64,
    /// Период опроса, мс (`mode = "period"` и `periodMs > 0`); `None` — каждый цикл.
    pub poll_period_ms: Option<u64>,
}

#[derive(Deserialize)]
struct Wire {
    deadband: Option<f64>,
    poll: Option<WirePoll>,
}

#[derive(Deserialize)]
struct WirePoll {
    mode: Option<String>,
    #[serde(rename = "periodMs")]
    period_ms: Option<f64>,
}

impl TagSettings {
    /// Разобрать значение записи. Чего в записи нет — по умолчанию контракта: `deadband = 0`, `mode = "cycle"`. Зона
    /// не число или меньше нуля → 0; период у `period` без числа или ≤ 0 → опрос каждый цикл.
    pub fn parse(payload: &[u8]) -> Result<Self, serde_json::Error> {
        let wire: Wire = serde_json::from_slice(payload)?;
        let deadband = wire.deadband.filter(|d| d.is_finite() && *d >= 0.0).unwrap_or(0.0);
        let poll_period_ms = wire
            .poll
            .filter(|p| p.mode.as_deref() == Some("period"))
            .and_then(|p| p.period_ms)
            .filter(|ms| ms.is_finite() && *ms >= 1.0)
            .map(|ms| ms.round() as u64);
        Ok(TagSettings { deadband, poll_period_ms })
    }
}

/// Итог применения одной записи.
#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    /// Настройки тега заданы или заменены.
    Set,
    /// Настройки тега сброшены (tombstone).
    Cleared,
    /// Такого тега у шлюза нет (другой шлюз, выключенный канал): запись не нужна.
    UnknownTag,
    /// Значение не разобрано; прежние настройки тега остались.
    Invalid,
}

/// Теги шлюза по имени и счётчик тегов с настройками.
pub struct TagSettingsStore {
    tags: HashMap<String, Arc<Tag>>,
    active: IntGauge,
}

impl TagSettingsStore {
    /// Реестр тегов всех контроллеров (имена тегов уникальны: иначе шлюз не запустился бы).
    pub fn new(controllers: &[Arc<ControllerHandle>], active: IntGauge) -> Self {
        Self::from_tags(controllers.iter().flat_map(|c| c.ctrl.tags.iter().cloned()), active)
    }

    /// То же по списку тегов.
    pub fn from_tags(tags: impl IntoIterator<Item = Arc<Tag>>, active: IntGauge) -> Self {
        active.set(0);
        TagSettingsStore { tags: tags.into_iter().map(|t| (t.name.clone(), t)).collect(), active }
    }

    /// Применить запись: `payload = None` — tombstone.
    pub fn apply(&self, key: &str, payload: Option<&[u8]>) -> Applied {
        let Some(tag) = self.tags.get(key) else { return Applied::UnknownTag };
        let was_set = tag.tuning.is_set();
        let result = match payload {
            None => {
                tag.tuning.apply(None);
                Applied::Cleared
            }
            Some(bytes) => match TagSettings::parse(bytes) {
                Ok(s) => {
                    tag.tuning.apply(Some(&s));
                    Applied::Set
                }
                Err(e) => {
                    warn!("Настройки тега {key} не разобраны ({e}): прежние остаются");
                    return Applied::Invalid;
                }
            },
        };
        match (was_set, tag.tuning.is_set()) {
            (false, true) => self.active.inc(),
            (true, false) => self.active.dec(),
            _ => {}
        }
        result
    }
}

/// Читать топик настроек и применять записи, пока не остановят. Создание консьюмера — в блокирующем потоке (запросы к
/// брокеру до 10 с каждый не должны держать рабочий поток рантайма); топика может не быть — тогда ждём.
pub async fn consume(settings: KafkaSettings, store: Arc<TagSettingsStore>, cancel: CancellationToken) {
    let topic = settings.tag_settings_topic.clone();
    let mut fails = 0u32;
    while !cancel.is_cancelled() {
        let (cfg, name) = (settings.clone(), topic.clone());
        let created = tokio::task::spawn_blocking(move || assigned_consumer(&cfg, &name))
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("задача создания консьюмера: {e}")));
        match created {
            Ok((consumer, end)) => {
                fails = 0;
                info!("▶ Настройки тегов: читаю {topic}");
                follow(&consumer, &store, &topic, end, &cancel).await;
                let _ = tokio::task::spawn_blocking(move || drop(consumer)).await;
            }
            Err(e) => {
                fails += 1;
                if fails == 1 {
                    warn!("Настройки тегов: топик {topic} не читается ({e:#}); теги работают по YAML, повторю");
                } else {
                    debug!("Настройки тегов: попытка {fails}: {e:#}");
                }
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RETRY_PAUSE) => {}
        }
    }
}

/// Принимать записи до остановки или ошибки консьюмера. Когда прочитаны все записи, что были в топике на старте, —
/// строка в журнал: с этого момента копия настроек полная.
async fn follow(
    consumer: &StreamConsumer,
    store: &TagSettingsStore,
    topic: &str,
    mut end: HashMap<i32, i64>,
    cancel: &CancellationToken,
) {
    let (mut read, mut set, mut cleared, mut unknown) = (0u64, 0u64, 0u64, 0u64);
    let mut caught_up = end.is_empty();
    if caught_up {
        info!("Настройки тегов: топик {topic} пуст — теги работают по YAML");
    }
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return,
            m = consumer.recv() => m,
        };
        let m = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("Настройки тегов: чтение прервано: {e}");
                return;
            }
        };
        read += 1;
        let key = m.key().map(|k| String::from_utf8_lossy(k).into_owned()).unwrap_or_default();
        match store.apply(&key, m.payload()) {
            Applied::Set => set += 1,
            Applied::Cleared => cleared += 1,
            Applied::UnknownTag => unknown += 1,
            Applied::Invalid => {}
        }
        if !caught_up {
            if end.get(&m.partition()).is_some_and(|high| m.offset() + 1 >= *high) {
                end.remove(&m.partition());
            }
            if end.is_empty() {
                caught_up = true;
                info!(
                    "Настройки тегов: прочитано {read} записей — заданы {set}, сняты {cleared}, чужих тегов {unknown}; дальше — на лету"
                );
            }
        }
    }
}

/// Консьюмер с назначением всех партиций топика с начала и конечные позиции партиций (до них надо дочитать). Без группы:
/// каждый экземпляр шлюза читает топик целиком, позиций не фиксирует.
fn assigned_consumer(settings: &KafkaSettings, topic: &str) -> Result<(StreamConsumer, HashMap<i32, i64>)> {
    let consumer: StreamConsumer = settings
        .client_config()
        .set("group.id", "scada-gateway-tag-settings")
        .set("enable.auto.commit", "false")
        .create()?;
    let metadata = consumer.fetch_metadata(Some(topic), KAFKA_TIMEOUT)?;
    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .filter(|t| t.name() == topic && t.error().is_none())
        .flat_map(|t| t.partitions().iter().map(|p| p.id()))
        .collect();
    anyhow::ensure!(!partitions.is_empty(), "топика нет или в нём нет партиций");
    let mut tpl = TopicPartitionList::new();
    let mut end = HashMap::new();
    for p in partitions {
        tpl.add_partition_offset(topic, p, Offset::Beginning)?;
        let (low, high) = consumer.fetch_watermarks(topic, p, KAFKA_TIMEOUT)?;
        if high > low {
            end.insert(p, high);
        }
    }
    consumer.assign(&tpl)?;
    Ok((consumer, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> TagSettings {
        TagSettings::parse(json.as_bytes()).expect("разбор")
    }

    #[test]
    fn full_record_from_the_contract_is_parsed() {
        let s = parse(r#"{"log": true, "deadband": 0.2, "poll": {"mode": "period", "periodMs": 3000}}"#);
        assert_eq!(s, TagSettings { deadband: 0.2, poll_period_ms: Some(3000) });
    }

    #[test]
    fn cycle_mode_leaves_the_poll_period_to_yaml() {
        let s = parse(r#"{"log": false, "deadband": 0, "poll": {"mode": "cycle", "periodMs": 1}}"#);
        assert_eq!(
            s,
            TagSettings { deadband: 0.0, poll_period_ms: None },
            "при cycle periodMs смысла не имеет (в базе там 1)"
        );
    }

    #[test]
    fn missing_parts_take_the_contract_defaults() {
        assert_eq!(parse("{}"), TagSettings { deadband: 0.0, poll_period_ms: None });
        assert_eq!(parse(r#"{"deadband": 0.5}"#), TagSettings { deadband: 0.5, poll_period_ms: None });
        // period без числа или с нулём — каждый цикл, а не «никогда»
        assert_eq!(parse(r#"{"poll": {"mode": "period", "periodMs": null}}"#).poll_period_ms, None);
        assert_eq!(parse(r#"{"poll": {"mode": "period", "periodMs": 0}}"#).poll_period_ms, None);
        assert_eq!(parse(r#"{"poll": {"mode": "period"}}"#).poll_period_ms, None);
    }

    #[test]
    fn a_bad_deadband_becomes_zero_and_an_unknown_mode_means_every_cycle() {
        assert_eq!(parse(r#"{"deadband": -1}"#).deadband, 0.0);
        assert_eq!(parse(r#"{"poll": {"mode": "hourly", "periodMs": 5000}}"#).poll_period_ms, None);
        // периоды с дробной частью (если бэкенд пришлёт число с точкой) округляются
        assert_eq!(parse(r#"{"poll": {"mode": "period", "periodMs": 2500.4}}"#).poll_period_ms, Some(2500));
    }

    #[test]
    fn junk_is_an_error_not_a_panic() {
        assert!(TagSettings::parse(b"not json").is_err());
        assert!(TagSettings::parse(b"[]").is_err());
    }

    fn store() -> (TagSettingsStore, Arc<Tag>, IntGauge) {
        let cfg: crate::config::TagConfig = serde_yaml_ng::from_str(
            r#"{name: "A.B.V1.ST", nodeId: "ns=2;s=1", dataType: FLOAT, pollingRate: 1000, enabled: true}"#,
        )
        .expect("тег");
        let tag = Arc::new(Tag::from_config(&cfg));
        let gauge = IntGauge::new("test_tag_settings", "тест").expect("метрика");
        (TagSettingsStore::from_tags([tag.clone()], gauge.clone()), tag, gauge)
    }

    #[test]
    fn store_applies_clears_counts_and_ignores_foreign_tags() {
        let (store, tag, gauge) = store();
        let json = br#"{"deadband": 0.2, "poll": {"mode": "period", "periodMs": 3000}}"#;
        assert_eq!(store.apply("A.B.V1.ST", Some(json)), Applied::Set);
        assert_eq!((tag.tuning.poll_period(), tag.tuning.deadband()), (Some(Duration::from_secs(3)), Some(0.2)));
        assert_eq!(gauge.get(), 1);
        // повторная запись того же тега счётчик не увеличивает
        assert_eq!(store.apply("A.B.V1.ST", Some(br#"{"deadband": 1}"#)), Applied::Set);
        assert_eq!(
            (tag.tuning.poll_period(), tag.tuning.deadband()),
            (None, Some(1.0)),
            "новая запись заменяет старую целиком"
        );
        assert_eq!(gauge.get(), 1);
        assert_eq!(store.apply("чужой.тег", Some(json)), Applied::UnknownTag);
        assert_eq!(store.apply("A.B.V1.ST", None), Applied::Cleared);
        assert!(!tag.tuning.is_set());
        assert_eq!(gauge.get(), 0);
        assert_eq!(store.apply("A.B.V1.ST", None), Applied::Cleared, "повторный tombstone безвреден");
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn an_invalid_record_keeps_the_previous_settings() {
        let (store, tag, gauge) = store();
        store.apply("A.B.V1.ST", Some(br#"{"deadband": 0.5}"#));
        assert_eq!(store.apply("A.B.V1.ST", Some(b"oops")), Applied::Invalid);
        assert_eq!(tag.tuning.deadband(), Some(0.5));
        assert_eq!(gauge.get(), 1);
    }

    #[test]
    fn tuning_applies_and_clears() {
        let t = TagTuning::default();
        assert!(!t.is_set());
        assert_eq!((t.poll_period(), t.deadband()), (None, None));
        t.apply(Some(&TagSettings { deadband: 0.0, poll_period_ms: Some(3000) }));
        assert_eq!(t.poll_period(), Some(Duration::from_secs(3)));
        assert_eq!(t.deadband(), Some(0.0), "нулевая зона — тоже настройка: она заменяет history.deadband из YAML");
        t.apply(None);
        assert!(!t.is_set());
    }
}
