//! Горячее резервирование: выборы активного экземпляра через группу потребителей Kafka.
//!
//! Оба экземпляра пары подписаны одной группой (`gateway.ha.group-id`) на служебный топик из одной
//! партиции (`gateway.ha.topic`). Kafka отдаёт партицию ровно одному члену группы — он и активный.
//! Упал активный — брокер перестаёт получать от него heartbeat и через `session-timeout-ms` отдаёт
//! партицию резервному; остановили штатно — экземпляр покидает группу сразу, и резерв подхватывает за
//! доли секунды.
//!
//! **Почему Kafka, а не отдельный арбитр.** Kafka — единственный канал шлюза к монитору (телеметрия,
//! события, команды), поэтому выборы через неё не добавляют точки отказа. И split-brain здесь
//! безвреден: экземпляр, потерявший связь с брокером и не узнавший, что лидерство ушло, всё равно
//! ничего не может опубликовать или принять.
//!
//! **Защиты.**
//! * `cooperative-sticky`: перезапущенный (вернувшийся) резерв не отбирает лидерство у живого
//!   активного — лишних переключений нет.
//! * Самоотключение: librdkafka сам снимает назначение, если сессия с координатором группы не
//!   подтверждалась дольше `session-timeout-ms` (изоляция от сети) — активный уходит в резерв.
//! * Потеря партиции ловится и колбэком ребаланса (до его завершения), и опросом назначения
//!   раз в 100 мс.
//! * «Слепой» активный: если связь потеряна со ВСЕМИ контроллерами дольше `yield-after-ms`, а партнёр
//!   в группе есть, активный выходит из группы и отдаёт лидерство (партнёр мог сохранить связь —
//!   у него другой сетевой путь к ПЛК). Возвращается резервным: `cooperative-sticky` лидерство не
//!   отбирает. Без партнёра в группе ничего не меняется — BAD-кадры монитору всё равно нужны.
//! * Чужая станция в группе: станции на одном Kafka разводятся своим топиком команд (от него зависит группа выборов).
//!   Если две станции всё же попали в одну группу, активный один на всех, и телеметрия остальных молча
//!   остановилась бы. Поэтому экземпляр сообщает свою станцию в `client.id` ([`StationMark`]), не вступает в группу, где
//!   уже есть экземпляры другой станции, и, находясь в группе, раз в 30 с проверяет её состав — смешение видно в журнале,
//!   событии `HA`/`ERROR`, метрике `scada_ha_foreign_members` и `/api/ha`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use prometheus::IntGauge;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{HaSettings, KafkaSettings};
use crate::events::{Event, EventSink};
use crate::kafka;
use crate::leadership::Leadership;

/// Как часто опрашиваем группу: за столько же замечаем потерю или получение лидерства.
const POLL: Duration = Duration::from_millis(100);
/// Не чаще: проверка наличия партнёра в группе — запрос к брокеру.
const PEER_CHECK_EVERY: Duration = Duration::from_secs(5);
/// Минимальное время вне группы после передачи лидерства — партнёру нужно успеть его получить.
const MIN_YIELD_PAUSE: Duration = Duration::from_secs(5);
/// Пауза перед повтором после ошибки выборов (брокер недоступен, неверная настройка).
const RETRY_BACKOFF: Duration = Duration::from_secs(2);
/// Как часто экземпляр в группе проверяет, нет ли в ней экземпляров другой станции.
const MEMBERS_CHECK_EVERY: Duration = Duration::from_secs(30);
/// Экземпляр, не вступивший в группу из-за другой станции, проверяет её снова через столько.
const REFUSE_RETRY: Duration = Duration::from_secs(30);
/// Пока смешение станций не устранено, ошибка в журнале повторяется с этим периодом.
const MIX_LOG_EVERY: Duration = Duration::from_secs(600);
/// Сколько контроллеров (наименьших хешей) несёт отметка станции: `client.id` не растёт с размером станции.
const MARK_MAX: usize = 16;
/// Начало `client.id` консьюмера выборов.
const CLIENT_ID_PREFIX: &str = "scada-gateway-ha-";
/// Разделитель имени экземпляра и отметки станции в `client.id`.
const MARK_SEP: &str = ".st-";

/// Отметка станции: короткие хеши имён её контроллеров (не больше 16 наименьших). Передаётся в `client.id`
/// консьюмера выборов, который видят все члены группы. У экземпляров одной станции хотя бы один контроллер общий —
/// даже пока пара обновляется и конфигурации чуть различаются; у разных станций контроллеры разные.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StationMark(Vec<u32>);

impl StationMark {
    /// Отметка по именам включённых контроллеров станции.
    pub fn of<'a>(controllers: impl IntoIterator<Item = &'a str>) -> Self {
        let mut hashes: Vec<u32> = controllers.into_iter().map(name_hash).collect();
        hashes.sort_unstable();
        hashes.dedup();
        hashes.truncate(MARK_MAX);
        StationMark(hashes)
    }

    /// `client.id` консьюмера выборов: `scada-gateway-ha-<экземпляр>.st-<хеш>-<хеш>…`.
    fn client_id(&self, instance: &str) -> String {
        let mut id = format!("{CLIENT_ID_PREFIX}{instance}");
        if !self.0.is_empty() {
            id.push_str(MARK_SEP);
            id.push_str(&self.0.iter().map(|h| format!("{h:06x}")).collect::<Vec<_>>().join("-"));
        }
        id
    }

    /// Имя экземпляра и отметка из `client.id` члена группы; `None` — экземпляр прежней версии (без отметки).
    fn parse(client_id: &str) -> Option<(&str, Self)> {
        let (instance, hashes) = client_id.strip_prefix(CLIENT_ID_PREFIX)?.rsplit_once(MARK_SEP)?;
        let hashes = hashes
            .split('-')
            .map(|h| if h.len() == 6 { u32::from_str_radix(h, 16).ok() } else { None })
            .collect::<Option<Vec<u32>>>()?;
        Some((instance, StationMark(hashes)))
    }

    /// Другая станция — ни одного общего контроллера. Пустая отметка (станция без контроллеров) ни с кем не сравнивается.
    fn is_other_station(&self, other: &Self) -> bool {
        !self.0.is_empty() && !other.0.is_empty() && !other.0.iter().any(|h| self.0.contains(h))
    }
}

/// 24 бита FNV-1a от имени контроллера: одинаковы во всех версиях и на всех машинах (хешер std — нет).
fn name_hash(name: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in name.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h & 0x00ff_ffff
}

/// Члены группы другой станции (`экземпляр@хост`) из пар `(client.id, хост)`; экземпляры прежней версии не учитываются.
fn other_station<'a>(members: impl IntoIterator<Item = (&'a str, &'a str)>, mark: &StationMark) -> Vec<String> {
    let mut others: Vec<String> = members
        .into_iter()
        .filter_map(|(client_id, host)| {
            let (instance, other) = StationMark::parse(client_id)?;
            mark.is_other_station(&other).then(|| format!("{instance}@{}", host.trim_start_matches('/')))
        })
        .collect();
    others.sort();
    others
}

/// Члены группы выборов из другой станции — запросом состава группы к брокеру.
fn other_station_members(
    consumer: &BaseConsumer<ElectionContext>,
    group: &str,
    mark: &StationMark,
) -> Result<Vec<String>, KafkaError> {
    let list = consumer.fetch_group_list(Some(group), Duration::from_secs(5))?;
    let members = list.groups().iter().filter(|g| g.name() == group).flat_map(|g| g.members());
    Ok(other_station(members.map(|m| (m.client_id(), m.client_host())), mark))
}

/// Проверка группы выборов на экземпляры другой станции: своя отметка, куда сообщать и что уже сообщено.
pub struct StationCheck {
    mark: StationMark,
    events: EventSink,
    gauge: IntGauge,
    /// Последний сообщённый состав чужих экземпляров.
    reported: Vec<String>,
    /// Когда ошибка смешения в последний раз попала в журнал.
    last_log: Option<Instant>,
}

impl StationCheck {
    /// Проверка со своей отметкой; события — в журнал событий, число чужих экземпляров — в `gauge`.
    pub fn new(mark: StationMark, events: EventSink, gauge: IntGauge) -> Self {
        StationCheck { mark, events, gauge, reported: Vec::new(), last_log: None }
    }

    /// Результат проверки: метрика и `/api/ha` — всегда; ошибка в журнале — при изменении состава и раз в
    /// [`MIX_LOG_EVERY`]; событие — при изменении. `joining` — экземпляр ещё не в группе и вступать не будет.
    fn report(&mut self, leadership: &Leadership, group: &str, others: Vec<String>, joining: bool) {
        self.gauge.set(others.len() as i64);
        leadership.set_foreign_members(others.clone());
        let changed = others != self.reported;
        if others.is_empty() {
            if changed {
                info!("🛡 В группе выборов {group} больше нет экземпляров другой станции");
            }
            self.reported = others;
            return;
        }
        let consequence = if joining {
            format!(
                "экземпляр в группу не вступает и остаётся в резерве (повтор проверки через {} с)",
                REFUSE_RETRY.as_secs()
            )
        } else {
            "активный в группе один на все станции — у остальных телеметрия и команды остановлены".to_string()
        };
        let message = format!(
            "В группе выборов {group} экземпляры другой станции: {}; {consequence}. Станции на одном Kafka разводятся \
             своим топиком команд KAFKA_TOPICS_COMMANDS (от него зависит группа выборов) или своей GATEWAY_HA_GROUP_ID",
            others.join(", ")
        );
        if changed || self.last_log.is_none_or(|t| t.elapsed() >= MIX_LOG_EVERY) {
            error!("🛡 {message}");
            self.last_log = Some(Instant::now());
        }
        if changed {
            self.events.emit(Event::new("HA", "HotStandby", "ERROR", message).details(serde_json::json!({
                "instance": leadership.instance_id(), "group": group, "foreignMembers": others, "joining": joining,
            })));
        }
        self.reported = others;
    }
}

/// Контекст консьюмера выборов: колбэк ребаланса переводит роль в резерв ДО того, как партицию получит другой экземпляр.
struct ElectionContext {
    /// Роль экземпляра.
    leadership: Arc<Leadership>,
    /// Служебный топик выборов.
    topic: String,
}

impl ElectionContext {
    /// Есть ли в списке партиция выборов (нулевая партиция служебного топика).
    fn holds_leader_partition(&self, list: &TopicPartitionList) -> bool {
        list.elements().iter().any(|e| e.topic() == self.topic && e.partition() == 0)
    }
}

impl ClientContext for ElectionContext {}

impl ConsumerContext for ElectionContext {
    /// Потеря партиции — уходим в резерв ДО того, как её получит другой экземпляр (колбэк идёт внутри
    /// poll, до завершения ребаланса). Получение лидерства решает цикл выборов по `assignment()`.
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(list) if self.holds_leader_partition(list) => {
                self.leadership.set_active(false, "лидерство передано другому экземпляру");
            }
            Rebalance::Error(e) => {
                self.leadership.set_active(false, &format!("ошибка ребаланса: {e}"));
            }
            _ => {}
        }
    }
}

/// Запустить выборы. Для одиночного экземпляра (`enabled=false`) ничего не делает — он активен всегда.
/// Возвращает поток выборов; остановка — через `cancel` (сначала роль → резерв, потом выход из группы).
pub fn spawn(
    ha: HaSettings,
    kafka_settings: KafkaSettings,
    leadership: Arc<Leadership>,
    blind: Blind,
    check: StationCheck,
    cancel: CancellationToken,
) -> Option<std::thread::JoinHandle<()>> {
    if !ha.enabled {
        return None;
    }
    info!(
        "🛡 Горячий резерв: экземпляр {} (группа {}, топик {}, session {} мс, heartbeat {} мс)",
        ha.instance_id, ha.group_id, ha.topic, ha.session_timeout_ms, ha.heartbeat_interval_ms
    );
    let runtime = tokio::runtime::Handle::current();
    Some(
        std::thread::Builder::new()
            .name("ha-elector".into())
            .spawn(move || election_loop(ha, kafka_settings, leadership, blind, check, cancel, runtime))
            .expect("поток выборов"),
    )
}

/// «Слепота» шлюза: связь потеряна со всеми контроллерами (см. `App::all_links_down`).
pub type Blind = Arc<dyn Fn() -> bool + Send + Sync>;

/// Сколько непрерывно длится слепота.
#[derive(Default)]
struct BlindTracker {
    since: Option<Instant>,
}

impl BlindTracker {
    /// Сколько непрерывно длится слепота; любая связь с контроллером обнуляет отсчёт.
    fn update(&mut self, blind: bool, now: Instant) -> Duration {
        if !blind {
            self.since = None;
            return Duration::ZERO;
        }
        now.saturating_duration_since(*self.since.get_or_insert(now))
    }
}

/// Цикл выборов: сессия за сессией до остановки; ошибка — роль «резерв», пауза и повтор.
fn election_loop(
    ha: HaSettings,
    kafka_settings: KafkaSettings,
    leadership: Arc<Leadership>,
    blind: Blind,
    mut check: StationCheck,
    cancel: CancellationToken,
    runtime: tokio::runtime::Handle,
) {
    while !cancel.is_cancelled() {
        match run_session(&ha, &kafka_settings, &leadership, &blind, &mut check, &cancel, &runtime) {
            Ok(()) => {}
            Err(e) => {
                leadership.set_active(false, &format!("ошибка выборов: {e:#}"));
                error!(
                    "🛡 Выборы активного экземпляра прерваны: {e:#}. Проверьте брокер и настройки: session-timeout-ms ({}) \
                     должен быть в пределах group.min/max.session.timeout.ms брокера. Повтор через {} мс",
                    ha.session_timeout_ms,
                    RETRY_BACKOFF.as_millis()
                );
                sleep_or_cancel(&cancel, RETRY_BACKOFF);
            }
        }
    }
    leadership.set_active(false, "выборы остановлены");
}

/// Одна сессия в группе выборов. Возвращается, когда шлюз остановлен, лидерство передано партнёру (после паузы вне
/// группы) или в группе экземпляры другой станции (после паузы, не вступая); ошибка — `Err`, сессию пересоздаёт
/// [`election_loop`].
fn run_session(
    ha: &HaSettings,
    kafka_settings: &KafkaSettings,
    leadership: &Arc<Leadership>,
    blind: &Blind,
    check: &mut StationCheck,
    cancel: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> Result<()> {
    runtime.block_on(kafka::ensure_topic(kafka_settings, &ha.topic, 1));
    let consumer: BaseConsumer<ElectionContext> = kafka_settings
        .client_config()
        .set("group.id", &ha.group_id)
        .set("client.id", check.mark.client_id(&ha.instance_id))
        .set("session.timeout.ms", ha.session_timeout_ms.to_string())
        .set("heartbeat.interval.ms", ha.heartbeat_interval_ms.to_string())
        // Поток выборов опрашивает раз в 100 мс; если он завис на 30 с — пусть выпадет из группы и
        // отдаст лидерство (зависший экземпляр не должен оставаться активным).
        .set("max.poll.interval.ms", "30000")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set("allow.auto.create.topics", "false")
        .create_with_context(ElectionContext { leadership: leadership.clone(), topic: ha.topic.clone() })
        .context("не создан консьюмер выборов")?;
    // В группу, где уже есть экземпляры другой станции, не вступаем: работающая станция остаётся активной, а ошибка
    // настройки видна сразу. Состав не получен (брокер медлит) — вступаем как раньше, проверка повторится в группе.
    match other_station_members(&consumer, &ha.group_id, &check.mark) {
        Ok(others) if !others.is_empty() => {
            check.report(leadership, &ha.group_id, others, true);
            drop(consumer);
            sleep_or_cancel(cancel, REFUSE_RETRY);
            return Ok(());
        }
        Ok(others) => check.report(leadership, &ha.group_id, others, true),
        Err(e) => warn!("🛡 Состав группы выборов {} не получен ({e}) — вступаем без проверки станции", ha.group_id),
    }
    consumer.subscribe(&[&ha.topic]).context("подписка на служебный топик выборов")?;

    let mut last_error_log = Instant::now() - Duration::from_secs(60);
    let mut tracker = BlindTracker::default();
    let mut last_peer_check = Instant::now() - PEER_CHECK_EVERY;
    let mut last_members_check = Instant::now();
    while !cancel.is_cancelled() {
        // Другая станция могла вступить одновременно с нами или раньше, чем её экземпляр стал проверять группу.
        if last_members_check.elapsed() >= MEMBERS_CHECK_EVERY {
            last_members_check = Instant::now();
            if let Ok(others) = other_station_members(&consumer, &ha.group_id, &check.mark) {
                check.report(leadership, &ha.group_id, others, false);
            }
        }
        if let Some(Err(e)) = consumer.poll(POLL) {
            // Брокер недоступен — не фатально: librdkafka переподключается, а если сессия истекла,
            // сам снимет назначение, и мы уйдём в резерв (ниже).
            if is_fatal(&e) {
                return Err(e.into());
            }
            if last_error_log.elapsed() > Duration::from_secs(30) {
                last_error_log = Instant::now();
                warn!("🛡 Kafka (выборы): {e}");
            }
        }
        let owner = consumer
            .assignment()
            .map(|a| a.elements().iter().any(|e| e.topic() == ha.topic && e.partition() == 0))
            .unwrap_or(false);
        if owner {
            leadership.set_active(true, &format!("получено лидерство в группе {}", ha.group_id));
            let blind_for = tracker.update(!ha.yield_after.is_zero() && blind(), Instant::now());
            if !ha.yield_after.is_zero() && blind_for >= ha.yield_after && last_peer_check.elapsed() >= PEER_CHECK_EVERY
            {
                last_peer_check = Instant::now();
                if has_peer(&consumer, &ha.group_id) {
                    let reason = format!(
                        "нет связи ни с одним контроллером {} с — лидерство передано партнёру",
                        blind_for.as_secs()
                    );
                    warn!("🛡 {reason}");
                    leadership.set_active(false, &reason);
                    drop(consumer); // выход из группы: партнёр получает партицию сразу
                    sleep_or_cancel(cancel, ha.yield_after.max(MIN_YIELD_PAUSE));
                    return Ok(());
                }
            }
        } else {
            tracker.update(false, Instant::now());
            leadership.set_active(false, "партиция выборов у другого экземпляра или потеряна связь с брокером");
        }
    }
    // Штатная остановка: сначала перестаём публиковать, потом покидаем группу (drop → LeaveGroup) —
    // резерв получает лидерство без ожидания session-timeout.
    leadership.set_active(false, "остановка экземпляра");
    drop(consumer);
    Ok(())
}

/// В группе выборов есть ещё участник (кроме нас): иначе отдавать лидерство некому.
fn has_peer(consumer: &BaseConsumer<ElectionContext>, group: &str) -> bool {
    match consumer.fetch_group_list(Some(group), Duration::from_secs(5)) {
        Ok(list) => list.groups().iter().find(|g| g.name() == group).is_some_and(|g| g.members().len() >= 2),
        Err(e) => {
            warn!("🛡 Состав группы выборов не получен: {e}");
            false
        }
    }
}

/// Ошибка, после которой сессию надо пересоздавать (а не временная недоступность брокера, которую librdkafka переживает сама).
fn is_fatal(e: &KafkaError) -> bool {
    matches!(e, KafkaError::MessageConsumptionFatal(_) | KafkaError::ClientCreation(_))
}

/// Спать не дольше `d`, просыпаясь при остановке (поток выборов — обычный, не async).
fn sleep_or_cancel(cancel: &CancellationToken, d: Duration) {
    let until = Instant::now() + d;
    while Instant::now() < until && !cancel.is_cancelled() {
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Смена роли — в журнал (event_log) и, если экземпляр стал активным, в Kafka (`scada-events`):
/// монитор и оператор видят, что произошло переключение и кто теперь ведёт.
pub async fn record_events(leadership: Arc<Leadership>, events: crate::events::EventSink, cancel: CancellationToken) {
    if !leadership.is_ha_enabled() {
        return;
    }
    let mut rx = leadership.subscribe();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            changed = rx.changed() => if changed.is_err() { return },
        }
        let change = rx.borrow_and_update().clone();
        let me = leadership.instance_id();
        let message = if change.active {
            format!("Экземпляр {me} стал активным: {}", change.reason)
        } else {
            format!("Экземпляр {me} перешёл в резерв: {}", change.reason)
        };
        events.emit(
            crate::events::Event::new("HA", "HotStandby", if change.active { "INFO" } else { "WARNING" }, message).details(
                serde_json::json!({"instance": me, "role": if change.active { "ACTIVE" } else { "STANDBY" }, "reason": change.reason}),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blindness_is_measured_from_its_start_and_reset_by_any_sight() {
        let mut t = BlindTracker::default();
        let t0 = Instant::now();
        assert_eq!(t.update(false, t0), Duration::ZERO);
        assert_eq!(t.update(true, t0), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(7)), Duration::from_secs(7));
        // Хотя бы одна связь вернулась — отсчёт заново.
        assert_eq!(t.update(false, t0 + Duration::from_secs(8)), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(9)), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(12)), Duration::from_secs(3));
    }

    #[test]
    fn station_mark_travels_in_client_id() {
        let mark = StationMark::of(["Phoenix Contact", "PAC Savushkin", "Phoenix Contact"]);
        assert_eq!(mark.0.len(), 2, "повтор имени не считается");
        let id = mark.client_id("gw-a.st-1");
        assert!(id.starts_with("scada-gateway-ha-gw-a.st-1.st-"), "{id}");
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)), "{id}");
        assert_eq!(StationMark::parse(&id), Some(("gw-a.st-1", mark.clone())), "имя экземпляра с тем же разделителем");
        // Прежняя версия (без отметки) и чужие клиенты — не станция.
        for foreign in ["scada-gateway-ha-gw-a", "rdkafka", "scada-gateway-ha-x.st-zz", "scada-gateway-ha-x.st-12345"] {
            assert_eq!(StationMark::parse(foreign), None, "{foreign}");
        }
        // Большая станция: client.id ограничен.
        let names: Vec<String> = (0..500).map(|i| format!("Контроллер {i}")).collect();
        let big = StationMark::of(names.iter().map(String::as_str));
        assert_eq!(big.0.len(), MARK_MAX);
        assert!(big.client_id("gw").len() < 140);
        // Хеш не зависит от версии и машины (FNV-1a, контрольное значение для "a" — 0xe40c292c): отметка совпадает у
        // экземпляров пары разных версий.
        assert_eq!(name_hash("a"), 0x0c_292c);
    }

    #[test]
    fn other_station_is_one_without_a_common_controller() {
        let ours = StationMark::of(["Барановичи-1.BN1_MCA1", "WAGO"]);
        let peer = StationMark::of(["Барановичи-1.BN1_MCA1"]); // пара обновляется: контроллер убрали
        let other = StationMark::of(["Барановичи-1.BN1_NORM_TANKS"]);
        let members = [
            (ours.client_id("a1"), "/10.0.0.1"),
            (peer.client_id("a2"), "/10.0.0.2"),
            (other.client_id("b1"), "/10.0.0.3"),
            ("scada-gateway-ha-old".to_string(), "/10.0.0.4"),
        ];
        let found = other_station(members.iter().map(|(id, host)| (id.as_str(), *host)), &ours);
        assert_eq!(found, vec!["b1@10.0.0.3".to_string()]);
        assert!(!ours.is_other_station(&StationMark::default()), "станция без контроллеров ни с кем не сравнивается");
    }

    #[test]
    fn mixing_is_reported_once_per_change_and_cleared() {
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let (events, mut rx) = crate::events::channel(metrics.clone());
        let leadership = Leadership::standby("a1".into(), "g".into(), metrics.ha_gauge("a1"));
        let mut check = StationCheck::new(StationMark::of(["A"]), events, metrics.ha_foreign_members.clone());

        check.report(&leadership, "g", vec![], true);
        assert!(rx.try_recv().is_err(), "без смешения — тишина");
        check.report(&leadership, "g", vec!["b1@h".into()], true);
        let event = rx.try_recv().expect("событие");
        assert_eq!((event.event_type.as_str(), event.severity.as_str()), ("HA", "ERROR"));
        assert!(event.message.contains("b1@h") && event.message.contains("не вступает"), "{}", event.message);
        assert_eq!(metrics.ha_foreign_members.get(), 1);
        assert_eq!(leadership.foreign_members(), vec!["b1@h".to_string()]);
        check.report(&leadership, "g", vec!["b1@h".into()], true);
        assert!(rx.try_recv().is_err(), "тот же состав — без нового события");
        check.report(&leadership, "g", vec![], false);
        assert_eq!(metrics.ha_foreign_members.get(), 0);
        assert!(leadership.foreign_members().is_empty());
    }
}
