//! Общее состояние процесса и состояние связи с контроллерами.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::config::Settings;
use crate::events::{Event, EventSink, TelemetrySink};
use crate::kafka::KafkaOut;
use crate::leadership::Leadership;
use crate::metrics::Metrics;
use crate::model::{Controller, Tag};
use crate::opcua::OpcConnection;
use crate::pac::PacConnection;
use crate::script::Scripts;

/// Связь ещё не проверялась (после запуска).
const LINK_UNKNOWN: u8 = 0;
/// Последнее чтение удалось.
const LINK_UP: u8 = 1;
/// Связь потеряна.
const LINK_DOWN: u8 = 2;

/// Контроллер + живое состояние связи + соединение для записи команд.
pub struct ControllerHandle {
    /// Контроллер и его теги (неизменяемая конфигурация).
    pub ctrl: Controller,
    /// Состояние связи: одна из `LINK_*`.
    link: AtomicU8,
    /// Момент последнего удачного чтения, мс от начала эпохи: по нему определяется «давно нет данных».
    last_good_ms: AtomicI64,
    /// Ошибок подряд: чтобы раз в 30 ошибок напоминать в журнале, пока контроллер недоступен.
    error_streak: AtomicU32,
    /// Текущая OPC UA-сессия (её же использует запись команд).
    pub opc: Mutex<Option<Arc<OpcConnection>>>,
    /// Соединение PAC: команда идёт по тому же каналу, что и опрос (как у драйвера).
    pub pac: tokio::sync::Mutex<Option<PacConnection>>,
    /// «Опроси сейчас» после удачной записи (см. GatewaySettings::poll_after_write). Notify хранит
    /// одно разрешение: пачка записей между опросами даёт один внеочередной опрос, а не по одному на каждую.
    poll_now: tokio::sync::Notify,
}

impl ControllerHandle {
    /// Обработчик контроллера: связь «неизвестна», серия ошибок нулевая.
    pub fn new(ctrl: Controller) -> Self {
        ControllerHandle {
            ctrl,
            link: AtomicU8::new(LINK_UNKNOWN),
            last_good_ms: AtomicI64::new(now_ms()),
            error_streak: AtomicU32::new(0),
            opc: Mutex::new(None),
            pac: tokio::sync::Mutex::new(None),
            poll_now: tokio::sync::Notify::new(),
        }
    }

    /// Попросить цикл опроса прочитать контроллер вне расписания.
    pub fn request_poll(&self) {
        self.poll_now.notify_one();
    }

    /// Дождаться просьбы о внеочередном опросе и выдержать `delay`. Для выключенной функции
    /// (`None`) не завершается никогда — цикл живёт по своему таймеру.
    pub async fn poll_requested(&self, delay: Option<Duration>) {
        let Some(delay) = delay else { return std::future::pending().await };
        self.poll_now.notified().await;
        tokio::time::sleep(delay).await;
    }

    /// Связь потеряна (не «ещё не устанавливалась»).
    pub fn is_down(&self) -> bool {
        self.link.load(Ordering::Relaxed) == LINK_DOWN
    }

    /// Связь установлена (последнее чтение удалось).
    pub fn is_connected(&self) -> bool {
        self.link.load(Ordering::Relaxed) == LINK_UP
    }

    /// Не было удачных чтений дольше `after` — связь считаем мёртвой.
    pub fn is_stale(&self, after: Duration) -> bool {
        now_ms() - self.last_good_ms.load(Ordering::Relaxed) > after.as_millis() as i64
    }

    /// Отсчёт «свежести» с текущего момента (после подключения, до первого чтения).
    pub fn touch(&self) {
        self.last_good_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Текущая OPC UA-сессия, если она есть (её же использует запись команд).
    pub fn opc_connection(&self) -> Option<Arc<OpcConnection>> {
        self.opc.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Запомнить новую сессию или `None` после её закрытия.
    pub fn set_opc_connection(&self, conn: Option<Arc<OpcConnection>>) {
        *self.opc.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = conn;
    }
}

/// Текущее время, мс от начала эпохи.
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Где искать тег для команды.
pub struct TagRef {
    /// Найденный тег.
    pub tag: Arc<Tag>,
    /// Контроллер, которому тег принадлежит.
    pub controller: Arc<ControllerHandle>,
}

/// Состояние процесса, общее для всех задач: настройки, выходы наружу (Kafka, БД, события), контроллеры, роль в паре, скрипты.
pub struct App {
    /// Настройки процесса.
    pub settings: Settings,
    /// Метрики Prometheus.
    pub metrics: Arc<Metrics>,
    /// Продюсер Kafka; `None`, если Kafka выключена.
    pub kafka: Option<Arc<KafkaOut>>,
    /// Отправитель событий журнала.
    pub events: EventSink,
    /// Очередь строк истории; `None`, если история не пишется.
    pub telemetry: Option<TelemetrySink>,
    /// Пул БД; `None` без БД.
    pub db: Option<PgPool>,
    /// Включённые контроллеры в порядке конфигурации.
    pub controllers: Vec<Arc<ControllerHandle>>,
    /// Роль в паре горячего резерва (без резервирования — всегда активный).
    pub leadership: Arc<Leadership>,
    /// Пользовательские Lua-скрипты обработки значений.
    pub scripts: Arc<Scripts>,
    /// Тег и номер его контроллера по имени (команды монитора).
    by_name: HashMap<String, (Arc<Tag>, usize)>,
    /// То же по номеру в БД.
    by_id: HashMap<i64, (Arc<Tag>, usize)>,
    /// Момент запуска (для `uptime`).
    pub started: Instant,
}

/// Всё, что шлюз собирает до создания [`App`]: выходы в Kafka и БД, роль в паре, скрипты.
pub struct AppDeps {
    /// Метрики.
    pub metrics: Arc<Metrics>,
    /// Продюсер Kafka.
    pub kafka: Option<Arc<KafkaOut>>,
    /// Отправитель событий.
    pub events: EventSink,
    /// Очередь истории.
    pub telemetry: Option<TelemetrySink>,
    /// Пул БД.
    pub db: Option<PgPool>,
    /// Роль экземпляра в паре.
    pub leadership: Arc<Leadership>,
    /// Пользовательские скрипты.
    pub scripts: Arc<Scripts>,
}

impl App {
    /// Собрать состояние: обработчики контроллеров и индексы тегов по имени и по номеру (для команд).
    pub fn new(settings: Settings, deps: AppDeps, controllers: Vec<Controller>) -> Self {
        let AppDeps { metrics, kafka, events, telemetry, db, leadership, scripts } = deps;
        let controllers: Vec<Arc<ControllerHandle>> =
            controllers.into_iter().map(|c| Arc::new(ControllerHandle::new(c))).collect();
        let mut by_name = HashMap::new();
        let mut by_id = HashMap::new();
        for (idx, h) in controllers.iter().enumerate() {
            for tag in &h.ctrl.tags {
                by_name.insert(tag.name.clone(), (tag.clone(), idx));
                if tag.id > 0 {
                    by_id.insert(tag.id, (tag.clone(), idx));
                }
            }
        }
        metrics.controllers_total.set(controllers.len() as i64);
        App {
            settings,
            metrics,
            kafka,
            events,
            telemetry,
            db,
            controllers,
            leadership,
            scripts,
            by_name,
            by_id,
            started: Instant::now(),
        }
    }

    /// Тег по имени (так адресует команды монитор).
    pub fn tag_by_name(&self, name: &str) -> Option<TagRef> {
        self.by_name.get(name).map(|(t, i)| TagRef { tag: t.clone(), controller: self.controllers[*i].clone() })
    }

    /// Тег по номеру в БД (так адресует команды старый монитор); теги без БД имеют номер 0 и не находятся.
    pub fn tag_by_id(&self, id: i64) -> Option<TagRef> {
        self.by_id.get(&id).map(|(t, i)| TagRef { tag: t.clone(), controller: self.controllers[*i].clone() })
    }

    /// Число включённых тегов станции.
    pub fn tag_count(&self) -> usize {
        self.by_name.len()
    }

    /// Удачное чтение: при переходе BAD→GOOD — событие CONNECTED.
    pub fn mark_up(&self, h: &ControllerHandle) {
        h.touch();
        h.error_streak.store(0, Ordering::Relaxed);
        let was = h.link.swap(LINK_UP, Ordering::Relaxed);
        if was != LINK_UP {
            let restored = was == LINK_DOWN;
            info!("🟢 {}: связь {}", h.ctrl.name, if restored { "восстановлена" } else { "установлена" });
            self.events.emit(
                Event::new(
                    "CONNECTION",
                    "Gateway",
                    "INFO",
                    format!("Связь с {} {}", h.ctrl.name, if restored { "восстановлена" } else { "установлена" }),
                )
                .controller(h.ctrl.id)
                .details(json!({"controller": h.ctrl.name, "state": "CONNECTED",
                                "note": if restored { "link restored" } else { "initial connect" }})),
            );
            self.update_connected_gauge();
        }
    }

    /// Ошибка связи: первый переход — событие DISCONNECTED, дальше лог раз в 30 ошибок.
    pub fn mark_down(&self, h: &ControllerHandle, reason: &str) {
        let streak = h.error_streak.fetch_add(1, Ordering::Relaxed) + 1;
        let was = h.link.swap(LINK_DOWN, Ordering::Relaxed);
        if was != LINK_DOWN {
            warn!("🔴 Потеряна связь: {} ({reason})", h.ctrl.name);
            self.events.emit(
                Event::new("CONNECTION", "Gateway", "WARNING", format!("Потеряна связь с {}: {reason}", h.ctrl.name))
                    .controller(h.ctrl.id)
                    .details(json!({"controller": h.ctrl.name, "state": "DISCONNECTED", "reason": reason})),
            );
            self.update_connected_gauge();
        } else if streak.is_multiple_of(30) {
            warn!("🔴 {} всё ещё недоступен ({streak} ошибок подряд)", h.ctrl.name);
        }
    }

    /// Шлюз «слепой»: есть контроллеры, и связь потеряна со всеми (ни с одним нет данных).
    pub fn all_links_down(&self) -> bool {
        !self.controllers.is_empty() && self.controllers.iter().all(|c| c.is_down())
    }

    /// Пересчитать метрику «контроллеров на связи».
    fn update_connected_gauge(&self) {
        let up = self.controllers.iter().filter(|c| c.is_connected()).count();
        self.metrics.controllers_connected.set(up as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ControllerKind;

    fn handle() -> ControllerHandle {
        ControllerHandle::new(Controller {
            id: 0,
            name: "T".into(),
            endpoint: "pac://127.0.0.1:1".into(),
            kind: ControllerKind::Pac,
            enabled: true,
            tags: Vec::new(),
            opc_security: Default::default(),
        })
    }

    #[tokio::test]
    async fn write_triggers_one_early_poll_and_disabled_never_fires() {
        let h = handle();
        // Две записи до опроса — одно разрешение: первый вызов проходит, второй ждёт новой записи.
        h.request_poll();
        h.request_poll();
        let wait = Some(Duration::from_millis(10));
        assert!(tokio::time::timeout(Duration::from_secs(1), h.poll_requested(wait)).await.is_ok());
        assert!(tokio::time::timeout(Duration::from_millis(100), h.poll_requested(wait)).await.is_err());
        // Выключено — цикл живёт только по таймеру, даже если запись была.
        h.request_poll();
        assert!(tokio::time::timeout(Duration::from_millis(100), h.poll_requested(None)).await.is_err());
    }
}
