//! Роль экземпляра в паре горячего резервирования.
//!
//! Активный экземпляр — единственный, кто говорит с внешним миром: публикует телеметрию, события и
//! алармы в Kafka, пишет историю и исполняет команды. Резервный делает всё остальное (держит
//! соединения с ПЛК, опрашивает, следит за качеством), но наружу молчит — поэтому, став активным,
//! он выдаёт свежие значения уже на первом цикле. Выборы — [`crate::ha`].
//!
//! Без резервирования (`GATEWAY_HA_ENABLED=false`, по умолчанию) экземпляр всегда активный.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use prometheus::IntGauge;
use tokio::sync::watch;
use tracing::{info, warn};

/// Смена роли: новая роль и причина (для журнала).
#[derive(Debug, Clone, PartialEq)]
pub struct RoleChange {
    /// Новая роль: `true` — активный.
    pub active: bool,
    /// Причина смены (для журнала).
    pub reason: String,
}

/// Роль экземпляра и всё, что от неё зависит. Читается на горячем пути атомарно; смена роли — одна точка, [`Leadership::set_active`].
pub struct Leadership {
    enabled: bool,
    instance_id: String,
    group_id: String,
    active: AtomicBool,
    /// Растёт при каждом переходе в активную роль. Обработчики значений по нему узнают, что
    /// состояние фильтров публикации надо сбросить: прежний активный мог упасть, и новый обязан
    /// заново отправить значения всех тегов (иначе неизменившиеся молчали бы до полной отправки).
    activations: AtomicU64,
    since: Mutex<DateTime<Utc>>,
    /// Экземпляры другой станции, замеченные в группе выборов (для `/api/ha`); пусто — смешения нет.
    foreign: Mutex<Vec<String>>,
    tx: watch::Sender<RoleChange>,
    gauge: IntGauge,
}

impl Leadership {
    /// Экземпляр без резервирования: активный с самого начала.
    pub fn single(instance_id: String, gauge: IntGauge) -> Arc<Self> {
        Self::build(false, instance_id, String::new(), true, "резервирование выключено", gauge)
    }

    /// Экземпляр пары: резервный, пока выборы не отдадут ему лидерство.
    pub fn standby(instance_id: String, group_id: String, gauge: IntGauge) -> Arc<Self> {
        Self::build(true, instance_id, group_id, false, "ожидание выборов", gauge)
    }

    /// Общий конструктор: начальная роль, метрика, канал оповещений о смене роли.
    fn build(
        enabled: bool,
        instance_id: String,
        group_id: String,
        active: bool,
        reason: &str,
        gauge: IntGauge,
    ) -> Arc<Self> {
        gauge.set(i64::from(active));
        let (tx, _) = watch::channel(RoleChange { active, reason: reason.into() });
        Arc::new(Leadership {
            enabled,
            instance_id,
            group_id,
            active: AtomicBool::new(active),
            activations: AtomicU64::new(u64::from(active)),
            since: Mutex::new(Utc::now()),
            foreign: Mutex::new(Vec::new()),
            tx,
            gauge,
        })
    }

    /// true — этот экземпляр публикует в Kafka, пишет историю и исполняет команды.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Включено ли резервирование.
    pub fn is_ha_enabled(&self) -> bool {
        self.enabled
    }

    /// Имя экземпляра.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Группа выборов (пусто без резервирования).
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Сколько раз экземпляр становился активным: по росту счётчика обработчики значений узнают, что фильтры публикации надо сбросить.
    pub fn activations(&self) -> u64 {
        self.activations.load(Ordering::Acquire)
    }

    /// Когда роль менялась в последний раз.
    pub fn since(&self) -> DateTime<Utc> {
        *self.since.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Экземпляры другой станции в группе выборов (`экземпляр@хост`), по последней проверке.
    pub fn foreign_members(&self) -> Vec<String> {
        self.foreign.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Запомнить результат проверки группы выборов (см. [`crate::ha`]).
    pub fn set_foreign_members(&self, members: Vec<String>) {
        *self.foreign.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = members;
    }

    /// Подписка на смены роли (`watch`: сразу видно текущее значение).
    pub fn subscribe(&self) -> watch::Receiver<RoleChange> {
        self.tx.subscribe()
    }

    /// Единственная точка смены роли: лог, метка времени, метрика, оповещение подписчиков.
    /// Возвращает false, если роль не изменилась.
    pub fn set_active(&self, value: bool, reason: &str) -> bool {
        if self.active.swap(value, Ordering::AcqRel) == value {
            return false;
        }
        if value {
            self.activations.fetch_add(1, Ordering::AcqRel);
            warn!("🟢 Экземпляр {} АКТИВНЫЙ: {reason}", self.instance_id);
        } else {
            warn!("🟡 Экземпляр {} в РЕЗЕРВЕ: {reason}", self.instance_id);
        }
        *self.since.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Utc::now();
        self.gauge.set(i64::from(value));
        self.tx.send_replace(RoleChange { active: value, reason: reason.into() });
        info!("роль: {}", if value { "ACTIVE" } else { "STANDBY" });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge() -> IntGauge {
        IntGauge::new("test_active", "test").unwrap()
    }

    #[test]
    fn single_instance_is_always_active() {
        let l = Leadership::single("a".into(), gauge());
        assert!(l.is_active() && !l.is_ha_enabled());
        assert_eq!(l.activations(), 1);
    }

    #[test]
    fn standby_becomes_active_and_notifies_once_per_change() {
        let l = Leadership::standby("a".into(), "g".into(), gauge());
        let mut rx = l.subscribe();
        assert!(!l.is_active());

        assert!(l.set_active(true, "получено лидерство"));
        assert!(!l.set_active(true, "повтор"), "та же роль — не событие");
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update().clone(), RoleChange { active: true, reason: "получено лидерство".into() });
        assert_eq!((l.is_active(), l.activations()), (true, 1));

        assert!(l.set_active(false, "лидерство передано"));
        assert_eq!((l.is_active(), l.activations()), (false, 1));
        assert!(l.set_active(true, "снова"));
        assert_eq!(l.activations(), 2, "каждая активация — новая эпоха для сброса фильтров");
        assert_eq!(l.gauge.get(), 1);
    }
}
