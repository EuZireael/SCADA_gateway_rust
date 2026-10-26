//! Аварии прошивки, как их отдаёт OPC UA-фасад `ptusa-opcua`: список — JSON в узле `SYSTEM.ALARMS`, действие над аварией
//! — метод `SYSTEM.ALARM_CMD`. Нужны, чтобы проверять публикацию аварий и квитирование шлюзом без настоящей прошивки.
//!
//! Сообщение заводит запись числа в служебный узел `SYSTEM.ALARM_RAISE` (номер объекта): у объекта появляется сообщение
//! «ответ» (тип 102, номер — по порядку), как у ptusa, когда операция танка не включилась. Любая команда над сообщением
//! объекта удаляет его, как у ptusa; ACCEPT над несуществующим — молча ничего, тоже как у ptusa.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;

/// Тип объекта «танк» у ptusa: сообщения симулятора — от него.
pub const OBJECT_TYPE: i32 = 102;

/// Сообщение объекта в формате узла фасада.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Alarm {
    object_type: Option<i32>,
    object_number: i32,
    alarm_number: Option<i32>,
    critical: bool,
    description: String,
    group: &'static str,
    priority: i32,
    state: &'static str,
    suppressed: bool,
}

/// Список, как его сериализует фасад: счётчик изменений и сообщения.
#[derive(Serialize)]
struct List<'a> {
    id: i64,
    alarms: &'a [Alarm],
}

struct State {
    alarms: Vec<Alarm>,
    /// Счётчик изменений списка (`id` у ptusa).
    id: i64,
    /// Номер следующего сообщения (у ptusa — общий счётчик).
    next: i32,
}

/// Список аварий симулируемой прошивки.
pub struct Alarms {
    state: Mutex<State>,
    /// Список изменился, а в узел ещё не положен.
    dirty: AtomicBool,
}

impl Default for Alarms {
    fn default() -> Self {
        Alarms { state: Mutex::new(State { alarms: Vec::new(), id: 1, next: 1 }), dirty: AtomicBool::new(true) }
    }
}

impl Alarms {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Новое сообщение объекта `object_number`; номер сообщения.
    pub fn raise(&self, object_number: i32) -> i32 {
        let mut s = self.state();
        let number = s.next;
        s.next += 1;
        s.alarms.push(Alarm {
            object_type: Some(OBJECT_TYPE),
            object_number,
            alarm_number: Some(number),
            critical: false,
            description: format!("'Танк {object_number}' - не включена операция 1 'Мойка' (симулятор)"),
            group: "ответ",
            priority: 750,
            state: "ALARM",
            suppressed: false,
        });
        s.id += 1;
        self.dirty.store(true, Ordering::Release);
        number
    }

    /// Команда над сообщением (`ACCEPT`, `SUPPRESS`, `UNSUPPRESS`): сообщение объекта уходит. `false` — неизвестное действие.
    pub fn command(&self, action: &str, object_type: i32, object_number: i32, alarm_number: i32) -> bool {
        if !matches!(action, "ACCEPT" | "SUPPRESS" | "UNSUPPRESS") {
            return false;
        }
        let mut s = self.state();
        let before = s.alarms.len();
        s.alarms.retain(|a| {
            (a.object_type, a.object_number, a.alarm_number) != (Some(object_type), object_number, Some(alarm_number))
        });
        if s.alarms.len() != before {
            s.id += 1;
            self.dirty.store(true, Ordering::Release);
        }
        true
    }

    /// JSON списка, если он изменился с прошлого вызова.
    pub fn take_changed(&self) -> Option<String> {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return None;
        }
        let s = self.state();
        Some(serde_json::to_string(&List { id: s.id, alarms: &s.alarms }).expect("список аварий — JSON"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_are_raised_and_removed_by_any_command() {
        let a = Alarms::default();
        assert_eq!(a.take_changed().as_deref(), Some(r#"{"id":1,"alarms":[]}"#));
        assert_eq!(a.take_changed(), None, "не менялся");
        assert_eq!((a.raise(2), a.raise(3)), (1, 2));
        let json: serde_json::Value = serde_json::from_str(&a.take_changed().unwrap()).unwrap();
        assert_eq!(json["id"], 3);
        assert_eq!(json["alarms"][1]["objectNumber"], 3);
        assert_eq!(json["alarms"][1]["alarmNumber"], 2);
        assert!(a.command("SUPPRESS", OBJECT_TYPE, 2, 1));
        assert!(a.command("ACCEPT", OBJECT_TYPE, 2, 1), "уже нет — молча ничего");
        assert!(!a.command("DELETE", OBJECT_TYPE, 3, 2));
        let json: serde_json::Value = serde_json::from_str(&a.take_changed().unwrap()).unwrap();
        assert_eq!((json["id"].as_i64(), json["alarms"].as_array().unwrap().len()), (Some(4), 1));
    }
}
