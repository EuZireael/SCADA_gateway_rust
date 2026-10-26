//! Аварии прошивки: ответ GET_PAC_ERRORS (103) → список для узла `SYSTEM.ALARMS`, действие клиента → строка
//! SET_PAC_ERROR_CMD (104).
//!
//! Ответ — Lua-текст (сверено с ptusa: `g_device.cpp`, `g_errors.cpp`, `PAC_err.cpp`):
//!
//! ```lua
//! alarms[ 0 ] =
//!   {}alarms[ 0 ] =
//!   {    {
//!     description="'Танк 2' - не включена операция 1 'Мойка' - нет сигнала …",
//!     group="ответ",
//! priority=750,state=1,
//! type=4,
//! id_n=2,
//! id_object_alarm_number=3,
//! id_type=102,
//! suppress=false
//! },
//!   id = 50,
//!   }
//! ```
//!
//! Таблица присваивается дважды, поэтому ответ исполняется настоящим Lua (в песочнице, в своём стейте), а не
//! разбирается построчно. Пока у прошивки есть критические ошибки, приходят только они и в другом виде: состояние и тип —
//! константами (`state = AS_ALARM`, `type = AT_SPECIAL`), нет `id_type`, `id_object_alarm_number` и `suppress`.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use mlua::{Lua, Table, Value};
use serde::Serialize;

use crate::sandbox;

/// Потолок памяти стейта: список аварий — единицы КБ, но распакованный ответ бывает до 8 МБ.
pub const MEMORY_LIMIT: usize = 32 * 1024 * 1024;
/// Время на разбор одного ответа.
pub const TIME_BUDGET: Duration = Duration::from_secs(1);

/// Константы, которые прошивка пишет в критических ошибках словами (`g_errors.h`), и пустая `alarms` для ответа.
const PRELUDE: &str = "AS_NORMAL, AS_ALARM, AS_RETURN, AS_ACCEPT = 0, 1, 2, 3\n\
    AT_DISCRETE, AT_VALUE, AT_DEVIATION, AT_RATE_OF_CHANGE, AT_SPECIAL = 0, 1, 2, 3, 4\n\
    alarms = {}\n";

/// Состояние аварии (`AS_*`; `AS_NORMAL` в список не попадает).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum State {
    /// Есть и не квитирована.
    Alarm,
    /// Причина ушла, авария не квитирована.
    Return,
    /// Квитирована, причина есть.
    Accept,
}

/// Одна авария списка.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Alarm {
    /// Тип объекта (`id_type`): у объектов ptusa ≥ 100, у устройств — тип устройства; у критических нет.
    pub object_type: Option<i64>,
    /// Номер объекта, серийный номер устройства или параметр критической ошибки (`id_n`).
    pub object_number: i64,
    /// Номер сообщения внутри объекта (`id_object_alarm_number`; у устройств отрицательный); у критических нет.
    pub alarm_number: Option<i64>,
    /// Критическая ошибка прошивки (командой не квитируется).
    pub critical: bool,
    pub description: String,
    pub group: String,
    pub priority: i64,
    pub state: State,
    pub suppressed: bool,
}

/// Список аварий прошивки.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Alarms {
    /// Счётчик изменений списка у прошивки (после её перезапуска начинается заново).
    pub id: i64,
    pub alarms: Vec<Alarm>,
}

/// Lua-стейт ответов 103 в песочнице.
pub struct AlarmsReader {
    lua: Lua,
    budget: Duration,
}

impl AlarmsReader {
    /// Стейт с потолками памяти и времени по умолчанию.
    pub fn new() -> Result<Self> {
        Ok(AlarmsReader { lua: sandbox::new_lua(MEMORY_LIMIT)?, budget: TIME_BUDGET })
    }

    /// Исполнить ответ 103 и собрать список. Ошибка — ответ не Lua, исчерпаны лимиты или записи не того вида.
    pub fn parse(&self, text: &str) -> Result<Alarms> {
        sandbox::run_limited(&self.lua, self.budget, || {
            self.lua.load(PRELUDE).exec()?;
            self.lua.load(text).exec()
        })?;
        let alarms: Table = self.lua.globals().get("alarms")?;
        let list: Table = alarms.get::<Option<Table>>(0)?.context("в ответе нет alarms[0]")?;
        let id = int(&list, "id")?.context("нет id")?;
        let mut out = Vec::new();
        for (i, item) in list.sequence_values::<Table>().enumerate() {
            let parsed = item.map_err(anyhow::Error::from).and_then(|t| alarm(&t));
            if let Some(a) = parsed.with_context(|| format!("запись {}", i + 1))? {
                out.push(a);
            }
        }
        Ok(Alarms { id, alarms: out })
    }
}

/// Запись списка; `None` — `AS_NORMAL` (в список не попадает).
fn alarm(t: &Table) -> Result<Option<Alarm>> {
    let state = match int(t, "state")?.context("нет state")? {
        0 => return Ok(None),
        1 => State::Alarm,
        2 => State::Return,
        3 => State::Accept,
        other => bail!("неизвестное состояние {other}"),
    };
    let object_type = int(t, "id_type")?;
    Ok(Some(Alarm {
        object_type,
        object_number: int(t, "id_n")?.context("нет id_n")?,
        alarm_number: int(t, "id_object_alarm_number")?,
        critical: object_type.is_none(),
        description: text(t, "description")?,
        group: text(t, "group")?,
        priority: int(t, "priority")?.context("нет priority")?,
        state,
        suppressed: t.get::<Option<bool>>("suppress")?.unwrap_or(false),
    }))
}

/// Целое поле (Lua 5.1 хранит числа как double); `None` — поля нет.
fn int(t: &Table, field: &str) -> Result<Option<i64>> {
    match t.get::<Value>(field)? {
        Value::Nil => Ok(None),
        Value::Integer(i) => Ok(Some(i)),
        Value::Number(n) if n.fract() == 0.0 && n.abs() <= f64::from(i32::MAX) => Ok(Some(n as i64)),
        other => bail!("{field}: не целое ({other:?})"),
    }
}

/// Строковое поле; нет поля — пустая строка.
fn text(t: &Table, field: &str) -> Result<String> {
    Ok(t.get::<Option<mlua::LuaString>>(field)?.map(|s| s.to_string_lossy()).unwrap_or_default())
}

/// Действие клиента над аварией — команда `set_cmd` прошивки (`base_error::COMMANDS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Accept = 100,
    Suppress = 200,
    Unsuppress = 201,
}

impl Action {
    /// `ACCEPT`, `SUPPRESS`, `UNSUPPRESS`.
    pub fn parse(s: &str) -> Option<Action> {
        match s {
            "ACCEPT" => Some(Action::Accept),
            "SUPPRESS" => Some(Action::Suppress),
            "UNSUPPRESS" => Some(Action::Unsuppress),
            _ => None,
        }
    }
}

/// Строка команды 104. Только целые: текст клиента в Lua прошивки не попадает. Двоеточие перед `get_instance`
/// обязательно — статический метод tolua ждёт класс первым аргументом.
pub fn command_text(action: Action, object_type: i32, object_number: i32, alarm_number: i32) -> String {
    format!(
        "errors_manager:get_instance():set_cmd( {}, {object_type}, {object_number}, {alarm_number} )",
        action as u16
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ответ эмулятора прошивки с сообщением танка (как пришёл, с двойным присваиванием).
    const OBJECT: &str = "alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {\t{\n\tdescription=\"'Танк 2' - не включена операция 1 \
        'Мойка' - нет сигнала 'MCA4LINE1DI1 (Линия свободна)'.\",\n\tgroup=\"ответ\",\npriority=750,state=1,\ntype=4,\n\
        id_n=2,\nid_object_alarm_number=3,\nid_type=102,\nsuppress=false\n},\n  id = 50,\n  }\n";

    /// Ошибка устройства и критическая (их прошивка присылает словами-константами).
    const DEVICE: &str = "alarms[ 0 ] =\n  {\n{\ndescription=\"LINE1V0 - обратная связь\",\npriority=250,\nstate=3,\n\
        type=4,\ngroup=\"тревога\",\nid_n=7,\nid_object_alarm_number=-1,\nid_type=0,\nsuppress=true\n},\n  id = 3,\n  }\n";
    const CRITICAL: &str = "alarms[ 0 ] =\n  {\n\t{\n\tdescription = \"Нет связи с узлом 1\",\n\ttype = AT_SPECIAL,\n\
        \tgroup = 'тревога',\n\tpriority = 100,\n\tstate = AS_ALARM,\n\tid_n = 1,\n\t},\n  id = 7,\n  }\n";

    #[test]
    fn object_message_is_read() {
        let list = AlarmsReader::new().unwrap().parse(OBJECT).unwrap();
        assert_eq!(list.id, 50);
        assert_eq!(
            list.alarms,
            [Alarm {
                object_type: Some(102),
                object_number: 2,
                alarm_number: Some(3),
                critical: false,
                description: "'Танк 2' - не включена операция 1 'Мойка' - нет сигнала 'MCA4LINE1DI1 (Линия свободна)'."
                    .into(),
                group: "ответ".into(),
                priority: 750,
                state: State::Alarm,
                suppressed: false,
            }]
        );
    }

    #[test]
    fn device_and_critical_errors_are_read() {
        let reader = AlarmsReader::new().unwrap();
        let device = &reader.parse(DEVICE).unwrap().alarms[0];
        assert_eq!((device.object_type, device.object_number, device.alarm_number), (Some(0), 7, Some(-1)));
        assert_eq!((device.state, device.suppressed, device.critical), (State::Accept, true, false));
        let critical = &reader.parse(CRITICAL).unwrap().alarms[0];
        assert_eq!((critical.object_type, critical.object_number, critical.alarm_number), (None, 1, None));
        assert_eq!((critical.state, critical.priority, critical.critical), (State::Alarm, 100, true));
        assert_eq!((critical.group.as_str(), critical.suppressed), ("тревога", false));
    }

    #[test]
    fn empty_list_and_the_next_answer_replaces_the_previous() {
        let reader = AlarmsReader::new().unwrap();
        assert_eq!(reader.parse(OBJECT).unwrap().alarms.len(), 1);
        let empty = reader.parse("alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {  id = 46,\n  }\n").unwrap();
        assert_eq!(empty, Alarms { id: 46, alarms: vec![] });
    }

    #[test]
    fn normal_state_is_skipped_and_bad_records_are_errors() {
        let reader = AlarmsReader::new().unwrap();
        let normal =
            "alarms[0] = { {description=\"x\", group=\"g\", priority=250, state=0, id_n=1, id_type=0}, id = 1 }";
        assert!(reader.parse(normal).unwrap().alarms.is_empty());
        for (text, why) in [
            ("alarms[0] = { {description=\"x\", priority=1, state=9, id_n=1}, id = 1 }", "неизвестное состояние"),
            ("alarms[0] = { {description=\"x\", priority=1, state=1, id_n=1.5}, id = 1 }", "не целое"),
            ("alarms[0] = { {description=\"x\", priority=1, state=1}, id = 1 }", "нет id_n"),
            ("alarms[0] = { }", "нет id"),
            ("x = 1", "нет alarms[0]"),
        ] {
            let err = format!("{:#}", reader.parse(text).unwrap_err());
            assert!(err.contains(why), "{text}: {err}");
        }
    }

    #[test]
    fn unescaped_quote_in_a_description_breaks_the_whole_answer() {
        // Прошивка не экранирует описания: такой ответ — не Lua, и список целиком не разобран.
        let broken = OBJECT.replace("'Мойка'", "\"Мойка\"");
        assert!(AlarmsReader::new().unwrap().parse(&broken).is_err());
    }

    #[test]
    fn hostile_answer_is_contained() {
        sandbox::within(20, || {
            let reader = AlarmsReader::new().unwrap();
            assert!(reader.parse("while true do end").is_err());
            assert!(reader.parse("x = os.execute('true')").is_err());
            assert!(reader.parse("s = string.rep('x', 64 * 1024 * 1024)").is_err());
            assert_eq!(reader.parse(OBJECT).unwrap().id, 50, "после ошибки стейт годен");
        });
    }

    #[test]
    fn list_is_serialized_for_the_node() {
        let list = AlarmsReader::new().unwrap().parse(CRITICAL).unwrap();
        assert_eq!(
            serde_json::to_string(&list).unwrap(),
            r#"{"id":7,"alarms":[{"objectType":null,"objectNumber":1,"alarmNumber":null,"critical":true,"description":"Нет связи с узлом 1","group":"тревога","priority":100,"state":"ALARM","suppressed":false}]}"#
        );
    }

    #[test]
    fn command_text_has_only_numbers() {
        assert_eq!(Action::parse("ACCEPT"), Some(Action::Accept));
        assert_eq!(Action::parse("UNSUPPRESS"), Some(Action::Unsuppress));
        assert_eq!(Action::parse("accept"), None);
        assert_eq!(command_text(Action::Accept, 102, 2, 3), "errors_manager:get_instance():set_cmd( 100, 102, 2, 3 )");
        assert_eq!(command_text(Action::Suppress, 0, 7, -1), "errors_manager:get_instance():set_cmd( 200, 0, 7, -1 )");
    }
}
