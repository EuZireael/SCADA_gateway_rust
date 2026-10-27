//! Аварии прошивки по driver-master: ответ GET_PAC_ERRORS (103) → записи списка, действие над аварией → строка
//! SET_PAC_ERROR_CMD (104). Разбор тот же, что у OPC UA-фасада (`ptusa-opcua/src/alarms.rs`), снимок и команду ведёт
//! [`crate::pac_alarms`].
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
//! Таблица присваивается дважды, поэтому ответ исполняется настоящим Lua (в песочнице, в своём стейте — не в стейте
//! снимка приборов), а не разбирается построчно. Пока у прошивки есть критические ошибки, приходят только они и в другом
//! виде: состояние и тип — константами (`state = AS_ALARM`, `type = AT_SPECIAL`), нет `id_type`,
//! `id_object_alarm_number` и `suppress`.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use mlua::{Lua, Table, Value};

use crate::pac_alarms::Entry;
use crate::sandbox;

/// Потолок памяти стейта: список аварий — единицы КБ, но распакованный ответ бывает до 8 МБ.
pub const MEMORY_LIMIT: usize = 32 * 1024 * 1024;
/// Время на разбор одного ответа.
pub const TIME_BUDGET: Duration = Duration::from_secs(1);

/// Константы, которые прошивка пишет в критических ошибках словами (`g_errors.h`), и пустая `alarms` для ответа.
const PRELUDE: &str = "AS_NORMAL, AS_ALARM, AS_RETURN, AS_ACCEPT = 0, 1, 2, 3\n\
    AT_DISCRETE, AT_VALUE, AT_DEVIATION, AT_RATE_OF_CHANGE, AT_SPECIAL = 0, 1, 2, 3, 4\n\
    alarms = {}\n";

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

    /// Исполнить ответ 103: счётчик изменений списка у прошивки (`id`) и записи. Ошибка — ответ не Lua, исчерпаны
    /// лимиты или записи не того вида.
    pub fn parse(&self, text: &str) -> Result<(i64, Vec<Entry>)> {
        sandbox::run_limited(&self.lua, self.budget, || {
            self.lua.load(PRELUDE).exec()?;
            self.lua.load(text).exec()
        })?;
        let alarms: Table = self.lua.globals().get("alarms")?;
        let list: Table = alarms.get::<Option<Table>>(0)?.context("в ответе нет alarms[0]")?;
        let id = int(&list, "id")?.context("нет id")?;
        let mut out = Vec::new();
        for (i, item) in list.sequence_values::<Table>().enumerate() {
            let parsed = item.map_err(anyhow::Error::from).and_then(|t| entry(&t));
            if let Some(e) = parsed.with_context(|| format!("запись {}", i + 1))? {
                out.push(e);
            }
        }
        Ok((id, out))
    }
}

/// Запись списка; `None` — `AS_NORMAL` (в список не попадает).
fn entry(t: &Table) -> Result<Option<Entry>> {
    let state = match int(t, "state")?.context("нет state")? {
        0 => return Ok(None),
        1 => "ALARM",
        2 => "RETURN",
        3 => "ACCEPT",
        other => bail!("неизвестное состояние {other}"),
    };
    let object_type = int(t, "id_type")?;
    Ok(Some(Entry {
        object_type,
        object_number: int(t, "id_n")?.context("нет id_n")?,
        alarm_number: int(t, "id_object_alarm_number")?,
        critical: object_type.is_none(),
        description: text(t, "description")?,
        group: text(t, "group")?,
        priority: int(t, "priority")?.context("нет priority")?,
        state: state.into(),
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

/// Код действия `set_cmd` прошивки (`base_error::COMMANDS`): `ACCEPT` → 100, `SUPPRESS` → 200, `UNSUPPRESS` → 201.
pub fn action_code(action: &str) -> Option<u16> {
    match action {
        "ACCEPT" => Some(100),
        "SUPPRESS" => Some(200),
        "UNSUPPRESS" => Some(201),
        _ => None,
    }
}

/// Строка команды 104. Только целые: текст команды монитора в Lua прошивки не попадает. Двоеточие перед `get_instance`
/// обязательно — статический метод tolua ждёт класс первым аргументом.
pub fn command_text(code: u16, object_type: i32, object_number: i32, alarm_number: i32) -> String {
    format!("errors_manager:get_instance():set_cmd( {code}, {object_type}, {object_number}, {alarm_number} )")
}

/// Ответы 103 для тестов: как их присылает эмулятор прошивки.
#[cfg(test)]
pub mod samples {
    /// Сообщение танка (как пришло, с двойным присваиванием).
    pub const OBJECT: &str = "alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {\t{\n\tdescription=\"'Танк 2' - не включена операция 1 \
        'Мойка' - нет сигнала 'MCA4LINE1DI1 (Линия свободна)'.\",\n\tgroup=\"ответ\",\npriority=750,state=1,\ntype=4,\n\
        id_n=2,\nid_object_alarm_number=3,\nid_type=102,\nsuppress=false\n},\n  id = 50,\n  }\n";
    /// Ошибка устройства (квитирована, подавлена).
    pub const DEVICE: &str = "alarms[ 0 ] =\n  {\n{\ndescription=\"LINE1V0 - обратная связь\",\npriority=250,\nstate=3,\n\
        type=4,\ngroup=\"тревога\",\nid_n=7,\nid_object_alarm_number=-1,\nid_type=0,\nsuppress=true\n},\n  id = 3,\n  }\n";
    /// Критическая ошибка (константы словами, одинарные кавычки, без `id_type`).
    pub const CRITICAL: &str = "alarms[ 0 ] =\n  {\n\t{\n\tdescription = \"Нет связи с узлом 1\",\n\ttype = AT_SPECIAL,\n\
        \tgroup = 'тревога',\n\tpriority = 100,\n\tstate = AS_ALARM,\n\tid_n = 1,\n\t},\n  id = 7,\n  }\n";
}

#[cfg(test)]
mod tests {
    use super::samples::*;
    use super::*;

    #[test]
    fn object_message_is_read() {
        let (id, list) = AlarmsReader::new().unwrap().parse(OBJECT).unwrap();
        assert_eq!(id, 50);
        assert_eq!(
            list,
            [Entry {
                object_type: Some(102),
                object_number: 2,
                alarm_number: Some(3),
                critical: false,
                description: "'Танк 2' - не включена операция 1 'Мойка' - нет сигнала 'MCA4LINE1DI1 (Линия свободна)'."
                    .into(),
                group: "ответ".into(),
                priority: 750,
                state: "ALARM".into(),
                suppressed: false,
            }]
        );
    }

    #[test]
    fn device_and_critical_errors_are_read() {
        let reader = AlarmsReader::new().unwrap();
        let device = &reader.parse(DEVICE).unwrap().1[0];
        assert_eq!((device.object_type, device.object_number, device.alarm_number), (Some(0), 7, Some(-1)));
        assert_eq!((device.state.as_str(), device.suppressed, device.critical), ("ACCEPT", true, false));
        let critical = &reader.parse(CRITICAL).unwrap().1[0];
        assert_eq!((critical.object_type, critical.object_number, critical.alarm_number), (None, 1, None));
        assert_eq!((critical.state.as_str(), critical.priority, critical.critical), ("ALARM", 100, true));
        assert_eq!((critical.group.as_str(), critical.suppressed), ("тревога", false));
    }

    #[test]
    fn empty_list_and_the_next_answer_replaces_the_previous() {
        let reader = AlarmsReader::new().unwrap();
        assert_eq!(reader.parse(OBJECT).unwrap().1.len(), 1);
        assert_eq!(reader.parse("alarms[ 0 ] = \n  {}alarms[ 0 ] = \n  {  id = 46,\n  }\n").unwrap(), (46, vec![]));
    }

    #[test]
    fn normal_state_is_skipped_and_bad_records_are_errors() {
        let reader = AlarmsReader::new().unwrap();
        let normal =
            "alarms[0] = { {description=\"x\", group=\"g\", priority=250, state=0, id_n=1, id_type=0}, id = 1 }";
        assert!(reader.parse(normal).unwrap().1.is_empty());
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
            assert_eq!(reader.parse(OBJECT).unwrap().0, 50, "после ошибки стейт годен");
        });
    }

    #[test]
    fn command_text_has_only_numbers() {
        assert_eq!(action_code("ACCEPT"), Some(100));
        assert_eq!(action_code("UNSUPPRESS"), Some(201));
        assert_eq!(action_code("accept"), None);
        assert_eq!(command_text(100, 102, 2, 3), "errors_manager:get_instance():set_cmd( 100, 102, 2, 3 )");
        assert_eq!(command_text(200, 0, 7, -1), "errors_manager:get_instance():set_cmd( 200, 0, 7, -1 )");
    }
}
