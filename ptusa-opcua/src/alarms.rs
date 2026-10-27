//! Аварии прошивки: ответ GET_PAC_ERRORS (103) → список для узла `SYSTEM.ALARMS`, действие клиента → строка
//! SET_PAC_ERROR_CMD (104). Формат ответа, разбор и строка команды — общие со шлюзом (крейт [`ptusa_alarms`]); здесь —
//! песочница, в которой ответ исполняется (в своём стейте, не в стейте снимка приборов).

use std::time::Duration;

use anyhow::Result;
use mlua::Lua;
pub use ptusa_alarms::{Action, Alarm, Alarms, State, command_text};

use crate::sandbox;

/// Lua-стейт ответов 103 в песочнице.
pub struct AlarmsReader {
    lua: Lua,
    budget: Duration,
}

impl AlarmsReader {
    /// Стейт с потолками памяти и времени по умолчанию.
    pub fn new() -> Result<Self> {
        Ok(AlarmsReader { lua: sandbox::new_lua(ptusa_alarms::MEMORY_LIMIT)?, budget: ptusa_alarms::TIME_BUDGET })
    }

    /// Исполнить ответ 103 и собрать список. Ошибка — ответ не Lua, исчерпаны лимиты или записи не того вида.
    pub fn parse(&self, text: &str) -> Result<Alarms> {
        sandbox::run_limited(&self.lua, self.budget, || ptusa_alarms::exec(&self.lua, text))?;
        ptusa_alarms::collect(&self.lua)
    }
}

#[cfg(test)]
mod tests {
    use ptusa_alarms::samples::*;

    use super::*;

    #[test]
    fn answers_are_read_in_the_sandbox() {
        let reader = AlarmsReader::new().unwrap();
        assert_eq!(reader.parse(OBJECT).unwrap().alarms[0].state, State::Alarm);
        assert!(reader.parse(CRITICAL).unwrap().alarms[0].critical);
        assert_eq!(reader.parse(EMPTY).unwrap(), Alarms { id: 46, alarms: vec![] });
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
}
