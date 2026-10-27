//! Аварии прошивки по driver-master: ответ GET_PAC_ERRORS (103) исполняется здесь, в своём Lua-стейте песочницы (не в
//! стейте снимка приборов). Формат ответа, разбор и строка команды SET_PAC_ERROR_CMD (104) — общие с OPC UA-фасадом
//! (крейт [`ptusa_alarms`]); снимок и команду ведёт [`crate::pac_alarms`].

use std::time::Duration;

use anyhow::Result;
use mlua::Lua;
use ptusa_alarms::Alarms;

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
    use ptusa_alarms::State;
    use ptusa_alarms::samples::*;

    use super::*;

    #[test]
    fn answers_are_read_in_the_sandbox() {
        let reader = AlarmsReader::new().unwrap();
        assert_eq!(reader.parse(OBJECT).unwrap().alarms[0].state, State::Alarm);
        assert!(reader.parse(CRITICAL).unwrap().alarms[0].critical);
        assert_eq!(reader.parse(EMPTY).unwrap(), Alarms { id: 46, alarms: vec![] });
        assert!(reader.parse("x = 1").is_err(), "нет alarms[0]");
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
}
