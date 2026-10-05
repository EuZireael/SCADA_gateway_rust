//! Lua-стейт PAC: ответы контроллера — Lua-скрипт, значения читаются из снимка `t`.
//!
//! Снимок GET_DEVICES_STATES у ptusa — таблица `t` по имени прибора:
//! ```lua
//! t={ LINE1V0={M=0, ST=1}, LINE1M1={M=0, ST=0, FRQ=12.5, RPM=750, ...}, ... }
//! t.OBJECT1={CMD=0, CUR_REC='Танк №1', RT_PAR_F={0, 0, 1, ...}, PAR_MAIN={1, 0.20, ...}}
//! ```
//! Значение канала — `t[deviceName][fieldName]`; поле-массив `RT_PAR_F[12]` — Lua-индекс
//! с 1, хвост после `]` (`PAR_MAIN[1].P_CZAD_S`) — подпись канала, в адрес не входит.
//! Это настоящий Lua 5.1 (как в самом ptusa), а не эмуляция.

use std::time::Duration;

use anyhow::Result;
use mlua::{Lua, Table, Value};

use crate::model::{self, TagValue};
use crate::sandbox;

/// Потолок памяти стейта. Снимок станции — единицы МБ вместе с мусором между сборками.
pub const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Время на один скрипт. Разбор снимка — миллисекунды; скрипт исполняется прямо в задаче
/// опроса и держит соединение PAC (и команды к нему), поэтому зависать не должен.
pub const TIME_BUDGET: Duration = Duration::from_secs(1);

pub struct PacLua {
    lua: Lua,
    time_budget: Duration,
}

impl PacLua {
    /// Новый стейт. Скрипт присылает контроллер — то есть любой, кто ответит на порту PAC,
    /// поэтому только чистые вычисления: без io/os/package, без файлов и байткода, с
    /// потолком памяти и времени.
    pub fn new() -> Result<Self> {
        Self::with_limits(MEMORY_LIMIT, TIME_BUDGET)
    }

    pub fn with_limits(memory: usize, time_budget: Duration) -> Result<Self> {
        Ok(PacLua { lua: sandbox::new_lua(memory)?, time_budget })
    }

    /// Исполнить Lua-скрипт в стейте (наполняет глобальные переменные). Ошибка — в том
    /// числе исчерпание памяти или времени; стейт после неё соединение не переиспользует.
    pub fn exec(&self, script: &str) -> Result<()> {
        sandbox::run_limited(&self.lua, self.time_budget, || self.lua.load(script).exec())?;
        Ok(())
    }

    /// protocol_version из ответа GET_INFO_ON_CONNECT (0, если не задан).
    pub fn protocol_version(&self) -> i64 {
        match self.lua.globals().get::<Value>("protocol_version") {
            Ok(v) => number(&v).map(|n| n as i64).unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Значение поля прибора из снимка `t`, приведённое к типу тега. `None` — нет снимка,
    /// прибора, поля, элемента массива или значение не приводится к типу.
    pub fn read(&self, device: &str, field: &str, data_type: &str) -> Option<TagValue> {
        let snapshot: Table = self.lua.globals().get("t").ok()?;
        let dev: Table = snapshot.get(device).ok()?;
        convert(&field_value(&dev, field)?, data_type)
    }

    /// Значение тега: сначала плоская таблица симулятора `tags['<channelId>']` (так снимок отдаёт
    /// plc-simulator), затем `t[device][field]` настоящего ptusa — как PacLua.lookup Java-шлюза.
    pub fn read_tag(&self, channel_id: Option<i64>, device: Option<&str>, field: Option<&str>, data_type: &str) -> Option<TagValue> {
        if let Some(id) = channel_id
            && let Ok(tags) = self.lua.globals().get::<Table>("tags")
            && let Ok(v) = tags.get::<Value>(id.to_string())
            && !v.is_nil()
        {
            return convert(&v, data_type);
        }
        self.read(device?, field?, data_type)
    }

    /// Проверка, что стейт не видит опасные библиотеки (для тестов).
    #[cfg(test)]
    fn global_is_nil(&self, name: &str) -> bool {
        matches!(self.lua.globals().get::<Value>(name), Ok(Value::Nil))
    }
}

/// Поле прибора: `ST` или элемент массива `RT_PAR_F[12]` (подпись после `]` отбрасывается).
fn field_value(dev: &Table, field: &str) -> Option<Value> {
    let value = match field.find('[') {
        None => dev.get::<Value>(field).ok()?,
        Some(lb) => {
            let rb = lb + field[lb..].find(']')?;
            let array: Table = dev.get(&field[..lb]).ok()?;
            let index: i64 = field[lb + 1..rb].trim().parse().ok()?;
            array.get::<Value>(index).ok()?
        }
    };
    (!value.is_nil()).then_some(value)
}

fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Number(n) => Some(*n),
        Value::String(s) => s.to_str().ok()?.trim().parse().ok(),
        _ => None,
    }
}

fn convert(v: &Value, data_type: &str) -> Option<TagValue> {
    if model::is_string(data_type) {
        return Some(TagValue::Text(match v {
            Value::String(s) => s.to_string_lossy(),
            other => lua_number_text(number(other)?),
        }));
    }
    let n = number(v)?;
    Some(if model::is_bool(data_type) {
        TagValue::Bool(n != 0.0)
    } else if model::is_int(data_type) {
        TagValue::Int(n.trunc() as i64)
    } else {
        TagValue::F64(n)
    })
}

/// Число как его печатает Lua (`tostring`): целое без `.0`.
fn lua_number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 { format!("{}", n as i64) } else { format!("{n}") }
}

/// Скалярное значение для set_cmd: bool → 1/0, число — как есть.
pub fn scalar(value: &TagValue) -> String {
    match value {
        TagValue::Bool(b) => {
            if *b {
                "1".into()
            } else {
                "0".into()
            }
        }
        TagValue::Int(i) => i.to_string(),
        TagValue::F32(f) => f.to_string(),
        TagValue::F64(f) => f.to_string(),
        TagValue::Text(s) => s.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    /// Фрагмент ответа GET_DEVICES_STATES эмулятора ptusa 2026.4.2.1, значения подправлены.
    const SNAPSHOT: &str = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\
        \tLINE1M1={M=0, ST=0, V=0, R=0, FRQ=12.5, RPM=750, EST=0, AMP=0.0, MAX_FRQ=0.0, P_ON_TIME=1000},\n\t}\n\
        t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n\t{\n\tCMD=0,\n\tCUR_REC='Танк сырого молока №1',\n\
        \tRT_PAR_F=\n\t\t{\n\t\t0, 0, 0, 0, 0, 0, 0.98, 0, 0, 0, 0, 1, \n\t\t},\n\
        \tPAR_MAIN=\n\t\t{\n\t\t1, 0.20, 0.15, \n\t\t},\n\t}\n\
        t.SYSTEM =\n\t{\n\tP_V_OFF_DELAY_TIME=1000,\n\t}\n";

    fn snapshot() -> PacLua {
        let lua = PacLua::new().unwrap();
        lua.exec(SNAPSHOT).unwrap();
        lua
    }

    #[test]
    fn reads_device_fields_by_data_type() {
        let l = snapshot();
        assert_eq!(l.read("LINE1V0", "ST", "INT32"), Some(TagValue::Int(1)));
        assert_eq!(l.read("LINE1M1", "RPM", "INT32"), Some(TagValue::Int(750)));
        assert_eq!(l.read("LINE1M1", "FRQ", "FLOAT"), Some(TagValue::F64(12.5)));
        assert_eq!(l.read("LINE1V0", "ST", "BOOLEAN"), Some(TagValue::Bool(true)));
        assert_eq!(l.read("SYSTEM", "P_V_OFF_DELAY_TIME", "INT32"), Some(TagValue::Int(1000)));
        assert_eq!(l.read("LINE1M1", "FRQ", "INT"), Some(TagValue::Int(12)));
    }

    #[test]
    fn reads_array_elements_by_lua_index() {
        let l = snapshot();
        assert_eq!(l.read("OBJECT1", "RT_PAR_F[7]", "FLOAT"), Some(TagValue::F64(0.98)));
        assert_eq!(l.read("OBJECT1", "RT_PAR_F[12]", "FLOAT"), Some(TagValue::F64(1.0)));
        assert_eq!(l.read("OBJECT1", "PAR_MAIN[2].P_CMIN_S", "FLOAT"), Some(TagValue::F64(0.2)));
    }

    #[test]
    fn reads_strings() {
        assert_eq!(
            snapshot().read("OBJECT1", "CUR_REC", "STRING"),
            Some(TagValue::Text("Танк сырого молока №1".into()))
        );
    }

    #[test]
    fn missing_or_unreadable_is_none() {
        assert_eq!(PacLua::new().unwrap().read("LINE1V0", "ST", "INT32"), None);
        let l = snapshot();
        assert_eq!(l.read("LINE9V9", "ST", "INT32"), None);
        assert_eq!(l.read("LINE1V0", "V", "FLOAT"), None);
        assert_eq!(l.read("OBJECT1", "RT_PAR_F[99]", "FLOAT"), None);
        assert_eq!(l.read("OBJECT1", "RT_PAR_F[x]", "FLOAT"), None);
        assert_eq!(l.read("OBJECT1", "CUR_REC", "FLOAT"), None);
    }

    #[test]
    fn state_has_no_host_access() {
        let l = PacLua::new().unwrap();
        for name in ["os", "io", "package", "debug"].into_iter().chain(sandbox::REMOVED_GLOBALS.iter().copied()) {
            assert!(l.global_is_nil(name), "{name} не должен быть доступен скрипту контроллера");
        }
        l.exec("t = t or {}\nt.X = {V = math.max(1, 2), S = string.upper('ok')}").unwrap();
        assert_eq!(l.read("X", "V", "FLOAT"), Some(TagValue::F64(2.0)));
    }

    #[test]
    fn bytecode_cannot_be_loaded() {
        let l = PacLua::new().unwrap();
        assert!(l.exec("f = loadstring(string.dump(function() return 42 end))").is_err());
        assert!(l.exec("s = string.dump(print)").is_err());
    }

    #[test]
    fn endless_script_is_stopped_by_time_budget() {
        sandbox::within(20, || {
            let l = PacLua::with_limits(MEMORY_LIMIT, Duration::from_millis(200)).unwrap();
            let started = Instant::now();
            assert!(l.exec("while true do end").is_err());
            assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
            // Лимит — на каждый скрипт, а не на жизнь стейта.
            l.exec(SNAPSHOT).unwrap();
            assert_eq!(l.read("LINE1V0", "ST", "INT32"), Some(TagValue::Int(1)));
        });
    }

    #[test]
    fn coroutine_cannot_run_outside_the_time_budget() {
        // coroutine убран: в сопрограмме хука времени нет, и бесконечный цикл в ней ушёл бы из-под лимита.
        sandbox::within(20, || {
            let l = PacLua::with_limits(MEMORY_LIMIT, Duration::from_millis(200)).unwrap();
            assert!(l.exec("co = coroutine.create(function() while true do end end); coroutine.resume(co)").is_err());
        });
    }

    #[test]
    fn memory_bomb_is_stopped_by_memory_limit() {
        let l = PacLua::with_limits(8 * 1024 * 1024, TIME_BUDGET).unwrap();
        let err = l.exec("s = string.rep('x', 64 * 1024 * 1024)").unwrap_err();
        assert!(err.to_string().contains("memory"), "{err}");
    }

    #[test]
    fn protocol_version_from_info() {
        let l = PacLua::new().unwrap();
        l.exec("protocol_version = 104; PAC_name = \"BN1-МСА1\"; is_reset_params = 0;params_CRC=32634;").unwrap();
        assert_eq!(l.protocol_version(), 104);
    }

    #[test]
    fn scalar_bool_as_number() {
        assert_eq!(scalar(&TagValue::Bool(true)), "1");
        assert_eq!(scalar(&TagValue::Bool(false)), "0");
        assert_eq!(scalar(&TagValue::F64(42.5)), "42.5");
        assert_eq!(scalar(&TagValue::Int(-3)), "-3");
    }
}
