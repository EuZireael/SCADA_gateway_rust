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

use anyhow::{Result, anyhow};
use mlua::{Lua, Table, Value};

use crate::model::{self, TagValue};
use crate::sandbox;

/// Потолок памяти стейта. Снимок станции — единицы МБ вместе с мусором между сборками.
pub const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Время на один скрипт. Разбор снимка — миллисекунды; скрипт исполняется прямо в задаче
/// опроса и держит соединение PAC (и команды к нему), поэтому зависать не должен.
pub const TIME_BUDGET: Duration = Duration::from_secs(1);

/// Lua-стейт соединения PAC в песочнице: наполняется ответами контроллера, из него читаются значения тегов.
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

    /// Стейт с заданными потолками памяти и времени на скрипт (для тестов).
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

    /// `PAC_name` из ответа GET_INFO_ON_CONNECT — имя, которым PAC называет себя; `None`, если не задано или не строка.
    pub fn pac_name(&self) -> Option<String> {
        match self.lua.globals().get::<Value>("PAC_name") {
            Ok(Value::String(s)) => Some(s.to_string_lossy()),
            _ => None,
        }
    }

    /// Значение поля прибора из снимка `t`, приведённое к типу тега. `None` — нет снимка,
    /// прибора, поля, элемента массива или значение не приводится к типу.
    ///
    /// Прибор — обычная таблица `t.LINE1V0` или элемент массива `RECMAN[1]` (`t.RECMAN[1]`: менеджер рецептов ptusa
    /// лежит в снимке массивом, и ключа `"RECMAN[1]"` в нём нет).
    pub fn read(&self, device: &str, field: &str, data_type: &str) -> Option<TagValue> {
        let snapshot: Table = self.lua.globals().get("t").ok()?;
        let dev = device_table(&snapshot, device)?;
        convert(&field_value(&dev, field)?, data_type)
    }

    /// Проверка, что стейт не видит опасные библиотеки (для тестов).
    #[cfg(test)]
    fn global_is_nil(&self, name: &str) -> bool {
        matches!(self.lua.globals().get::<Value>(name), Ok(Value::Nil))
    }
}

/// Имя и необязательный индекс массива: `RT_PAR_F[12]` → (`RT_PAR_F`, 12), `ST` → (`ST`, нет). Хвост после `]`
/// (`PAR_MAIN[1].P_CZAD_S`) — подпись канала, в адрес не входит. `None` — индекс не число.
pub fn split_index(name: &str) -> Option<(&str, Option<i64>)> {
    let Some(lb) = name.find('[') else { return Some((name, None)) };
    let rb = lb + name[lb..].find(']')?;
    let index = name[lb + 1..rb].trim().parse().ok()?;
    Some((&name[..lb], Some(index)))
}

/// Таблица прибора в снимке: `t.NAME` или, если в имени индекс, элемент массива `t.NAME[i]`.
fn device_table(snapshot: &Table, device: &str) -> Option<Table> {
    match split_index(device)? {
        (name, None) => snapshot.get(name).ok(),
        (name, Some(index)) => snapshot.get::<Table>(name).ok()?.get(index).ok(),
    }
}

/// Поле прибора: `ST` или элемент массива `RT_PAR_F[12]` (Lua-индекс с 1; подпись после `]` отбрасывается).
fn field_value(dev: &Table, field: &str) -> Option<Value> {
    let value = match split_index(field)? {
        (name, None) => dev.get::<Value>(name).ok()?,
        (name, Some(index)) => dev.get::<Table>(name).ok()?.get::<Value>(index).ok()?,
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

/// Значение Lua → значение тега по его типу; несовместимое — `None`.
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

/// Скалярное значение для set_cmd: bool → 1/0, конечное число — как есть. Текст и NaN/inf отклоняются: значение
/// подставляется прямо в Lua-текст команды, и строка в нём — это код.
pub fn scalar(value: &TagValue) -> Result<String> {
    Ok(match value {
        TagValue::Bool(b) => if *b { "1" } else { "0" }.into(),
        TagValue::Int(i) => i.to_string(),
        TagValue::F32(f) if f.is_finite() => f.to_string(),
        TagValue::F64(f) if f.is_finite() => f.to_string(),
        TagValue::F32(_) | TagValue::F64(_) => anyhow::bail!("PAC принимает только конечные числа"),
        TagValue::Text(_) => anyhow::bail!("PAC принимает только числовые значения"),
    })
}

/// Текст команды `EXEC_DEVICE_COMMAND` для записи `device.field = value`.
///
/// * Обычный прибор: `__LINE1V0:set_cmd('ST', 1, 1)`. Индекс массива — **отдельным аргументом**, а не в имени поля:
///   `RT_PAR_F[62]` → `set_cmd('RT_PAR_F', 62, v)`. Вариант `set_cmd('RT_PAR_F[62]', 1, v)` прошивка принимает с кодом 0
///   и ничего не меняет, то есть запись выглядела бы выполненной.
/// * Элемент массива приборов (`RECMAN[1]`, менеджер рецептов): ptusa разбирает такую команду не Lua, а регулярным
///   выражением `__RECMAN\[(\d+)\]:set_cmd\( "(\w+)", (\d+), ((?:"(.*)")|([\d\.]+)) \)` — строго двойные кавычки,
///   по пробелу внутри скобок, строковое значение в кавычках, число только из цифр и точки (без знака). Любой другой
///   вид ptusa молча не применяет, поэтому отрицательное число и строка с кавычкой отклоняются здесь.
pub fn command_text(device: &str, field: &str, value: &TagValue) -> Result<String> {
    let (base, index) = split_index(field).ok_or_else(|| anyhow!("поле {field:?}: индекс не число"))?;
    let index = index.unwrap_or(1);
    match split_index(device).ok_or_else(|| anyhow!("прибор {device:?}: индекс не число"))? {
        (name, None) => Ok(format!("__{name}:set_cmd('{base}', {index}, {})", scalar(value)?)),
        (name, Some(n)) => Ok(format!("__{name}[{n}]:set_cmd( \"{base}\", {index}, {} )", recman_value(value)?)),
    }
}

/// Значение для команды менеджера рецептов: число из цифр и точки или строка в двойных кавычках.
fn recman_value(value: &TagValue) -> Result<String> {
    match value {
        TagValue::Text(s) => {
            anyhow::ensure!(
                !s.contains(['"', '\n', '\r', '\0']),
                "строка для менеджера рецептов не может содержать кавычки и переводы строк"
            );
            Ok(format!("\"{s}\""))
        }
        other => {
            let text = scalar(other)?;
            anyhow::ensure!(!text.starts_with('-'), "менеджер рецептов принимает только неотрицательные числа");
            Ok(text)
        }
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

    /// Менеджер рецептов лежит в снимке массивом: тег `deviceName: "RECMAN[1]"` читается как `t.RECMAN[1]`.
    #[test]
    fn reads_devices_that_are_array_elements() {
        let l = PacLua::new().unwrap();
        l.exec("t = t or {}\nt.RECMAN = {\n {CMD=0, NMR=1, PAR={0.5, 1.5}},\n {CMD=2, NMR=2},\n}\nt.LINE1V0 = {ST=1}")
            .unwrap();
        assert_eq!(l.read("RECMAN[1]", "CMD", "INT32"), Some(TagValue::Int(0)));
        assert_eq!(l.read("RECMAN[2]", "NMR", "INT32"), Some(TagValue::Int(2)));
        assert_eq!(l.read("RECMAN[1]", "PAR[2]", "FLOAT"), Some(TagValue::F64(1.5)));
        assert_eq!(l.read("RECMAN[3]", "CMD", "INT32"), None, "нет такого элемента");
        assert_eq!(l.read("RECMAN", "CMD", "INT32"), None, "массив без индекса — не прибор");
        assert_eq!(l.read("RECMAN[x]", "CMD", "INT32"), None);
        assert_eq!(l.read("LINE1V0", "ST", "INT32"), Some(TagValue::Int(1)), "обычные приборы читаются как раньше");
    }

    #[test]
    fn index_is_split_from_the_name_and_the_label_is_dropped() {
        assert_eq!(split_index("ST"), Some(("ST", None)));
        assert_eq!(split_index("RT_PAR_F[12]"), Some(("RT_PAR_F", Some(12))));
        assert_eq!(split_index("PAR_MAIN[ 3 ].P_CZAD_S"), Some(("PAR_MAIN", Some(3))));
        assert_eq!(split_index("RT_PAR_F[x]"), None);
        assert_eq!(split_index("RT_PAR_F[1"), None);
    }

    #[test]
    fn command_text_puts_the_array_index_in_its_own_argument() {
        let v = TagValue::F64(7.5);
        assert_eq!(command_text("LINE1V0", "ST", &TagValue::Int(1)).unwrap(), "__LINE1V0:set_cmd('ST', 1, 1)");
        assert_eq!(command_text("OBJECT1", "RT_PAR_F[62]", &v).unwrap(), "__OBJECT1:set_cmd('RT_PAR_F', 62, 7.5)");
        assert_eq!(
            command_text("OBJECT1", "PAR_MAIN[1].P_CZAD_S", &TagValue::Int(3)).unwrap(),
            "__OBJECT1:set_cmd('PAR_MAIN', 1, 3)",
            "подпись после ] — не часть адреса"
        );
        assert!(command_text("OBJECT1", "RT_PAR_F[x]", &v).is_err());
    }

    /// Формат менеджера рецептов — регулярное выражение прошивки: двойные кавычки, пробелы в скобках, число без знака.
    #[test]
    fn recipe_manager_command_follows_the_firmware_pattern() {
        assert_eq!(
            command_text("RECMAN[1]", "CMD", &TagValue::Int(1001)).unwrap(),
            "__RECMAN[1]:set_cmd( \"CMD\", 1, 1001 )"
        );
        assert_eq!(
            command_text("RECMAN[2]", "PAR[3]", &TagValue::F64(0.25)).unwrap(),
            "__RECMAN[2]:set_cmd( \"PAR\", 3, 0.25 )"
        );
        assert_eq!(
            command_text("RECMAN[1]", "NAME", &TagValue::Text("Творог 5%".into())).unwrap(),
            "__RECMAN[1]:set_cmd( \"NAME\", 1, \"Творог 5%\" )"
        );
        assert_eq!(
            command_text("RECMAN[1]", "CMD", &TagValue::Bool(true)).unwrap(),
            "__RECMAN[1]:set_cmd( \"CMD\", 1, 1 )"
        );
        // То, что регулярное выражение не примет, прошивка молча проигнорирует: отказ здесь честнее.
        assert!(command_text("RECMAN[1]", "CMD", &TagValue::Int(-5)).is_err(), "знак минус");
        assert!(command_text("RECMAN[1]", "NAME", &TagValue::Text("a\"b".into())).is_err(), "кавычка в строке");
        assert!(command_text("RECMAN[1]", "NAME", &TagValue::Text("a\nb".into())).is_err(), "перевод строки");
        assert!(command_text("RECMAN[1]", "CMD", &TagValue::F64(f64::NAN)).is_err());
        // У обычного прибора строка по-прежнему недопустима: в Lua-тексте она была бы кодом.
        assert!(command_text("LINE1V0", "ST", &TagValue::Text("1".into())).is_err());
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
        assert_eq!(l.pac_name().as_deref(), Some("BN1-МСА1"));
        assert_eq!(PacLua::new().unwrap().pac_name(), None, "до handshake имени нет");
        let l = PacLua::new().unwrap();
        l.exec("PAC_name = 5").unwrap();
        assert_eq!(l.pac_name(), None, "не строка — имени нет");
    }

    #[test]
    fn scalar_bool_as_number() {
        assert_eq!(scalar(&TagValue::Bool(true)).unwrap(), "1");
        assert_eq!(scalar(&TagValue::Bool(false)).unwrap(), "0");
        assert_eq!(scalar(&TagValue::F64(42.5)).unwrap(), "42.5");
        assert_eq!(scalar(&TagValue::Int(-3)).unwrap(), "-3");
    }

    /// Значение уходит в Lua-текст команды: строка там — код, а NaN/inf — не число.
    #[test]
    fn scalar_refuses_what_would_not_be_a_number_in_the_command_text() {
        assert!(scalar(&TagValue::Text("1); os.exit() --".into())).is_err());
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(scalar(&TagValue::F64(f)).is_err());
        }
        assert!(scalar(&TagValue::F32(f32::NAN)).is_err());
    }
}
