//! Аварии ПЛК (ptusa) для монитора: список аварий контроллера → снимок в compact-топик `scada.pac-alarms` (ключ — id
//! контроллера), команда `PAC_ALARM` → действие над аварией.
//!
//! Шлюз ходит к контроллерам ptusa только по OPC UA, поэтому протокол прошивки (103 — список, 104 — действие) берёт на
//! себя OPC UA-фасад `ptusa-opcua`: список — JSON в узле [`ALARMS_NODE`], действие — метод [`ALARM_CMD_METHOD`]. Шлюз
//! читает узел раз в `GATEWAY_PAC_ALARMS_INTERVAL_MS` (только активный экземпляр) и публикует снимок, когда меняется
//! список или связь; у сервера без узла (не фасад) аварий нет. Контракт с монитором — `docs/MONITOR_INTEGRATION.md`.

use std::sync::{Arc, Weak};
use std::time::Duration;

use ::opcua::types::{StatusCode, Variant};
use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::app::{App, ControllerHandle};
use crate::command::Outcome;
use crate::events::Throttle;
use crate::messages::{CommandMessage, PacAlarm, PacAlarmsMessage};
use crate::model::{Controller, ControllerKind};
use crate::opcua::{self, OpcConnection, ReadValueId, same_status};

/// Узел фасада со списком аварий (JSON).
pub const ALARMS_NODE: &str = "ns=2;s=SYSTEM.ALARMS";
/// Объект фасада, которому принадлежит метод действия над аварией.
pub const SYSTEM_OBJECT: &str = "ns=2;s=SYSTEM";
/// Метод фасада: `(Action: String, ObjectType: Int32, ObjectNumber: Int32, AlarmNumber: Int32)`.
pub const ALARM_CMD_METHOD: &str = "ns=2;s=SYSTEM.ALARM_CMD";
/// Как часто писать в журнал повторяющуюся ошибку чтения списка.
const ERROR_LOG_EVERY: Duration = Duration::from_secs(60);

/// id контроллера для аварий: `id` из `controllers.yaml`, не задан — имя.
pub fn controller_id(c: &Controller) -> &str {
    c.config_id.as_deref().unwrap_or(&c.name)
}

/// Путь проекта контроллера в мониторе: `alarmsPath`, иначе общий префикс имён тегов по сегментам через точку (без
/// последнего сегмента). `None` — пути нет: аварии контроллера не публикуются.
pub fn path(c: &Controller) -> Option<String> {
    if let Some(p) = &c.alarms_path {
        return Some(p.clone());
    }
    let mut names = c.tags.iter().map(|t| t.name.split('.').collect::<Vec<_>>());
    let first = names.next()?;
    // Последний сегмент — само поле: даже у единственного тега путь проекта — то, что перед ним.
    let mut len = first.len().saturating_sub(1);
    for segments in names {
        len = len.min(segments.len().saturating_sub(1));
        len = first.iter().zip(&segments).take(len).take_while(|(a, b)| a == b).count();
    }
    (len > 0).then(|| first[..len].join("."))
}

/// Список фасада: счётчик изменений ПЛК и аварии.
#[derive(Debug, Deserialize)]
struct FacadeList {
    id: i64,
    alarms: Vec<FacadeAlarm>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FacadeAlarm {
    object_type: Option<i64>,
    object_number: i64,
    alarm_number: Option<i64>,
    #[serde(default)]
    critical: bool,
    #[serde(default)]
    description: String,
    #[serde(default)]
    group: String,
    priority: i64,
    state: String,
    #[serde(default)]
    suppressed: bool,
}

/// JSON узла фасада → счётчик ПЛК и аварии снимка (ключ: `<тип>:<номер>:<авария>`, у критических — `C:<номер>`).
pub fn parse_facade(json: &str) -> Result<(i64, Vec<PacAlarm>)> {
    let list: FacadeList = serde_json::from_str(json).context("список аварий фасада не разобран")?;
    let alarms = list
        .alarms
        .into_iter()
        .map(|a| {
            let (object_type, alarm_number) = if a.critical { (None, None) } else { (a.object_type, a.alarm_number) };
            let key = match (object_type, alarm_number) {
                (Some(t), Some(n)) => format!("{t}:{}:{n}", a.object_number),
                _ => format!("C:{}", a.object_number),
            };
            PacAlarm {
                key,
                object_type,
                object_number: a.object_number,
                alarm_number,
                description: a.description,
                group: a.group,
                priority: a.priority,
                state: a.state,
                suppressed: a.suppressed,
            }
        })
        .collect();
    Ok((list.id, alarms))
}

/// Что показало очередное чтение узла.
#[derive(Debug, Clone, PartialEq)]
pub enum Seen {
    /// Список прочитан: счётчик ПЛК и аварии.
    List(i64, Vec<PacAlarm>),
    /// Связи с ПЛК нет (у контроллера или у фасада с прошивкой).
    Offline,
    /// У сервера нет узла аварий: это не фасад ptusa-opcua.
    Unsupported,
    /// Фасад ещё не прочитал список у прошивки (сразу после своего запуска): ни снимка, ни журнала.
    Waiting,
    /// Не прочитано при живой связи (ответ ПЛК не разобран, ошибка чтения): снимок не публикуется.
    Unreadable(String),
}

/// Снимок к публикации.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// Есть ли связь с ПЛК.
    pub online: bool,
    /// Счётчик ПЛК последнего удачного чтения.
    pub pac_version: Option<i64>,
    /// Момент последнего удачного чтения.
    pub polled_at: Option<String>,
    /// Последний известный список.
    pub alarms: Vec<PacAlarm>,
}

/// Когда публиковать: первый прочитанный список, изменение списка (сравнивается сам список, а не счётчик: после
/// перезапуска ПЛК он начинается заново), потеря и возврат связи. Без событий снимок не повторяется.
#[derive(Debug, Default)]
pub struct Tracker {
    /// Аварии у контроллера точно есть (`alarmsPath` в YAML или список уже читался): только тогда обрыв связи публикуется
    /// и до первого чтения — монитор видит «нет связи», а не прошлый снимок из топика.
    expected: bool,
    /// Последний удачно прочитанный список.
    last: Option<(i64, String, Vec<PacAlarm>)>,
    /// Что опубликовано последним (связь и список); `None` — ещё ничего или экземпляр был резервным.
    published: Option<(bool, Vec<PacAlarm>)>,
}

impl Tracker {
    /// `expected` — у контроллера задан `alarmsPath`.
    pub fn new(expected: bool) -> Self {
        Tracker { expected, ..Default::default() }
    }

    /// Экземпляр стал резервным: снимок должен опубликовать активный, а этот — заново, когда снова станет активным.
    pub fn forget_published(&mut self) {
        self.published = None;
    }

    /// Учесть чтение; `Some` — снимок, который надо опубликовать.
    pub fn on(&mut self, seen: Seen, now: impl FnOnce() -> String) -> Option<Snapshot> {
        let online = match seen {
            Seen::List(version, alarms) => {
                self.expected = true;
                self.last = Some((version, now(), alarms));
                true
            }
            Seen::Offline if self.expected => false,
            Seen::Offline | Seen::Unsupported | Seen::Waiting | Seen::Unreadable(_) => return None,
        };
        let alarms = self.last.as_ref().map(|(_, _, a)| a.clone()).unwrap_or_default();
        if self.published.as_ref().is_some_and(|(o, a)| *o == online && *a == alarms) {
            return None;
        }
        self.published = Some((online, alarms.clone()));
        let (pac_version, polled_at) = match &self.last {
            Some((v, t, _)) => (Some(*v), Some(t.clone())),
            None => (None, None),
        };
        Some(Snapshot { online, pac_version, polled_at, alarms })
    }
}

/// Прочитать узел аварий в текущей сессии контроллера. `unsupported` — сессия, у сервера которой узла нет (её не читаем;
/// `Weak` не даёт памяти сессии освободиться, поэтому новая сессия с тем же адресом за неё не сойдёт).
async fn observe(handle: &ControllerHandle, node: &ReadValueId, unsupported: &mut Weak<OpcConnection>) -> Seen {
    if handle.is_down() {
        return Seen::Offline;
    }
    let Some(conn) = handle.opc_connection() else {
        return Seen::Unreadable("нет сессии OPC UA".into());
    };
    if std::ptr::eq(unsupported.as_ptr(), Arc::as_ptr(&conn)) {
        return Seen::Unsupported;
    }
    let dv = match conn.read(std::slice::from_ref(node)).await {
        Ok(mut values) => values.remove(0),
        Err(_) if handle.is_down() => return Seen::Offline,
        Err(e) => return Seen::Unreadable(format!("{e:#}")),
    };
    let status = dv.status();
    if status.is_good() {
        return match dv.value {
            Some(Variant::String(json)) => match parse_facade(json.as_ref()) {
                Ok((version, alarms)) => Seen::List(version, alarms),
                Err(e) => Seen::Unreadable(format!("{e:#}")),
            },
            other => Seen::Unreadable(format!("узел не строка: {other:?}")),
        };
    }
    if same_status(status, StatusCode::BadNodeIdUnknown) {
        *unsupported = Arc::downgrade(&conn);
        Seen::Unsupported
    } else if same_status(status, StatusCode::BadCommunicationError) {
        Seen::Offline
    } else if same_status(status, StatusCode::BadWaitingForInitialData) {
        Seen::Waiting
    } else {
        Seen::Unreadable(format!("узел {ALARMS_NODE}: {status}"))
    }
}

/// Аварии одного контроллера до остановки: чтение узла, снимок при изменении. Резервный экземпляр не читает.
pub async fn run(app: Arc<App>, handle: Arc<ControllerHandle>, path: String, cancel: CancellationToken) {
    let name = handle.ctrl.name.clone();
    let id = controller_id(&handle.ctrl).to_string();
    let node = opcua::read_value_id(ALARMS_NODE).expect("адрес узла аварий");
    let mut tracker = Tracker::new(handle.ctrl.alarms_path.is_some());
    let (mut unsupported, mut told_unsupported) = (Weak::new(), false);
    let errors = Throttle::new(ERROR_LOG_EVERY);
    let mut tick = interval(app.settings.gateway.pac_alarms_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        if !app.leadership.is_active() {
            tracker.forget_published();
            continue;
        }
        let seen = observe(&handle, &node, &mut unsupported).await;
        match &seen {
            Seen::Unsupported if !told_unsupported => {
                told_unsupported = true;
                info!("{name}: на сервере нет узла {ALARMS_NODE} (это не фасад ptusa-opcua) — аварии ПЛК не читаются");
            }
            Seen::Unreadable(why) if handle.opc_connection().is_some() && errors.ready() => {
                warn!("{name}: аварии ПЛК не прочитаны: {why} (повтор — не чаще раза в минуту)");
            }
            _ => {}
        }
        let now = || Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        if let (Some(snapshot), Some(kafka)) = (tracker.on(seen, now), &app.kafka) {
            kafka.send_pac_alarms(&PacAlarmsMessage {
                controller_id: &id,
                controller_name: &name,
                path: &path,
                online: snapshot.online,
                pac_version: snapshot.pac_version,
                polled_at: snapshot.polled_at,
                alarms: &snapshot.alarms,
            });
        }
    }
}

/// Удалить из топика снимки выключенных контроллеров (`ids`) — при каждом получении роли активного.
pub async fn tombstones(app: Arc<App>, ids: Vec<String>, cancel: CancellationToken) {
    let mut role = app.leadership.subscribe();
    loop {
        if role.borrow_and_update().active
            && let Some(kafka) = &app.kafka
        {
            for id in &ids {
                kafka.send_pac_alarms_tombstone(id);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            changed = role.changed() => if changed.is_err() {
                // Роль больше не меняется (процесс останавливается): ждём остановки, а не выходим — выход для
                // надзирателя был бы сбоем.
                cancel.cancelled().await;
                return;
            },
        }
    }
}

// ------------------------------------------------------------------------ команда --

/// Действие над аварией из команды `PAC_ALARM`, проверенное.
#[derive(Debug, Clone, PartialEq)]
pub struct AlarmCommand {
    /// Контроллер, как его назвала команда.
    pub controller_id: String,
    /// `ACCEPT`, `SUPPRESS` или `UNSUPPRESS`.
    pub action: &'static str,
    /// Тип объекта.
    pub object_type: i32,
    /// Номер объекта.
    pub object_number: i32,
    /// Номер аварии.
    pub alarm_number: i32,
}

impl AlarmCommand {
    /// Ключ аварии, как в снимке.
    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.object_type, self.object_number, self.alarm_number)
    }
}

/// Проверить команду `PAC_ALARM`; `Err` — пояснение для `REJECTED_INVALID`. В ПЛК уходят только целые: строки из
/// команды в Lua прошивки не попадают.
pub fn parse_command(cmd: &CommandMessage) -> Result<AlarmCommand, String> {
    let controller_id = match &cmd.controller_id {
        Value::String(s) if !s.trim().is_empty() => s.clone(),
        other => return Err(format!("controllerId должен быть строкой, пришло {other}")),
    };
    let action = match cmd.action.as_str() {
        Some("ACCEPT") => "ACCEPT",
        Some("SUPPRESS") => "SUPPRESS",
        Some("UNSUPPRESS") => "UNSUPPRESS",
        _ => return Err(format!("action — ACCEPT, SUPPRESS или UNSUPPRESS, пришло {}", cmd.action)),
    };
    let int = |field: &str, v: &Value| -> Result<i32, String> {
        v.as_i64().and_then(|n| i32::try_from(n).ok()).ok_or_else(|| match v {
            Value::Null => format!("{field} не задан (у критических ошибок ПЛК действия нет)"),
            other => format!("{field} должен быть целым в пределах i32, пришло {other}"),
        })
    };
    Ok(AlarmCommand {
        controller_id,
        action,
        object_type: int("objectType", &cmd.object_type)?,
        object_number: int("objectNumber", &cmd.object_number)?,
        alarm_number: int("alarmNumber", &cmd.alarm_number)?,
    })
}

/// Контроллер команды: включённый, OPC UA, аварии не выключены; `Err` — исход `REJECTED_UNKNOWN_TAG`.
pub fn controller(app: &App, id: &str) -> Result<Arc<ControllerHandle>, Outcome> {
    if !app.settings.gateway.pac_alarms_enabled {
        return Err(Outcome::fail("REJECTED_UNKNOWN_TAG", "Аварии ПЛК выключены (GATEWAY_PAC_ALARMS_ENABLED=false)"));
    }
    let handle = app
        .controllers
        .iter()
        .find(|h| controller_id(&h.ctrl) == id)
        .ok_or_else(|| Outcome::fail("REJECTED_UNKNOWN_TAG", format!("Контроллер не найден или выключен: {id}")))?;
    if handle.ctrl.kind != ControllerKind::OpcUa {
        return Err(Outcome::fail(
            "REJECTED_UNKNOWN_TAG",
            format!("Аварии ПЛК — только у OPC UA-контроллеров (фасад ptusa-opcua): {id}"),
        ));
    }
    Ok(handle.clone())
}

/// Исполнить действие: вызов метода фасада. Итог виден по следующему снимку — от себя снимок не публикуется.
pub async fn execute(handle: &ControllerHandle, cmd: &AlarmCommand) -> Outcome {
    let Some(conn) = handle.opc_connection() else {
        return Outcome::fail("FAILED_NO_CONNECTION", "Контроллер не подключён");
    };
    let (object, method) = match (opcua::parse_node_id(SYSTEM_OBJECT), opcua::parse_node_id(ALARM_CMD_METHOD)) {
        (Ok(o), Ok(m)) => (o, m),
        _ => unreachable!("адреса метода аварий — константы"),
    };
    let args = vec![
        Variant::from(cmd.action),
        Variant::Int32(cmd.object_type),
        Variant::Int32(cmd.object_number),
        Variant::Int32(cmd.alarm_number),
    ];
    let key = cmd.key();
    match conn.call(object, method, args).await {
        Ok(status) if status.is_good() => {
            info!("✍ {}: авария {key} — {}", handle.ctrl.name, cmd.action);
            let done = match cmd.action {
                "ACCEPT" => "Квитирована",
                "SUPPRESS" => "Подавлена",
                _ => "Снято подавление:",
            };
            Outcome { status: "APPLIED", message: format!("{done} авария {key}"), applied: None }
        }
        Ok(status) if same_status(status, StatusCode::BadInvalidState) => {
            Outcome::fail("FAILED_WRITE", format!("ПЛК не выполнил команду над аварией {key}"))
        }
        Ok(status) if same_status(status, StatusCode::BadCommunicationError) => {
            Outcome::fail("FAILED_NO_CONNECTION", "Нет связи с ПЛК (фасад ptusa-opcua не связан с прошивкой)")
        }
        Ok(status)
            if [StatusCode::BadNodeIdUnknown, StatusCode::BadMethodInvalid, StatusCode::BadNotImplemented]
                .into_iter()
                .any(|s| same_status(status, s)) =>
        {
            Outcome::fail(
                "REJECTED_UNKNOWN_TAG",
                format!("У контроллера нет метода {ALARM_CMD_METHOD} (это не фасад ptusa-opcua): {status}"),
            )
        }
        Ok(status) => Outcome::fail(opcua::classify_write_status(status), format!("OPC UA отклонил команду: {status}")),
        Err(_) if !conn.is_alive() => Outcome::fail("FAILED_NO_CONNECTION", "Сессия OPC UA закрыта"),
        Err(e) => Outcome::fail("FAILED_WRITE", format!("Команда над аварией не выполнена: {e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::ServerConfig;

    fn ctrl(yaml_extra: &str, names: &[&str]) -> Controller {
        let tags: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(i, n)| format!("{{name: \"{n}\", nodeId: \"ns=2;s={i}\", dataType: INT32, enabled: true}}"))
            .collect();
        let yaml = format!(
            "name: Танки\nendpoint: opc.tcp://x:4840\nenabled: true\n{yaml_extra}tags: [{}]\n",
            tags.join(", ")
        );
        let cfg: ServerConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        Controller::from_config(&cfg).unwrap()
    }

    #[test]
    fn path_is_alarms_path_or_the_common_prefix_of_tag_names() {
        let tanks =
            ["Барановичи-1.BN1_NORM_TANKS.STATION.SYSTEM.UP_TIME", "Барановичи-1.BN1_NORM_TANKS.TANK1.TANK.CMD"];
        assert_eq!(path(&ctrl("", &tanks)).as_deref(), Some("Барановичи-1.BN1_NORM_TANKS"));
        assert_eq!(path(&ctrl("alarmsPath: BN1.Танки\n", &tanks)).as_deref(), Some("BN1.Танки"));
        assert_eq!(path(&ctrl("", &["A.B.C"])).as_deref(), Some("A.B"), "у одного тега — всё до поля");
        assert_eq!(path(&ctrl("", &["A.B", "A.B.C"])).as_deref(), Some("A"), "префикс не съедает имя тега целиком");
        assert_eq!(path(&ctrl("", &["A.B", "X.B"])), None, "общего начала нет");
        assert_eq!(path(&ctrl("", &[])), None);
        assert_eq!(path(&ctrl("alarmsPath: \"  \"\n", &["A.B", "X.B"])), None, "пустой alarmsPath не считается");
    }

    #[test]
    fn controller_id_is_the_yaml_id_or_the_name() {
        assert_eq!(controller_id(&ctrl("id: ptusa-танки\n", &["A.B"])), "ptusa-танки");
        assert_eq!(controller_id(&ctrl("", &["A.B"])), "Танки");
    }

    #[test]
    fn facade_list_becomes_snapshot_alarms_with_keys() {
        let json = r#"{"id":87,"alarms":[
            {"objectType":102,"objectNumber":2,"alarmNumber":1,"critical":false,"description":"'Танк 2' - не включена",
             "group":"ответ","priority":750,"state":"ALARM","suppressed":false},
            {"objectType":null,"objectNumber":3,"alarmNumber":null,"critical":true,"description":"Нет связи с узлом",
             "group":"тревога","priority":100,"state":"ALARM","suppressed":false},
            {"objectType":0,"objectNumber":7,"alarmNumber":-1,"critical":false,"description":"LINE1V0 - обратная связь",
             "group":"тревога","priority":250,"state":"ACCEPT","suppressed":true}]}"#;
        let (version, alarms) = parse_facade(json).unwrap();
        assert_eq!(version, 87);
        let keys: Vec<&str> = alarms.iter().map(|a| a.key.as_str()).collect();
        assert_eq!(keys, ["102:2:1", "C:3", "0:7:-1"]);
        assert_eq!(
            serde_json::to_value(&alarms[0]).unwrap(),
            json!({"key": "102:2:1", "objectType": 102, "objectNumber": 2, "alarmNumber": 1,
                   "description": "'Танк 2' - не включена", "group": "ответ", "priority": 750, "state": "ALARM",
                   "suppressed": false})
        );
        assert_eq!((alarms[1].object_type, alarms[1].alarm_number), (None, None));
        assert!(parse_facade("{}").is_err());
        assert!(parse_facade("").is_err());
    }

    fn alarm(key: &str) -> PacAlarm {
        PacAlarm {
            key: key.into(),
            object_type: Some(102),
            object_number: 2,
            alarm_number: Some(1),
            description: "x".into(),
            group: "ответ".into(),
            priority: 750,
            state: "ALARM".into(),
            suppressed: false,
        }
    }

    fn at(t: &str) -> impl FnOnce() -> String + '_ {
        move || t.to_string()
    }

    #[test]
    fn snapshot_is_published_on_first_list_change_and_link_loss_only() {
        let mut t = Tracker::new(false);
        // Без alarmsPath до первого списка обрыв не публикуется: у контроллера может не быть аварий вовсе.
        assert_eq!(t.on(Seen::Offline, at("t0")), None);
        let first = t.on(Seen::List(50, vec![alarm("102:2:1")]), at("t1")).unwrap();
        assert_eq!(
            first,
            Snapshot {
                online: true,
                pac_version: Some(50),
                polled_at: Some("t1".into()),
                alarms: vec![alarm("102:2:1")]
            }
        );
        assert_eq!(t.on(Seen::List(50, vec![alarm("102:2:1")]), at("t2")), None, "тот же список");
        assert_eq!(t.on(Seen::List(51, vec![alarm("102:2:1")]), at("t3")), None, "сменился только счётчик");
        assert_eq!(t.on(Seen::Unreadable("x".into()), at("t4")), None);
        let empty = t.on(Seen::List(52, vec![]), at("t5")).unwrap();
        assert!(empty.online && empty.alarms.is_empty());
        // Обрыв: один снимок с последним списком и временем последнего удачного чтения.
        t.on(Seen::List(53, vec![alarm("102:2:2")]), at("t6")).unwrap();
        let offline = t.on(Seen::Offline, at("t7")).unwrap();
        assert_eq!(
            offline,
            Snapshot {
                online: false,
                pac_version: Some(53),
                polled_at: Some("t6".into()),
                alarms: vec![alarm("102:2:2")]
            }
        );
        assert_eq!(t.on(Seen::Offline, at("t8")), None, "обрыв — один раз");
        // Связь вернулась с тем же списком — снимок «на связи».
        assert!(t.on(Seen::List(1, vec![alarm("102:2:2")]), at("t9")).unwrap().online);
        // Резервный не публикует; снова активный — публикует то же заново.
        t.forget_published();
        assert!(t.on(Seen::List(1, vec![alarm("102:2:2")]), at("t10")).is_some());
    }

    #[test]
    fn with_alarms_path_link_loss_is_published_before_the_first_list() {
        let mut t = Tracker::new(true);
        let offline = t.on(Seen::Offline, at("t0")).unwrap();
        assert_eq!(offline, Snapshot { online: false, pac_version: None, polled_at: None, alarms: vec![] });
        assert_eq!(t.on(Seen::Unsupported, at("t1")), None);
        assert_eq!(t.on(Seen::Waiting, at("t2")), None, "фасад ещё не читал прошивку");
    }

    fn command(body: Value) -> CommandMessage {
        serde_json::from_value(body).unwrap()
    }

    #[test]
    fn alarm_command_accepts_only_integers_and_known_actions() {
        let ok = json!({"commandId": "c", "type": "PAC_ALARM", "controllerId": "ptusa-танки", "action": "ACCEPT",
                        "objectType": 102, "objectNumber": 2, "alarmNumber": 3});
        let parsed = parse_command(&command(ok.clone())).unwrap();
        assert_eq!(
            parsed,
            AlarmCommand {
                controller_id: "ptusa-танки".into(),
                action: "ACCEPT",
                object_type: 102,
                object_number: 2,
                alarm_number: 3
            }
        );
        assert_eq!(parsed.key(), "102:2:3");
        for (field, value, why) in [
            ("objectType", json!(null), "не задан"),
            ("alarmNumber", json!(null), "не задан"),
            ("objectNumber", json!(2.5), "целым"),
            ("objectNumber", json!("2"), "целым"),
            ("alarmNumber", json!(1u64 << 40), "i32"),
            ("action", json!("DELETE"), "ACCEPT, SUPPRESS"),
            ("action", json!(null), "ACCEPT, SUPPRESS"),
            ("controllerId", json!(5), "строкой"),
            ("controllerId", json!(""), "строкой"),
        ] {
            let mut body = ok.clone();
            body[field] = value;
            let err = parse_command(&command(body)).unwrap_err();
            assert!(err.contains(why), "{field}: {err}");
        }
        let mut negative = ok;
        negative["alarmNumber"] = json!(-1);
        assert_eq!(parse_command(&command(negative)).unwrap().alarm_number, -1, "у устройств номер отрицательный");
    }
}
