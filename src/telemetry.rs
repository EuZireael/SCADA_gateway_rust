//! Обработка снятых значений. Порядок — как у Java-шлюза и контракта «по исключению»
//! (`docs/TELEMETRY_BY_EXCEPTION_CONTRACT.md`):
//!
//! 1. пользовательский скрипт канала (`script.rs`): масштаб, отсев кода обрыва, фильтр — **до** всего
//!    остального, дальше все видят уже обработанное значение;
//! 2. смена качества → одно сводное событие на цикл;
//! 3. алармы (по флагу);
//! 4. Kafka — по исключению (`filter.rs`): первое значение, смена качества, изменение за зону,
//!    полная отправка раз в `full-resend-ms`. Резервный экземпляр пары не публикует и состояния
//!    фильтров не ведёт; став активным, начинает с чистого листа и заново отправляет все теги;
//! 5. локальная история — только значимые точки (тот же фильтр со своими настройками и
//!    поканальными переопределениями).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use tracing::{error, info, warn};

use crate::app::{App, ControllerHandle};
use crate::db::TelemetryRow;
use crate::events::Event;
use crate::filter::{self, FilterParams, Last};
use crate::messages::{AlarmMessage, TelemetryMessage};
use crate::model::{Quality, Reading, Tag, Timestamp};

/// Состояние тега в обработчике контроллера (вектор по `Tag::slot`).
#[derive(Default)]
struct TagState {
    /// Доля периода до первой полной отправки (разброс, чтобы теги не уходили залпом).
    phase: f64,
    quality: Option<Quality>,
    /// Последнее опубликованное в Kafka.
    published: Option<Last>,
    /// Последнее записанное в историю.
    history: Option<Last>,
}

/// Обработчик одного контроллера (состояние — своё, без блокировок).
pub struct Processor {
    app: Arc<App>,
    controller: Arc<ControllerHandle>,
    states: Vec<TagState>,
    alarms: AlarmEvaluator,
    publish_params: FilterParams,
    /// Эпоха активации, при которой состояние фильтров актуально (см. `Leadership::activations`).
    epoch: u64,
    /// Эпоха доставки Kafka: после восстановления связи с брокером все теги отправляются заново.
    delivery_epoch: u64,
}

impl Processor {
    /// Обработчик контроллера: хранит состояние по слоту на каждый его тег.
    pub fn new(app: Arc<App>, controller: Arc<ControllerHandle>) -> Self {
        let name = &controller.ctrl.name;
        let states = (0..controller.ctrl.tags.len())
            .map(|slot| TagState { phase: filter::phase(name, slot), ..TagState::default() })
            .collect();
        let publish_params = FilterParams::publish(&app.settings.gateway.publish);
        let epoch = app.leadership.activations();
        let delivery_epoch = app.kafka.as_ref().map_or(0, |k| k.delivery().resync_epoch());
        Processor { app, controller, states, alarms: AlarmEvaluator::default(), publish_params, epoch, delivery_epoch }
    }

    /// Результат цикла опроса.
    pub fn process(&mut self, readings: &[Reading]) {
        self.process_at(readings, Instant::now());
    }

    /// То же с заданным «сейчас»: интервалы фильтров считаются по нему (тесты управляют временем).
    pub fn process_at(&mut self, readings: &[Reading], now: Instant) {
        let started = Instant::now();
        let app = self.app.clone();
        let gw = &app.settings.gateway;
        let active = app.leadership.is_active();
        self.resync_filters(&app, active);
        let scripts = app.scripts.snapshot();
        let have_scripts = !scripts.is_empty();
        let (mut to_bad, mut to_good) = (Vec::new(), Vec::new());
        let (mut sent, mut suppressed) = (0u64, 0u64);
        let mut history_rows = Vec::new();

        for r in readings {
            // 1. Пользовательский скрипт канала.
            let processed;
            let chain = if have_scripts { scripts.chain(&r.tag.name) } else { None };
            let (value, quality) = match chain {
                Some(chain) => {
                    processed = app.scripts.process(chain, &r.tag, r.value.as_ref(), r.quality, r.timestamp);
                    (processed.value.as_ref(), processed.quality)
                }
                None => (r.value.as_ref(), r.quality),
            };
            let Some(state) = self.states.get_mut(r.tag.slot) else { continue };

            // 2. Смена качества.
            if let Some(prev) = state.quality.replace(quality)
                && prev != quality
            {
                if quality == Quality::Bad { to_bad.push(r.tag.name.clone()) } else { to_good.push(r.tag.name.clone()) }
            }

            // 3. Алармы.
            if gw.alarms_enabled
                && let Some(v) = value.filter(|v| v.is_numeric()).and_then(|v| v.as_f64())
            {
                self.alarms.evaluate(&app, &self.controller, &r.tag, v);
            }

            if !active {
                continue; // резерв опрашивает и следит за качеством, но наружу молчит
            }

            // 4. Kafka — по исключению. BAD-кадр без включённой отправки BAD уходить не должен, и решение
            //    фильтру не отдаём: иначе он запомнил бы неотправленное как отправленное.
            if let Some(kafka) = &app.kafka
                && (value.is_some() || gw.send_bad_frames)
            {
                if !gw.publish.enabled
                    || filter::decide_phased(
                        &self.publish_params,
                        &mut state.published,
                        value,
                        quality,
                        now,
                        state.phase,
                    )
                {
                    if kafka.send_telemetry(&r.tag.name, &TelemetryMessage { value, quality, timestamp: r.timestamp }) {
                        sent += 1;
                    } else {
                        // Очередь продюсера полна (брокер не успевает): забываем, что тег опубликован, —
                        // следующий цикл опроса отправит его первым же делом. Остальные теги это не трогает.
                        state.published = None;
                    }
                } else {
                    suppressed += 1;
                }
            }

            // 5. Локальная история — только значимые точки.
            if app.telemetry.is_some() && r.tag.id.get() > 0 {
                let mut params = FilterParams::history(&gw.history, &r.tag.history);
                if let Some(deadband) = r.tag.tuning.deadband() {
                    params.deadband = deadband; // настройка из channel.tag-settings важнее YAML
                }
                if filter::decide_phased(&params, &mut state.history, value, quality, now, state.phase) {
                    history_rows.push(TelemetryRow {
                        tag_id: r.tag.id.get(),
                        time: r.timestamp.0,
                        quality: quality.as_str(),
                        value: value.cloned(),
                    });
                }
            }
        }

        if sent > 0 {
            app.metrics.telemetry_sent.inc_by(sent);
        }
        if suppressed > 0 {
            app.metrics.telemetry_suppressed.inc_by(suppressed);
        }
        if let Some(sink) = &app.telemetry
            && !history_rows.is_empty()
        {
            sink.push(history_rows);
        }
        self.emit_quality_change(to_bad, to_good);
        app.metrics.process_seconds.observe(started.elapsed().as_secs_f64());
    }

    /// Сбросить состояние фильтров, если надо отправить всё заново: экземпляр стал активным или Kafka снова доступна.
    fn resync_filters(&mut self, app: &App, active: bool) {
        if active && self.epoch != app.leadership.activations() {
            // Стали активными: прежний активный мог упасть, и потребитель не знает значений тегов —
            // отправляем и пишем всё заново, а не ждём полной отправки неизменившихся.
            self.epoch = app.leadership.activations();
            for st in &mut self.states {
                st.published = None;
                st.history = None;
            }
            info!("{}: состояние фильтров публикации сброшено — полная отправка", self.controller.ctrl.name);
        }
        if let Some(kafka) = &app.kafka
            && self.delivery_epoch != kafka.delivery().resync_epoch()
        {
            // Брокер был недоступен: недоставленное фильтр уже считал отправленным. Историю не трогаем.
            self.delivery_epoch = kafka.delivery().resync_epoch();
            self.states.iter_mut().for_each(|st| st.published = None);
            info!("{}: доставка в Kafka восстановлена — полная отправка", self.controller.ctrl.name);
        }
    }

    /// Смена качества — одно сводное событие на цикл. (Java-шлюз писал событие на КАЖДЫЙ
    /// тег: обрыв OPC UA-контроллера давал 2133 строки в журнале и столько же в Kafka.)
    fn emit_quality_change(&self, to_bad: Vec<String>, to_good: Vec<String>) {
        let total = to_bad.len() + to_good.len();
        if total == 0 {
            return;
        }
        let severity = if to_bad.is_empty() { "INFO" } else { "WARNING" };
        let sample: Vec<&String> = to_bad.iter().chain(to_good.iter()).take(20).collect();
        self.app.events.emit(
            Event::new(
                "QUALITY_CHANGE",
                "Gateway",
                severity,
                format!(
                    "{}: качество сменилось у {total} тегов (→BAD {}, →GOOD {})",
                    self.controller.ctrl.name,
                    to_bad.len(),
                    to_good.len()
                ),
            )
            .controller(self.controller.ctrl.id.get())
            .details(json!({"toBad": to_bad.len(), "toGood": to_good.len(), "tags": sample})),
        );
    }
}

/// Кадры BAD для всех тегов (обрыв на уровне запроса).
pub fn all_bad(tags: &[Arc<Tag>]) -> Vec<Reading> {
    let ts = Timestamp::now();
    tags.iter().map(|t| Reading { tag: t.clone(), value: None, quality: Quality::Bad, timestamp: ts }).collect()
}

// ---------------------------------------------------------------------- алармы --

/// Активное нарушение тега: условие, идентификатор аларма и нарушенный порог.
struct ActiveAlarm {
    /// `HIGH` или `LOW`.
    condition: &'static str,
    alarm_id: String,
    severity: &'static str,
    threshold: f64,
}

/// Edge-триггер по minValue/maxValue: аларм — один раз при выходе за предел, снятие — один
/// раз при возврате в норму с гистерезисом 2 % диапазона. Логика как у Java AlarmEvaluator.
#[derive(Default)]
pub struct AlarmEvaluator {
    active: HashMap<String, ActiveAlarm>,
}

impl AlarmEvaluator {
    /// Проверить значение тега на выход за пределы: возникновение и снятие аларма уходят в события и Kafka.
    pub fn evaluate(&mut self, app: &App, ctrl: &ControllerHandle, tag: &Tag, value: f64) {
        if let Some(action) = self.decide(tag, value) {
            match action {
                AlarmAction::Raise { alarm_id, severity, message, threshold, cleared_previous } => {
                    if let Some(prev) = cleared_previous {
                        publish_clear(app, ctrl, tag, &prev, value);
                    }
                    if severity == "CRITICAL" {
                        error!("🚨 ALARM [{severity}]: {message}")
                    } else {
                        warn!("🚨 ALARM [{severity}]: {message}")
                    }
                    app.events.emit(
                        Event::new("ALARM", "Gateway", severity, message.clone())
                            .tag(tag.id.get())
                            .controller(ctrl.ctrl.id.get())
                            .details(json!({"threshold": threshold, "currentValue": value, "tagName": tag.name, "unit": tag.unit})),
                    );
                    publish_alarm(app, ctrl, tag, &alarm_id, severity, &message, threshold, value, false);
                }
                AlarmAction::Clear(prev) => publish_clear(app, ctrl, tag, &prev, value),
            }
        }
    }

    /// Состояние аларма тега после нового значения: возникновение (в том числе смена условия), снятие с гистерезисом 2 % диапазона или «ничего» (то же нарушение не повторяется — анти-флуд).
    fn decide(&mut self, tag: &Tag, value: f64) -> Option<AlarmAction> {
        let (min, max) = (tag.min_value, tag.max_value);
        if min.is_none() && max.is_none() {
            return None;
        }
        let unit = tag.unit.as_deref().unwrap_or("");
        let range = match (min, max) {
            (Some(lo), Some(hi)) => (hi - lo).max(0.0001),
            _ => value.abs().max(1.0),
        };
        let deadband = 0.02 * range;
        let violation = match (min, max) {
            (_, Some(hi)) if value > hi => Some((
                "HIGH",
                if value > hi + 0.3 * range { "CRITICAL" } else { "MAJOR" },
                hi,
                format!("High value: {value:.2} > {hi:.2} {unit}"),
            )),
            (Some(lo), _) if value < lo => Some((
                "LOW",
                if value < lo - 0.3 * range { "MAJOR" } else { "MINOR" },
                lo,
                format!("Low value: {value:.2} < {lo:.2} {unit}"),
            )),
            _ => None,
        };
        match violation {
            Some((condition, severity, threshold, message)) => {
                if self.active.get(&tag.name).is_some_and(|a| a.condition == condition) {
                    return None; // то же нарушение — молчим (анти-флуд)
                }
                let alarm_id =
                    format!("ALARM_{}_{}_{}", tag.id.get(), condition, chrono::Utc::now().timestamp_millis());
                let previous = self.active.insert(
                    tag.name.clone(),
                    ActiveAlarm { condition, alarm_id: alarm_id.clone(), severity, threshold },
                );
                Some(AlarmAction::Raise { alarm_id, severity, message, threshold, cleared_previous: previous })
            }
            None => {
                let back = min.is_none_or(|lo| value >= lo + deadband) && max.is_none_or(|hi| value <= hi - deadband);
                if back { self.active.remove(&tag.name).map(AlarmAction::Clear) } else { None }
            }
        }
    }
}

/// Что сделать после нового значения: поднять аларм (сняв прежний, если условие сменилось) или снять.
enum AlarmAction {
    Raise {
        alarm_id: String,
        severity: &'static str,
        message: String,
        threshold: f64,
        cleared_previous: Option<ActiveAlarm>,
    },
    Clear(ActiveAlarm),
}

/// Снятие аларма: событие `ALARM_CLEARED` и сообщение в Kafka.
fn publish_clear(app: &App, ctrl: &ControllerHandle, tag: &Tag, prev: &ActiveAlarm, value: f64) {
    info!("✅ Аларм снят: {} ({value:.2})", tag.name);
    app.events.emit(
        Event::new("ALARM_CLEARED", "Gateway", "INFO", format!("Alarm cleared for {} ({value:.2})", tag.name))
            .tag(tag.id.get())
            .controller(ctrl.ctrl.id.get())
            .details(json!({"tagName": tag.name, "alarmId": prev.alarm_id})),
    );
    publish_alarm(
        app,
        ctrl,
        tag,
        &prev.alarm_id,
        prev.severity,
        &format!("Cleared: {value:.2} back to normal"),
        prev.threshold,
        value,
        true,
    );
}

/// Аларм или его снятие (`cleared`): сообщение в Kafka (его пошлёт только активный экземпляр).
#[allow(clippy::too_many_arguments)]
fn publish_alarm(
    app: &App,
    ctrl: &ControllerHandle,
    tag: &Tag,
    alarm_id: &str,
    severity: &str,
    message: &str,
    threshold: f64,
    value: f64,
    cleared: bool,
) {
    if let Some(kafka) = &app.kafka {
        kafka.send_alarm(&AlarmMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            kind: "ALARM",
            alarm_id: alarm_id.to_string(),
            // tagId на проводе — id канала общей базы (как у Java-шлюза), иначе PK шлюза.
            tag_id: tag.channel_id.or(Some(tag.id.get()).filter(|id| *id > 0)),
            tag_name: tag.name.clone(),
            severity: severity.to_string(),
            message: message.to_string(),
            threshold,
            current_value: value,
            timestamp: Timestamp::now(),
            acknowledged: false,
            cleared,
            controller_id: Some(ctrl.ctrl.id.get()).filter(|id| *id > 0),
            controller_name: Some(ctrl.ctrl.name.clone()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TagConfig;

    fn tag(min: Option<f64>, max: Option<f64>) -> Tag {
        Tag::from_config(&TagConfig {
            name: "T".into(),
            node_id: "ns=2;s=1".into(),
            channel_id: Some(1),
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: None,
            data_type: "FLOAT".into(),
            polling_rate: 1000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: min,
            max_value: max,
            write_min: None,
            write_max: None,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
            history: None,
        })
    }

    fn kind(a: &Option<AlarmAction>) -> &'static str {
        match a {
            Some(AlarmAction::Raise { severity, .. }) => severity,
            Some(AlarmAction::Clear(_)) => "CLEAR",
            None => "-",
        }
    }

    #[test]
    fn edge_trigger_with_hysteresis() {
        let t = tag(Some(0.0), Some(100.0));
        let mut ev = AlarmEvaluator::default();
        assert_eq!(kind(&ev.decide(&t, 50.0)), "-");
        assert_eq!(kind(&ev.decide(&t, 110.0)), "MAJOR");
        assert_eq!(kind(&ev.decide(&t, 120.0)), "-", "то же нарушение — без повторов");
        assert_eq!(kind(&ev.decide(&t, 99.0)), "-", "в полосе гистерезиса — ещё не норма");
        assert_eq!(kind(&ev.decide(&t, 97.0)), "CLEAR");
        assert_eq!(kind(&ev.decide(&t, 97.0)), "-");
    }

    #[test]
    fn severity_by_distance_and_direction_change() {
        let t = tag(Some(0.0), Some(100.0));
        let mut ev = AlarmEvaluator::default();
        assert_eq!(kind(&ev.decide(&t, 140.0)), "CRITICAL");
        match ev.decide(&t, -40.0) {
            Some(AlarmAction::Raise { severity, cleared_previous, .. }) => {
                assert_eq!(severity, "MAJOR");
                assert!(cleared_previous.is_some(), "смена направления закрывает прежний аларм");
            }
            _ => panic!("ожидался новый аларм"),
        }
    }

    #[test]
    fn no_limits_no_alarms() {
        assert_eq!(kind(&AlarmEvaluator::default().decide(&tag(None, None), 1e9)), "-");
    }
}

/// Обработчик целиком: что реально уходит в Kafka, в историю и в журнал. Контракт публикации —
/// `docs/TELEMETRY_BY_EXCEPTION_CONTRACT.md`; здесь проверяется связка с ролью в паре, скриптами и
/// отправкой BAD-кадров, правила фильтра — в `filter.rs`.
#[cfg(test)]
mod processor_tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;
    use crate::app::AppDeps;
    use crate::config::{ServerConfig, Settings, TagConfig};
    use crate::events;
    use crate::kafka::KafkaOut;
    use crate::leadership::Leadership;
    use crate::metrics::Metrics;
    use crate::model::{Controller, TagValue};
    use crate::script::Scripts;

    use Quality::{Bad, Good};

    struct Rig {
        proc: Processor,
        kafka: Arc<KafkaOut>,
        events_rx: mpsc::Receiver<Event>,
        history_rx: mpsc::Receiver<Vec<TelemetryRow>>,
        leadership: Arc<Leadership>,
        app: Arc<App>,
        base: Instant,
    }

    fn tag_cfg(name: &str, dt: &str) -> TagConfig {
        TagConfig {
            name: name.into(),
            node_id: format!("ns=2;s={name}"),
            channel_id: None,
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: None,
            data_type: dt.into(),
            polling_rate: 1000,
            enabled: true,
            writable: true,
            unit: None,
            min_value: None,
            max_value: None,
            write_min: None,
            write_max: None,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
            history: None,
        }
    }

    fn rig(
        tweak: impl FnOnce(&mut Settings),
        tags: &[(&str, &str)],
        scripts_dir: Option<&std::path::Path>,
        ha: bool,
    ) -> Rig {
        rig_full(tweak, tags.iter().map(|(n, d)| tag_cfg(n, d)).collect(), scripts_dir, ha)
    }

    /// `ha` — экземпляр пары (резервный, пока тест не сделает его активным).
    fn rig_full(
        tweak: impl FnOnce(&mut Settings),
        tags: Vec<TagConfig>,
        scripts_dir: Option<&std::path::Path>,
        ha: bool,
    ) -> Rig {
        let mut settings = Settings::from_env().expect("настройки по умолчанию");
        settings.gateway.publish.deadband = 0.0;
        tweak(&mut settings);
        let metrics = Arc::new(Metrics::new());
        let leadership = if ha {
            Leadership::standby("test".into(), "g".into(), metrics.ha_gauge("test"))
        } else {
            Leadership::single("test".into(), metrics.ha_gauge("test"))
        };
        let kafka = Arc::new(KafkaOut::new(&settings.kafka, metrics.clone(), leadership.clone()).expect("продюсер"));
        let (events, events_rx) = events::channel(metrics.clone());
        let (history, history_rx) = events::telemetry_channel(metrics.clone());
        let mut controller = Controller::from_config(&ServerConfig {
            id: None,
            name: "C".into(),
            endpoint: "opc.tcp://h:4840".into(),
            security: None,
            username: None,
            password: None,
            enabled: true,
            tags,
        })
        .unwrap();
        // Как после синхронизации с БД: у тега есть id (история пишется только тегам с id).
        for (n, t) in controller.tags.iter_mut().enumerate() {
            t.id.set(n as i64 + 1);
        }
        let names: Vec<String> = controller.tags.iter().map(|t| t.name.clone()).collect();
        let scripts = match scripts_dir {
            Some(dir) => {
                let mut s = settings.gateway.scripts.clone();
                s.dir = dir.to_path_buf();
                Scripts::load(
                    s,
                    names.clone(),
                    metrics.script_errors.clone(),
                    metrics.scripts_bound_tags.clone(),
                    events.clone(),
                )
                .expect("скрипты")
            }
            None => Scripts::none(),
        };
        let app = Arc::new(App::new(
            settings,
            AppDeps {
                metrics,
                kafka: Some(kafka.clone()),
                events: events.clone(),
                telemetry: Some(history),
                db: None,
                leadership: leadership.clone(),
                scripts,
            },
            vec![controller],
        ));
        let proc = Processor::new(app.clone(), app.controllers[0].clone());
        Rig { proc, kafka, events_rx, history_rx, leadership, app, base: Instant::now() }
    }

    impl Rig {
        fn readings(&self, rows: &[(usize, Option<TagValue>, Quality)]) -> Vec<Reading> {
            rows.iter()
                .map(|(slot, v, q)| Reading {
                    tag: self.app.controllers[0].ctrl.tags[*slot].clone(),
                    value: v.clone(),
                    quality: *q,
                    timestamp: Timestamp::now(),
                })
                .collect()
        }

        /// Цикл опроса через `secs` секунд от старта.
        fn cycle(&mut self, secs: u64, rows: &[(usize, Option<TagValue>, Quality)]) {
            let readings = self.readings(rows);
            self.proc.process_at(&readings, self.base + Duration::from_secs(secs));
        }

        /// Забрать отправленное в Kafka: (ключ, разобранное тело).
        fn sent(&self) -> Vec<(String, serde_json::Value)> {
            std::mem::take(&mut *self.kafka.captured.lock().unwrap())
                .into_iter()
                .map(|(_, key, body)| (key, serde_json::from_str(&body).unwrap()))
                .collect()
        }

        fn sent_values(&self) -> Vec<(String, serde_json::Value)> {
            self.sent().into_iter().map(|(k, b)| (k, b["value"].clone())).collect()
        }

        fn events(&mut self) -> Vec<Event> {
            std::iter::from_fn(|| self.events_rx.try_recv().ok()).collect()
        }
    }

    fn i(v: i64) -> Option<TagValue> {
        Some(TagValue::Int(v))
    }

    fn f(v: f64) -> Option<TagValue> {
        Some(TagValue::F64(v))
    }

    #[test]
    fn repeats_are_not_published_changes_are_and_full_resend_repeats() {
        let mut r = rig(|s| s.gateway.publish.full_resend = Duration::from_secs(30), &[("A", "INT32")], None, false);
        r.cycle(0, &[(0, i(42), Good)]);
        r.cycle(2, &[(0, i(42), Good)]);
        r.cycle(4, &[(0, i(43), Good)]);
        r.cycle(6, &[(0, i(43), Good)]);
        assert_eq!(r.sent_values(), [("A".into(), serde_json::json!(42)), ("A".into(), serde_json::json!(43))]);
        r.cycle(36, &[(0, i(43), Good)]); // 30 с с последней публикации
        assert_eq!(r.sent_values().len(), 1, "полная отправка неизменившегося");
        let text = r.app.metrics.render();
        assert!(text.contains("scada_telemetry_sent_total{application=\"scada-gateway\"} 3"), "{text}");
        assert!(text.contains("scada_telemetry_suppressed_total{application=\"scada-gateway\"} 2"), "{text}");
    }

    #[test]
    fn a_full_producer_queue_defers_only_that_tag_to_the_next_cycle() {
        let mut r = rig(|_| {}, &[("A", "INT32"), ("B", "INT32")], None, false);
        r.kafka.reject_next.store(1, std::sync::atomic::Ordering::SeqCst); // А не принято, Б принято
        r.cycle(0, &[(0, i(1), Good), (1, i(2), Good)]);
        assert_eq!(r.sent_values(), [("B".into(), serde_json::json!(2))]);
        // Значения не менялись, но А не ушло — оно уходит следующим циклом; Б — повтор, подавлен.
        r.cycle(2, &[(0, i(1), Good), (1, i(2), Good)]);
        assert_eq!(r.sent_values(), [("A".into(), serde_json::json!(1))]);
        r.cycle(4, &[(0, i(1), Good), (1, i(2), Good)]);
        assert!(r.sent().is_empty(), "дальше всё по исключению");
        assert_eq!(
            r.kafka.delivery().resync_epoch(),
            0,
            "переполнение очереди не запускает повторную отправку всех тегов"
        );
        let text = r.app.metrics.render();
        assert!(text.contains("scada_kafka_queue_full_total{application=\"scada-gateway\"} 1"), "{text}");
        assert!(text.contains("scada_telemetry_sent_total{application=\"scada-gateway\"} 2"), "{text}");
    }

    #[test]
    fn disabled_publish_filter_sends_every_tag_every_cycle() {
        let mut r = rig(|s| s.gateway.publish.enabled = false, &[("A", "INT32"), ("B", "INT32")], None, false);
        for k in 0..3 {
            r.cycle(k * 2, &[(0, i(1), Good), (1, i(2), Good)]);
        }
        assert_eq!(r.sent().len(), 6);
    }

    #[test]
    fn message_format_is_unchanged() {
        let mut r = rig(|_| {}, &[("Барановичи-1.BN1.LINE1.V0.ST", "INT32")], None, false);
        r.cycle(0, &[(0, i(1), Good)]);
        let (topic_key, body) = r.sent().remove(0);
        assert_eq!(topic_key, "Барановичи-1.BN1.LINE1.V0.ST");
        let o = body.as_object().unwrap();
        assert_eq!(
            o.keys().cloned().collect::<std::collections::BTreeSet<_>>(),
            ["quality", "timestamp", "value"].map(String::from).into()
        );
        assert_eq!((o["value"].clone(), o["quality"].clone()), (serde_json::json!(1), serde_json::json!("GOOD")));
        assert!(o["timestamp"].is_number());
    }

    #[test]
    fn quality_loss_and_recovery_are_published_at_once_and_noted_in_the_journal() {
        let mut r = rig(|s| s.gateway.publish.min_interval = Duration::from_secs(60), &[("A", "FLOAT")], None, false);
        r.cycle(0, &[(0, f(1.5), Good)]);
        r.cycle(2, &[(0, None, Bad)]);
        r.cycle(4, &[(0, None, Bad)]);
        r.cycle(6, &[(0, f(1.5), Good)]);
        let sent = r.sent();
        assert_eq!(
            sent.iter().map(|(_, b)| b["quality"].as_str().unwrap()).collect::<Vec<_>>(),
            ["GOOD", "BAD", "GOOD"]
        );
        assert!(sent[1].1["value"].is_null());
        let quality_events: Vec<_> = r.events().into_iter().filter(|e| e.event_type == "QUALITY_CHANGE").collect();
        assert_eq!(quality_events.len(), 2, "один сводный события на смену в каждую сторону");
    }

    #[test]
    fn bad_frames_disabled_are_neither_sent_nor_remembered() {
        let mut r = rig(|s| s.gateway.send_bad_frames = false, &[("A", "INT32")], None, false);
        r.cycle(0, &[(0, i(5), Good)]);
        r.cycle(2, &[(0, None, Bad)]); // не уходит и не запоминается как отправленный
        r.cycle(4, &[(0, i(5), Good)]); // сравнивается с реально отправленным — то же значение
        assert_eq!(r.sent_values(), [("A".into(), serde_json::json!(5))]);
    }

    #[test]
    fn standby_is_silent_and_becoming_active_resends_every_tag() {
        let mut r = rig(|_| {}, &[("A", "INT32"), ("B", "INT32")], None, true);
        let rows = [(0, i(1), Good), (1, i(2), Good)];
        r.cycle(0, &rows);
        r.cycle(2, &rows);
        assert!(r.sent().is_empty(), "резервный наружу молчит");
        assert!(r.history_rx.try_recv().is_err(), "и историю не пишет");

        r.leadership.set_active(true, "тест");
        r.cycle(4, &rows);
        assert_eq!(r.sent().len(), 2, "стали активными — значения всех тегов уходят сразу, не ждут полной отправки");
        r.cycle(6, &rows);
        assert!(r.sent().is_empty(), "дальше — по исключению");

        // Потеряли и снова получили лидерство — снова всё с чистого листа.
        r.leadership.set_active(false, "тест");
        r.cycle(8, &rows);
        assert!(r.sent().is_empty());
        r.leadership.set_active(true, "тест");
        r.cycle(10, &rows);
        assert_eq!(r.sent().len(), 2);
    }

    /// Брокер был недоступен: недоставленное фильтр уже считал отправленным. Когда доставка снова пошла,
    /// все теги уходят заново, а не ждут полной отправки; история при этом не дублируется.
    #[test]
    fn delivery_recovery_resends_every_tag_but_not_the_history() {
        // Полная отправка редкая, чтобы не мешать: проверяем именно восстановление доставки.
        let mut r = rig(
            |s| s.gateway.publish.full_resend = Duration::from_secs(3600),
            &[("A", "INT32"), ("B", "INT32")],
            None,
            false,
        );
        let rows = [(0, i(1), Good), (1, i(2), Good)];
        r.cycle(0, &rows);
        r.cycle(2, &rows);
        assert!(r.sent().len() >= 2);
        r.cycle(4, &rows);
        assert!(r.sent().is_empty(), "по исключению повторов нет");
        let _ = std::iter::from_fn(|| r.history_rx.try_recv().ok()).count();

        let kafka = r.kafka.clone();
        kafka.delivery().record_failure(1_000);
        r.cycle(6, &rows);
        assert!(r.sent().is_empty(), "пока доставка не восстановилась — ничего нового");
        kafka.delivery().record_success(1_000_000);
        r.cycle(8, &rows);
        assert_eq!(r.sent().len(), 2, "доставка восстановилась — значения всех тегов уходят заново");
        r.cycle(10, &rows);
        assert!(r.sent().is_empty(), "дальше снова по исключению");
        assert!(r.history_rx.try_recv().is_err(), "историю восстановление доставки не трогает");
    }

    #[test]
    fn standby_still_tracks_quality_changes() {
        let mut r = rig(|_| {}, &[("A", "INT32")], None, true);
        r.cycle(0, &[(0, i(1), Good)]);
        r.cycle(2, &[(0, None, Bad)]);
        assert_eq!(r.events().iter().filter(|e| e.event_type == "QUALITY_CHANGE").count(), 1);
    }

    #[test]
    fn history_keeps_only_significant_points_and_tag_override_applies() {
        use crate::config::HistoryConfig;
        // A — умолчания (любое изменение, «пульс» раз в 600 с); B — зона 1.0 из блока history: тега.
        let mut b = tag_cfg("B", "FLOAT");
        b.history = Some(HistoryConfig { deadband: Some(1.0), ..Default::default() });
        let mut r = rig_full(
            |s| s.gateway.history.max_interval = Duration::from_secs(600),
            vec![tag_cfg("A", "FLOAT"), b],
            None,
            false,
        );
        // A стоит на 10.0; B дрейфует 10.0, 10.4, 10.8, 11.2, 11.6 — первая точка, затем зона 1.0 от записанного.
        let drift = [10.0, 10.4, 10.8, 11.2, 11.6];
        for (k, v) in drift.iter().enumerate() {
            r.cycle(k as u64 * 2, &[(0, f(10.0), Good), (1, f(*v), Good)]);
        }
        let rows: Vec<TelemetryRow> = std::iter::from_fn(|| r.history_rx.try_recv().ok()).flatten().collect();
        let of = |id: i64| rows.iter().filter(|x| x.tag_id == id).map(|x| x.value.clone()).collect::<Vec<_>>();
        assert_eq!(of(1), [f(10.0)], "A: значение стоит — одна точка");
        assert_eq!(of(2), [f(10.0), f(11.2)], "B: 10.0, затем дрейф накопился за зону 1.0 (сравнение с записанным)");
        // «Пульс» раз в max-interval: стоящее значение A записывается снова через 600 с.
        r.cycle(600, &[(0, f(10.0), Good), (1, f(11.2), Good)]);
        let pulse: Vec<TelemetryRow> = std::iter::from_fn(|| r.history_rx.try_recv().ok()).flatten().collect();
        assert_eq!(pulse.iter().filter(|x| x.tag_id == 1).count(), 1);
    }

    #[test]
    fn history_deadband_from_tag_settings_replaces_the_yaml_one() {
        use crate::config::HistoryConfig;
        // A — умолчания; B — зона 1.0 из блока history: тега. Настройки из channel.tag-settings: A получает зону 2.0,
        // B — зону 0 (она заменяет 1.0 из YAML, а не складывается с ней).
        let mut b = tag_cfg("B", "FLOAT");
        b.history = Some(HistoryConfig { deadband: Some(1.0), ..Default::default() });
        let mut r = rig_full(
            |s| s.gateway.history.max_interval = Duration::from_secs(600),
            vec![tag_cfg("A", "FLOAT"), b],
            None,
            false,
        );
        let tags = r.app.controllers[0].ctrl.tags.clone();
        tags[0].tuning.apply(Some(&crate::tag_settings::TagSettings { deadband: 2.0, poll_period_ms: None }));
        tags[1].tuning.apply(Some(&crate::tag_settings::TagSettings { deadband: 0.0, poll_period_ms: None }));
        for (k, (a, b)) in [(10.0, 10.0), (11.0, 10.4), (12.5, 10.8)].into_iter().enumerate() {
            r.cycle(k as u64 * 2, &[(0, f(a), Good), (1, f(b), Good)]);
        }
        let rows: Vec<TelemetryRow> = std::iter::from_fn(|| r.history_rx.try_recv().ok()).flatten().collect();
        let of = |id: i64| rows.iter().filter(|x| x.tag_id == id).map(|x| x.value.clone()).collect::<Vec<_>>();
        assert_eq!(of(1), [f(10.0), f(12.5)], "A: зона 2.0 — 11.0 пропущено, 12.5 записано");
        assert_eq!(
            of(2),
            [f(10.0), f(10.4), f(10.8)],
            "B: зона 0 — каждое изменение значимо, 1.0 из YAML не действует"
        );
        // Сняли настройки (tombstone) — вернулись к YAML: B снова с зоной 1.0.
        tags[1].tuning.apply(None);
        r.cycle(6, &[(0, f(12.5), Good), (1, f(11.0), Good)]);
        r.cycle(8, &[(0, f(12.5), Good), (1, f(11.9), Good)]);
        let rows: Vec<TelemetryRow> = std::iter::from_fn(|| r.history_rx.try_recv().ok()).flatten().collect();
        let b_after: Vec<_> = rows.iter().filter(|x| x.tag_id == 2).map(|x| x.value.clone()).collect();
        assert_eq!(
            b_after,
            [f(11.9)],
            "B: снова зона 1.0 из YAML — от записанного 10.8 значение 11.0 мало, 11.9 значимо"
        );
    }

    #[test]
    fn ids_assigned_after_start_reach_history_and_commands() {
        // Как при запуске без БД: номеров нет — история не пишется, команда по tagId тег не находит (по имени — находит).
        let mut r = rig(|_| {}, &[("A", "FLOAT")], None, false);
        let tag = r.app.controllers[0].ctrl.tags[0].clone();
        tag.id.set(0);
        r.app.reindex_ids();
        r.cycle(0, &[(0, f(1.0), Good)]);
        assert!(r.history_rx.try_recv().is_err(), "без номера тега история не пишется");
        assert!(r.app.tag_by_id(1).is_none());
        assert!(r.app.tag_by_name("A").is_some());
        // БД подключилась: синхронизация записала номер в ту же ячейку, индекс перестроен.
        tag.id.set(42);
        r.app.reindex_ids();
        assert_eq!(r.app.tag_by_id(42).map(|t| t.tag.name.clone()).as_deref(), Some("A"));
        r.cycle(2, &[(0, f(2.0), Good)]);
        let rows: Vec<TelemetryRow> = std::iter::from_fn(|| r.history_rx.try_recv().ok()).flatten().collect();
        assert_eq!(rows.iter().map(|x| x.tag_id).collect::<Vec<_>>(), [42]);
    }

    #[test]
    fn user_script_runs_before_everything_else() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sensor_break.lua"), "function process(v, q, ctx)\n  if v ~= nil and (v < ctx.params.lo or v > ctx.params.hi) then return nil, 'BAD' end\n  return v, q\nend\n").unwrap();
        std::fs::write(
            dir.path().join("scale.lua"),
            "function process(v, q, ctx)\n  if v == nil then return nil, q end\n  return v * ctx.params.k, q\nend\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("scripts.yaml"),
            "scripts:\n  - {script: sensor_break.lua, tags: [\"T*\"], params: {lo: -50, hi: 150}}\n  - {script: scale.lua, tags: [\"P*\"], params: {k: 0.001}}\n",
        )
        .unwrap();
        let mut r = rig(|_| {}, &[("T1", "FLOAT"), ("P1", "FLOAT"), ("X1", "FLOAT")], Some(dir.path()), false);
        r.cycle(0, &[(0, f(21.5), Good), (1, f(1500.0), Good), (2, f(3276.7), Good)]);
        r.cycle(2, &[(0, f(3276.7), Good), (1, f(1500.0), Good), (2, f(3276.7), Good)]); // обрыв датчика T1
        let sent = r.sent();
        let get = |tag: &str| -> Vec<(serde_json::Value, serde_json::Value)> {
            sent.iter().filter(|(k, _)| k == tag).map(|(_, b)| (b["value"].clone(), b["quality"].clone())).collect()
        };
        assert_eq!(
            get("T1"),
            [(serde_json::json!(21.5), serde_json::json!("GOOD")), (serde_json::Value::Null, serde_json::json!("BAD"))]
        );
        assert_eq!(
            get("P1"),
            [(serde_json::json!(1.5), serde_json::json!("GOOD"))],
            "масштаб применён, повтор не ушёл"
        );
        assert_eq!(get("X1"), [(serde_json::json!(3276.7), serde_json::json!("GOOD"))], "канал без скрипта — как есть");
        assert_eq!(
            r.events().iter().filter(|e| e.event_type == "QUALITY_CHANGE").count(),
            1,
            "скрипт перевёл T1 в BAD — это смена качества"
        );
    }
}
