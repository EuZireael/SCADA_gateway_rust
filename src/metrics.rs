//! Метрики Prometheus (`/actuator/prometheus`). Имена — как у Java-шлюза (Micrometer),
//! чтобы дашборд Grafana работал без правок: scada_telemetry_sent_total,
//! scada_commands_total{status}, scada_controllers_connected, scada_controllers_total.

use std::collections::HashMap;

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Все метрики шлюза. Поля увеличиваются из любого места; регистрация и текстовый вывод — внутри.
pub struct Metrics {
    registry: Registry,
    /// Точки телеметрии, поставленные в очередь Kafka.
    pub telemetry_sent: IntCounter,
    /// Значения, не ушедшие в Kafka по правилам «по исключению» (повтор того же значения).
    pub telemetry_suppressed: IntCounter,
    /// Длительность цикла обработки одного контроллера: от снятого значения до постановки в очереди.
    pub process_seconds: Histogram,
    /// Весь цикл опроса контроллера: чтение + обработка. Растёт вровень с периодом — шлюз не успевает.
    pub poll_seconds: Histogram,
    /// Циклы, не уложившиеся в период опроса (следующий опрос начался позже положенного).
    pub poll_overruns: IntCounter,
    /// Ошибки пользовательских скриптов по имени скрипта.
    pub script_errors: IntCounterVec,
    /// Сколько каналов сейчас со скриптами.
    pub scripts_bound_tags: IntGauge,
    /// Сколько тегов сейчас с настройками из топика `channel.tag-settings` (период опроса, зона истории).
    pub tag_settings_tags: IntGauge,
    ha_active: IntGaugeVec,
    /// Команды по исходу (`status`).
    pub commands: IntCounterVec,
    /// Контроллеров на связи сейчас.
    pub controllers_connected: IntGauge,
    /// Сколько контроллеров настроено.
    pub controllers_total: IntGauge,
    /// Ошибки постановки в очередь и доставки в Kafka.
    pub kafka_send_errors: IntCounter,
    /// События, отброшенные при переполнении очереди.
    pub events_dropped: IntCounter,
    /// Точки истории, отброшенные при переполнении очереди писателя БД.
    pub telemetry_rows_dropped: IntCounter,
    /// Доставка в Kafka восстановилась после сбоя — тегам запущена повторная отправка.
    pub kafka_resyncs: IntCounter,
    /// Очередь продюсера переполнена (брокер не успевает): телеметрия тега уйдёт следующим циклом опроса.
    pub kafka_queue_full: IntCounter,
    /// Перезапуски задач шлюза надзирателем (`task` — имя задачи).
    pub task_restarts: IntCounterVec,
    /// Неудачные записи в БД (`kind`: events, history, config).
    pub db_write_errors: IntCounterVec,
    /// Строки, потерянные из-за недоступной БД (`kind`: events, history).
    pub db_rows_lost: IntCounterVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Создать и зарегистрировать все метрики; на каждой метка `application="scada-gateway"`.
    pub fn new() -> Self {
        let labels = HashMap::from([("application".to_string(), "scada-gateway".to_string())]);
        let registry = Registry::new_custom(None, Some(labels)).expect("реестр метрик");
        let counter = |name: &str, help: &str| {
            let c = IntCounter::new(name, help).expect("метрика");
            registry.register(Box::new(c.clone())).expect("регистрация метрики");
            c
        };
        let gauge = |name: &str, help: &str| {
            let g = IntGauge::new(name, help).expect("метрика");
            registry.register(Box::new(g.clone())).expect("регистрация метрики");
            g
        };
        let commands = IntCounterVec::new(Opts::new("scada_commands_total", "Команды записи по исходу"), &["status"])
            .expect("метрика");
        registry.register(Box::new(commands.clone())).expect("регистрация метрики");
        let script_errors =
            IntCounterVec::new(Opts::new("scada_script_errors_total", "Ошибки пользовательских скриптов"), &["script"])
                .expect("метрика");
        registry.register(Box::new(script_errors.clone())).expect("регистрация метрики");
        let vec = |name: &str, help: &str, label: &str| {
            let v = IntCounterVec::new(Opts::new(name, help), &[label]).expect("метрика");
            registry.register(Box::new(v.clone())).expect("регистрация метрики");
            v
        };
        let task_restarts = vec("scada_task_restarts_total", "Перезапуски задач шлюза после падения", "task");
        let db_write_errors = vec("scada_db_write_errors_total", "Неудачные записи в БД", "kind");
        let db_rows_lost = vec("scada_db_rows_lost_total", "Строки, потерянные из-за недоступной БД", "kind");
        let ha_active =
            IntGaugeVec::new(Opts::new("scada_ha_active", "1 — экземпляр активный, 0 — резервный"), &["instance"])
                .expect("метрика");
        registry.register(Box::new(ha_active.clone())).expect("регистрация метрики");
        let process_seconds = Histogram::with_opts(
            HistogramOpts::new("scada_process_seconds", "Обработка цикла опроса одного контроллера")
                .buckets(vec![0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )
        .expect("метрика");
        registry.register(Box::new(process_seconds.clone())).expect("регистрация метрики");
        let poll_seconds = Histogram::with_opts(
            HistogramOpts::new("scada_poll_seconds", "Цикл опроса одного контроллера: чтение и обработка")
                .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]),
        )
        .expect("метрика");
        registry.register(Box::new(poll_seconds.clone())).expect("регистрация метрики");
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(prometheus::process_collector::ProcessCollector::for_self()))
            .expect("метрики процесса");

        Metrics {
            telemetry_sent: counter("scada_telemetry_sent_total", "Отправлено точек телеметрии в Kafka"),
            telemetry_suppressed: counter(
                "scada_telemetry_suppressed_total",
                "Значения, не отправленные в Kafka: повтор прежнего (телеметрия по исключению)",
            ),
            process_seconds,
            poll_seconds,
            poll_overruns: counter("scada_poll_overruns_total", "Циклы опроса, не уложившиеся в период"),
            script_errors,
            scripts_bound_tags: gauge("scada_scripts_bound_tags", "Каналов с пользовательскими скриптами"),
            tag_settings_tags: gauge("scada_tag_settings_tags", "Тегов с настройками из топика channel.tag-settings"),
            ha_active,
            commands,
            controllers_connected: gauge("scada_controllers_connected", "Контроллеров на связи"),
            controllers_total: gauge("scada_controllers_total", "Всего сконфигурировано контроллеров"),
            kafka_send_errors: counter("scada_kafka_send_errors_total", "Ошибки постановки/доставки в Kafka"),
            events_dropped: counter("scada_events_dropped_total", "События, отброшенные при переполнении очереди"),
            telemetry_rows_dropped: counter(
                "scada_telemetry_rows_dropped_total",
                "Точки истории, отброшенные при переполнении очереди записи в БД",
            ),
            kafka_resyncs: counter(
                "scada_kafka_resyncs_total",
                "Доставка в Kafka восстановилась после сбоя: все теги отправлены заново",
            ),
            kafka_queue_full: counter(
                "scada_kafka_queue_full_total",
                "Очередь продюсера Kafka переполнена: сообщение не принято, тег отправится следующим циклом",
            ),
            task_restarts,
            db_write_errors,
            db_rows_lost,
            registry,
        }
    }

    /// Метрика роли экземпляра в паре: `scada_ha_active{instance="…"}`.
    pub fn ha_gauge(&self, instance: &str) -> IntGauge {
        self.ha_active.with_label_values(&[instance])
    }

    /// Текстовый формат Prometheus.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf).ok();
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_names_match_java_gateway() {
        let m = Metrics::new();
        m.telemetry_sent.inc();
        m.commands.with_label_values(&["APPLIED"]).inc();
        let text = m.render();
        for name in ["scada_telemetry_sent_total", "scada_commands_total", "scada_controllers_connected"] {
            assert!(text.contains(name), "нет метрики {name}");
        }
        assert!(text.contains(r#"application="scada-gateway""#));
    }
}
