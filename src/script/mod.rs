//! Пользовательские Lua-скрипты обработки значений каналов: папка `gateway.scripts.dir` (по умолчанию
//! `scripts/`) с `scripts.yaml` и файлами `*.lua`. Контракт — тот же, что у Java-шлюза
//! (`SCADA-gateway/scripts/`), поэтому скрипты переносятся между версиями без правок.
//!
//! ```yaml
//! scripts:
//!   - script: scale.lua                   # файл в этой папке
//!     tags: ["*.LINE?M*.P_ON_TIME"]       # маски имён каналов: * — любые символы, ? — один
//!     params: {k: 0.001}                  # доступны скрипту как ctx.params
//!     enabled: false                      # временно выключить привязку
//! ```
//!
//! ```lua
//! function process(value, quality, ctx)   -- обязательна; value = nil у кадра BAD
//!   return value, quality                 -- quality ("GOOD"/"BAD") можно не возвращать
//! end
//! function write(value, ctx)              -- необязательна: команда оператора → значение ПЛК
//!   return value
//! end
//! ```
//! `ctx`: tag, device, field, device_type, data_type, unit, params, timestamp (мс) и `state` —
//! таблица канала, живущая между вызовами (фильтры, мёртвая зона).
//!
//! * Несколько привязок на канал — цепочка в порядке файла; команда проходит `write` в обратном.
//! * Правки подхватываются без перезапуска; версия с ошибкой не применяется — работает прежняя.
//!   Ошибка при старте (битый YAML, нет файла, синтаксис Lua) — шлюз не стартует: лучше сразу, чем
//!   публиковать значения без нужного пересчёта.
//! * Ошибка скрипта на значении — кадр BAD (`value=null`): необработанное значение в чужих единицах
//!   ввело бы оператора в заблуждение. Счётчик `scada_script_errors_total{script}`, лог и событие SCRIPT
//!   — не чаще раза в минуту на скрипт.
//! * Песочница — [`crate::sandbox`]: только чистые вычисления, потолок памяти и время на вызов.
//!   Вызовы скрипта сериализуются (его стейт один), скрипты разных привязок идут независимо.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use prometheus::{IntCounterVec, IntGauge};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::ScriptSettings;
use crate::events::{Event, EventSink};
use crate::model::{Quality, Tag, TagValue, Timestamp};
#[cfg(test)]
use crate::sandbox;

/// Имя файла привязок в папке скриптов.
const BINDINGS_FILE: &str = "scripts.yaml";
/// Потолок памяти стейта одного скрипта.
const MEMORY_LIMIT: usize = 32 * 1024 * 1024;
/// Как часто ошибку одного скрипта пишут в журнал и события (остальные только считаются).
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);

mod bound;
mod convert;
mod glob;

pub use bound::{BoundScript, Processed};
pub use glob::TagGlob;

// ------------------------------------------------------------------------ набор скриптов --

/// Загруженный набор: привязки и готовые цепочки по имени канала.
pub struct Loaded {
    /// Все включённые привязки в порядке файла.
    scripts: Vec<Arc<BoundScript>>,
    /// Цепочка скриптов по имени канала; собирается при загрузке.
    by_tag: HashMap<String, Vec<Arc<BoundScript>>>,
    /// Отпечаток папки: по его смене понимаем, что файлы изменились.
    fingerprint: String,
}

impl Loaded {
    /// Пустой набор: скриптов нет.
    fn empty() -> Self {
        Loaded { scripts: Vec::new(), by_tag: HashMap::new(), fingerprint: String::new() }
    }

    /// Скриптов нет — обработчик значений вообще не заглядывает в набор.
    pub fn is_empty(&self) -> bool {
        self.by_tag.is_empty()
    }

    /// Цепочка скриптов канала в порядке привязок; `None` — скриптов нет.
    pub fn chain(&self, tag_name: &str) -> Option<&[Arc<BoundScript>]> {
        self.by_tag.get(tag_name).map(Vec::as_slice)
    }
}

/// Статистика ошибок одного скрипта: для `GET /api/scripts` и ограничения частоты журнала.
#[derive(Default)]
struct Stats {
    /// Сколько ошибок всего.
    errors: AtomicU64,
    /// Последняя ошибка: «канал: текст» и время.
    last: Mutex<Option<(String, DateTime<Utc>)>>,
    /// Когда ошибка последний раз попала в журнал.
    logged_at: Mutex<Option<Instant>>,
}

/// Управляющий скриптами: загрузка, горячая перезагрузка, обработка значений и команд.
pub struct Scripts {
    /// Папка, лимит времени и период проверки.
    settings: ScriptSettings,
    /// Имена всех тегов станции: по ним маски привязываются к каналам при загрузке.
    tag_names: Vec<String>,
    /// Текущая версия; при перезагрузке подменяется целиком.
    current: RwLock<Arc<Loaded>>,
    /// Статистика ошибок по имени скрипта.
    stats: Mutex<HashMap<String, Arc<Stats>>>,
    /// Последняя неудачная перезагрузка (видна в REST).
    last_reload_error: Mutex<Option<String>>,
    /// Метрика ошибок скриптов.
    errors_metric: Option<IntCounterVec>,
    /// Метрика «каналов со скриптами».
    bound_gauge: Option<IntGauge>,
    /// Куда писать события `SCRIPT`.
    events: Option<EventSink>,
}

/// Файл привязок `scripts.yaml`.
#[derive(Deserialize)]
struct BindingsFile {
    #[serde(default)]
    scripts: Vec<Binding>,
}

/// Одна привязка: скрипт, маски тегов и параметры.
#[derive(Deserialize)]
struct Binding {
    /// Имя `.lua`-файла в папке скриптов.
    script: Option<String>,
    /// Маски имён тегов (`*` — любые символы, `?` — один).
    #[serde(default)]
    tags: Vec<String>,
    /// Параметры; скрипту доступны как `ctx.params`.
    #[serde(default)]
    params: serde_yaml_ng::Value,
    /// `false` — привязка временно выключена (по умолчанию включена).
    #[serde(default = "yes")]
    enabled: bool,
}

/// Умолчание `enabled: true` для serde.
fn yes() -> bool {
    true
}

impl Scripts {
    /// Без скриптов (тесты, одиночный запуск без папки).
    pub fn none() -> Arc<Self> {
        Arc::new(Scripts {
            settings: ScriptSettings {
                dir: PathBuf::new(),
                timeout: Duration::from_millis(50),
                reload_interval: Duration::from_secs(5),
            },
            tag_names: Vec::new(),
            current: RwLock::new(Arc::new(Loaded::empty())),
            stats: Mutex::new(HashMap::new()),
            last_reload_error: Mutex::new(None),
            errors_metric: None,
            bound_gauge: None,
            events: None,
        })
    }

    /// Загрузить скрипты при старте. Ошибка — ошибка старта шлюза.
    pub fn load(
        settings: ScriptSettings,
        tag_names: Vec<String>,
        errors_metric: IntCounterVec,
        bound_gauge: IntGauge,
        events: EventSink,
    ) -> Result<Arc<Self>> {
        let scripts = Arc::new(Scripts {
            settings,
            tag_names,
            current: RwLock::new(Arc::new(Loaded::empty())),
            stats: Mutex::new(HashMap::new()),
            last_reload_error: Mutex::new(None),
            errors_metric: Some(errors_metric),
            bound_gauge: Some(bound_gauge),
            events: Some(events),
        });
        let loaded = scripts
            .read_dir()
            .with_context(|| format!("Пользовательские скрипты ({})", scripts.settings.dir.display()))?;
        scripts.install(loaded);
        Ok(scripts)
    }

    /// Текущая загруженная версия: клон `Arc`, горячий путь не ждёт перезагрузки.
    pub fn snapshot(&self) -> Arc<Loaded> {
        self.current.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Подменить загруженную версию и обновить метрику числа каналов со скриптами.
    fn install(&self, loaded: Loaded) {
        if let Some(g) = &self.bound_gauge {
            g.set(loaded.by_tag.len() as i64);
        }
        *self.current.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(loaded);
    }

    // ------------------------------------------------------------------ горячий путь --

    /// Прогнать значение через цепочку скриптов канала. Ошибка — значение null и качество BAD.
    pub fn process(
        &self,
        chain: &[Arc<BoundScript>],
        tag: &Tag,
        value: Option<&TagValue>,
        quality: Quality,
        ts: Timestamp,
    ) -> Processed {
        let mut current = Processed { value: value.cloned(), quality };
        for script in chain {
            match script.process(tag, current.value.as_ref(), current.quality, ts) {
                Ok(next) => current = next,
                Err(message) => {
                    self.on_error(script, tag, &message);
                    return Processed { value: None, quality: Quality::Bad };
                }
            }
        }
        current
    }

    /// Значение команды оператора → значение для ПЛК: `write` скриптов канала в обратном порядке.
    pub fn to_plc(&self, tag: &Tag, value: &Json) -> Result<Json, String> {
        let loaded = self.snapshot();
        let Some(chain) = loaded.chain(&tag.name) else { return Ok(value.clone()) };
        let mut v = value.clone();
        for script in chain.iter().rev() {
            v = script.to_plc(tag, &v).map_err(|message| {
                self.on_error(script, tag, &format!("write: {message}"));
                format!("скрипт {}: {message}", script.file)
            })?;
        }
        Ok(v)
    }

    /// Есть ли у канала скрипты (для сообщения об исходе команды).
    pub fn has_chain(&self, tag_name: &str) -> bool {
        self.snapshot().chain(tag_name).is_some()
    }

    /// Статистика ошибок скрипта; создаётся при первом обращении.
    fn stats_of(&self, file: &str) -> Arc<Stats> {
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(file.to_string())
            .or_default()
            .clone()
    }

    /// Учесть ошибку скрипта: счётчик, последняя ошибка, метрика; в журнал и события — не чаще раза в минуту.
    fn on_error(&self, script: &BoundScript, tag: &Tag, message: &str) {
        let stats = self.stats_of(&script.file);
        let total = stats.errors.fetch_add(1, Ordering::Relaxed) + 1;
        *stats.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((format!("{}: {message}", tag.name), Utc::now()));
        if let Some(m) = &self.errors_metric {
            m.with_label_values(&[&script.file]).inc();
        }
        let mut logged = stats.logged_at.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if logged.is_none_or(|t| t.elapsed() >= ERROR_LOG_INTERVAL) {
            *logged = Some(Instant::now());
            error!("📜 Скрипт {} на канале {}: {message} (ошибок всего: {total})", script.file, tag.name);
            if let Some(events) = &self.events {
                events.emit(
                    Event::new("SCRIPT", "Scripts", "ERROR", format!("Скрипт {}: {message}", script.file))
                        .details(json!({"script": script.file, "tagName": tag.name, "errors": total})),
                );
            }
        }
    }

    // ---------------------------------------------------------------------- загрузка --

    /// Загрузить папку целиком: привязки, файлы, маски → теги. Любая ошибка — `Err`, прежняя версия остаётся в работе.
    fn read_dir(&self) -> Result<Loaded> {
        let bindings_path = self.settings.dir.join(BINDINGS_FILE);
        let fingerprint = self.fingerprint();
        if !bindings_path.is_file() {
            info!("📜 Пользовательских скриптов нет ({} не найден)", bindings_path.display());
            return Ok(Loaded { fingerprint, ..Loaded::empty() });
        }
        let text = std::fs::read_to_string(&bindings_path)
            .with_context(|| format!("не прочитан {}", bindings_path.display()))?;
        let file: BindingsFile =
            serde_yaml_ng::from_str(&text).with_context(|| format!("{BINDINGS_FILE} не разобран"))?;

        let mut scripts: Vec<Arc<BoundScript>> = Vec::new();
        for (n, b) in file.scripts.iter().enumerate() {
            let n = n + 1;
            if !b.enabled {
                continue;
            }
            let name = b
                .script
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| anyhow!("{BINDINGS_FILE}: привязка №{n} без script"))?;
            let path = safe_join(&self.settings.dir, name)
                .ok_or_else(|| anyhow!("{BINDINGS_FILE}: {name} вне папки скриптов"))?;
            if !path.is_file() {
                bail!("{BINDINGS_FILE}: нет файла {name}");
            }
            if b.tags.is_empty() {
                bail!("{BINDINGS_FILE}: у {name} не заданы tags");
            }
            let source = std::fs::read_to_string(&path).with_context(|| format!("не прочитан {name}"))?;
            let globs = b.tags.iter().map(|t| TagGlob::new(t)).collect();
            scripts.push(Arc::new(BoundScript::new(name, &source, globs, &b.params, self.settings.timeout)?));
        }

        let mut by_tag: HashMap<String, Vec<Arc<BoundScript>>> = HashMap::new();
        let mut matched = vec![0usize; scripts.len()];
        for tag in &self.tag_names {
            for (i, s) in scripts.iter().enumerate() {
                if s.matches(tag) {
                    by_tag.entry(tag.clone()).or_default().push(s.clone());
                    matched[i] += 1;
                }
            }
        }
        for (s, count) in scripts.iter().zip(&matched) {
            if *count == 0 {
                warn!(
                    "📜 Скрипт {} ни к одному каналу не привязан: маски {:?}",
                    s.file,
                    s.globs.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
            }
        }
        info!("📜 Скрипты загружены: {} привязок, каналов со скриптами: {}", scripts.len(), by_tag.len());
        Ok(Loaded { scripts, by_tag, fingerprint })
    }

    /// Отпечаток папки: имена, размеры и время изменения файлов.
    fn fingerprint(&self) -> String {
        let Ok(entries) = std::fs::read_dir(&self.settings.dir) else { return String::new() };
        let mut files: Vec<String> = entries
            .flatten()
            .filter_map(|e| {
                let meta = e.metadata().ok().filter(|m| m.is_file())?;
                let mtime = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_millis();
                Some(format!("{}:{}:{mtime};", e.file_name().to_string_lossy(), meta.len()))
            })
            .collect();
        files.sort();
        files.concat()
    }

    /// Подхват изменений: папка изменилась — загрузить заново; с ошибкой — оставить прежние.
    pub fn reload_if_changed(&self) {
        let fingerprint = self.fingerprint();
        if fingerprint == self.snapshot().fingerprint {
            return;
        }
        match self.read_dir() {
            Ok(loaded) => {
                let summary =
                    format!("{} привязок, каналов со скриптами: {}", loaded.scripts.len(), loaded.by_tag.len());
                self.install(loaded);
                *self.last_reload_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                if let Some(e) = &self.events {
                    e.emit(
                        Event::new("SCRIPT", "Scripts", "INFO", format!("Скрипты перезагружены: {summary}"))
                            .details(json!({"dir": self.settings.dir.display().to_string()})),
                    );
                }
            }
            Err(e) => {
                let message = format!("{e:#}");
                // Отпечаток сломанной версии запоминаем, чтобы не повторять ошибку каждые 5 с.
                let current = self.snapshot();
                self.install(Loaded { scripts: current.scripts.clone(), by_tag: current.by_tag.clone(), fingerprint });
                *self.last_reload_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(message.clone());
                error!("📜 Скрипты не перезагружены, работают прежние: {message}");
                if let Some(events) = &self.events {
                    events.emit(
                        Event::new(
                            "SCRIPT",
                            "Scripts",
                            "ERROR",
                            format!("Скрипты не перезагружены, работают прежние: {message}"),
                        )
                        .details(json!({"dir": self.settings.dir.display().to_string()})),
                    );
                }
            }
        }
    }

    /// Фоновая задача: проверка папки раз в `reload-interval`.
    pub async fn run_reload(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.settings.reload_interval);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            let this = self.clone();
            let _ = tokio::task::spawn_blocking(move || this.reload_if_changed()).await;
        }
    }

    /// Состояние для `GET /api/scripts`.
    pub fn info(&self) -> Json {
        let loaded = self.snapshot();
        let bindings: Vec<Json> = loaded
            .scripts
            .iter()
            .map(|s| {
                let matched = loaded.by_tag.values().filter(|chain| chain.iter().any(|c| Arc::ptr_eq(c, s))).count();
                let stats = self.stats.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&s.file).cloned();
                let (errors, last) = stats
                    .map(|st| {
                        (
                            st.errors.load(Ordering::Relaxed),
                            st.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
                        )
                    })
                    .unwrap_or((0, None));
                json!({
                    "script": s.file,
                    "tags": s.globs.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    "matchedTags": matched,
                    "write": s.has_write,
                    "errors": errors,
                    "lastError": last.as_ref().map(|(m, _)| m),
                    "lastErrorAt": last.as_ref().map(|(_, t)| t.to_rfc3339()),
                })
            })
            .collect();
        json!({
            "dir": self.settings.dir.display().to_string(),
            "bindings": bindings,
            "taggedChannels": loaded.by_tag.len(),
            "lastReloadError": *self.last_reload_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        })
    }
}

/// `dir/name`, только если `name` — относительный путь без `..` (скрипт не выходит из папки).
fn safe_join(dir: &Path, name: &str) -> Option<PathBuf> {
    let rel = Path::new(name);
    rel.components().all(|c| matches!(c, Component::Normal(_) | Component::CurDir)).then(|| dir.join(rel))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::TagConfig;
    use crate::events;
    use crate::metrics::Metrics;

    const TE: &str = "Барановичи-1.BN1_MCA1.TE_V.LINE2TE1.V";
    const ON_TIME: &str = "Барановичи-1.BN1_MCA1.M_P_ON_TIME.LINE1M1.P_ON_TIME";
    const QT: &str = "Барановичи-1.BN1_MCA1.QT_V.LINE2QT1.V";
    const COUNTER: &str = "Барановичи-1.BN1_MCA1.Счётчик.OBJECT1.CNT";
    const STATE: &str = "Барановичи-1.BN1_MCA1.Состояние.OBJECT1.STATE";

    fn tag(name: &str, dt: &str) -> Tag {
        Tag::from_config(&TagConfig {
            name: name.into(),
            node_id: "ns=2;s=1".into(),
            channel_id: None,
            device_name: Some("D".into()),
            field_name: Some("F".into()),
            device_type: Some("V".into()),
            protocol: None,
            data_type: dt.into(),
            polling_rate: 1000,
            enabled: true,
            writable: true,
            unit: Some("°C".into()),
            min_value: None,
            max_value: None,
            write_min: None,
            write_max: None,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
            history: None,
        })
    }

    fn all_tags() -> Vec<Tag> {
        vec![
            tag(TE, "FLOAT"),
            tag(ON_TIME, "FLOAT"),
            tag(QT, "FLOAT"),
            tag(COUNTER, "INT32"),
            tag(STATE, "STRING"),
            tag("A.OBJECT1.RT_PAR_F[7]", "FLOAT"),
            tag("A.OBJECT1.RT_PAR_F[17]", "FLOAT"),
        ]
    }

    fn by_name(name: &str) -> Tag {
        all_tags().into_iter().find(|t| t.name == name).unwrap()
    }

    fn scripts_in(dir: &Path, timeout_ms: u64) -> Result<Arc<Scripts>> {
        let m = Metrics::new();
        let (events, _rx) = events::channel(Arc::new(Metrics::new()));
        Scripts::load(
            ScriptSettings {
                dir: dir.to_path_buf(),
                timeout: Duration::from_millis(timeout_ms),
                reload_interval: Duration::from_secs(5),
            },
            all_tags().into_iter().map(|t| t.name).collect(),
            m.script_errors.clone(),
            m.scripts_bound_tags.clone(),
            events,
        )
    }

    /// Папка с примерами из `config/scripts/` репозитория и привязками `yaml`.
    fn dir_with(yaml: &str, files: &[(&str, &str)], examples: bool) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        if examples {
            for f in ["sensor_break.lua", "scale.lua", "deadband.lua"] {
                std::fs::copy(Path::new(env!("CARGO_MANIFEST_DIR")).join("config/scripts").join(f), dir.path().join(f))
                    .unwrap();
            }
        }
        for (n, c) in files {
            std::fs::write(dir.path().join(n), c).unwrap();
        }
        std::fs::write(dir.path().join(BINDINGS_FILE), yaml).unwrap();
        dir
    }

    fn run(s: &Scripts, name: &str, dt: &str, value: Option<TagValue>) -> Processed {
        let t = tag(name, dt);
        let q = if value.is_some() { Quality::Good } else { Quality::Bad };
        match s.snapshot().chain(name) {
            Some(chain) => s.process(chain, &t, value.as_ref(), q, Timestamp::now()),
            None => Processed { value, quality: q },
        }
    }

    fn f(v: f64) -> Option<TagValue> {
        Some(TagValue::F64(v))
    }

    const BAD: Processed = Processed { value: None, quality: Quality::Bad };

    // ---------------------------------------------------------------- маски --

    #[test]
    fn glob_brackets_are_literal_and_question_mark_is_one_char() {
        let g = TagGlob::new("*.OBJECT1.RT_PAR_F[7]");
        assert!(g.matches("A.OBJECT1.RT_PAR_F[7]"));
        assert!(!g.matches("A.OBJECT1.RT_PAR_F[17]"));
        assert!(!g.matches("A.OBJECT1.RT_PAR_F7"));
        let m = TagGlob::new("*.LINE?M*.P_ON_TIME");
        assert!(m.matches(ON_TIME));
        assert!(!m.matches("X.LINE12M1.P_ON_TIME"));
        assert!(TagGlob::new("*").matches("") && TagGlob::new("").matches("") && !TagGlob::new("").matches("a"));
        assert!(TagGlob::new("a*b*c").matches("aXXbYYc") && !TagGlob::new("a*b*c").matches("aXXbYY"));
    }

    // -------------------------------------------------------------- примеры --

    #[test]
    fn sensor_break_example_turns_break_codes_into_bad() {
        let dir = dir_with(
            "scripts:\n  - script: sensor_break.lua\n    tags: [\"Барановичи-1.BN1_MCA1.TE_V.*.V\"]\n    params: {valid_min: -50, valid_max: 150}\n",
            &[],
            true,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, TE, "FLOAT", f(3276.7)), BAD);
        assert_eq!(run(&s, TE, "FLOAT", f(-3276.7)), BAD);
        assert_eq!(run(&s, TE, "FLOAT", f(64.7)), Processed { value: f(64.7), quality: Quality::Good });
        assert_eq!(run(&s, QT, "FLOAT", f(3276.7)).value, f(3276.7), "канал без привязки — как есть");
    }

    #[test]
    fn scale_example_converts_both_ways() {
        let dir = dir_with(
            "scripts:\n  - script: scale.lua\n    tags: [\"Барановичи-1.BN1_MCA1.M_P_ON_TIME.LINE?M*.P_ON_TIME\"]\n    params: {k: 0.001}\n",
            &[],
            true,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, ON_TIME, "FLOAT", f(1500.0)).value, f(1.5));
        let t = by_name(ON_TIME);
        assert_eq!(s.to_plc(&t, &serde_json::json!(2.5)).unwrap(), serde_json::json!(2500));
        assert_eq!(
            s.to_plc(&by_name(QT), &serde_json::json!(2.5)).unwrap(),
            serde_json::json!(2.5),
            "без скрипта — как есть"
        );
        assert!(s.has_chain(ON_TIME) && !s.has_chain(QT));
    }

    #[test]
    fn deadband_example_keeps_state_per_channel() {
        let dir = dir_with(
            "scripts:\n  - script: deadband.lua\n    tags: [\"*.QT_V.*\", \"*.TE_V.*\"]\n    params: {delta: 0.05}\n",
            &[],
            true,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(10.0));
        assert_eq!(run(&s, QT, "FLOAT", f(10.03)).value, f(10.0));
        assert_eq!(run(&s, TE, "FLOAT", f(50.0)).value, f(50.0), "у другого канала своё состояние");
        assert_eq!(run(&s, QT, "FLOAT", f(10.2)).value, f(10.2));
        assert_eq!(run(&s, QT, "FLOAT", None), BAD);
        assert_eq!(run(&s, QT, "FLOAT", f(10.21)).value, f(10.21), "после обрыва — сразу");
    }

    // ---------------------------------------------------------------- типы --

    #[test]
    fn wire_types_follow_data_type() {
        let dir = dir_with(
            "scripts:\n  - {script: same.lua, tags: [\"*.TE_V.*\"]}\n  - {script: half.lua, tags: [\"*.CNT\"]}\n  - {script: text.lua, tags: [\"*.STATE\"]}\n",
            &[
                ("same.lua", "function process(v, q, ctx) return v, q end"),
                ("half.lua", "function process(v, q, ctx) return v / 2 + 0.6 end"),
                ("text.lua", "function process(v, q, ctx) return 'Мойка ' .. v end"),
            ],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        // FLOAT, снятый как f32, остаётся f32: в JSON 64.7, а не 64.69999694824219.
        let out = run(&s, TE, "FLOAT", Some(TagValue::F32(64.7))).value.unwrap();
        assert_eq!(out, TagValue::F32(64.7));
        assert_eq!(serde_json::to_string(&out).unwrap(), "64.7");
        assert_eq!(run(&s, COUNTER, "INT32", Some(TagValue::Int(5))).value, Some(TagValue::Int(3)), "5/2+0.6=3.1 → 3");
        assert_eq!(
            run(&s, STATE, "STRING", Some(TagValue::Text("ожидание".into()))).value,
            Some(TagValue::Text("Мойка ожидание".into()))
        );
    }

    #[test]
    fn bad_results_become_bad_frames() {
        let dir = dir_with(
            "scripts:\n  - {script: str.lua, tags: [\"*.TE_V.*\"]}\n  - {script: nan.lua, tags: [\"*.QT_V.*\"]}\n  - {script: qual.lua, tags: [\"*.CNT\"]}\n  - {script: tbl.lua, tags: [\"*.STATE\"]}\n",
            &[
                ("str.lua", "function process(v, q, ctx) return 'abc' end"),
                ("nan.lua", "function process(v, q, ctx) return 0/0 end"),
                ("qual.lua", "function process(v, q, ctx) return v, 'OK' end"),
                ("tbl.lua", "function process(v, q, ctx) return {} end"),
            ],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, TE, "FLOAT", f(1.0)), BAD, "строка в числовом канале");
        assert_eq!(run(&s, QT, "FLOAT", f(1.0)), BAD, "NaN");
        assert_eq!(run(&s, COUNTER, "INT32", Some(TagValue::Int(1))), BAD, "качество не GOOD/BAD");
        assert_eq!(run(&s, STATE, "STRING", Some(TagValue::Text("x".into()))), BAD, "таблица вместо значения");
        let info = s.info();
        assert_eq!(info["bindings"][0]["errors"], 1);
        assert!(info["bindings"][0]["lastError"].as_str().unwrap().contains(TE));
    }

    #[test]
    fn nil_value_is_a_bad_frame_even_if_the_script_says_nothing_about_quality() {
        let dir = dir_with(
            "scripts:\n  - {script: nil.lua, tags: [\"*.QT_V.*\"]}\n",
            &[("nil.lua", "function process(v, q, ctx) return nil end")],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, QT, "FLOAT", f(1.0)), BAD, "нет значения — не может быть GOOD");
        assert_eq!(s.info()["bindings"][0]["errors"], 0, "это не ошибка скрипта");
    }

    #[test]
    fn chain_runs_in_file_order_and_commands_in_reverse() {
        let dir = dir_with(
            "scripts:\n  - {script: scale.lua, tags: [\"*.QT_V.*\"], params: {k: 2}}\n  - {script: scale.lua, tags: [\"*.QT_V.*\"], params: {b: 1}}\n",
            &[],
            true,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(21.0), "10·2 + 1");
        assert_eq!(s.to_plc(&by_name(QT), &serde_json::json!(21.0)).unwrap(), serde_json::json!(10), "(21−1)/2");
    }

    #[test]
    fn failing_write_rejects_the_command() {
        let dir = dir_with(
            "scripts:\n  - {script: w.lua, tags: [\"*.QT_V.*\"]}\n",
            &[("w.lua", "function process(v, q, ctx) return v, q end\nfunction write(v, ctx) return nil end")],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        let err = s.to_plc(&by_name(QT), &serde_json::json!(1)).unwrap_err();
        assert!(err.contains("w.lua") && err.contains("nil"), "{err}");
    }

    // ----------------------------------------------------------- песочница --

    #[test]
    fn endless_loop_is_stopped_by_the_time_limit_and_the_state_survives() {
        let dir = dir_with(
            "scripts:\n  - {script: loop.lua, tags: [\"*.QT_V.*\"]}\n",
            &[("loop.lua", "function process(v, q, ctx) while true do end end")],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        sandbox::within(20, move || {
            let t0 = Instant::now();
            assert_eq!(run(&s, QT, "FLOAT", f(1.0)), BAD);
            assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
            assert!(s.info()["bindings"][0]["lastError"].as_str().unwrap().contains("дольше"));
            assert_eq!(run(&s, QT, "FLOAT", f(2.0)), BAD, "стейт жив: следующий вызов снова обрывается, а не висит");
            drop(dir);
        });
    }

    /// Зависший скрипт на тысячах каналов не должен тратить `timeout` на каждый канал каждого цикла.
    #[test]
    fn hung_script_is_paused_after_repeated_timeouts_and_comes_back() {
        sandbox::within(30, || {
            let source = "function process(v, q, ctx) if v > 0 then while true do end end return v end";
            let script = bound::BoundScript::new(
                "hang.lua",
                source,
                vec![TagGlob::new("*")],
                &serde_yaml_ng::Value::Null,
                Duration::from_millis(20),
            )
            .unwrap()
            .with_trip_pause(Duration::from_millis(300));
            let t = tag(QT, "FLOAT");
            let call = |v: f64| script.process(&t, Some(&TagValue::F64(v)), Quality::Good, Timestamp::now());

            for _ in 0..3 {
                assert!(call(1.0).unwrap_err().contains("дольше лимита"), "обычное превышение времени");
            }
            // Пауза: значения не доходят до Lua вовсе — даже безобидные, и без траты времени.
            let t0 = Instant::now();
            for _ in 0..1000 {
                assert!(call(0.0).unwrap_err().contains("приостановлен"));
            }
            assert!(t0.elapsed() < Duration::from_millis(200), "в паузе вызов не делается: {:?}", t0.elapsed());

            // Пауза вышла — пробный вызов проходит, скрипт снова в строю.
            std::thread::sleep(Duration::from_millis(350));
            assert_eq!(call(0.0).unwrap().value, Some(TagValue::F64(0.0)));
            assert!(call(1.0).unwrap_err().contains("дольше лимита"), "после возврата скрипт исполняется");
        });
    }

    #[test]
    fn coroutine_cannot_swallow_the_time_limit() {
        let dir = dir_with(
            "scripts:\n  - {script: co.lua, tags: [\"*.QT_V.*\"]}\n",
            &[(
                "co.lua",
                "function process(v, q, ctx)\n  local co = coroutine.create(function() while true do end end)\n  coroutine.resume(co)\n  return v\nend",
            )],
            false,
        );
        // coroutine убран из стейта — вызов падает сразу, а не «успешно» после бесконечного цикла.
        let s = scripts_in(dir.path(), 50).unwrap();
        sandbox::within(20, move || {
            let t0 = Instant::now();
            assert_eq!(run(&s, QT, "FLOAT", f(1.0)), BAD);
            assert!(t0.elapsed() < Duration::from_secs(2));
            drop(dir);
        });
    }

    #[test]
    fn sandbox_has_no_host_access_and_memory_is_capped() {
        let dir = dir_with(
            "scripts:\n  - {script: probe.lua, tags: [\"*.STATE\"]}\n  - {script: bomb.lua, tags: [\"*.TE_V.*\"]}\n",
            &[
                (
                    "probe.lua",
                    "function process(v, q, ctx)\n  return tostring(os) .. tostring(io) .. tostring(load) .. tostring(loadstring) .. tostring(pcall) .. tostring(debug) .. tostring(require) .. tostring(string.dump) .. tostring(coroutine)\nend",
                ),
                ("bomb.lua", "function process(v, q, ctx) return #string.rep('x', 512 * 1024 * 1024) end"),
            ],
            false,
        );
        let s = scripts_in(dir.path(), 500).unwrap();
        assert_eq!(
            run(&s, STATE, "STRING", Some(TagValue::Text("x".into()))).value,
            Some(TagValue::Text("nil".repeat(9)))
        );
        assert_eq!(run(&s, TE, "FLOAT", f(1.0)), BAD, "string.rep на 512 МБ упирается в потолок памяти");
    }

    // ----------------------------------------------------- загрузка и подхват --

    #[test]
    fn no_folder_or_bindings_means_pass_through() {
        let dir = tempfile::tempdir().unwrap();
        let s = scripts_in(&dir.path().join("нет"), 50).unwrap();
        assert!(s.snapshot().is_empty());
        assert_eq!(run(&s, TE, "FLOAT", f(3276.7)).value, f(3276.7));
    }

    #[test]
    fn startup_errors_fail_fast_with_a_clear_message() {
        let err = |yaml: &str, files: &[(&str, &str)]| {
            let dir = dir_with(yaml, files, false);
            format!("{:#}", scripts_in(dir.path(), 50).err().expect("ошибка старта"))
        };
        assert!(err("scripts:\n  - {script: nope.lua, tags: [\"*\"]}\n", &[]).contains("нет файла nope.lua"));
        assert!(
            err(
                "scripts:\n  - {script: s.lua, tags: [\"*\"]}\n",
                &[("s.lua", "function process(v, q, ctx) return v +")]
            )
            .contains("s.lua")
        );
        assert!(
            err("scripts:\n  - {script: n.lua, tags: [\"*\"]}\n", &[("n.lua", "x = 1")])
                .contains("нет функции process")
        );
        assert!(err("scripts:\n  - {script: ../x.lua, tags: [\"*\"]}\n", &[]).contains("вне папки"));
        assert!(err("scripts:\n  - {script: n.lua}\n", &[("n.lua", "x = 1")]).contains("не заданы tags"));
        assert!(err("scripts: [", &[]).contains("не разобран"));
    }

    #[test]
    fn hot_reload_applies_new_version_and_keeps_the_old_one_on_error() {
        let dir = dir_with(
            "scripts:\n  - {script: k.lua, tags: [\"*.QT_V.*\"]}\n",
            &[("k.lua", "function process(v, q, ctx) return v * 2 end")],
            false,
        );
        let s = scripts_in(dir.path(), 50).unwrap();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(20.0));

        let mut bump = 0u64;
        let mut rewrite = |src: &str| {
            let p = dir.path().join("k.lua");
            std::fs::write(&p, src).unwrap();
            // Время изменения — вперёд: в пределах одной миллисекунды отпечаток мог бы не измениться.
            bump += 10;
            let t = std::time::SystemTime::now() + Duration::from_secs(bump);
            std::fs::File::options().write(true).open(&p).unwrap().set_modified(t).unwrap();
        };
        rewrite("function process(v, q, ctx) return v * 3 end");
        s.reload_if_changed();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(30.0));

        rewrite("function process(v, q, ctx) return v *");
        s.reload_if_changed();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(30.0), "работает прежняя версия");
        assert!(s.info()["lastReloadError"].as_str().unwrap().contains("k.lua"));
        s.reload_if_changed(); // ошибка не повторяется на тот же отпечаток
        rewrite("function process(v, q, ctx) return v * 4 end");
        s.reload_if_changed();
        assert_eq!(run(&s, QT, "FLOAT", f(10.0)).value, f(40.0));
        assert!(s.info()["lastReloadError"].is_null(), "после удачной перезагрузки ошибка снята");
    }
}
