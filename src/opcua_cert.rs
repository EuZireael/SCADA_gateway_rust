//! Сертификат OPC UA-клиента шлюза: свой (`GATEWAY_OPCUA_CERT_FILE` + `GATEWAY_OPCUA_KEY_FILE`, например от УЦ
//! предприятия) или самоподписанный в каталоге PKI, который шлюз выпускает сам при первом запуске. Срок действия
//! проверяется при запуске и раз в час: метрика `scada_opcua_cert_expiry_days`, предупреждение в журнал и событие за
//! `GATEWAY_OPCUA_CERT_WARN_DAYS` дней до окончания. Просроченный сертификат ПЛК отвергнет, и защищённое подключение
//! перестанет устанавливаться — без предупреждения это выглядело бы как внезапная потеря связи.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::{DateTime, Utc};
use opcua::crypto::{CertificateStore, KeySize, PrivateKey, X509, X509Data};
use opcua::types::{ApplicationDescription, ApplicationType, LocalizedText, UAString};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::events::{Event, EventSink};
use crate::metrics::Metrics;

/// Имя приложения в описании клиента и в сертификате, который шлюз выпускает сам.
pub const APPLICATION_NAME: &str = "SCADA Gateway";
/// URI приложения по умолчанию (`GATEWAY_OPCUA_APPLICATION_URI`).
pub const DEFAULT_APPLICATION_URI: &str = "urn:scada:gateway";
/// Срок самоподписанного сертификата по умолчанию, дней (5 лет; async-opcua сам выпускает на 365).
pub const DEFAULT_DAYS: u32 = 1825;
/// За сколько дней до окончания предупреждать по умолчанию.
pub const DEFAULT_WARN_DAYS: u32 = 30;
/// Как часто перепроверять срок (метрика обновляется, предупреждение — не чаще [`WARN_EVERY`]).
const CHECK_EVERY: Duration = Duration::from_secs(3600);
/// Не чаще раза в сутки повторять предупреждение о сроке.
const WARN_EVERY: Duration = Duration::from_secs(24 * 3600);

/// Сертификат и ключ в каталоге PKI — имена, которые читает async-opcua.
const OWN_CERT: &str = "own/cert.der";
const OWN_KEY: &str = "private/private.pem";

/// Откуда шлюз берёт сертификат клиента OPC UA.
#[derive(Debug, Clone, PartialEq)]
pub struct CertSettings {
    /// Каталог PKI: `own/`, `private/`, `trusted/`, `rejected/`.
    pub pki_dir: PathBuf,
    /// Свой сертификат (`.der` или `.pem`) и закрытый ключ (`.pem`). Нет — самоподписанный в `pki_dir`.
    pub own: Option<(PathBuf, PathBuf)>,
    /// URI приложения: первым альтернативным именем (SAN) сертификата должен быть он, иначе сервер отвергнет клиента.
    pub application_uri: String,
    /// Срок самоподписанного сертификата, который шлюз выпускает сам, дней.
    pub days: u32,
    /// За сколько дней до окончания предупреждать.
    pub warn_days: u32,
}

impl CertSettings {
    /// Самоподписанный сертификат в `pki_dir` с настройками по умолчанию.
    pub fn in_dir(pki_dir: impl Into<PathBuf>) -> Self {
        CertSettings {
            pki_dir: pki_dir.into(),
            own: None,
            application_uri: DEFAULT_APPLICATION_URI.into(),
            days: DEFAULT_DAYS,
            warn_days: DEFAULT_WARN_DAYS,
        }
    }

    /// Пути сертификата и ключа: свои из настроек или стандартные в каталоге PKI.
    pub fn paths(&self) -> (PathBuf, PathBuf) {
        match &self.own {
            Some((cert, key)) => (cert.clone(), key.clone()),
            None => (self.pki_dir.join(OWN_CERT), self.pki_dir.join(OWN_KEY)),
        }
    }
}

/// Сертификат клиента, с которым шлюз подключается к серверам OPC UA.
#[derive(Debug, Clone)]
pub struct OwnCert {
    /// Файл сертификата.
    pub cert_path: PathBuf,
    /// Файл закрытого ключа.
    pub key_path: PathBuf,
    /// Задан настройкой (`GATEWAY_OPCUA_CERT_FILE`), а не выпущен шлюзом.
    pub configured: bool,
    /// Шлюз выпустил его только что.
    pub issued: bool,
    /// Действует до.
    pub not_after: DateTime<Utc>,
    /// Отпечаток SHA-1 (так его показывают ПЛК и инженерное ПО в списке доверенных).
    pub thumbprint: String,
}

/// Состояние срока сертификата.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// Действует дольше порога предупреждения.
    Valid,
    /// Осталось не больше порога.
    Soon,
    /// Истёк.
    Expired,
}

impl OwnCert {
    /// Сколько целых дней осталось (у истёкшего — отрицательное число).
    pub fn days_left(&self, now: DateTime<Utc>) -> i64 {
        (self.not_after - now).num_seconds().div_euclid(86_400)
    }

    /// Срок относительно порога предупреждения.
    pub fn expiry(&self, now: DateTime<Utc>, warn_days: u32) -> Expiry {
        if self.not_after <= now {
            Expiry::Expired
        } else if self.days_left(now) < i64::from(warn_days) {
            Expiry::Soon
        } else {
            Expiry::Valid
        }
    }
}

/// Выпуск и чтение — под одной блокировкой на процесс: контроллеры подключаются одновременно, и при первом запуске
/// каждый выпускал бы свою пару (сертификат одного с ключом другого). Между процессами — блокировка файла в каталоге
/// PKI ([`lock_pki`]).
static ISSUE: Mutex<()> = Mutex::new(());

/// Сертификат клиента: свой из настроек или самоподписанный из каталога PKI; если самоподписанного ещё нет — выпустить.
/// Проверяет, что ключ от этого сертификата и что URI приложения совпадает с первым альтернативным именем.
pub fn ensure(s: &CertSettings) -> Result<OwnCert> {
    let _guard = ISSUE.lock().unwrap_or_else(|e| e.into_inner());
    ensure_locked(s)
}

/// [`ensure`] без блокировки процесса: каталог PKI блокируется файлом.
fn ensure_locked(s: &CertSettings) -> Result<OwnCert> {
    let (cert_path, key_path) = s.paths();
    if s.own.is_some() {
        return load(s, &cert_path, &key_path, false);
    }
    let _lock = lock_pki(&s.pki_dir)?;
    let issued = match (cert_path.exists(), key_path.exists()) {
        (true, true) => false,
        (false, false) => {
            issue(s, &cert_path, &key_path, false)?;
            true
        }
        // Половина пары — след прерванного выпуска (или гонки в прежних версиях): ключ без сертификата бесполезен.
        _ => bail!(
            "в {} есть только один из файлов {OWN_CERT} и {OWN_KEY}; удалите оставшийся, шлюз выпустит пару заново \
             (или `scada-gateway opcua-cert renew`)",
            s.pki_dir.display()
        ),
    };
    load(s, &cert_path, &key_path, issued)
}

/// Текущий сертификат без выпуска: `None`, если самоподписанного ещё нет.
pub fn current(s: &CertSettings) -> Result<Option<OwnCert>> {
    let (cert_path, key_path) = s.paths();
    if s.own.is_none() && !cert_path.exists() && !key_path.exists() {
        return Ok(None);
    }
    ensure(s).map(Some)
}

/// Перевыпустить самоподписанный сертификат: прежняя пара сохраняется рядом с суффиксом `.<время>.bak`.
pub fn renew(s: &CertSettings) -> Result<OwnCert> {
    let _guard = ISSUE.lock().unwrap_or_else(|e| e.into_inner());
    ensure!(
        s.own.is_none(),
        "сертификат задан настройкой GATEWAY_OPCUA_CERT_FILE — его перевыпускает тот, кто выдал (УЦ предприятия)"
    );
    let (cert_path, key_path) = s.paths();
    let _lock = lock_pki(&s.pki_dir)?;
    let stamp = Utc::now().format("%Y%m%d-%H%M%S");
    for path in [&cert_path, &key_path] {
        if path.exists() {
            let backup = PathBuf::from(format!("{}.{stamp}.bak", path.display()));
            std::fs::rename(path, &backup)
                .with_context(|| format!("не переименовать {} в {}", path.display(), backup.display()))?;
        }
    }
    issue(s, &cert_path, &key_path, true)?;
    load(s, &cert_path, &key_path, true)
}

/// Блокировка каталога PKI между процессами (пара резерва на одном хосте, параллельные тесты с общим каталогом):
/// выпуск и чтение пары — под ней. Снимается сама при закрытии файла, в том числе если процесс упал.
fn lock_pki(dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(dir).with_context(|| format!("каталог PKI {} не создаётся", dir.display()))?;
    let path = dir.join(".cert.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("{} не открывается", path.display()))?;
    file.lock().with_context(|| format!("{} не блокируется", path.display()))?;
    Ok(file)
}

/// Выпустить самоподписанный сертификат клиента на `s.days` дней.
fn issue(s: &CertSettings, cert_path: &Path, key_path: &Path, overwrite: bool) -> Result<()> {
    ensure!(s.days > 0, "срок сертификата — хотя бы 1 день");
    let description = ApplicationDescription {
        application_uri: UAString::from(s.application_uri.as_str()),
        product_uri: UAString::from(s.application_uri.as_str()),
        application_name: LocalizedText::new("", APPLICATION_NAME),
        application_type: ApplicationType::Client,
        gateway_server_uri: UAString::null(),
        discovery_profile_uri: UAString::null(),
        discovery_urls: None,
    };
    // Альтернативные имена: URI приложения первым, затем имя хоста и адреса — как у async-opcua.
    let mut data = X509Data::from(description);
    data.certificate_duration_days = s.days;
    CertificateStore::create_certificate_and_key(&data, overwrite, cert_path, key_path)
        .map_err(|e| anyhow!("сертификат OPC UA не выпущен в {}: {e}", s.pki_dir.display()))?;
    info!(
        "🔐 Выпущен сертификат OPC UA-клиента на {} дн.: {} — внесите его в доверенные на ПЛК",
        s.days,
        cert_path.display()
    );
    Ok(())
}

/// Прочитать пару и проверить её.
fn load(s: &CertSettings, cert_path: &Path, key_path: &Path, issued: bool) -> Result<OwnCert> {
    let cert = CertificateStore::read_cert(cert_path).map_err(|e| anyhow!("сертификат OPC UA: {e}"))?;
    let key = PrivateKey::read_pem_file(key_path)
        .map_err(|_| anyhow!("закрытый ключ OPC UA {} не читается (нужен PEM, PKCS#8)", key_path.display()))?;
    ensure!(
        key_matches(&cert, &key)?,
        "закрытый ключ {} не от сертификата {}{}",
        key_path.display(),
        cert_path.display(),
        if s.own.is_none() { " — перевыпустите пару: `scada-gateway opcua-cert renew`" } else { "" }
    );
    cert.is_application_uri_valid(&s.application_uri).map_err(|_| {
        anyhow!(
            "первое альтернативное имя (SAN) сертификата {} — не URI приложения «{}»: сервер OPC UA отвергнет клиента. \
             Задайте GATEWAY_OPCUA_APPLICATION_URI как в сертификате{}",
            cert_path.display(),
            s.application_uri,
            if s.own.is_none() {
                " или перевыпустите его: `scada-gateway opcua-cert renew`"
            } else {
                ""
            }
        )
    })?;
    // Пути — абсолютные: async-opcua относительный путь считает от каталога PKI, а не от рабочего каталога.
    let absolute = |p: &Path| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    Ok(OwnCert {
        cert_path: absolute(cert_path),
        key_path: absolute(key_path),
        configured: s.own.is_some(),
        issued,
        not_after: cert.not_after().map_err(|_| anyhow!("в сертификате {} не читается срок", cert_path.display()))?,
        thumbprint: cert.thumbprint().as_hex_string(),
    })
}

/// Ключ от этого сертификата: подпись ключом проверяется открытым ключом сертификата.
fn key_matches(cert: &X509, key: &PrivateKey) -> Result<bool> {
    const PROBE: &[u8] = b"scada-gateway: own certificate check";
    let mut signature = vec![0u8; key.size()];
    key.sign_sha256(PROBE, &mut signature).map_err(|e| anyhow!("подпись ключом: {e}"))?;
    let public = cert.public_key().map_err(|e| anyhow!("открытый ключ сертификата: {e}"))?;
    Ok(public.verify_sha256(PROBE, &signature).unwrap_or(false))
}

/// Записать срок в метрику и, если он подходит к концу или истёк, — предупреждение в журнал и событие.
pub fn report(cert: &OwnCert, warn_days: u32, now: DateTime<Utc>, metrics: &Metrics, events: &EventSink) -> Expiry {
    let days = cert.days_left(now);
    metrics.opcua_cert_expiry_days.with_label_values(&[&cert.cert_path.display().to_string()]).set(days);
    let expiry = cert.expiry(now, warn_days);
    let (severity, text) = match expiry {
        Expiry::Valid => return expiry,
        Expiry::Soon => (
            "WARNING",
            format!(
                "Сертификат OPC UA-клиента истекает {} (через {days} дн.): после этого ПЛК отвергнут защищённое \
                 подключение. Перевыпустите его и внесите новый в доверенные на ПЛК",
                cert.not_after.format("%Y-%m-%d")
            ),
        ),
        Expiry::Expired => (
            "ERROR",
            format!(
                "Сертификат OPC UA-клиента истёк {}: защищённое подключение к ПЛК не установится. Перевыпустите его \
                 и внесите новый в доверенные на ПЛК",
                cert.not_after.format("%Y-%m-%d")
            ),
        ),
    };
    warn!("⚠ {text} ({}; docs/OPERATIONS.md, «Сертификаты»)", cert.cert_path.display());
    events.emit(Event::new("SYSTEM", "Gateway", severity, text).details(json!({
        "certificate": cert.cert_path.display().to_string(),
        "thumbprint": cert.thumbprint,
        "notAfter": cert.not_after.to_rfc3339(),
        "daysLeft": days,
    })));
    expiry
}

/// Перепроверять срок раз в час (сертификат могли перевыпустить без перезапуска); предупреждение — не чаще раза в сутки.
pub async fn watch(settings: CertSettings, metrics: Arc<Metrics>, events: EventSink, cancel: CancellationToken) {
    let mut last_warning: Option<tokio::time::Instant> = None;
    let mut tick = tokio::time::interval(CHECK_EVERY);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        let s = settings.clone();
        let cert = match tokio::task::spawn_blocking(move || ensure(&s)).await {
            Ok(Ok(cert)) => cert,
            Ok(Err(e)) => {
                warn!("Сертификат OPC UA-клиента не проверен: {e:#}");
                continue;
            }
            Err(_) => continue,
        };
        let now = Utc::now();
        if cert.expiry(now, settings.warn_days) == Expiry::Valid {
            report(&cert, settings.warn_days, now, &metrics, &events);
            last_warning = None;
        } else if last_warning.is_none_or(|t| t.elapsed() >= WARN_EVERY) {
            report(&cert, settings.warn_days, now, &metrics, &events);
            last_warning = Some(tokio::time::Instant::now());
        } else {
            // Только метрика: повтор предупреждения — не чаще раза в сутки.
            metrics
                .opcua_cert_expiry_days
                .with_label_values(&[&cert.cert_path.display().to_string()])
                .set(cert.days_left(now));
        }
    }
}

/// Строка для журнала при запуске.
pub fn describe(cert: &OwnCert, now: DateTime<Utc>) -> String {
    format!(
        "{} сертификат OPC UA-клиента {}: отпечаток {}, действует до {} ({} дн.)",
        if cert.configured { "Свой" } else { "Самоподписанный" },
        cert.cert_path.display(),
        cert.thumbprint,
        cert.not_after.format("%Y-%m-%d"),
        cert.days_left(now)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> tempfile::TempDir {
        tempfile::Builder::new().prefix(&format!("opcua-cert-{name}-")).tempdir().unwrap()
    }

    fn settings(pki: &Path, days: u32) -> CertSettings {
        CertSettings { days, ..CertSettings::in_dir(pki) }
    }

    #[test]
    fn self_signed_certificate_is_issued_once_with_the_configured_lifetime() {
        let pki = dir("issue");
        let s = settings(pki.path(), 3650);
        let first = ensure(&s).unwrap();
        assert!(first.issued && !first.configured);
        assert!(pki.path().join(OWN_CERT).exists() && pki.path().join(OWN_KEY).exists());
        let days = first.days_left(Utc::now());
        assert!((3648..=3650).contains(&days), "{days}");
        let again = ensure(&s).unwrap();
        assert!(!again.issued);
        assert_eq!(again.thumbprint, first.thumbprint, "существующий сертификат не перевыпускается");
    }

    #[test]
    fn concurrent_first_start_yields_one_consistent_pair() {
        // Прежде контроллеры одновременно выпускали каждый свою пару: сертификат одного с ключом другого.
        let pki = dir("race");
        let s = settings(pki.path(), 30);
        let prints: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..6).map(|_| scope.spawn(|| ensure(&s).unwrap().thumbprint)).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(prints.windows(2).all(|w| w[0] == w[1]), "{prints:?}");
    }

    #[test]
    fn processes_sharing_a_pki_dir_issue_one_pair() {
        // Как два процесса (пара резерва на одном хосте): без блокировки процесса, только файл в каталоге PKI.
        let pki = dir("processes");
        let s = settings(pki.path(), 30);
        let prints: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..6).map(|_| scope.spawn(|| ensure_locked(&s).unwrap().thumbprint)).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(prints.windows(2).all(|w| w[0] == w[1]), "{prints:?}");
    }

    #[test]
    fn expiry_is_reported_before_the_end_and_after_it() {
        let pki = dir("soon");
        let soon = ensure(&settings(pki.path(), 10)).unwrap();
        let now = Utc::now();
        assert_eq!(soon.expiry(now, 30), Expiry::Soon);
        assert_eq!(soon.expiry(now, 5), Expiry::Valid);
        assert_eq!(soon.expiry(now + chrono::Duration::days(11), 30), Expiry::Expired);
        assert!(soon.days_left(now + chrono::Duration::days(11)) < 0);

        let metrics = Metrics::new();
        let (events, mut rx) = crate::events::channel(Arc::new(Metrics::new()));
        assert_eq!(report(&soon, 30, now, &metrics, &events), Expiry::Soon);
        let text = metrics.render();
        assert!(text.contains("scada_opcua_cert_expiry_days{"), "{text}");
        assert!(text.lines().any(|l| l.starts_with("scada_opcua_cert_expiry_days{") && l.ends_with(" 9")), "{text}");
        let event = rx.try_recv().expect("событие о сроке");
        assert_eq!((event.event_type.as_str(), event.severity.as_str()), ("SYSTEM", "WARNING"));

        assert_eq!(report(&soon, 30, now + chrono::Duration::days(11), &metrics, &events), Expiry::Expired);
        assert_eq!(rx.try_recv().expect("событие об истёкшем").severity, "ERROR");
        assert_eq!(report(&soon, 5, now, &metrics, &events), Expiry::Valid);
        assert!(rx.try_recv().is_err(), "без повода события нет");
    }

    #[test]
    fn configured_certificate_is_used_and_checked() {
        let pki = dir("own");
        let other = dir("other");
        // «Сертификат от УЦ»: пара в своих файлах, URI приложения — свой.
        let uri = "urn:plant:scada-gw-1";
        let own = |root: &Path| -> (PathBuf, PathBuf) {
            let s = CertSettings { application_uri: uri.into(), ..settings(root, 400) };
            ensure(&s).unwrap();
            (root.join(OWN_CERT), root.join(OWN_KEY))
        };
        let (cert, key) = own(other.path());
        let s = CertSettings {
            own: Some((cert.clone(), key.clone())),
            application_uri: uri.into(),
            ..settings(pki.path(), 1)
        };
        let loaded = ensure(&s).unwrap();
        assert!(loaded.configured && !loaded.issued);
        assert_eq!(loaded.cert_path, cert);
        assert!(!pki.path().join(OWN_CERT).exists(), "при своём сертификате самоподписанный не выпускается");

        // URI приложения не совпадает с сертификатом — сервер отверг бы клиента.
        let wrong_uri = CertSettings { application_uri: DEFAULT_APPLICATION_URI.into(), ..s.clone() };
        assert!(format!("{:#}", ensure(&wrong_uri).unwrap_err()).contains("SAN"));

        // Ключ от другой пары.
        let third = dir("third");
        let (_, foreign_key) = own(third.path());
        let mismatch = CertSettings { own: Some((cert.clone(), foreign_key)), ..s.clone() };
        assert!(format!("{:#}", ensure(&mismatch).unwrap_err()).contains("не от сертификата"));

        // Нет файла.
        let missing = CertSettings { own: Some((pki.path().join("нет.der"), key)), ..s.clone() };
        assert!(ensure(&missing).is_err());
        assert!(renew(&s).is_err(), "свой сертификат шлюз не перевыпускает");
    }

    #[test]
    fn renew_keeps_the_previous_pair_and_issues_a_new_one() {
        let pki = dir("renew");
        let s = settings(pki.path(), 5);
        let old = ensure(&s).unwrap();
        let new = renew(&CertSettings { days: 900, ..s.clone() }).unwrap();
        assert_ne!(new.thumbprint, old.thumbprint);
        assert!(new.days_left(Utc::now()) >= 898);
        let backups = std::fs::read_dir(pki.path().join("own"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".bak"))
            .count();
        assert_eq!(backups, 1);
    }

    #[test]
    fn half_a_pair_is_an_error_not_a_silent_reissue() {
        let pki = dir("half");
        let s = settings(pki.path(), 30);
        ensure(&s).unwrap();
        std::fs::remove_file(pki.path().join(OWN_KEY)).unwrap();
        assert!(format!("{:#}", ensure(&s).unwrap_err()).contains("только один"));
    }
}
