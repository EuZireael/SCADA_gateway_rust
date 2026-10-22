//! Защита OPC UA-фасада: политики безопасности, пользователи с ролями, сертификат сервера.
//!
//! Фасад отдаёт значения станции и принимает команды в прошивку, поэтому без настроек он не стартует: ни анонимный вход, ни
//! канал без защиты не включаются молча — только явно (`OPCUA_ALLOW_ANONYMOUS`, политика `None` в `OPCUA_SECURITY`).
//!
//! Окружение (имена и значения см. [`Security::from_env`]):
//!   OPCUA_SECURITY          политики через запятую, по умолчанию `Basic256Sha256_SignAndEncrypt`
//!   OPCUA_USERS_FILE        файл пользователей: `имя  хеш-argon2  роль` (роль `read` или `write`)
//!   OPCUA_ALLOW_ANONYMOUS   разрешить анонимный вход (по умолчанию нет)
//!   OPCUA_PKI_DIR           постоянный каталог сертификатов (по умолчанию /var/lib/ptusa-opcua/pki)
//!   OPCUA_TRUST_ALL_CLIENTS доверять любому сертификату клиента (по умолчанию нет: только из `trusted/`)
//!   OPCUA_CERT_DAYS         срок выпускаемого сертификата сервера, дней (по умолчанию 3650)
//!   OPCUA_CERT_HOSTS        доп. имена и адреса в сертификате сервера через запятую

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use argon2::Argon2;
use argon2::password_hash::phc::PasswordHash;
use argon2::password_hash::{PasswordHasher, PasswordVerifier};
use async_trait::async_trait;
use opcua::crypto::{CertificateStore, SecurityPolicy, X509Data};
use opcua::server::ServerEndpoint;
use opcua::server::ServerUserToken;
use opcua::server::address_space::AccessLevel;
use opcua::server::authenticator::{AuthManager, DefaultAuthenticator, Password, UserToken};
use opcua::types::{
    ApplicationDescription, ApplicationType, Error, LocalizedText, MessageSecurityMode, NodeId, StatusCode, UAString,
};
use tracing::{info, warn};

/// Что пользователю разрешено.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Только чтение.
    Read,
    /// Чтение и запись (команды прошивке).
    Write,
}

/// Пользователь с хешем пароля (пароль в открытом виде фасад не хранит).
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    hash: String,
    pub role: Role,
}

/// Хеш argon2 (PHC-строка) пароля — для файла пользователей (`ptusa-opcua hash-password`).
pub fn hash_password(password: &str) -> Result<String> {
    // Соль — случайная, из системного генератора (getrandom).
    Argon2::default().hash_password(password.as_bytes()).map(|h| h.to_string()).map_err(|e| anyhow!("хеш пароля: {e}"))
}

/// Файл пользователей: строка `имя  хеш  роль`; пустые строки и `#…` пропускаются. Хеш должен быть PHC-строкой argon2.
pub fn parse_users(text: &str) -> Result<Vec<User>> {
    let mut users: Vec<User> = Vec::new();
    for (n, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let at = |msg: &str| anyhow!("файл пользователей, строка {}: {msg}", n + 1);
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [name, hash, role] = fields[..] else {
            return Err(at("ожидается «имя хеш роль»"));
        };
        let role = match role {
            "read" => Role::Read,
            "write" => Role::Write,
            other => return Err(at(&format!("роль {other:?}: допустимы read и write"))),
        };
        let parsed =
            PasswordHash::new(hash).map_err(|e| at(&format!("хеш пароля не разобран ({e}); нужна строка argon2")))?;
        ensure!(parsed.algorithm.as_str().starts_with("argon2"), at("хеш не argon2"));
        ensure!(users.iter().all(|u| u.name != name), at(&format!("пользователь {name:?} уже есть")));
        users.push(User { name: name.to_string(), hash: hash.to_string(), role });
    }
    Ok(users)
}

/// Политика и режим из имени: `None`, `Basic256Sha256_Sign`, `Aes256Sha256RsaPss_SignAndEncrypt` (регистр, `_`, `-`, пробелы
/// не важны; без режима — `SignAndEncrypt`).
fn parse_policy(item: &str) -> Result<(SecurityPolicy, MessageSecurityMode)> {
    let norm: String = item.chars().filter(|c| !matches!(c, '_' | '-' | ' ')).collect::<String>().to_ascii_lowercase();
    if norm == "none" {
        return Ok((SecurityPolicy::None, MessageSecurityMode::None));
    }
    let (name, mode) = if let Some(n) = norm.strip_suffix("signandencrypt") {
        (n, MessageSecurityMode::SignAndEncrypt)
    } else if let Some(n) = norm.strip_suffix("sign") {
        (n, MessageSecurityMode::Sign)
    } else {
        (norm.as_str(), MessageSecurityMode::SignAndEncrypt)
    };
    let policy = match name {
        "basic256sha256" => SecurityPolicy::Basic256Sha256,
        "aes128sha256rsaoaep" => SecurityPolicy::Aes128Sha256RsaOaep,
        "aes256sha256rsapss" => SecurityPolicy::Aes256Sha256RsaPss,
        _ => bail!(
            "OPCUA_SECURITY: политика {item:?} неизвестна (None, Basic256Sha256, Aes128_Sha256_RsaOaep, Aes256_Sha256_RsaPss; \
             к ней _Sign или _SignAndEncrypt)"
        ),
    };
    Ok((policy, mode))
}

/// Защита фасада.
#[derive(Debug, Clone)]
pub struct Security {
    /// Точки подключения: политика и режим.
    pub endpoints: Vec<(SecurityPolicy, MessageSecurityMode)>,
    /// Разрешён ли анонимный вход.
    pub allow_anonymous: bool,
    pub users: Vec<User>,
    pub pki_dir: PathBuf,
    /// Доверять любому сертификату клиента.
    pub trust_all_clients: bool,
    /// Срок выпускаемого сертификата сервера, дней.
    pub cert_days: u32,
    /// Дополнительные имена и адреса сертификата (хост анонса и `OPCUA_CERT_HOSTS`).
    pub cert_hosts: Vec<String>,
}

impl Security {
    /// Без защиты: канал `None`, анонимный вход, любые клиенты. Стенд и тесты — боевой запуск такого не получает без
    /// явных `OPCUA_SECURITY=None` и `OPCUA_ALLOW_ANONYMOUS=true`.
    pub fn open(pki_dir: PathBuf) -> Self {
        Security {
            endpoints: vec![(SecurityPolicy::None, MessageSecurityMode::None)],
            allow_anonymous: true,
            users: Vec::new(),
            pki_dir,
            trust_all_clients: true,
            cert_days: 3650,
            cert_hosts: Vec::new(),
        }
    }

    /// Настройки из окружения (`get` — чтение переменной, пустое значение считается незаданным). Ошибка — фасад не стартует:
    /// без пользователей и без явного разрешения анонимного входа подключиться было бы некому, а «молча открыть» нельзя.
    pub fn from_env(get: impl Fn(&str) -> Option<String>, announce_host: &str) -> Result<Self> {
        let var = |name: &str| get(name).filter(|v| !v.trim().is_empty());
        let flag = |name: &str| -> Result<bool> {
            match var(name).map(|v| v.trim().to_ascii_lowercase()).as_deref() {
                None | Some("false" | "0" | "no") => Ok(false),
                Some("true" | "1" | "yes") => Ok(true),
                Some(other) => bail!("{name}: ожидается true или false, а не {other:?}"),
            }
        };
        let mut endpoints = Vec::new();
        for item in var("OPCUA_SECURITY").as_deref().unwrap_or("Basic256Sha256_SignAndEncrypt").split(',') {
            let e = parse_policy(item.trim())?;
            if !endpoints.contains(&e) {
                endpoints.push(e);
            }
        }
        let allow_anonymous = flag("OPCUA_ALLOW_ANONYMOUS")?;
        let users = match var("OPCUA_USERS_FILE") {
            Some(path) => {
                let text = std::fs::read_to_string(&path).with_context(|| format!("OPCUA_USERS_FILE {path}"))?;
                parse_users(&text).with_context(|| format!("OPCUA_USERS_FILE {path}"))?
            }
            None => Vec::new(),
        };
        let secure = endpoints.iter().any(|(p, _)| *p != SecurityPolicy::None);
        let open_channel = endpoints.iter().any(|(p, _)| *p == SecurityPolicy::None);
        ensure!(
            allow_anonymous || !users.is_empty(),
            "некому подключаться: задайте OPCUA_USERS_FILE (пользователи с хешем пароля: `ptusa-opcua hash-password`) \
             или явно разрешите анонимный вход — OPCUA_ALLOW_ANONYMOUS=true (только стенд)"
        );
        ensure!(
            secure || allow_anonymous || users.is_empty(),
            "OPCUA_SECURITY=None без анонимного входа: пользователь с паролем по открытому каналу не разрешается"
        );
        if open_channel && !users.is_empty() && !allow_anonymous {
            bail!("OPCUA_SECURITY содержит None: пароль пользователя пошёл бы по открытому каналу — уберите None");
        }
        let cert_days: u32 = match var("OPCUA_CERT_DAYS") {
            Some(v) => {
                v.trim().parse().ok().filter(|d| *d > 0).context("OPCUA_CERT_DAYS: ожидается число дней, больше 0")?
            }
            None => 3650,
        };
        let mut cert_hosts: Vec<String> = var("OPCUA_CERT_HOSTS")
            .map(|v| v.split(',').map(|h| h.trim().to_string()).filter(|h| !h.is_empty()).collect())
            .unwrap_or_default();
        if !announce_host.is_empty() && !cert_hosts.iter().any(|h| h == announce_host) {
            cert_hosts.push(announce_host.to_string());
        }
        Ok(Security {
            endpoints,
            allow_anonymous,
            users,
            pki_dir: PathBuf::from(var("OPCUA_PKI_DIR").unwrap_or_else(|| "/var/lib/ptusa-opcua/pki".into())),
            trust_all_clients: flag("OPCUA_TRUST_ALL_CLIENTS")?,
            cert_days,
            cert_hosts,
        })
    }

    /// Что фасад делает по настройкам — строки для журнала при запуске (с предупреждениями о слабой защите).
    pub fn summary(&self) -> Vec<String> {
        let mut out = Vec::new();
        let policies: Vec<String> = self.endpoints.iter().map(|(p, m)| format!("{p:?}/{m:?}")).collect();
        out.push(format!("защита: {}", policies.join(", ")));
        out.push(format!(
            "пользователи: {} (запись — {})",
            self.users.len(),
            self.users.iter().filter(|u| u.role == Role::Write).map(|u| u.name.as_str()).collect::<Vec<_>>().join(", ")
        ));
        if self.allow_anonymous {
            out.push("⚠ АНОНИМНЫЙ ВХОД РАЗРЕШЁН: любой, кто достучится до порта, читает значения и пишет команды в прошивку — только стенд".into());
        }
        if self.endpoints.iter().any(|(p, _)| *p == SecurityPolicy::None) {
            out.push("⚠ точка без защиты (None): трафик не подписан и не шифруется — только стенд".into());
        }
        if self.trust_all_clients {
            out.push("⚠ OPCUA_TRUST_ALL_CLIENTS: сертификаты клиентов не проверяются — только стенд".into());
        }
        out
    }

    /// Пара сертификат/ключ сервера в постоянном PKI-каталоге: выпускается один раз (`OPCUA_CERT_DAYS`), дальше читается.
    /// Возвращает пути и срок действия. Перевыпуск — удалить `own/cert.der` и `private/private.pem` и перезапустить фасад
    /// (клиентов с `trusted/` придётся ввести заново).
    pub fn ensure_cert(&self, application_uri: &str) -> Result<OwnCert> {
        let cert_path = self.pki_dir.join("own").join("cert.der");
        let key_path = self.pki_dir.join("private").join("private.pem");
        for dir in [cert_path.parent(), key_path.parent()].into_iter().flatten() {
            std::fs::create_dir_all(dir).with_context(|| format!("каталог PKI {}", dir.display()))?;
        }
        match (cert_path.exists(), key_path.exists()) {
            (true, true) => {}
            (false, false) => {
                let description = ApplicationDescription {
                    application_uri: UAString::from(application_uri),
                    product_uri: UAString::from(application_uri),
                    application_name: LocalizedText::new("", "ptusa-opcua"),
                    application_type: ApplicationType::Server,
                    gateway_server_uri: UAString::null(),
                    discovery_profile_uri: UAString::null(),
                    discovery_urls: None,
                };
                let mut data = X509Data::from((description, Some(self.cert_hosts.clone())));
                data.certificate_duration_days = self.cert_days;
                CertificateStore::create_certificate_and_key(&data, false, &cert_path, &key_path)
                    .map_err(|e| anyhow!("сертификат сервера не выпущен в {}: {e}", self.pki_dir.display()))?;
                info!("🔐 выпущен сертификат OPC UA-сервера на {} дн.: {}", self.cert_days, cert_path.display());
            }
            _ => bail!(
                "в {} только половина пары сертификат/ключ — удалите оставшийся файл, чтобы выпустить новую пару, или \
                 верните недостающий",
                self.pki_dir.display()
            ),
        }
        let cert = CertificateStore::read_cert(&cert_path).map_err(|e| anyhow!("сертификат сервера: {e}"))?;
        let not_after =
            cert.not_after().map_err(|_| anyhow!("в сертификате {} не читается срок", cert_path.display()))?;
        let absolute = |p: &Path| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
        Ok(OwnCert {
            cert_path: absolute(&cert_path),
            key_path: absolute(&key_path),
            days_left: (not_after.timestamp() - now_secs()).div_euclid(86_400),
        })
    }

    /// Менеджер входа для сервера: пароли сверяются по хешу, роли ограничивают запись.
    pub fn authenticator(&self) -> Arc<dyn AuthManager> {
        Arc::new(Gate {
            inner: DefaultAuthenticator::new(self.user_tokens().into_iter().collect()),
            users: self.users.clone(),
            allow_anonymous: self.allow_anonymous,
        })
    }

    /// Токены пользователей для конфигурации сервера. Пароль в них — заглушка: токены нужны серверу только для списка
    /// политик входа у точки (`DefaultAuthenticator::user_token_policies`), а проверяет пароль `Gate` по хешу.
    pub fn user_tokens(&self) -> Vec<(String, ServerUserToken)> {
        let placeholder = "-".to_string();
        self.users.iter().map(|u| (u.name.clone(), ServerUserToken::user_pass(&u.name, &placeholder))).collect()
    }

    /// Идентификаторы токенов для точек: пользователи (по защищённым каналам) и, если разрешено, анонимный.
    pub fn token_ids(&self, policy: SecurityPolicy) -> Vec<String> {
        let mut ids = Vec::new();
        if self.allow_anonymous {
            ids.push("ANONYMOUS".to_string());
        }
        // Пароль по открытому каналу не пускаем: он ушёл бы без шифрования.
        if policy != SecurityPolicy::None {
            ids.extend(self.users.iter().map(|u| u.name.clone()));
        }
        ids
    }
}

/// Сертификат сервера.
#[derive(Debug, Clone)]
pub struct OwnCert {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub days_left: i64,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// Предупреждение, если срок сертификата подходит к концу.
pub fn warn_if_expiring(cert: &OwnCert) {
    if cert.days_left < 0 {
        warn!(
            "⛔ сертификат OPC UA-сервера истёк {} дн. назад: клиенты его отвергнут — перевыпустите (см. docs/OPERATIONS.md)",
            -cert.days_left
        );
    } else if cert.days_left < 30 {
        warn!(
            "⚠ сертификат OPC UA-сервера истекает через {} дн. — перевыпустите (см. docs/OPERATIONS.md)",
            cert.days_left
        );
    } else {
        info!("сертификат OPC UA-сервера действует ещё {} дн.", cert.days_left);
    }
}

/// Вход и права: пароль по хешу, запись только роли `write`; события входа и отказа — в журнал.
struct Gate {
    inner: DefaultAuthenticator,
    users: Vec<User>,
    allow_anonymous: bool,
}

impl Gate {
    fn role(&self, token: &UserToken) -> Option<Role> {
        self.users.iter().find(|u| u.name == token.0).map(|u| u.role)
    }
}

#[async_trait]
impl AuthManager for Gate {
    async fn authenticate_anonymous_token(&self, endpoint: &ServerEndpoint) -> Result<(), Error> {
        if !self.allow_anonymous {
            warn!("🚫 анонимный вход отклонён (OPCUA_ALLOW_ANONYMOUS не включён)");
            return Err(Error::new(StatusCode::BadIdentityTokenRejected, "Anonymous login is not allowed"));
        }
        self.inner.authenticate_anonymous_token(endpoint).await
    }

    async fn authenticate_username_identity_token(
        &self,
        endpoint: &ServerEndpoint,
        username: &str,
        password: &Password,
    ) -> Result<UserToken, Error> {
        let user = self.users.iter().find(|u| u.name == username && endpoint.user_token_ids.contains(&u.name));
        // Хеш считаем и для неизвестного имени: время отказа не выдаёт, есть ли такой пользователь.
        let (hash, known) = match user {
            Some(u) => (u.hash.as_str(), true),
            None => (dummy_hash(), false),
        };
        let ok = Argon2::default().verify_password(password.get().as_bytes(), hash).is_ok();
        match (user, ok && known) {
            (Some(u), true) => {
                info!(
                    "🔓 вход {} (роль {})",
                    u.name,
                    if u.role == Role::Write { "write" } else { "read: запись запрещена" }
                );
                Ok(UserToken(u.name.clone()))
            }
            _ => {
                warn!("🚫 вход отклонён: пользователь {username:?}, неверное имя или пароль");
                Err(Error::new(StatusCode::BadUserAccessDenied, "Cannot authenticate"))
            }
        }
    }

    fn effective_user_access_level(&self, token: &UserToken, level: AccessLevel, _node: &NodeId) -> AccessLevel {
        // Анонимный вход (стенд) и роль `write` — как у узла; роль `read` — без записи: сервер ответит BadUserAccessDenied.
        match self.role(token) {
            Some(Role::Read) => level - AccessLevel::CURRENT_WRITE,
            _ => level,
        }
    }

    fn user_token_policies(&self, endpoint: &ServerEndpoint) -> Vec<opcua::types::UserTokenPolicy> {
        self.inner.user_token_policies(endpoint)
    }
}

/// Хеш argon2 для сверки с неизвестным пользователем (чтобы отказ занимал столько же времени, сколько и неверный пароль).
fn dummy_hash() -> &'static str {
    static HASH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    HASH.get_or_init(|| hash_password("нет такого пользователя").expect("хеш"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn passwords_are_hashed_and_users_parsed() {
        let hash = hash_password("s3cret").unwrap();
        assert!(hash.starts_with("$argon2"));
        let text = format!("# операторы\nanna {hash} write\n\nboris {hash} read  # смотрит\n");
        let users = parse_users(&text).unwrap();
        assert_eq!(
            users.iter().map(|u| (u.name.as_str(), u.role)).collect::<Vec<_>>(),
            [("anna", Role::Write), ("boris", Role::Read)]
        );
        assert!(Argon2::default().verify_password(b"s3cret", users[0].hash.as_str()).is_ok());
        assert!(Argon2::default().verify_password(b"other", users[0].hash.as_str()).is_err());
    }

    /// Хеш, выпущенный `ptusa-opcua hash-password` на argon2 0.5 (до обновления библиотеки): файлы пользователей на станциях
    /// остаются рабочими. Параметры по умолчанию — те же (Argon2id v19, m=19456, t=2, p=1), новые хеши того же вида.
    #[test]
    fn hashes_from_argon2_0_5_are_accepted() {
        const OLD: &str =
            "$argon2id$v=19$m=19456,t=2,p=1$0TF+g3g5854cYRgmYDD1Ag$NBJth5LX5L54WSgVxj8Dx2c5A3V+bdM7BuYNOMir50k";
        let users = parse_users(&format!("anna {OLD} write")).unwrap();
        assert!(Argon2::default().verify_password(b"argon2-0.5-pass", users[0].hash.as_str()).is_ok());
        assert!(Argon2::default().verify_password(b"other", users[0].hash.as_str()).is_err());
        assert!(hash_password("x").unwrap().starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
    }

    #[test]
    fn users_file_errors_name_the_line() {
        let hash = hash_password("x").unwrap();
        for (text, part) in [
            ("anna plaintext write", "хеш пароля не разобран"),
            (&format!("anna {hash}") as &str, "ожидается"),
            (&format!("anna {hash} admin"), "допустимы read и write"),
            (&format!("anna {hash} read\nanna {hash} write"), "уже есть"),
        ] {
            let err = format!("{:#}", parse_users(text).unwrap_err());
            assert!(err.contains(part), "{err}");
            assert!(!err.contains(&hash), "хеш не печатается: {err}");
        }
    }

    #[test]
    fn policies_are_parsed_like_the_gateways() {
        assert_eq!(
            parse_policy("Basic256Sha256_SignAndEncrypt").unwrap(),
            (SecurityPolicy::Basic256Sha256, MessageSecurityMode::SignAndEncrypt)
        );
        assert_eq!(parse_policy("basic256sha256-sign").unwrap().1, MessageSecurityMode::Sign);
        assert_eq!(parse_policy("Aes256_Sha256_RsaPss").unwrap().0, SecurityPolicy::Aes256Sha256RsaPss);
        assert_eq!(parse_policy("None").unwrap().0, SecurityPolicy::None);
        assert!(parse_policy("Basic128Rsa15").is_err());
    }

    #[test]
    fn nothing_configured_does_not_start_and_nothing_opens_silently() {
        let err = Security::from_env(env(&[]), "").unwrap_err().to_string();
        assert!(err.contains("OPCUA_ALLOW_ANONYMOUS") && err.contains("OPCUA_USERS_FILE"), "{err}");
        // Анонимный вход — только явно, и по умолчанию канал защищённый.
        let s = Security::from_env(env(&[("OPCUA_ALLOW_ANONYMOUS", "true")]), "host").unwrap();
        assert!(s.allow_anonymous && !s.trust_all_clients);
        assert_eq!(s.endpoints, vec![(SecurityPolicy::Basic256Sha256, MessageSecurityMode::SignAndEncrypt)]);
        assert_eq!(s.pki_dir, PathBuf::from("/var/lib/ptusa-opcua/pki"));
        assert!(s.cert_hosts.contains(&"host".to_string()));
        assert!(Security::from_env(env(&[("OPCUA_ALLOW_ANONYMOUS", "maybe")]), "").is_err());
        assert!(
            Security::from_env(env(&[("OPCUA_ALLOW_ANONYMOUS", "true"), ("OPCUA_SECURITY", "nonsense")]), "").is_err()
        );
    }

    #[test]
    fn users_never_log_in_over_a_channel_without_protection() {
        let dir = std::env::temp_dir().join(format!("ptusa-sec-users-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("users");
        std::fs::write(&file, format!("anna {} write\n", hash_password("p").unwrap())).unwrap();
        let path = file.to_str().unwrap();
        let err =
            Security::from_env(env(&[("OPCUA_USERS_FILE", path), ("OPCUA_SECURITY", "None, Basic256Sha256")]), "")
                .unwrap_err();
        assert!(err.to_string().contains("None"), "{err}");
        let s = Security::from_env(env(&[("OPCUA_USERS_FILE", path)]), "").unwrap();
        assert_eq!(s.token_ids(SecurityPolicy::Basic256Sha256), vec!["anna".to_string()]);
        assert!(s.token_ids(SecurityPolicy::None).is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn server_certificate_is_issued_once_and_kept() {
        let pki = std::env::temp_dir().join(format!("ptusa-sec-cert-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&pki);
        let s = Security { pki_dir: pki.clone(), cert_days: 400, ..Security::open(pki.clone()) };
        let first = s.ensure_cert("urn:test:facade").unwrap();
        assert!((398..=400).contains(&first.days_left), "{}", first.days_left);
        let bytes = std::fs::read(&first.cert_path).unwrap();
        let again = s.ensure_cert("urn:test:facade").unwrap();
        assert_eq!(std::fs::read(&again.cert_path).unwrap(), bytes, "второй запуск сертификат не перевыпускает");
        std::fs::remove_file(&first.key_path).unwrap();
        assert!(s.ensure_cert("urn:test:facade").unwrap_err().to_string().contains("половина пары"));
        let _ = std::fs::remove_dir_all(pki);
    }
}
