//! Сверка контроллера, к которому подключились, с ожидаемым (`identity` в `controllers.yaml`).
//!
//! Перепутанный адрес, заменённый ПЛК или переназначенный IP — и шлюз читал бы чужой контроллер и писал в него команды.
//! Поэтому при каждом подключении имя, названное самим контроллером, сверяется с ожидаемым; несовпадение — отказ от
//! соединения, и ни одна команда в такой контроллер не уходит.

use std::fmt;

/// Контроллер назвал не то имя, что ожидалось.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// Что сверяли: «имя PAC», «ApplicationUri сервера», «узел SYSTEM.PAC_NAME».
    pub what: String,
    /// Ожидаемое значение из конфигурации.
    pub expected: String,
    /// Значение, которое назвал контроллер; `None` — не назвал совсем.
    pub actual: Option<String>,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ожидали {}, контроллер назвал ", self.what, show(&self.expected))?;
        match &self.actual {
            Some(actual) => write!(f, "{}", show(actual)),
            None => write!(f, "ничего"),
        }
    }
}

impl std::error::Error for Mismatch {}

/// Имя в кавычках; если в нём есть не-ASCII (в именах станции кириллица, и «М», «С», «А» легко принять за латинские),
/// рядом — экранированная запись, в которой разница видна.
fn show(name: &str) -> String {
    if name.is_ascii() { format!("{name:?}") } else { format!("{name:?} ({})", escape(name)) }
}

/// Не-ASCII символы как `\u{41c}`.
pub fn escape(name: &str) -> String {
    name.chars().map(|c| if c.is_ascii() { c.to_string() } else { format!("\\u{{{:x}}}", c as u32) }).collect()
}

/// Сверка: имена равны с точностью до пробелов по краям (прошивка и PLC-проекты иногда их оставляют).
pub fn check(what: &str, expected: &str, actual: Option<&str>) -> Result<(), Mismatch> {
    match actual {
        Some(actual) if actual.trim() == expected.trim() => Ok(()),
        other => {
            Err(Mismatch { what: what.to_string(), expected: expected.to_string(), actual: other.map(str::to_string) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_names_match_ignoring_edge_spaces() {
        assert!(check("имя PAC", "BN1-МСА1", Some("BN1-МСА1")).is_ok());
        assert!(check("имя PAC", "BN1", Some(" BN1\n")).is_ok());
    }

    #[test]
    fn a_different_or_missing_name_is_a_mismatch() {
        assert!(check("имя PAC", "BN1", Some("BN2")).is_err());
        assert!(check("имя PAC", "BN1", None).is_err());
        assert!(check("имя PAC", "BN1", Some("")).is_err());
    }

    /// Латинские «MCA» и русские «МСА» неотличимы на глаз — в сообщении видна разница в кодах.
    #[test]
    fn look_alike_letters_are_told_apart_in_the_message() {
        let e = check("имя PAC", "BN1-МСА1", Some("BN1-MCA1")).unwrap_err();
        let text = e.to_string();
        assert!(text.contains(r"BN1-\u{41c}\u{421}\u{410}1"), "{text}");
        assert!(text.contains("\"BN1-MCA1\""), "{text}");
        assert!(!text.contains("BN1-MCA1\" ("), "ASCII-имя без экранирования: {text}");
        assert_eq!(check("x", "a", None).unwrap_err().to_string(), "x: ожидали \"a\", контроллер назвал ничего");
    }
}
