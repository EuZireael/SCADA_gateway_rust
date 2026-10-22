//! Modbus TCP: батч-чтение holding-регистров (FC03). Контроллер только на чтение.
//!
//! Теги группируются в блоки (≤120 регистров на запрос, промежутки между тегами читаются
//! заодно) и читаются одной FC03 на блок — для WAGO это ~десятки запросов за цикл вместо
//! запроса на каждый тег. Если в промежутке есть регистр, которого у ПЛК нет, ПЛК отвечает
//! на блок исключением IllegalDataAddress; тогда блок делится пополам, пока не останутся
//! части без дыр, — план подстраивается под карту регистров сам, без настройки.
//! Декодирование как у Java-шлюза и симулятора: FLOAT — 2 регистра little-endian по
//! словам (`struct.pack('<f')`), целые — регистр со знаком (int16), BOOL — регистр ≠ 0.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::*;
use tracing::{debug, warn};

use crate::model::{self, Tag, TagValue};

/// Holding-регистры адресуются с 40001; в протоколе — смещение с 0.
const ADDRESS_BASE: i32 = 40001;
/// Лимит FC03 — 125 регистров; берём с запасом.
const MAX_BLOCK_REGISTERS: i32 = 120;

/// `host:port` → (host, port) с портом по умолчанию.
pub fn split_host_port(s: &str, default_port: u16) -> (String, u16) {
    let s = s.trim().trim_end_matches('/');
    match s.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(default_port)),
        None => (s.to_string(), default_port),
    }
}

/// Хост и порт из `modbus://host:port` (порт по умолчанию 502).
pub fn endpoint(endpoint: &str) -> (String, u16) {
    split_host_port(endpoint.trim_start_matches("modbus://"), 502)
}

/// Блок регистров одной FC03 и теги, которые из него читаются.
#[derive(Debug, Clone)]
pub struct Block {
    /// 0-based адрес первого регистра.
    pub start: u16,
    /// Сколько регистров читать.
    pub count: u16,
    /// Теги блока (отсортированы по адресу).
    pub tags: Vec<Arc<Tag>>,
}

impl Block {
    /// Блок ровно под теги (отсортированы по адресу).
    fn covering(tags: Vec<Arc<Tag>>) -> Self {
        let start = tags.iter().map(|t| reg0(t)).min().unwrap_or(0);
        let end = tags.iter().map(|t| reg0(t) + width(t)).max().unwrap_or(start);
        Block { start: start as u16, count: (end - start) as u16, tags }
    }

    /// Две половины по тегам (для блока из ≥2 тегов).
    fn halves(&self) -> [Block; 2] {
        let (a, b) = self.tags.split_at(self.tags.len() / 2);
        [Block::covering(a.to_vec()), Block::covering(b.to_vec())]
    }

    /// Адреса в нотации 4xxxx — для логов.
    fn span(&self) -> String {
        let first = self.start as i32 + ADDRESS_BASE;
        format!("{first}–{}", first + self.count as i32 - 1)
    }
}

/// Адрес первого регистра тега в протоколе (с нуля).
fn reg0(tag: &Tag) -> i32 {
    tag.modbus_address.unwrap_or(ADDRESS_BASE) - ADDRESS_BASE
}

/// Сколько регистров занимает значение: вещественное — два, остальное — один.
fn width(tag: &Tag) -> i32 {
    if model::is_float(&tag.data_type) { 2 } else { 1 }
}

/// План чтения: теги с адресом по возрастанию, набранные в блоки ≤120 регистров.
pub fn plan_blocks(tags: &[Arc<Tag>]) -> Vec<Block> {
    let mut sorted: Vec<Arc<Tag>> = tags.iter().filter(|t| reg0(t) >= 0).cloned().collect();
    sorted.sort_by_key(|t| reg0(t));
    let mut blocks: Vec<Block> = Vec::new();
    for tag in sorted {
        let (start, end) = (reg0(&tag), reg0(&tag) + width(&tag));
        match blocks.last_mut() {
            Some(b) if end - b.start as i32 <= MAX_BLOCK_REGISTERS => {
                b.count = b.count.max((end - b.start as i32) as u16);
                b.tags.push(tag);
            }
            _ => blocks.push(Block { start: start as u16, count: (end - start) as u16, tags: vec![tag] }),
        }
    }
    blocks
}

/// Значение тега из сырых регистров блока (`off` — смещение тега от начала блока).
pub fn decode(tag: &Tag, regs: &[u16], off: usize) -> Option<TagValue> {
    if model::is_float(&tag.data_type) {
        let (lo, hi) = (*regs.get(off)?, *regs.get(off + 1)?);
        return Some(TagValue::F32(f32::from_bits(((hi as u32) << 16) | lo as u32)));
    }
    let reg = *regs.get(off)?;
    if model::is_bool(&tag.data_type) {
        return Some(TagValue::Bool(reg != 0));
    }
    // Регистр беззнаковый, а целый тег знаковый: коды состояния ptusa бывают отрицательными.
    Some(TagValue::Int(reg as i16 as i64))
}

/// Клиент одного Modbus-контроллера: ленивое соединение, сброс при ошибке.
pub struct ModbusClient {
    host: String,
    port: u16,
    unit_id: u8,
    op_timeout: Duration,
    ctx: Option<Context>,
}

impl ModbusClient {
    /// Клиент без соединения; оно создаётся при первом чтении.
    pub fn new(host: String, port: u16, unit_id: u8, op_timeout: Duration) -> Self {
        ModbusClient { host, port, unit_id, op_timeout, ctx: None }
    }

    /// Соединение с контроллером: создаётся лениво при первом чтении, с таймаутом подключения.
    async fn context(&mut self) -> Result<&mut Context> {
        if self.ctx.is_none() {
            let stream = timeout(self.op_timeout, TcpStream::connect((self.host.as_str(), self.port)))
                .await
                .context("таймаут подключения")??;
            stream.set_nodelay(true).ok();
            self.ctx = Some(tokio_modbus::client::tcp::attach_slave(stream, Slave(self.unit_id)));
        }
        Ok(self.ctx.as_mut().expect("соединение только что создано"))
    }

    /// Прочитать все блоки плана.
    ///
    /// * Сбой связи или таймаут — ошибка всего цикла, соединение сбрасывается.
    /// * Исключение ПЛК о несуществующем адресе на блоке из нескольких тегов — блок в плане
    ///   заменяется двумя половинами, и они читаются тут же (в этом же цикле).
    /// * Исключение на одиночном теге (или другое исключение) — `None` только у тегов блока.
    /// * Исключения на всех блоках — ошибка цикла: ПЛК отвечает, но данных нет (например,
    ///   шлюз Modbus TCP→RTU без связи с прибором).
    pub async fn read(&mut self, plan: &mut Vec<Block>) -> Result<Vec<(Arc<Tag>, Option<TagValue>)>> {
        let result = self.read_inner(plan).await;
        if result.is_err() {
            self.ctx = None;
        }
        result
    }

    /// Чтение плана блок за блоком; блок с исключением «нет такого адреса» делится пополам прямо в плане.
    async fn read_inner(&mut self, plan: &mut Vec<Block>) -> Result<Vec<(Arc<Tag>, Option<TagValue>)>> {
        let op_timeout = self.op_timeout;
        let endpoint = format!("{}:{}", self.host, self.port);
        let ctx = self.context().await?;
        let mut out = Vec::with_capacity(plan.iter().map(|b| b.tags.len()).sum());
        let mut last_exception = None;
        let mut read_blocks = 0;
        let mut i = 0;
        while i < plan.len() {
            let block = &plan[i];
            let answer = timeout(op_timeout, ctx.read_holding_registers(block.start, block.count))
                .await
                .context("таймаут чтения")??;
            match answer {
                Ok(regs) => {
                    read_blocks += 1;
                    for tag in &block.tags {
                        let off = (reg0(tag) - block.start as i32) as usize;
                        out.push((tag.clone(), decode(tag, &regs, off)));
                    }
                }
                Err(code @ (ExceptionCode::IllegalDataAddress | ExceptionCode::IllegalDataValue))
                    if block.tags.len() > 1 =>
                {
                    let halves = block.halves();
                    warn!(
                        "Modbus {endpoint}: блок {} ответил {code:?} — делю на {} и {}",
                        block.span(),
                        halves[0].span(),
                        halves[1].span()
                    );
                    plan.splice(i..=i, halves);
                    continue; // первая половина — на месте i
                }
                Err(code) => {
                    debug!("Modbus {endpoint}: блок {} ответил {code:?}", block.span());
                    out.extend(block.tags.iter().map(|t| (t.clone(), None)));
                    last_exception = Some((block.span(), code));
                }
            }
            i += 1;
        }
        if let (0, Some((span, code))) = (read_blocks, last_exception) {
            return Err(anyhow!("исключение Modbus {code:?} на всех блоках (последний {span})"));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::config::TagConfig;

    fn tag(addr: i32, dt: &str) -> Arc<Tag> {
        Arc::new(Tag::from_config(&TagConfig {
            name: format!("t{addr}"),
            node_id: format!("modbus:{addr}"),
            channel_id: None,
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: Some("modbus".into()),
            data_type: dt.into(),
            polling_rate: 2000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: None,
            max_value: None,
            write_min: None,
            write_max: None,
            modbus_address: Some(addr),
            modbus_type: None,
            modbus_unit_id: None,
            history: None,
        }))
    }

    #[test]
    fn endpoint_parsing() {
        assert_eq!(endpoint("modbus://simulator:5020"), ("simulator".into(), 5020));
        assert_eq!(endpoint("modbus://10.0.0.5"), ("10.0.0.5".into(), 502));
    }

    #[test]
    fn contiguous_tags_share_a_block() {
        let tags = vec![tag(40003, "FLOAT"), tag(40001, "INT32"), tag(40002, "INT32")];
        let blocks = plan_blocks(&tags);
        assert_eq!(blocks.len(), 1);
        assert_eq!((blocks[0].start, blocks[0].count), (0, 4));
    }

    #[test]
    fn block_is_split_at_120_registers() {
        let tags: Vec<_> = (0..70).map(|i| tag(40001 + i * 2, "FLOAT")).collect();
        let blocks = plan_blocks(&tags);
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|b| b.count <= 120));
        assert_eq!(blocks.iter().map(|b| b.tags.len()).sum::<usize>(), 70);
    }

    #[test]
    fn decode_like_simulator() {
        let bits = 64.7f32.to_bits();
        let regs = [(bits & 0xFFFF) as u16, (bits >> 16) as u16, 0xFFFB, 1];
        assert_eq!(decode(&tag(40001, "FLOAT"), &regs, 0), Some(TagValue::F32(64.7)));
        assert_eq!(decode(&tag(40003, "INT32"), &regs, 2), Some(TagValue::Int(-5)));
        assert_eq!(decode(&tag(40004, "BOOLEAN"), &regs, 3), Some(TagValue::Bool(true)));
        assert_eq!(decode(&tag(40004, "FLOAT"), &regs, 3), None, "второго регистра нет");
    }

    /// Фейковый Modbus TCP-сервер: FC03 по карте регистров. Запрос, задевающий регистр вне
    /// карты, — исключение IllegalDataAddress, как у реального ПЛК; `exception` — отвечать
    /// этим кодом на всё. `requests` — (start, count) принятых запросов.
    mod fake {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        pub type Requests = Arc<Mutex<Vec<(u16, u16)>>>;

        pub async fn spawn(regs: HashMap<u16, u16>, exception: Option<u8>) -> (u16, Requests) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let requests: Requests = Arc::default();
            let seen = requests.clone();
            tokio::spawn(async move {
                let Ok((mut s, _)) = listener.accept().await else { return };
                loop {
                    let mut mbap = [0u8; 7];
                    if s.read_exact(&mut mbap).await.is_err() {
                        return;
                    }
                    let mut pdu = vec![0u8; u16::from_be_bytes([mbap[4], mbap[5]]) as usize - 1];
                    if s.read_exact(&mut pdu).await.is_err() {
                        return;
                    }
                    let start = u16::from_be_bytes([pdu[1], pdu[2]]);
                    let count = u16::from_be_bytes([pdu[3], pdu[4]]);
                    seen.lock().unwrap().push((start, count));
                    let values: Option<Vec<u16>> = (start..start + count).map(|r| regs.get(&r).copied()).collect();
                    let answer = match (exception, values) {
                        (None, Some(values)) => {
                            let mut a = vec![0x03, (count * 2) as u8];
                            a.extend(values.iter().flat_map(|v| v.to_be_bytes()));
                            a
                        }
                        (code, _) => vec![0x83, code.unwrap_or(0x02)],
                    };
                    let mut frame = mbap[..4].to_vec();
                    frame.extend_from_slice(&(answer.len() as u16 + 1).to_be_bytes());
                    frame.push(mbap[6]);
                    frame.extend_from_slice(&answer);
                    if s.write_all(&frame).await.is_err() {
                        return;
                    }
                }
            });
            (port, requests)
        }
    }

    fn client(port: u16) -> ModbusClient {
        ModbusClient::new("127.0.0.1".into(), port, 1, Duration::from_secs(2))
    }

    fn values(out: &[(Arc<Tag>, Option<TagValue>)]) -> Vec<(String, Option<TagValue>)> {
        let mut v: Vec<_> = out.iter().map(|(t, v)| (t.name.clone(), v.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    #[tokio::test]
    async fn block_over_missing_registers_is_split_until_readable() {
        // Теги 40001, 40002, 40005 (FLOAT, 2 регистра), 40010, 40011; у ПЛК нет 40003–40004
        // и 40007–40009 — план из одного блока 40001–40011 на дырах.
        let f = 1.5f32.to_bits();
        let regs = HashMap::from([(0, 7), (1, 0xFFFB), (4, f as u16), (5, (f >> 16) as u16), (9, 1), (10, 2)]);
        let (port, requests) = fake::spawn(regs, None).await;
        let tags = vec![
            tag(40001, "INT32"),
            tag(40002, "INT32"),
            tag(40005, "FLOAT"),
            tag(40010, "INT32"),
            tag(40011, "INT32"),
        ];
        let mut plan = plan_blocks(&tags);
        assert_eq!(plan.len(), 1);
        let mut c = client(port);

        let out = c.read(&mut plan).await.unwrap();
        let expected = vec![
            ("t40001".to_string(), Some(TagValue::Int(7))),
            ("t40002".to_string(), Some(TagValue::Int(-5))),
            ("t40005".to_string(), Some(TagValue::F32(1.5))),
            ("t40010".to_string(), Some(TagValue::Int(1))),
            ("t40011".to_string(), Some(TagValue::Int(2))),
        ];
        assert_eq!(values(&out), expected, "все теги прочитаны уже в первом цикле");
        let spans: Vec<(u16, u16)> = plan.iter().map(|b| (b.start, b.count)).collect();
        assert_eq!(spans, vec![(0, 2), (4, 2), (9, 2)], "блоки без дыр");

        // Следующий цикл — по новому плану, без исключений.
        requests.lock().unwrap().clear();
        assert_eq!(values(&c.read(&mut plan).await.unwrap()), expected);
        assert_eq!(*requests.lock().unwrap(), spans);
    }

    #[tokio::test]
    async fn missing_register_of_a_tag_is_bad_only_for_that_tag() {
        let (port, _) = fake::spawn(HashMap::from([(0, 1), (2, 3)]), None).await;
        let mut plan = plan_blocks(&[tag(40001, "INT32"), tag(40002, "INT32"), tag(40003, "INT32")]);
        let out = client(port).read(&mut plan).await.unwrap();
        assert_eq!(
            values(&out),
            vec![
                ("t40001".to_string(), Some(TagValue::Int(1))),
                ("t40002".to_string(), None),
                ("t40003".to_string(), Some(TagValue::Int(3))),
            ]
        );
    }

    #[tokio::test]
    async fn exception_on_every_block_fails_the_cycle_without_splitting() {
        // GatewayTargetDevice: шлюз TCP→RTU не достучался до прибора — делить блоки незачем.
        let (port, requests) = fake::spawn(HashMap::from([(0, 1), (1, 2)]), Some(0x0B)).await;
        let mut plan = plan_blocks(&[tag(40001, "INT32"), tag(40002, "INT32")]);
        let err = client(port).read(&mut plan).await.unwrap_err();
        assert!(err.to_string().contains("GatewayTargetDevice"), "{err}");
        assert_eq!(plan.len(), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }
}
