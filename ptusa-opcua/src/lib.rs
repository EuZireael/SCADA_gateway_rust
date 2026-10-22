//! OPC UA-фасад эмулятора мойки: все каналы станции — узлами OPC UA, данные — от настоящей прошивки
//! ptusa (эмулятор из папки Moika, `scripts/ptusa_emulator.sh` / `docker-compose.moika.yml`).
//!
//! Зачем: собственный OPC UA-сервер ptusa (open62541, `--opc r|rw`) публикует только
//! `<прибор>.state/.value` — 398 из 1834 каналов станции, без режимов, уставок, техобъектов и SYSTEM,
//! а запись `state` на прибор не действует. Шлюз ходит к контроллеру только по OPC UA, поэтому фасад
//! берёт у прошивки ВСЁ её родным протоколом (driver-master) и отдаёт по OPC UA:
//!
//! * адресное пространство — каналы из конфигурации станции (`config/stations/*.yaml`): узел
//!   `ns=2;s=<прибор>.<поле>` (`LINE1V0.ST`, `OBJECT1.RT_PAR_F[12]`), тип — `dataType` канала
//!   (INT32 → Int32, FLOAT → Float, STRING → String), запись — `writable`;
//! * чтение — снимок GET_DEVICES_STATES раз в `POLL_MS`, исполняется настоящим Lua в песочнице;
//!   SourceTimestamp — момент снимка;
//! * качество — нет значения в снимке: `BadNoData`; нет связи с прошивкой: `BadCommunicationError` у
//!   всех узлов (сам OPC UA-сервер при этом жив — как у настоящего ПЛК с отвалившейся шиной);
//! * запись клиента в узел — команда прошивке `__<прибор>:set_cmd('<поле>', <индекс>, <значение>)` ДО
//!   сохранения значения; клиент получает её исход: код 0 — `Good`, иначе `BadInvalidState`, нет связи
//!   — `BadCommunicationError`. Узел без `writable` — запись отклоняется сервером.

pub mod channel;
pub mod pac;
pub mod protocol;
pub mod sandbox;
pub mod security;
pub mod server;
pub mod snapshot;
