# Эксплуатация шлюза

Для того, кто разворачивает и сопровождает шлюз. Контракт с монитором — `docs/TELEMETRY_BY_EXCEPTION.md`
и `docs/MONITOR_INTEGRATION.md`; здесь — настройки, здоровье, метрики и что делает шлюз при сбоях.

## Состав и запуск

Один исполняемый файл (образ `ghcr.io/euzireael/scada_rust`, ~165 МБ, пользователь не root). Нужны
Kafka и, по желанию, PostgreSQL (журнал событий, история, список каналов для REST). Конфигурация
контроллеров и тегов — `controllers.yaml` (`CONTROLLERS_YAML`), всё остальное — переменные окружения.

```sh
docker run -d --name scada-gateway -p 8888:8888 \
  -v $PWD/controllers.yaml:/app/config/controllers.yaml:ro \
  -v scada-gateway-pki:/app/pki \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/scada_db \
  -e GATEWAY_API_TOKEN_FILE=/run/secrets/gateway_api_token \
  ghcr.io/euzireael/scada_rust:v0.4.1
```

Ключи переменных те же, что у Java-шлюза (Spring relaxed binding): и `GATEWAY_PUBLISH_FULL_RESEND_MS`, и
`GATEWAY_PUBLISH_FULLRESENDMS` работают. Ошибка в конфигурации (нет файла, неизвестная переменная в
`${…}` без значения по умолчанию, ошибка в скрипте) — шлюз не стартует и пишет, что не так; полуживого
состояния нет.

## Настройки

### Основные

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `CONTROLLERS_YAML` (`CONTROLLERS_CONFIG`) | `config/controllers.yaml` | контроллеры и теги; `${VAR:умолчание}` подставляются, `${VAR}` без значения — ошибка |
| `SERVER_PORT`, `GATEWAY_HTTP_BIND` | `8888`, `0.0.0.0` | HTTP |
| `GATEWAY_API_TOKEN` / `GATEWAY_API_TOKEN_FILE` | — | токен Bearer для `/api/*`; без него REST открыт (в журнале предупреждение) |
| `GATEWAY_SEND_BAD_FRAMES` | `true` | при обрыве связи слать кадр `{"value":null,"quality":"BAD",…}` — монитор по нему показывает «нет данных» |
| `GATEWAY_ALARMS_ENABLED` | `false` | алармы по `minValue`/`maxValue` тега |
| `GATEWAY_HEARTBEAT_INTERVAL_MS` | `30000` | событие HEARTBEAT («шлюз жив») |
| `RUST_LOG` | `info,…` | уровни журнала |

### Телеметрия «по исключению» — `docs/TELEMETRY_BY_EXCEPTION.md`

`GATEWAY_PUBLISH_ENABLED` (`true`), `GATEWAY_PUBLISH_DEADBAND` (0), `GATEWAY_PUBLISH_DEADBAND_PERCENT` (0),
`GATEWAY_PUBLISH_MIN_INTERVAL_MS` (0), `GATEWAY_PUBLISH_FULL_RESEND_MS` (30000). Монитору
`runtime.telemetry.max-silence-ms` (40 с) нужно держать **больше** `full-resend-ms`.

### Kafka

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | брокеры |
| `KAFKA_TOPICS_TELEMETRY` / `_COMMANDS` / `_COMMAND_RESULTS` / `_EVENTS` / `_ALARMS` | `scada.tags` / `scada-commands` / `scada-command-results` / `scada-events` / `scada-alarms` | топики |
| `KAFKA_TOPICS_REPLICATION` | `1` | фактор репликации топиков, которые создаёт шлюз (на кластере — по числу брокеров) |
| `KAFKA_PUBLISH_EVENTS`, `KAFKA_PUBLISH_ALARMS` | `true` | публиковать события и алармы |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | — | TLS и SASL (PLAIN, SCRAM-SHA-256/512) |
| `KAFKA_SSL_CA_LOCATION`, `KAFKA_SSL_CERTIFICATE_LOCATION`, `KAFKA_SSL_KEY_LOCATION`, `KAFKA_SSL_KEY_PASSWORD` | — | сертификаты (в том числе клиентский — mTLS) |
| `KAFKA_CLIENT_<СВОЙСТВО>` | — | любое свойство librdkafka: `KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=none` |
| `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL`, `…_SASL_MECHANISM`, `…_SASL_JAAS_CONFIG` | — | как у Java-шлюза (из JAAS берутся `username` и `password`) |

Пароли в журнал не попадают. Команды читаются группой `scada-gateway-group` с ручным назначением
партиций; брокеру для горячего резерва нужен `group.min.session.timeout.ms` ≤ `GATEWAY_HA_SESSION_TIMEOUT_MS`.

### База данных

`SPRING_DATASOURCE_URL` (`jdbc:postgresql://localhost:5433/scada_db`), `…_USERNAME`, `…_PASSWORD`; `DB_ENABLED=false` —
без БД. Схема создаётся и обновляется шлюзом сам (миграции, совместимы со схемой Java-шлюза;
`pg_advisory_xact_lock` — два экземпляра пары не гоняются). `GATEWAY_PERSIST_TELEMETRY` (`false`) —
писать историю в таблицу `telemetry`; `GATEWAY_HISTORY_*` — фильтр истории, `GATEWAY_TELEMETRY_RETENTION_HOURS` (72) — срок.

**Защита канала к БД.** В адресе понимаются `sslmode` (`disable`, `allow`, `prefer` — по умолчанию, `require`,
`verify-ca`, `verify-full`) и `sslrootcert=/путь/ca.pem`; в стиле драйвера JDBC `ssl=true` — это `verify-full`,
`ssl=false` — `disable`. То же без правки адреса: `DB_SSLMODE`, `DB_SSLROOTCERT` (сильнее адреса). Корневые
сертификаты берутся из системного хранилища (в образе — `ca-certificates`), для своего УЦ нужен `sslrootcert`.
`prefer` шифрует, если сервер умеет, но сервер не проверяет; **на объекте — `require`, а лучше `verify-full`**.
Неверное значение останавливает запуск: настройку защиты молча отбросить нельзя. Неверный пароль или
непроверяемый сертификат шлюз показывает сразу, а не после минуты повторов (повторяются только сетевые сбои).

### Безопасность OPC UA

В `controllers.yaml` у контроллера (только OPC UA):

```yaml
- name: "Площадка-1"
  endpoint: "opc.tcp://${PLC_HOST}:4840"
  security: Basic256Sha256        # None (по умолчанию) | Basic256Sha256 | Aes128_Sha256_RsaOaep | Aes256_Sha256_RsaPss | Basic256 | Basic128Rsa15
                                  # режим — суффикс _Sign или _SignAndEncrypt (по умолчанию SignAndEncrypt)
  username: "gateway"
  password: "${PLC_PASSWORD}"     # пароль — из окружения, в файл и в журнал он не попадает
```

* Ошибка в `security`, логин без пароля, `security`/`username` у Modbus и PAC — **шлюз не стартует** и говорит почему
  (молча работать без защиты он не будет). У Modbus и PAC защиты на уровне протокола нет.
* Логин и пароль на канале `None` идут открытым текстом — в журнале предупреждение; задавайте политику.
* **Сертификаты.** Шлюз создаёт свой самоподписанный сертификат в `GATEWAY_OPCUA_PKI_DIR` (`/app/pki` в образе — том;
  в нём `own/`, `private/`, `trusted/`, `rejected/`) и **не доверяет** сертификату сервера, пока его нет в `trusted/`.
  Порядок, как принято в OPC UA: первое подключение кладёт сертификат сервера в `rejected/` и соединение
  не устанавливается → перенесите (не копируйте) файл из `rejected/` в `trusted/` → шлюз подключится. Сертификат шлюза
  (`own/cert.der`) в свою очередь надо внести в доверенные на стороне ПЛК.
* **Том `/app/pki` — именованный, а не анонимный** (`-v scada-gateway-pki:/app/pki`, в compose — `volumes:`). Без него
  пересоздание контейнера (`docker rm` + `docker run` при обновлении образа) даёт **новый** сертификат шлюза, которого ПЛК
  не знает, и теряет `trusted/`: защищённое подключение перестаёт устанавливаться.
* **Срок сертификата шлюза.** Самоподписанный сертификат шлюз выпускает на `GATEWAY_OPCUA_CERT_DAYS` (5 лет); выпущенный
  до версии с этой настройкой — на 365 дней. Срок виден при запуске (`🔐 … действует до …`), в
  `scada-gateway opcua-cert` и в метрике `scada_opcua_cert_expiry_days`. За `GATEWAY_OPCUA_CERT_WARN_DAYS` (30) дней до
  окончания — предупреждение в журнал и событие `SYSTEM` (раз в сутки), после окончания — событие `ERROR`: ПЛК отвергнет
  просроченный сертификат, и все теги станут `BAD`. Оповещение: `scada_opcua_cert_expiry_days < 30`.
* **Продление.** В окно обслуживания: `docker exec scada-gateway scada-gateway opcua-cert renew` — новая пара в
  `own/`/`private/`, прежняя остаётся рядом с суффиксом `.bak`; команда печатает отпечаток. Внесите новый `own/cert.der`
  в доверенные на **каждом** ПЛК и перезапустите шлюз (без перезапуска он перейдёт на новый сертификат при следующем
  подключении). Старый сертификат на ПЛК удалите после того, как связь по новому установилась.
* **Свой сертификат** (например, от УЦ предприятия): `GATEWAY_OPCUA_CERT_FILE` и `GATEWAY_OPCUA_KEY_FILE`. Первым
  альтернативным именем (SAN) в нём должен быть URI приложения — `GATEWAY_OPCUA_APPLICATION_URI` (по умолчанию
  `urn:scada:gateway`); ключ должен быть от этого сертификата. Шлюз проверяет оба условия при запуске и иначе не
  стартует. Продлевает такой сертификат выдавший его УЦ; предупреждение о сроке работает так же.
* **Пара горячего резерва — два сертификата.** У каждого экземпляра свой каталог PKI и свой сертификат: внесите в
  доверенные на ПЛК **оба** (или дайте экземплярам один сертификат от УЦ через `GATEWAY_OPCUA_CERT_FILE`). Иначе после
  переключения на резерв защищённое подключение не установится.
* `GATEWAY_OPCUA_TRUST_SERVER_CERTS=true` — доверять любому сертификату сервера. Только для стенда: без проверки сервера
  защищённый канал не защищает от подмены сервера (в журнале предупреждение).

### Опрос и связь

`GATEWAY_OPCUA_OP_TIMEOUT_MS` (5000), `GATEWAY_MODBUS_OP_TIMEOUT_MS` (3000), `GATEWAY_PAC_OP_TIMEOUT_MS` (3000),
`GATEWAY_STALE_AFTER_MS` (30000 — нет удачных чтений дольше → пересоздать сессию OPC UA и признать связь потерянной).
Период опроса контроллера — наименьший `pollingRate` его тегов.

### Команды

`GATEWAY_COMMANDS_MAX_AGE_MS` (30000): команда старше (по метке записи Kafka, иначе по `timestamp` в теле)
не исполняется — `REJECTED_EXPIRED`. Команды исполняются «не более одного раза»: позиция коммитится до
исполнения, повтор того же `commandId` отбрасывается.

| Статус результата | Когда |
|---|---|
| `APPLIED` | записано и подтверждено контроллером |
| `REJECTED_UNKNOWN_TAG` | такого тега нет |
| `REJECTED_NOT_WRITABLE` | тег только для чтения (или контроллер не принял запись как неразрешённую) |
| `REJECTED_TYPE_MISMATCH` | значение не приводится к типу тега / сервер отверг тип |
| `REJECTED_EXPIRED` | команда ждала дольше `GATEWAY_COMMANDS_MAX_AGE_MS` (шлюз был недоступен) |
| `FAILED_NO_CONNECTION` | нет связи с контроллером |
| `FAILED_WRITE` | контроллер ответил отказом |
| `FAILED_NOT_APPLIED` | только при `GATEWAY_COMMANDS_VERIFY_MS` > 0: ПЛК принял запись, но значение не изменилось (программа не разрешает действие, например клапан не в ручном режиме `M=1`) |

`GATEWAY_COMMANDS_VERIFY_MS` (0 — выключено): шлюз читает значение до записи и через указанное время
после (OPC UA — читает узел, PAC — запрашивает свежий снимок); если значение осталось прежним и не равно записанному — `FAILED_NOT_APPLIED`, `success=false`. Если
значение изменилось на другое или чтение не удалось — результат `APPLIED`, как без проверки. Задержку берите
больше цикла программы ПЛК (для ptusa — 500–1000 мс). Команда по PAC не проверяется (код 0 = «принято»).

### Горячее резервирование

Два экземпляра с `GATEWAY_HA_ENABLED=true` и общим Kafka: активный публикует и принимает команды, резерв
опрашивает контроллеры и молчит. Выборы — группа потребителей на служебном топике `GATEWAY_HA_TOPIC`
(`scada-gateway-ha`, одна партиция). `GATEWAY_HA_SESSION_TIMEOUT_MS` (6000) — за столько замечается отказ
активного (на стенде 2000 → переключение ≈ 2 с; штатная остановка — доли секунды),
`GATEWAY_HA_HEARTBEAT_INTERVAL_MS` (1000), `GATEWAY_HA_INSTANCE_ID`, `GATEWAY_HA_GROUP_ID`.
`GATEWAY_HA_YIELD_AFTER_MS` (30000): активный, потерявший связь со **всеми** контроллерами дольше, передаёт
роль партнёру (если тот в группе) и не отбирает её обратно; 0 — выключено. Новый активный сразу
отправляет значения всех тегов. Состояние — `GET /api/ha`, метрика `scada_ha_active`, события `HA`.

### Скрипты Lua

`GATEWAY_SCRIPTS_DIR` (`scripts`), `GATEWAY_SCRIPTS_TIMEOUT_MS` (50 на вызов), `GATEWAY_SCRIPTS_RELOAD_INTERVAL_MS`
(5000). См. `config/scripts/scripts.yaml`. Песочница: без файлов, ОС, модулей, `load`, `pcall`, `coroutine`;
потолок памяти и времени. Ошибка скрипта на значении — кадр BAD по этому тегу; правки подхватываются без
перезапуска, версия с ошибкой не применяется (остаётся прежняя).

## Здоровье

`GET /actuator/health` (всегда открыт, нужен для healthcheck контейнера) отвечает `200` и
`{"status":"UP","components":{"db":…,"kafka":…,"controllers":{…}}}`. Статус верхнего уровня `UP`, пока процесс
обслуживает запросы: потеря БД, брокера или контроллеров не должна перезапускать контейнер (перезапуск
ничего не починит и сбросит состояние). Состояние смотрите в компонентах: `db` — `UP`/`DOWN`/`DISABLED`,
`kafka` — `DOWN`, пока последние доставки не удались, `controllers` — связь по каждому контроллеру.

## Метрики Prometheus

`GET /actuator/prometheus` (открыт). Имена, которые были у Java-шлюза, сохранены.

| Метрика | Смысл / на что смотреть |
|---|---|
| `scada_telemetry_sent_total`, `scada_telemetry_suppressed_total` | отправлено / подавлено повторов («по исключению») |
| `scada_kafka_send_errors_total` | недоставленные сообщения; растёт при обрыве брокера |
| `scada_kafka_resyncs_total` | доставка восстановилась после сбоя — все теги отправлены заново |
| `scada_kafka_queue_full_total` | очередь продюсера переполнена (брокер не успевает): тег отправится следующим циклом опроса, повторной отправки всех тегов нет. Растёт постоянно — брокеру не хватает мощности или раздела |
| `scada_controllers_connected` / `_total` | контроллеров на связи / всего |
| `scada_opcua_cert_expiry_days{file}` | дней до окончания сертификата OPC UA-клиента (у истёкшего — отрицательное); только при контроллерах OPC UA |
| `scada_poll_seconds`, `scada_poll_overruns_total` | цикл опроса; **overruns > 0 — шлюз не успевает за периодом** |
| `scada_process_seconds` | обработка цикла (скрипты, фильтры, алармы) |
| `scada_commands_total{status}` | команды по исходу |
| `scada_task_restarts_total{task}` | **задача шлюза падала и перезапущена** — смотрите журнал и события SYSTEM/ERROR |
| `scada_db_write_errors_total{kind}`, `scada_db_rows_lost_total{kind}` | БД недоступна: ошибки записи и потерянные строки (events, history) |
| `scada_events_dropped_total`, `scada_telemetry_rows_dropped_total` | очередь записи переполнена, строки отброшены |
| `scada_ha_active{instance}` | 1 — активный, 0 — резерв |
| `scada_script_errors_total{script}`, `scada_scripts_bound_tags` | скрипты |

Рекомендуемые оповещения: `increase(scada_poll_overruns_total[5m]) > 0`; `scada_controllers_connected <
scada_controllers_total` дольше N минут; `increase(scada_task_restarts_total[10m]) > 0`;
`increase(scada_kafka_send_errors_total[5m]) > 0`; `increase(scada_db_rows_lost_total[5m]) > 0`;
`sum(scada_ha_active) != 1` для пары (оба активны или оба в резерве).

Первый запуск на реальном оборудовании — по ступеням, `docs/PILOT_CHECKLIST.md`.

## Чек-лист безопасности перед пуском на объекте

1. **REST.** Задайте `GATEWAY_API_TOKEN` (или `…_FILE`) — случайный, от 16 символов (`openssl rand -hex 24`); без него
   `/api/*` открыт всем, кто достанет порт, в том числе квитирование алармов. Токен короче 16 символов шлюз отмечает
   предупреждением.
2. **Порт 8888.** `/actuator/health` (имена контроллеров, состояние БД и Kafka) и `/actuator/prometheus` токеном не
   закрываются — им нужен доступ от проверки контейнера и Prometheus. Не публикуйте порт за пределы служебной сети
   (файрвол) или привяжите HTTP к служебному интерфейсу: `GATEWAY_HTTP_BIND`.
3. **База данных.** Смените пароль по умолчанию (`scada_password`), включите `sslmode=require` или строже, порт БД не
   публикуйте наружу. В compose-стендах репозитория порты БД и Kafka привязаны к `127.0.0.1`; для доступа с других
   хостов замените привязку осознанно.
4. **Kafka.** `KAFKA_SECURITY_PROTOCOL=SASL_SSL` (или `SSL` с mTLS) и ACL на топики: запись в `scada-commands` равна праву
   писать в ПЛК. Без аутентификации любой, кто достанет брокер, может послать шлюзу команду.
5. **OPC UA.** Политика `Basic256Sha256` или строже и пользователь для контроллеров, которые это умеют; сертификаты серверов — в
   `trusted/`, а не `GATEWAY_OPCUA_TRUST_SERVER_CERTS`. Modbus и PAC защиты на уровне протокола не имеют — отдельный
   сегмент сети.
6. **Запись в ПЛК.** `writable: true` только у тех тегов, которые монитору действительно можно менять; Modbus шлюз не пишет
   вовсе. Включите проверку эффекта (OPC UA и PAC): `GATEWAY_COMMANDS_VERIFY_MS`.
7. **Секреты.** Пароли ПЛК и БД — из окружения или секретов оркестратора (`${PLC_PASSWORD}` в `controllers.yaml`,
   `GATEWAY_API_TOKEN_FILE`); в репозиторий и журнал они не попадают.

## REST (`/api/*`, под токеном, если он задан)

| Путь | Что |
|---|---|
| `GET /api/health`, `GET /api/status` | живость, контроллеры, число тегов |
| `GET /api/ha` | роль экземпляра в паре, группа, с какого момента |
| `GET /api/scripts` | привязки скриптов, ошибки |
| `GET /api/events`, `/api/events/type/{type}`, `/api/events/severity/{severity}`, `/api/events/stats` | журнал событий (всегда с `LIMIT`) |
| `GET /api/events/alarms`, `/api/events/alarms/unacknowledged` | алармы |
| `POST /api/events/{id}/acknowledge?userId=…` | квитировать аларм: `404`, если такого аларма нет, `400` — пустой или длиннее 255 символов `userId` |

## Что шлюз делает при сбоях

| Сбой | Поведение | Как увидеть |
|---|---|---|
| Контроллер недоступен | кадр BAD по его тегам, событие `CONNECTION`, переподключение; остальные контроллеры не страдают | `scada_controllers_connected`, health |
| Все контроллеры недоступны у активного в паре | через `GATEWAY_HA_YIELD_AFTER_MS` роль уходит партнёру | события `HA`, `/api/ha` |
| Брокер Kafka недоступен | опрос и обработка идут, сообщения копятся в очереди продюсера и теряются через 30 с; когда доставка вернулась, **все теги отправляются заново** (не ждут полной отправки) | health `kafka: DOWN`, `scada_kafka_send_errors_total`, `scada_kafka_resyncs_total` |
| БД недоступна | телеметрия и команды не страдают; события и история теряются со счётчиком; шлюз подключается к БД сам, когда она вернулась | health `db: DOWN`, `scada_db_rows_lost_total` |
| Задача шлюза упала (паника) | надзиратель перезапускает её (пауза 1 → 30 с), событие SYSTEM/ERROR | `scada_task_restarts_total` |
| Процесс убит (`kill -9`) | в паре — резерв активен за `session-timeout`, шлюз без резерва — перезапуск оркестратором | — |
| Скрипт завис или упал | прерывается лимитом времени, кадр BAD по этому тегу; после 3 превышений времени подряд скрипт отключается на 30 с (его каналы идут как BAD, остальные не страдают), затем одна пробная попытка | `scada_script_errors_total`, `GET /api/scripts` |
| Мусор вместо ответа PAC (бесконечный Lua, zlib-бомба) | прерывается лимитами памяти/времени/распаковки, связь с контроллером считается потерянной | журнал |

## Обновление с Java-шлюза

Контракты те же: `controllers.yaml`, топики и формат Kafka, статусы команд, переменные окружения, таблицы
БД (`tags`, `event_log`, `telemetry`; миграции совместимы с Flyway-схемой), `/actuator/*`, имена метрик.
Отличия — в README («Отличия от Java-шлюза»): `pollingRate` учитывается, кадры BAD включены, история
выключена по умолчанию, команды старше 30 с отбрасываются, новый статус `REJECTED_EXPIRED`. Порядок:
остановить Java-шлюз, запустить Rust-шлюз с теми же переменными и `controllers.yaml`; монитор менять не нужно.
