# Нагрузочный стенд

`loadsim` — N OPC UA-серверов («ПЛК») по M узлов; часть узлов меняется каждую секунду. Масштаб по
умолчанию — как в контракте «по исключению»: 20 контроллеров × 2740 тегов, треть меняется каждую
секунду (≈18 000 изменений/с). Отдельный crate (не входит в сборку шлюза).

```sh
cd loadtest/loadsim && cargo build --release
./target/release/loadsim config > /tmp/load.yaml     # controllers.yaml: теги LOAD-<i>.T<j>.V
./target/release/loadsim run &                       # серверы на портах 4900…4919

# шлюз против стенда (Kafka и PostgreSQL — любые, например docker compose up -d kafka postgres)
CONTROLLERS_YAML=/tmp/load.yaml GATEWAY_PERSIST_TELEMETRY=true ../../target/release/scada-gateway
curl -s localhost:8888/actuator/prometheus | grep -E 'scada_(telemetry|poll|process)'
```

Параметры: `--servers`, `--tags`, `--base-port`, `--first` (номер первого сервера: источник из сотен серверов разносят по нескольким процессам), `--fraction` (доля узлов, меняющихся за период),
`--period-ms`, `--poll-ms` (только для `config`). Смотреть: `scada_telemetry_sent_total` /
`scada_telemetry_suppressed_total` (скорость), `scada_poll_seconds` и `scada_poll_overruns_total`
(успевает ли шлюз за периодом), `scada_process_seconds`, `scada_telemetry_rows_dropped_total`.
Результаты — в `docs/TELEMETRY_BY_EXCEPTION.md`.

`ACQUISITION=subscription scripts/load_check.sh` — то же с подписками OPC UA вместо опроса (`docs/STRESS_TEST.md`).

Автоматическая проверка (`scripts/load_check.sh`, job `load` в CI): 10 серверов × 2740 тегов, замер 30 с,
падает при пропущенных циклах опроса, ошибках доставки в Kafka, перезапусках задач или памяти больше 500 МБ.
На машине разработчика: 9 140 сообщений/с, ≈ 10 % одного ядра, 75 МБ, обработка цикла 14 мс.

## Стресс-тест до миллиона значений в секунду

`scripts/stress_test.sh` поднимает источник из сотен серверов (несколькими процессами `loadsim`), Kafka в Docker и шлюз,
по ступеням — от одной станции (2 647 тегов) до 200 контроллеров × 5 000 тегов (1 000 000 тегов, опрос раз в секунду) — и
замеряет скорость обработки, нагрузку на процессор и память, пропущенные циклы и доставку в Kafka. Методика, результаты и
ограничения — `docs/STRESS_TEST.md`.

**По PAC («EasyDriver»).** `PROTOCOL=pac scripts/stress_test.sh` — те же тег и ступени, но «контроллеры» loadsim говорят на протоколе
driver-master (`loadsim run --protocol pac`, `loadsim config --protocol pac`). Предел протокола — 64 КБ сжатого снимка на контроллер:
`loadsim pac-size <полей>` печатает размер. Сравнение с OPC UA — `docs/PROTOCOL_COMPARISON.md`.

`scripts/compare_protocols.py --source firmware|sim` — сравнение двух режимов на одной станции (охват, точность значений, нагрузка шлюза и ПЛК,
несколько шлюзов, запись, обрыв связи).
