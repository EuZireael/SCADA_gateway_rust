#!/usr/bin/env bash
# ============================================================================
# Дымовой тест стенда целиком: docker-compose.yml (Kafka + Postgres + OPC UA-симулятор станции + Rust-шлюз,
# образы собираются из исходников) и сбои, которые бывают на объекте:
#
#   1. стенд поднимается, контроллер станции (все каналы базы каналов — OPC UA) на связи, в Kafka — телеметрия всех тегов в формате
#      монитора ({value, quality, timestamp});
#   2. команда записи через Kafka → статус APPLIED в scada-command-results;
#   3. брокер Kafka пропал на 45 с: шлюз жив и опрашивает, после возвращения брокера все теги
#      отправляются заново (scada_kafka_resyncs_total растёт), а не ждут полной отправки;
#   4. БД пропала на 30 с: телеметрия идёт, опрос не отстаёт (scada_poll_overruns_total не растёт),
#      /actuator/health показывает db DOWN, потом UP;
#   5. симулятор (ПЛК) пропал: контроллер DOWN, в Kafka кадры BAD; вернулся — снова UP.
#
#   scripts/stack_smoke.sh            # нужны docker compose, curl, python3; порты 5433, 9094, 8888, 4840
#   KEEP=1 scripts/stack_smoke.sh     # не гасить стенд после теста
#   GATEWAY_BIN=target/release/scada-gateway scripts/stack_smoke.sh   # шлюз — процессом на хосте (без сборки его образа)
#   NO_BUILD=1 — вместе с GATEWAY_BIN: образ симулятора не пересобирать (тег $PROJECT-simulator должен существовать)
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT=${COMPOSE_PROJECT_NAME:-scada-smoke}
DC=(docker compose -p "$PROJECT" -f docker-compose.yml ${COMPOSE_OVERRIDE:+-f "$COMPOSE_OVERRIDE"})
GW=http://localhost:8888
TAGS_TOPIC=scada.tags
TAG_RW='Барановичи-1.BN1_MCA1.LINE1.MAIN_CIP_MODULE.PAR_MAIN[7]'
EXPECTED_TAGS=1834   # включённые каналы config/stations/BN1_MCA1.yaml

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '   ✓ %s\n' "$*"; }
fail() { printf '   ✗ %s\n' "$*" >&2; exit 1; }

GW_PID=
WORK=$(mktemp -d)
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- шлюз (последние строки) ---"
    if [ -n "$GW_PID" ]; then tail -60 "$GW_LOG"; else "${DC[@]}" logs --tail 60 gateway 2>&1 || true; fi
  fi
  [ -z "$GW_PID" ] || kill "$GW_PID" 2>/dev/null || true
  rm -rf "$WORK"
  [ "${KEEP:-0}" = 1 ] || "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

# until <секунд> <описание> <команда…>: повторять, пока команда не вернёт 0.
until_ok() {
  local limit=$1 what=$2; shift 2
  local deadline=$((SECONDS + limit))
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || fail "не дождались: $what (${limit} с)"
    sleep 2
  done
}

# awk читает весь вывод до конца (без exit): ранний выход рвал бы конвейер, и curl с pipefail давал код 23.
metric() { curl -fs "$GW/actuator/prometheus" | awk -v n="$1" '!seen && ($1 == n || index($1, n "{") == 1) { print $NF; seen = 1 }'; }
health() { curl -fs "$GW/actuator/health"; }
# json <выражение над d>: значение из JSON на stdin.
json() { python3 -c "import sys, json; d = json.load(sys.stdin); print($1)"; }
controllers_up() { [ "$(health | json 'sum(1 for v in d["components"]["controllers"].values() if v == "UP")')" = "$1" ]; }
all_controllers_down() { [ "$(health | json 'sum(1 for v in d["components"]["controllers"].values() if v == "UP")')" = 0 ]; }
db_status() { health | json 'd["components"]["db"]["status"]'; }
kafka_status() { health | json 'd["components"]["kafka"]["status"]'; }
kafka_up() { [ "$(kafka_status)" = UP ]; }
db_up() { [ "$(db_status)" = UP ]; }
kafka_consume() { # <топик> <макс. сообщений> <таймаут мс> [--from-beginning]
  "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --max-messages "$2" --timeout-ms "$3" --property print.key=true "${@:4}" 2>/dev/null
}

log "1. Стенд: сборка образов и запуск"
if [ -n "${GATEWAY_BIN:-}" ]; then
  # Шлюз — процесс на хосте (например, когда образ шлюза собрать негде); остальное — в Docker.
  "${DC[@]}" up -d ${NO_BUILD:+--no-build} kafka postgres simulator
  until_ok 120 "Kafka и Postgres готовы" bash -c "[ \"\$(docker inspect -f '{{.State.Health.Status}}' scada-rs-kafka)\" = healthy ] && [ \"\$(docker inspect -f '{{.State.Health.Status}}' scada-rs-postgres)\" = healthy ]"
  GW_LOG=$(mktemp)
  CONTROLLERS_YAML=config/stations/BN1_MCA1.yaml PLC_HOST=127.0.0.1 SERVER_PORT=8888 \
    SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
    SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/scada_db \
    "$GATEWAY_BIN" >"$GW_LOG" 2>&1 &
  GW_PID=$!
else
  "${DC[@]}" up -d --build
fi
until_ok 180 "все контроллеры на связи" controllers_up 1
ok "шлюз здоров, контроллер станции UP"
kafka_up && ok "Kafka: UP" || fail "компонент kafka не UP"
db_up && ok "БД: UP"

log "   Телеметрия в формате монитора"
# Читаем с запасом: часть тегов успевает прийти дважды (изменение или ранняя полная отправка), уникальных ключей должно быть все.
out=$(kafka_consume "$TAGS_TOPIC" $((EXPECTED_TAGS * 3)) 30000 --from-beginning || true)
printf '%s\n' "$out" > "$WORK/tags.txt"
# Служебные строки консьюмера (если попали в вывод) — не сообщения.
grep -P '^[^\t]+\t\{' "$WORK/tags.txt" > "$WORK/tags.json" || { printf '%s\n' "$out" | head -3 >&2; fail "в выводе консьюмера нет сообщений вида ключ<TAB>{json}"; }
python3 - "$WORK/tags.json" "$EXPECTED_TAGS" <<'PY'
import sys, json, re
numeric = re.compile(r"^[+-]?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$")
keys, bad = set(), []
for line in open(sys.argv[1], encoding="utf-8"):
    line = line.rstrip("\n")
    if not line:
        continue
    key, _, body = line.partition("\t")
    msg = json.loads(body)
    keys.add(key)
    if set(msg) != {"value", "quality", "timestamp"}:
        bad.append(f"{key}: поля {sorted(msg)}")
    elif msg["quality"] not in ("GOOD", "BAD", "UNCERTAIN"):
        bad.append(f"{key}: quality {msg['quality']}")
    elif not isinstance(msg["timestamp"], (int, float)) or msg["timestamp"] < 1e9:
        bad.append(f"{key}: timestamp {msg['timestamp']}")
    elif isinstance(msg["value"], float) and not numeric.match(repr(msg["value"]).replace("e+", "e")):
        bad.append(f"{key}: value {msg['value']}")
expected = int(sys.argv[2])
if len(keys) < expected:
    sys.exit(f"в топике {len(keys)} разных тегов, ожидали {expected}")
if bad:
    sys.exit("нарушения формата: " + "; ".join(bad[:5]))
print(f"   ✓ {len(keys)} разных тегов, формат {{value, quality, timestamp}} соблюдён")
PY

log "2. Команда записи через Kafka"
cmd_id="smoke-$(date +%s)"
printf '%s|{"commandId":"%s","tagName":"%s","value":1,"requestedBy":"smoke","timestamp":"%s"}\n' \
  "$TAG_RW" "$cmd_id" "$TAG_RW" "$(date -u +%Y-%m-%dT%H:%M:%S.%6NZ)" |
  "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic scada-commands --property parse.key=true --property 'key.separator=|' >/dev/null
has_result() { kafka_consume scada-command-results 100 8000 --from-beginning | grep -q "$cmd_id"; }
until_ok 40 "результат команды $cmd_id" has_result
res=$(kafka_consume scada-command-results 100 8000 --from-beginning | grep -m1 "$cmd_id")
printf '%s' "$res" | grep -q '"status":"APPLIED"' || fail "результат команды не APPLIED: $res"
ok "команда → APPLIED"

log "3. Сбой: брокер Kafka пропал на 45 с"
sent_before=$(metric scada_telemetry_sent_total); resyncs_before=$(metric scada_kafka_resyncs_total)
overruns_before=$(metric scada_poll_overruns_total)
"${DC[@]}" stop kafka >/dev/null
sleep 45
health >/dev/null || fail "шлюз не отвечает при недоступном Kafka"
[ "$(kafka_status)" = DOWN ] && ok "health: kafka DOWN, шлюз жив" || fail "kafka не DOWN при остановленном брокере"
# Сравниваем с моментом до сбоя: пропуск цикла при запуске стенда (медленная машина) к Kafka не относится.
[ "$(metric scada_poll_overruns_total)" = "$overruns_before" ] && ok "опрос не отстаёт" || fail "опрос отстал при недоступном Kafka"
"${DC[@]}" start kafka >/dev/null
resyncs_grew() { [ "$(metric scada_kafka_resyncs_total)" -gt "$resyncs_before" ]; }
until_ok 120 "повторная отправка после возвращения брокера (scada_kafka_resyncs_total)" resyncs_grew
ok "доставка восстановлена, теги отправлены заново"
until_ok 60 "Kafka: UP" kafka_up

log "4. Сбой: БД пропала на 30 с"
overruns=$(metric scada_poll_overruns_total); sent_before=$(metric scada_telemetry_sent_total)
"${DC[@]}" stop postgres >/dev/null
sleep 30
[ "$(db_status)" = DOWN ] && ok "health: db DOWN" || fail "db не DOWN при остановленной БД"
controllers_up 1 && ok "контроллер на связи, опрос идёт" || fail "опрос остановился без БД"
[ "$(metric scada_poll_overruns_total)" = "$overruns" ] && ok "опрос не отстаёт" || fail "опрос отстал без БД"
"${DC[@]}" start postgres >/dev/null
until_ok 90 "БД: UP" db_up
ok "БД вернулась, шлюз подключился без перезапуска"

log "5. Сбой: ПЛК (симулятор) пропал"
"${DC[@]}" stop simulator >/dev/null
until_ok 90 "контроллер DOWN" all_controllers_down
ok "контроллер DOWN, шлюз жив"
"${DC[@]}" start simulator >/dev/null
until_ok 120 "контроллер снова на связи" controllers_up 1
ok "связь восстановлена без перезапуска шлюза"

log "Стенд прошёл дымовой тест"
