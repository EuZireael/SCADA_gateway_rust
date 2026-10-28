#!/usr/bin/env bash
# ============================================================================
# Стресс-тест шлюза по ступеням: сколько значений тегов в секунду шлюз опрашивает по OPC UA, обрабатывает и
# отдаёт в Kafka, и чего это стоит (процессор, память, пропущенные циклы).
#
# Источник — loadtest/loadsim: сотни OPC UA-серверов («контроллеров») или, с PROTOCOL=pac, PAC-серверов на протоколе
# драйвера старой SCADA («EasyDriver»), по несколько тысяч узлов, часть узлов
# меняется каждую секунду. Шлюз — настоящий release-процесс (публикация «по исключению», как на объекте),
# Kafka — настоящая, в Docker (одна нода, один раздел топика, как в контракте). БД выключена (DB_ENABLED=false).
#
# Скорость «значений в секунду» = Δ(scada_telemetry_sent_total + scada_telemetry_suppressed_total) / время:
# каждое снятое значение либо уходит в Kafka, либо подавляется как повтор — в обоих случаях оно прошло весь путь
# опрос → фильтр → скрипты. Сообщений в Kafka — Δ sent; доставлено — по конечному смещению раздела.
#
#   scripts/stress_test.sh                    # все ступени по умолчанию (≈ 12 минут), OPC UA
#   PROTOCOL=pac scripts/stress_test.sh       # то же по PAC (EasyDriver): те же теги и ступени
#   STAGES="r500k 100 5000 1000 0.333 4" scripts/stress_test.sh      # одна ступень
#   WARMUP=30 MEASURE=60 OUT=/tmp/stress scripts/stress_test.sh
#
# Ступень — строка «метка серверов узлов период_опроса_мс доля_меняющихся процессов_источника».
# Результат: таблица Markdown в stdout и $OUT/results.json (по умолчанию loadtest/results/<дата>).
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

WARMUP=${WARMUP:-30}
MEASURE=${MEASURE:-60}
PORT=${GATEWAY_PORT:-8899}
BASE_PORT=${BASE_PORT:-4900}
KAFKA_HEAP=${KAFKA_HEAP:-2g}
PROTOCOL=${PROTOCOL:-opcua}   # opcua | pac
OUT=${OUT:-loadtest/results/$(date +%Y-%m-%d)-$PROTOCOL}
KAFKA=scada-stress-kafka
TOPIC_PREFIX=stress
DEFAULT_STAGES='
station   1  2647 1000 0.1   1
s55k     20  2740 1000 0.333 1
r100k    40  2500 1000 0.333 1
r250k    50  5000 1000 0.333 2
r500k   100  5000 1000 0.333 4
r750k   150  5000 1000 0.333 6
r1m     200  5000 1000 0.333 8
'
STAGES=${STAGES:-$DEFAULT_STAGES}

WORK=$(mktemp -d)
mkdir -p "$OUT"
PIDS=()

log()  { printf '\n\033[1m== %s\033[0m\n' "$*" >&2; }
note() { printf '   %s\n' "$*" >&2; }
stop_pids() { for p in "${PIDS[@]:-}"; do [ -z "$p" ] || kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; PIDS=(); }
cleanup() {
  status=$?
  [ "$status" -eq 0 ] || { echo "--- шлюз (последние строки) ---" >&2; tail -30 "$WORK/gateway.log" 2>/dev/null >&2 || true; }
  stop_pids
  docker rm -f -v "$KAFKA" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

GATEWAY_BIN=${GATEWAY_BIN:-}
LOADSIM_BIN=${LOADSIM_BIN:-}
if [ -z "$GATEWAY_BIN" ]; then cargo build --release --locked >&2; GATEWAY_BIN=target/release/scada-gateway; fi
if [ -z "$LOADSIM_BIN" ]; then
  cargo build --release --locked --manifest-path loadtest/loadsim/Cargo.toml >&2
  LOADSIM_BIN=loadtest/loadsim/target/release/loadsim
fi

# --- Kafka: одна нода, один раздел, с запасом по памяти и с быстрой очисткой старых сегментов ---------------------
log "Kafka (куча $KAFKA_HEAP, один раздел)"
docker rm -f -v "$KAFKA" >/dev/null 2>&1 || true
docker run -d --name "$KAFKA" -p 127.0.0.1:9094:9094 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS="PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094" \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092,EXTERNAL://127.0.0.1:9094" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT" \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true -e KAFKA_NUM_NETWORK_THREADS=8 -e KAFKA_NUM_IO_THREADS=8 \
  -e KAFKA_LOG_SEGMENT_BYTES=67108864 -e KAFKA_LOG_RETENTION_MS=30000 -e KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=10000 \
  -e KAFKA_LOG_ROLL_MS=10000 \
  -e KAFKA_HEAP_OPTS="-Xmx$KAFKA_HEAP -Xms$KAFKA_HEAP" -e CLUSTER_ID=scada-stress-cluster-01 \
  apache/kafka:4.3.1 >/dev/null
until docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
note "готов"

metric() { curl -fs "localhost:$PORT/actuator/prometheus" | awk -v n="$1" '!seen && ($1 == n || index($1, n "{") == 1) { print $NF; seen = 1 }'; }
cpu_ticks() { awk '{print $14+$15}' "/proc/$1/stat"; }
end_offset() {
  docker exec "$KAFKA" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null |
    awk -F: '{s += $3} END {print s + 0}'
}
# Гистограмма (накопительные корзины) → JSON {le: счётчик} для расчёта процентилей по разности двух снимков.
histogram() { curl -fs "localhost:$PORT/actuator/prometheus" | python3 -c '
import sys, re, json
name, out = sys.argv[1], {}
for line in sys.stdin:
    m = re.match(r"%s_bucket\{[^}]*le=\"([^\"]+)\"[^}]*\} (\S+)" % name, line)
    if m: out[m.group(1)] = float(m.group(2))
    elif line.startswith(name + "_sum"): out["sum"] = float(line.split()[-1])
    elif line.startswith(name + "_count"): out["count"] = float(line.split()[-1])
print(json.dumps(out))' "$1"; }

RESULTS="$WORK/results.jsonl"
: > "$RESULTS"

run_stage() {
  local label=$1 servers=$2 tags=$3 poll=$4 fraction=$5 procs=$6
  local total=$((servers * tags)) per_proc=$(( (servers + procs - 1) / procs ))
  local expected=$((total * 1000 / poll))
  local topic="$TOPIC_PREFIX.$label"
  log "[$PROTOCOL] Ступень $label: $servers контроллеров × $tags тегов = $total тегов, опрос $poll мс → ожидается $expected значений/с"

  "$LOADSIM_BIN" config --protocol "$PROTOCOL" --servers "$servers" --tags "$tags" --base-port "$BASE_PORT" --poll-ms "$poll" > "$WORK/stage.yaml"
  local first=0
  while [ "$first" -lt "$servers" ]; do
    local n=$per_proc; [ $((first + n)) -le "$servers" ] || n=$((servers - first))
    "$LOADSIM_BIN" run --protocol "$PROTOCOL" --servers "$n" --first "$first" --tags "$tags" --base-port "$BASE_PORT" \
      --fraction "$fraction" --period-ms 1000 > "$WORK/loadsim-$first.log" 2>&1 &
    PIDS+=($!); LOADSIM_PIDS+=($!)
    first=$((first + n))
  done
  sleep 8

  CONTROLLERS_YAML="$WORK/stage.yaml" DB_ENABLED=false SERVER_PORT=$PORT SPRING_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9094 \
    KAFKA_TOPICS_TELEMETRY="$topic" KAFKA_TOPICS_COMMANDS="$topic.cmd" KAFKA_TOPICS_COMMAND_RESULTS="$topic.res" \
    KAFKA_TOPICS_EVENTS="$topic.ev" KAFKA_TOPICS_ALARMS="$topic.al" \
    "$GATEWAY_BIN" > "$WORK/gateway.log" 2>&1 &
  local gw=$!; PIDS+=($gw)
  local up=0 t0=$SECONDS
  while [ $((SECONDS - t0)) -lt 600 ]; do
    up=$(curl -fs "localhost:$PORT/actuator/health" 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); print(sum(1 for v in d["components"]["controllers"].values() if v=="UP"))' 2>/dev/null || echo 0)
    [ "$up" = "$servers" ] && break; sleep 3
  done
  [ "$up" = "$servers" ] || { note "на связи только $up из $servers контроллеров"; echo "{\"label\":\"$label\",\"error\":\"connected $up/$servers\"}" >> "$RESULTS"; stop_pids; LOADSIM_PIDS=(); return; }
  note "$servers контроллеров на связи за $((SECONDS - t0)) с; прогрев $WARMUP с, замер $MEASURE с"
  sleep "$WARMUP"

  local sent0 sup0 over0 err0 off0 cpu0 lcpu0 t_start
  sent0=$(metric scada_telemetry_sent_total); sup0=$(metric scada_telemetry_suppressed_total)
  over0=$(metric scada_poll_overruns_total); err0=$(metric scada_kafka_send_errors_total)
  local qf0 qf1; qf0=$(metric scada_kafka_queue_full_total); qf0=${qf0:-0}
  off0=$(end_offset "$topic"); cpu0=$(cpu_ticks "$gw")
  local lcpu0=0; for p in "${LOADSIM_PIDS[@]}"; do lcpu0=$((lcpu0 + $(cpu_ticks "$p"))); done
  histogram scada_poll_seconds > "$WORK/poll0.json"; histogram scada_process_seconds > "$WORK/proc0.json"
  t_start=$(date +%s.%N)
  sleep "$MEASURE"
  local sent1 sup1 over1 err1 off1 cpu1 lcpu1 t_end rss hwm
  sent1=$(metric scada_telemetry_sent_total); sup1=$(metric scada_telemetry_suppressed_total)
  over1=$(metric scada_poll_overruns_total); err1=$(metric scada_kafka_send_errors_total)
  off1=$(end_offset "$topic"); cpu1=$(cpu_ticks "$gw"); qf1=$(metric scada_kafka_queue_full_total); qf1=${qf1:-0}
  local lcpu1=0; for p in "${LOADSIM_PIDS[@]}"; do lcpu1=$((lcpu1 + $(cpu_ticks "$p"))); done
  t_end=$(date +%s.%N)
  histogram scada_poll_seconds > "$WORK/poll1.json"; histogram scada_process_seconds > "$WORK/proc1.json"
  rss=$(awk '/^VmRSS/ {print $2}' "/proc/$gw/status"); hwm=$(awk '/^VmHWM/ {print $2}' "/proc/$gw/status")
  local lrss=0; for p in "${LOADSIM_PIDS[@]}"; do lrss=$((lrss + $(awk '/^VmRSS/ {print $2}' "/proc/$p/status"))); done
  local restarts; restarts=$(curl -fs "localhost:$PORT/actuator/prometheus" | awk '/^scada_task_restarts_total/ {s += $NF} END {print s + 0}')

  python3 - "$label" "$servers" "$tags" "$poll" "$fraction" "$expected" "$t_start" "$t_end" \
    "$sent0" "$sent1" "$sup0" "$sup1" "$over0" "$over1" "$err0" "$err1" "$off0" "$off1" \
    "$cpu0" "$cpu1" "$lcpu0" "$lcpu1" "$rss" "$hwm" "$lrss" "$restarts" "$((qf1 - qf0))" "$WORK" >> "$RESULTS" <<'PY'
import sys, json, os
a = sys.argv[1:]
label, servers, tags, poll = a[0], int(a[1]), int(a[2]), int(a[3])
fraction, expected = float(a[4]), int(a[5])
dt = float(a[7]) - float(a[6])
sent0, sent1, sup0, sup1, over0, over1, err0, err1, off0, off1 = (int(float(x)) for x in a[8:18])
cpu0, cpu1, lcpu0, lcpu1, rss, hwm, lrss, restarts, queue_full = (int(float(x)) for x in a[18:27])
work = a[27]
hz = os.sysconf("SC_CLK_TCK")
def hist(path):
    return json.load(open(path))
def delta(n0, n1):
    h0, h1 = hist(f"{work}/{n0}.json"), hist(f"{work}/{n1}.json")
    return {k: h1[k] - h0.get(k, 0) for k in h1}
def pct(d, q):
    count = d.get("count", 0)
    if not count: return None
    bounds = sorted((float(k) if k != "+Inf" else float("inf"), v) for k, v in d.items() if k not in ("sum", "count"))
    for le, c in bounds:
        if c >= q * count: return le
    return None
poll, proc = delta("poll0", "poll1"), delta("proc0", "proc1")
values = (sent1 + sup1) - (sent0 + sup0)
res = {
    "label": label, "controllers": servers, "tags_per_controller": tags, "tags": servers * tags,
    "poll_ms": poll, "changing_fraction_per_s": fraction, "expected_values_per_s": expected,
    "window_s": round(dt, 1),
    "values_per_s": round(values / dt), "kafka_sent_per_s": round((sent1 - sent0) / dt),
    "kafka_delivered_per_s": round((off1 - off0) / dt), "suppressed_per_s": round((sup1 - sup0) / dt),
    "poll_overruns": over1 - over0, "kafka_send_errors": err1 - err0, "kafka_queue_full": queue_full, "task_restarts": restarts,
    "gateway_cpu_cores": round((cpu1 - cpu0) / hz / dt, 2), "source_cpu_cores": round((lcpu1 - lcpu0) / hz / dt, 2),
    "gateway_rss_mb": round(rss / 1024), "gateway_peak_rss_mb": round(hwm / 1024), "source_rss_mb": round(lrss / 1024),
    "cycle_p50_s": pct(poll, 0.5), "cycle_p99_s": pct(poll, 0.99),
    "process_mean_ms": round(proc["sum"] / proc["count"] * 1000, 1) if proc.get("count") else None,
}
print(json.dumps(res, ensure_ascii=False))
PY
  tail -1 "$RESULTS" | python3 -c '
import sys, json
r = json.loads(sys.stdin.read())
print("   значений/с: %(values_per_s)s (ожидали %(expected_values_per_s)s); в Kafka отправлено %(kafka_sent_per_s)s/с, доставлено %(kafka_delivered_per_s)s/с" % r, file=sys.stderr)
print("   шлюз: %(gateway_cpu_cores)s ядер, %(gateway_rss_mb)s МБ; источник: %(source_cpu_cores)s ядер; пропущено циклов %(poll_overruns)s, ошибок Kafka %(kafka_send_errors)s, отказов очереди %(kafka_queue_full)s" % r, file=sys.stderr)'
  stop_pids; LOADSIM_PIDS=()
  docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic" >/dev/null 2>&1 || true
  sleep 3
}

LOADSIM_PIDS=()
while read -r label servers tags poll fraction procs; do
  [ -z "${label:-}" ] && continue
  run_stage "$label" "$servers" "$tags" "$poll" "$fraction" "$procs"
done <<< "$STAGES"

python3 - "$RESULTS" "$OUT/results.json" <<'PY'
import sys, json
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
json.dump({"machine": {"cpu_threads": __import__("os").cpu_count()}, "stages": rows}, open(sys.argv[2], "w"), ensure_ascii=False, indent=2)
print("| ступень | контроллеров × тегов | значений/с | в Kafka/с | ядер шлюза | RSS шлюза, МБ | цикл p99, с | пропущено циклов |")
print("|---|---|---|---|---|---|---|---|")
for r in rows:
    if "error" in r:
        print(f"| {r['label']} | — | не стартовала: {r['error']} | | | | | |"); continue
    print(f"| {r['label']} | {r['controllers']} × {r['tags_per_controller']} | {r['values_per_s']:,} | {r['kafka_delivered_per_s']:,} | "
          f"{r['gateway_cpu_cores']} | {r['gateway_peak_rss_mb']} | {r['cycle_p99_s']} | {r['poll_overruns']} |".replace(",", " "))
PY
log "Результаты: $OUT/results.json"
