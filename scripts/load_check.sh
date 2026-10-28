#!/usr/bin/env bash
# ============================================================================
# Проверка производительности на масштабе стенда из контракта «по исключению»: N OPC UA-серверов по M
# узлов (loadtest/loadsim), треть узлов меняется каждую секунду; шлюз — процессом, Kafka — в Docker.
# Падает, если шлюз не успевает за периодом опроса, теряет сообщения или раздувается по памяти.
#
#   scripts/load_check.sh                     # 10 серверов × 2740 тегов = 27 400 (как у Bidway), 30 с замера
#   ACQUISITION=subscription scripts/load_check.sh   # то же, но шлюз получает значения подписками, а не опросом
#   SERVERS=20 TAGS=2740 scripts/load_check.sh   # 54 800 тегов
#   GATEWAY_BIN=target/release/scada-gateway LOADSIM_BIN=loadtest/loadsim/target/release/loadsim …  # без сборки
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

SERVERS=${SERVERS:-10}
TAGS=${TAGS:-2740}
WARMUP=${WARMUP:-20}
MEASURE=${MEASURE:-30}
MAX_RSS_MB=${MAX_RSS_MB:-500}
ACQUISITION=${ACQUISITION:-poll}
PORT=8899
PROJECT=scada-load
DC=(docker compose -p "$PROJECT" -f docker-compose.yml)
WORK=$(mktemp -d)
PIDS=()

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '   ✓ %s\n' "$*"; }
fail() { printf '   ✗ %s\n' "$*" >&2; exit 1; }
cleanup() {
  status=$?
  [ "$status" -eq 0 ] || { echo "--- шлюз (последние строки) ---"; tail -40 "$WORK/gateway.log" 2>/dev/null || true; }
  for p in "${PIDS[@]:-}"; do [ -z "$p" ] || kill "$p" 2>/dev/null || true; done
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

# awk читает весь вывод до конца (без exit): ранний выход рвал бы конвейер, и curl с pipefail давал код 23.
metric() { curl -fs "localhost:$PORT/actuator/prometheus" | awk -v n="$1" '!seen && ($1 == n || index($1, n "{") == 1) { print $NF; seen = 1 }'; }

GATEWAY_BIN=${GATEWAY_BIN:-}
LOADSIM_BIN=${LOADSIM_BIN:-}
if [ -z "$GATEWAY_BIN" ]; then cargo build --release --locked; GATEWAY_BIN=target/release/scada-gateway; fi
if [ -z "$LOADSIM_BIN" ]; then cargo build --release --locked --manifest-path loadtest/loadsim/Cargo.toml; LOADSIM_BIN=loadtest/loadsim/target/release/loadsim; fi

log "Источник: $SERVERS серверов × $TAGS тегов"
"$LOADSIM_BIN" config --servers "$SERVERS" --tags "$TAGS" > "$WORK/load.yaml"
if [ "$ACQUISITION" != poll ]; then
  sed -E -i 's/^([[:space:]]*)endpoint: .*$/&\n\1acquisition: '"$ACQUISITION"'/' "$WORK/load.yaml"
  grep -c "acquisition: $ACQUISITION" "$WORK/load.yaml" | grep -qx "$SERVERS" || fail "acquisition не добавлен во все $SERVERS серверов"
fi
"$LOADSIM_BIN" run --servers "$SERVERS" --tags "$TAGS" > "$WORK/loadsim.log" 2>&1 &
SIM=$!; PIDS+=($SIM)

log "Kafka"
"${DC[@]}" up -d kafka
until [ "$(docker inspect -f '{{.State.Health.Status}}' scada-rs-kafka 2>/dev/null)" = healthy ]; do sleep 2; done

log "Шлюз"
CONTROLLERS_YAML="$WORK/load.yaml" DB_ENABLED=false SERVER_PORT=$PORT SPRING_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9094 \
  KAFKA_TOPICS_TELEMETRY=load.tags KAFKA_TOPICS_COMMANDS=load.commands KAFKA_TOPICS_COMMAND_RESULTS=load.results \
  KAFKA_TOPICS_EVENTS=load.events KAFKA_TOPICS_ALARMS=load.alarms \
  "$GATEWAY_BIN" > "$WORK/gateway.log" 2>&1 &
GW=$!; PIDS+=($GW)
for _ in $(seq 120); do
  up=$(curl -fs "localhost:$PORT/actuator/health" 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); print(sum(1 for v in d["components"]["controllers"].values() if v=="UP"))' 2>/dev/null || echo 0)
  [ "$up" = "$SERVERS" ] && break; sleep 2
done
[ "$up" = "$SERVERS" ] || fail "на связи $up из $SERVERS контроллеров"
ok "$SERVERS контроллеров на связи, $((SERVERS * TAGS)) тегов"
if [ "$ACQUISITION" = subscription ]; then
  [ "$(grep -c 'подписки OPC UA' "$WORK/gateway.log")" = "$SERVERS" ] || fail "подписки созданы не на всех серверах: $(grep -h 'подписки' "$WORK/gateway.log" | head -3)"
  ok "подписки созданы на всех серверах: $(grep -h 'подписки OPC UA' "$WORK/gateway.log" | head -1 | sed 's/.*подписки OPC UA//')"
fi

log "Замер: прогрев ${WARMUP} с, затем ${MEASURE} с"
sleep "$WARMUP"
sent0=$(metric scada_telemetry_sent_total); sup0=$(metric scada_telemetry_suppressed_total); over0=$(metric scada_poll_overruns_total)
cpu0=$(awk '{print $14+$15}' "/proc/$GW/stat"); sim0=$(awk '{print $14+$15}' "/proc/$SIM/stat"); lo0=$(awk '$1 == "lo:" {print $2}' /proc/net/dev); t0=$SECONDS
sleep "$MEASURE"
sent1=$(metric scada_telemetry_sent_total); sup1=$(metric scada_telemetry_suppressed_total); over1=$(metric scada_poll_overruns_total)
cpu1=$(awk '{print $14+$15}' "/proc/$GW/stat"); sim1=$(awk '{print $14+$15}' "/proc/$SIM/stat"); lo1=$(awk '$1 == "lo:" {print $2}' /proc/net/dev); dt=$((SECONDS - t0))
rss_mb=$(( $(awk '/^VmRSS/ {print $2}' "/proc/$GW/status") / 1024 ))
hz=$(getconf CLK_TCK)
rate=$(( (sent1 - sent0) / dt ))
cpu_pct=$(python3 -c "print(round(($cpu1 - $cpu0) / $hz / $dt * 100))")
sim_pct=$(python3 -c "print(round(($sim1 - $sim0) / $hz / $dt * 100))")
proc_ms=$(curl -fs "localhost:$PORT/actuator/prometheus" | python3 -c '
import sys
s = c = 0.0
for line in sys.stdin:
    if line.startswith("scada_process_seconds_sum"): s = float(line.split()[-1])
    if line.startswith("scada_process_seconds_count"): c = float(line.split()[-1])
print(round(s / c * 1000, 1) if c else 0)')

printf '   публикуется: %s сообщ./с, подавлено повторов: %s\n' "$rate" "$((sup1 - sup0))"
printf '   режим: %s; процессор шлюза: %s%% одного ядра, серверов-источников (loadsim): %s%%, память: %s МБ, обработка цикла: %s мс\n' \
  "$ACQUISITION" "$cpu_pct" "$sim_pct" "$rss_mb" "$proc_ms"
# Весь трафик через loopback: шлюз ↔ серверы-источники и шлюз → Kafka (последний в обоих режимах одинаков).
printf '   трафик через lo: %s КБ/с\n' "$(( (lo1 - lo0) / dt / 1024 ))"

[ "$over1" = "$over0" ] && ok "опрос не отстаёт (пропущенных циклов нет)" || fail "пропущено циклов опроса: $((over1 - over0))"
[ "${rate:-0}" -gt 1000 ] && ok "поток публикации идёт" || fail "публикуется слишком мало: $rate/с"
[ "$(metric scada_kafka_send_errors_total)" = 0 ] && ok "ошибок доставки в Kafka нет" || fail "ошибки доставки в Kafka"
[ "$rss_mb" -lt "$MAX_RSS_MB" ] && ok "память ${rss_mb} МБ < ${MAX_RSS_MB} МБ" || fail "память ${rss_mb} МБ ≥ ${MAX_RSS_MB} МБ"
metric scada_task_restarts_total >/dev/null 2>&1 && [ -n "$(metric scada_task_restarts_total)" ] && fail "задачи шлюза перезапускались"
log "Нагрузочная проверка пройдена"
