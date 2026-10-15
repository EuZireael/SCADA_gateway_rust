#!/usr/bin/env bash
# ============================================================================
# Стенд на симуляторе в одном из двух режимов — одна и та же станция BN1_MCA1 (2647 каналов, 1834 включено):
#
#   scripts/stand.sh opcua        # симулятор отдаёт всё по OPC UA, шлюз читает config/stations/BN1_MCA1.yaml
#   scripts/stand.sh easydriver   # симулятор отдаёт всё по PAC (driver-master) — протоколу драйвера старой SCADA
#                                 # (EasyDriver), шлюз читает config/stations/BN1_MCA1_easydriver.yaml
#   scripts/stand.sh <режим> down [-v]      # остановить (-v — стереть БД)
#   scripts/stand.sh <режим> logs [сервис]  # журнал
#
# Режимы не работают одновременно: порты общие (8888, 9094, 5433). Сравнение режимов по скорости, нагрузке и
# надёжности — scripts/compare_protocols.sh и docs/PROTOCOL_COMPARISON.md.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=${1:-}
case "$MODE" in
  opcua)      export SIM_CONFIG=config/station_opcua.yaml GATEWAY_STATION=BN1_MCA1.yaml ;;
  easydriver) export SIM_CONFIG=config/station_pac.yaml   GATEWAY_STATION=BN1_MCA1_easydriver.yaml ;;
  *) sed -n 2,12p "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac
shift
DC=(docker compose -f docker-compose.yml)

case "${1:-up}" in
  up)
    "${DC[@]}" up -d --build
    echo "▶ Режим $MODE: шлюз http://localhost:8888/actuator/health · Kafka для монитора localhost:9094"
    echo "  симулятор: ${SIM_CONFIG#config/} · станция шлюза: $GATEWAY_STATION"
    ;;
  down) "${DC[@]}" down "${@:2}" ;;
  logs) "${DC[@]}" logs -f "${@:2}" ;;
  *) echo "неизвестная команда: $1 (up | down | logs)" >&2; exit 2 ;;
esac
