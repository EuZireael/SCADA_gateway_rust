#!/usr/bin/env bash
# ============================================================================
# Стенд на эмуляторе мойки одной командой: прошивка ptusa (проект станции) + OPC UA-фасад +
# шлюз (Rust) + Kafka + Postgres. Подробности — docker-compose.moika.yml, docs/SPECIFICATION.md §5.
#
#   MOIKA_PROJECT=<папка проекта ПЛК> ./up-moika.sh          # весь стенд, шлюз читает станцию по OPC UA (через фасад)
#   MOIKA_PROJECT=… ./up-moika.sh easydriver                 # весь стенд, шлюз читает ту же станцию напрямую по PAC
#   MOIKA_PROJECT=… ./up-moika.sh [opcua|easydriver] plc     # только прошивка (+ фасад в opcua): шлюз и Kafka — свои
#   ./up-moika.sh down [-v]                                  # остановить (-v — стереть БД)
#
# MOIKA_PROJECT — папка проекта станции (BN1-CIP1): main.plua, main.io.lua, main.objects.lua…
# и подмодуль bin/ (git submodule update --init). Проект проприетарный, в репозиторий не входит;
# по умолчанию ищется ../Moika/BN1-CIP1. Windows: те же команды docker compose напрямую (см.
# docs/MONITOR_INTEGRATION.md).
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"
MODE=opcua
case "${1:-}" in opcua|easydriver) MODE=$1; shift ;; esac
if [ "$MODE" = easydriver ]; then
  export GATEWAY_STATION=BN1_MCA1_easydriver.yaml
  COMPOSE=(docker compose -f docker-compose.moika.yml)
else
  export GATEWAY_STATION=BN1_MCA1.yaml
  COMPOSE=(docker compose -f docker-compose.moika.yml --profile opcua)   # профиль opcua — фасад
fi

if [ "${1:-}" = "down" ]; then
  MOIKA_PROJECT="${MOIKA_PROJECT:-.}" "${COMPOSE[@]}" --profile opcua down "${@:2}"
  exit 0
fi

export MOIKA_PROJECT="${MOIKA_PROJECT:-../Moika/BN1-CIP1}"
[ -f "$MOIKA_PROJECT/main.plua" ] || { echo "Нет проекта ПЛК в $MOIKA_PROJECT (нужен main.plua). Задайте MOIKA_PROJECT." >&2; exit 1; }
[ -f "$MOIKA_PROJECT/bin/linux-default/Release/ptusa_main" ] || {
  echo "В проекте нет прошивки bin/linux-default/Release/ptusa_main — подмодуль bin не загружен:" >&2
  echo "  git -C \"$MOIKA_PROJECT\" submodule update --init" >&2; exit 1; }

if [ "${1:-}" = "plc" ]; then
  if [ "$MODE" = easydriver ]; then
    "${COMPOSE[@]}" up -d ptusa
    echo "▶ PAC (driver-master) прошивки: localhost:10100"
    echo "  Шлюз: CONTROLLERS_YAML=$PWD/config/stations/BN1_MCA1_easydriver.yaml PAC_HOST=localhost PAC_PORT=10100"
  else
    "${COMPOSE[@]}" up -d --build ptusa ptusa-opcua
    echo "▶ OPC UA станции: opc.tcp://localhost:4840 (driver-master прошивки: localhost:10100)"
    echo "  Шлюз: CONTROLLERS_YAML=$PWD/config/stations/BN1_MCA1.yaml PLC_HOST=localhost"
  fi
  exit 0
fi

"${COMPOSE[@]}" up -d --build
echo "▶ Режим $MODE. Шлюз: http://localhost:8888/actuator/health · Kafka для монитора: localhost:9094"
[ "$MODE" = opcua ] && echo "  OPC UA станции: localhost:4840" || echo "  PAC прошивки: localhost:10100"
