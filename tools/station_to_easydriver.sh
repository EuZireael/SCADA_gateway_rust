#!/usr/bin/env bash
# ============================================================================
# Станция «EasyDriver»: тот же config/stations/BN1_MCA1.yaml, но каждый канал читается по PAC (driver-master) —
# протоколу, на котором работает драйвер старой SCADA (EasyDriver, savushkin-r-d/driver), — а не по OPC UA.
# Получается из файла станции для OPC UA подстановкой, поэтому набор каналов, имена (Kafka-key), типы, доступ на запись
# и выключенные каналы совпадают; различаются протокол, адрес и пробелы в индексах полей (`ST_CH[ 1 ]` → `ST_CH[1]`:
# у PAC поле — адрес в ПЛК, пробелов в нём быть не может; в имени канала они остаются). Тест
# `config::tests::easydriver_station_has_the_same_channels_as_the_opcua_one` следит, что файл не разошёлся с исходным.
#
#   tools/station_to_easydriver.sh [станция-opcua.yaml] [выход.yaml]
#   (по умолчанию config/stations/BN1_MCA1.yaml → config/stations/BN1_MCA1_easydriver.yaml)
# Адрес контроллера — env PAC_HOST, порт — PAC_PORT (10000 по умолчанию; у эмулятора мойки 10100).
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
IN="${1:-config/stations/BN1_MCA1.yaml}"
OUT="${2:-config/stations/BN1_MCA1_easydriver.yaml}"

STATION="$(basename "$IN" .yaml)"
{
  cat <<HEAD
# ============================================================================
# Реальная станция ${STATION} (Барановичи-1): один контроллер ptusa, все каналы — PAC (driver-master), то есть по
# протоколу «EasyDriver»: тому, на котором драйвер старой SCADA (EasyServer) говорит с контроллером.
# СГЕНЕРИРОВАНО tools/station_to_easydriver.sh из config/stations/${STATION}.yaml (тот же набор каналов, те же имена,
# типы и доступ на запись) — не править руками. Тот же набор по OPC UA — config/stations/${STATION}.yaml.
# Адрес контроллера — env PAC_HOST, порт — PAC_PORT (10000 по умолчанию). IP объекта в репозиторий не пишется.
# ============================================================================
HEAD
  # Заголовок исходного файла (до `opcua:`) отбрасываем, дальше — подстановка протокола и адреса.
  sed -n '/^opcua:/,$p' "$IN" | sed -E \
    -e 's|^(    - id: )opcua-|\1pac-|' \
    -e 's|endpoint: "opc\.tcp://\$\{PLC_HOST\}:\$\{PLC_OPCUA_PORT:4840\}"|endpoint: "pac://${PAC_HOST}:${PAC_PORT:10000}"|' \
    -e 's|nodeId: "ns=2;s=([^"]*)"|nodeId: "pac:\1"|' \
    -e 's|protocol: opcua|protocol: pac|' \
    -e ':a' -e 's|(fieldName: "[^" ]*) ([^"]*")|\1\2|' -e 'ta'
} > "$OUT"
echo "$OUT: $(grep -c 'deviceName' "$OUT") каналов"
