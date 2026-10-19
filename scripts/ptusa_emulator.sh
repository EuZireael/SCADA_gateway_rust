#!/usr/bin/env bash
# ============================================================================
# Эмулятор НАСТОЯЩЕГО PAC: прошивка ptusa_main (сборка под ПК из submodule bin
# проекта ПЛК) исполняет проект станции без модулей ввода-вывода (--no_io).
# Эталон протокола driver-master для шлюза: приветствие, снимок t[прибор][поле],
# коды результата записи. Значения датчиков нулевые — живые данные даёт симулятор.
#
#   ./scripts/ptusa_emulator.sh <папка проекта ПЛК> [порт]   # порт по умолчанию 10100
#   PTUSA_OPC=r ./scripts/ptusa_emulator.sh ...               # + OPC UA ptusa на :4841
#   simulator/target/release/simulator probe-pac 127.0.0.1 10100   # посмотреть снимок t[прибор][поле]
#   docker rm -f ptusa-emulator                             # остановить
#
# Если в проекте нет рабочего бинарника (submodule bin не выгружен, файл пустой) — PTUSA_BIN=<файл ptusa_main>
# (например ../Moika/BN1-CIP1/bin/linux-default/Release/ptusa_main). Несколько эмуляторов рядом — PTUSA_NAME=<имя>.
#
# Проект монтируется только на чтение; бинарник копируется в контейнер (в исходной
# папке у него может не быть права на исполнение). Нужен glibc ≥ 2.38 — debian:stable-slim.
# ============================================================================
set -euo pipefail

PROJECT=$(realpath "${1:?укажи папку проекта ПЛК, например ../Moika/BN1-CIP1}")
PORT=${2:-10100}
OPC=${PTUSA_OPC:-off}
NAME=${PTUSA_NAME:-ptusa-emulator}
BIN=bin/linux-default/Release/ptusa_main
HOST_BIN=${PTUSA_BIN:-$PROJECT/$BIN}

[ -s "$HOST_BIN" ] || { echo "нет (или пустой) $HOST_BIN: submodule bin проекта ПЛК не выгружен, укажи PTUSA_BIN" >&2; exit 1; }
[ -f "$PROJECT/main.plua" ] || { echo "нет $PROJECT/main.plua — это не проект ptusa" >&2; exit 1; }

PORTS=(-p "$PORT:10000")
[ "$OPC" != "off" ] && PORTS+=(-p "4841:4841")

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" "${PORTS[@]}" -v "$PROJECT:/project:ro" -v "$(realpath "$HOST_BIN"):/ptusa_main.src:ro" -w /project \
  debian:stable-slim sh -c "cp /ptusa_main.src /tmp/ptusa_main && chmod +x /tmp/ptusa_main && \
    exec /tmp/ptusa_main main.plua --no_io --opc $OPC --path . --sys_path ./sys --extra_paths ./dairy-sys" \
  >/dev/null

echo "▶ $NAME: driver-master localhost:$PORT (OPC UA: $OPC)"
echo "  логи: docker logs -f $NAME"
