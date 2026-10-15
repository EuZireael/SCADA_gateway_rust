#!/usr/bin/env python3
"""Проверка безопасности протокола PAC (driver-master, «EasyDriver»): что может посторонний клиент без учётных данных.

Подключается к порту PAC (10000 у настоящего контроллера), ничего не зная о станции, и показывает:
  * тело ответа — открытый текст (zlib — сжатие, не шифрование);
  * имя контроллера и версию протокола без какой-либо аутентификации;
  * весь снимок станции (все приборы и поля) одним запросом;
  * с --write ПРИБОР ПОЛЕ ЗНАЧЕНИЕ — выполняет команду записи (`set_cmd`), тоже без аутентификации.

Запускать только против своего эмулятора или стенда:
  scripts/ptusa_emulator.sh ../Moika/BN1-CIP1 10110
  scripts/pac_probe_noauth.py 127.0.0.1 10110
  scripts/pac_probe_noauth.py 127.0.0.1 10110 --write OBJECT1 'PAR_MAIN' 7   # запись (индекс 1 по умолчанию)
"""
import argparse
import re
import socket
import sys
import zlib


def frame(pidx, cmd, extra=b""):
    payload = bytes([cmd]) + extra
    return bytes([ord("s"), 1, 1, pidx, len(payload) >> 8, len(payload) & 0xFF]) + payload


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("соединение закрыто")
        buf += chunk
    return buf


def request(sock, pidx, cmd, extra=b""):
    sock.sendall(frame(pidx, cmd, extra))
    head = read_exact(sock, 5)
    length = (head[3] << 8) | head[4]
    body = read_exact(sock, length) if length else b""
    request.last_packed = length  # размер сжатого тела: кадр PAC ограничен 65 535 байтами
    return head[1], zlib.decompress(body) if body else b""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument("--write", nargs=3, metavar=("ПРИБОР", "ПОЛЕ", "ЗНАЧЕНИЕ"))
    a = ap.parse_args()

    s = socket.create_connection((a.host, a.port), 5)
    s.settimeout(5)
    banner = read_exact(s, 10)
    print(f"приветствие без запроса учётных данных: {banner!r}")

    status, info = request(s, 1, 10)
    text = info.decode("utf-8", "replace")
    print(f"GET_INFO_ON_CONNECT → статус {status}, открытый текст: {text.strip()!r}")

    status, states = request(s, 2, 101)
    lua = states[2:].decode("utf-8", "replace")
    devices = re.findall(r"^\t(\S+)=\{", lua, flags=re.M)
    fields = len(re.findall(r"[A-Za-z_][A-Za-z_0-9]*=|\[\d+\]=", lua))
    print(f"GET_DEVICES_STATES → статус {status}: {len(states)} байт Lua (сжато {request.last_packed} из 65 535 возможных), "
          f"приборов ≈ {len(devices)}, полей ≈ {fields}")
    print("  начало ответа (открытый текст):", lua[:160].replace("\n", " ").replace("\t", " "))

    if a.write:
        dev, field, value = a.write
        cmd = f"__{dev}:set_cmd('{field}', 1, {value})".encode()
        status, code = request(s, 3, 102, cmd)
        print(f"EXEC_DEVICE_COMMAND {cmd.decode()} → статус {status}, код результата {code!r} (0 — команда принята)")
    s.close()
    print("вывод: протокол не требует ни логина, ни ключа; всё, что видит контроллер, читает и пишет любой, кто достучался до порта")


if __name__ == "__main__":
    sys.exit(main())
