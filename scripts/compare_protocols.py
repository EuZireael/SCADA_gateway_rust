#!/usr/bin/env python3
"""Сравнение двух способов читать ОДНУ И ТУ ЖЕ станцию: «EasyDriver» (PAC, driver-master) и OPC UA.

Режимы шлюза (одни и те же 2647 каналов, 1834 включено, те же имена и типы):
  easydriver  config/stations/BN1_MCA1_easydriver.yaml — шлюз говорит с контроллером по PAC, как драйвер старой SCADA;
  opcua       config/stations/BN1_MCA1.yaml            — шлюз говорит по OPC UA (у прошивки ptusa — через OPC UA-фасад
                                                          ptusa-opcua, потому что собственный OPC UA прошивки отдаёт 22 % каналов).

Источник данных (--source):
  firmware  настоящая прошивка ptusa (эмулятор мойки, проект BN1-CIP1): сценарии coverage, load, clients, writes, recovery;
  sim       Rust-симулятор станции (реплей архива, значения меняются): сценарии coverage, load, writes.

Сценарии (--scenarios, через запятую):
  coverage   сколько включённых каналов получили значение с качеством GOOD, чем кончается холодный старт, совпадают ли
             значения у двух режимов;
  load       нагрузка шлюза и источника в установившемся режиме при опросе раз в секунду и каждые 100 мс;
  clients    нагрузка на ПЛК от N одновременных шлюзов (1, 2, 4, 8) — только firmware;
  writes     команды записи: статус, время до эффекта в телеметрии, ловит ли режим «принял, но не применил»;
  writes_fast то же при опросе 100 мс и 12 командах (время до эффекта почти не зависит от периода опроса шлюза);
  recovery   обрыв и возврат контроллера: за сколько шлюз это замечает и когда данные снова GOOD — только firmware.

Нужны: docker (Kafka и эмулятор прошивки), собранные release-бинарники шлюза, симулятора и ptusa-opcua, проект ПЛК
(MOIKA_PROJECT, по умолчанию ../Moika/BN1-CIP1) для --source firmware. Результат — JSON и таблицы Markdown в --out.

  scripts/compare_protocols.py --source firmware --out loadtest/results/protocols/<дата>
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATEWAY = ROOT / "target/release/scada-gateway"
SIMULATOR = ROOT / "simulator/target/release/simulator"
FACADE = ROOT / "ptusa-opcua/target/release/ptusa-opcua"
STATIONS = {"opcua": ROOT / "config/stations/BN1_MCA1.yaml", "easydriver": ROOT / "config/stations/BN1_MCA1_easydriver.yaml"}
KAFKA = "scada-cmp-kafka"
KAFKA_PORT = 9194
ENABLED_CHANNELS = 1834
PTUSA = "ptusa-emulator"
PTUSA_PAC_PORT = 10110
FACADE_PORT = 4850
FACADE_POLL_MS = os.environ.get("FACADE_POLL_MS", "500")   # период опроса прошивки фасадом (--facade-poll-ms)
SIM_PAC_PORT = 10210
SIM_OPC_PORT = 4860
CLK = os.sysconf("SC_CLK_TCK")


def log(msg):
    print(f"\033[1m== {msg}\033[0m", file=sys.stderr, flush=True)


def note(msg):
    print(f"   {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- процессы и метрики
def cpu_ticks(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat.rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def rss_mb(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS"):
            return int(line.split()[1]) / 1024
    return 0.0


def container_pid(name):
    out = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", name], capture_output=True, text=True).stdout.strip()
    return int(out) if out.isdigit() and int(out) > 0 else None


def http_get(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def prom(port):
    """Метрики шлюза: имя → значение (для гистограмм — _bucket с le)."""
    out, buckets = {}, {}
    for line in http_get(f"http://127.0.0.1:{port}/actuator/prometheus").splitlines():
        if line.startswith("#") or not line.strip():
            continue
        m = re.match(r'(\w+?)(\{[^}]*\})? (\S+)$', line)
        if not m:
            continue
        name, labels, val = m.group(1), m.group(2) or "", float(m.group(3))
        if name.endswith("_bucket"):
            le = re.search(r'le="([^"]+)"', labels)
            if le:
                buckets.setdefault(name[:-7], {})[le.group(1)] = val
        else:
            out[name] = out.get(name, 0.0) + val
    out["_buckets"] = buckets
    return out


def percentile(b0, b1, q):
    """Квантиль по разности двух снимков накопительных корзин гистограммы (верхняя граница корзины, секунды)."""
    d = {k: b1[k] - b0.get(k, 0.0) for k in b1}
    total = d.get("+Inf", 0.0)
    if total <= 0:
        return None
    bounds = sorted((float("inf") if k == "+Inf" else float(k), v) for k, v in d.items())
    for le, c in bounds:
        if c >= q * total:
            return le
    return None


class Proc:
    """Дочерний процесс с журналом в файл."""

    def __init__(self, argv, env, logfile, cwd=ROOT):
        self.log_path = Path(logfile)
        self.log = open(logfile, "wb")
        self.p = subprocess.Popen(argv, env={**os.environ, **env}, stdout=self.log, stderr=subprocess.STDOUT, cwd=cwd,
                                  start_new_session=True)

    @property
    def pid(self):
        return self.p.pid

    def stop(self):
        if self.p.poll() is None:
            try:
                os.killpg(self.p.pid, signal.SIGTERM)
                self.p.wait(timeout=10)
            except Exception:
                os.killpg(self.p.pid, signal.SIGKILL)
        self.log.close()


# ---------------------------------------------------------------- Kafka
class Kafka:
    def start(self):
        subprocess.run(["docker", "rm", "-f", "-v", KAFKA], capture_output=True)
        env = {
            "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": f"PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:{KAFKA_PORT}",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:9092,EXTERNAL://localhost:{KAFKA_PORT}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0", "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
            "KAFKA_LOG_RETENTION_MS": "600000", "KAFKA_HEAP_OPTS": "-Xmx1g -Xms1g", "CLUSTER_ID": "scada-cmp-cluster-01",
        }
        argv = ["docker", "run", "-d", "--name", KAFKA, "-p", f"{KAFKA_PORT}:{KAFKA_PORT}"]
        for k, v in env.items():
            argv += ["-e", f"{k}={v}"]
        subprocess.run(argv + ["apache/kafka:4.3.1"], check=True, capture_output=True)
        for _ in range(60):
            r = subprocess.run(["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                                "localhost:9092", "--list"], capture_output=True)
            if r.returncode == 0:
                return
            time.sleep(2)
        raise RuntimeError("Kafka не поднялась")

    def stop(self):
        subprocess.run(["docker", "rm", "-f", "-v", KAFKA], capture_output=True)

    def tail(self, topic):
        """Постоянный потребитель топика с начала: сообщения копятся, их можно читать частями (без запуска JVM на каждое чтение)."""
        return Tail(topic)

    def produce(self, topic, lines):
        subprocess.run(["docker", "exec", "-i", KAFKA, "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                        "localhost:9092", "--topic", topic, "--property", "parse.key=true", "--property", "key.separator=|"],
                       input="\n".join(lines) + "\n", text=True, capture_output=True, timeout=60)


class Tail:
    """Долгоживущий `kafka-console-consumer` в контейнере: разбирает строки `CreateTime<TAB>ключ<TAB>JSON` в список."""

    def __init__(self, topic):
        import threading
        self.rows, self.lock, self.topic = [], threading.Lock(), topic
        self.p = subprocess.Popen(
            ["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
             "--topic", topic, "--from-beginning", "--property", "print.key=true", "--property", "print.timestamp=true"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()

    def _run(self):
        for line in self.p.stdout:
            m = re.match(r"CreateTime:(\d+)\t([^\t]*)\t(\{.*\})$", line.rstrip("\n"))
            if m:
                try:
                    row = (int(m.group(1)), m.group(2), json.loads(m.group(3)))
                except ValueError:
                    continue
                with self.lock:
                    self.rows.append(row)

    def mark(self):
        with self.lock:
            return len(self.rows)

    def since(self, mark=0):
        with self.lock:
            return self.rows[mark:]

    def close(self):
        self.p.terminate()


# ---------------------------------------------------------------- источники
class Firmware:
    """Настоящая прошивка ptusa (эмулятор мойки) + OPC UA-фасад для режима opcua."""
    name = "firmware"

    def __init__(self, project, workdir):
        self.project, self.workdir = Path(project).resolve(), workdir
        self.facade = None
        self.pac_port = PTUSA_PAC_PORT
        self.opc_port = FACADE_PORT

    def start_plc(self):
        """Поднять эмулятор прошивки; контейнер иногда не успевает освободить порт после прошлого — пробуем ещё."""
        for attempt in range(3):
            subprocess.run([str(ROOT / "scripts/ptusa_emulator.sh"), str(self.project), str(PTUSA_PAC_PORT)], check=True,
                           capture_output=True)
            try:
                wait_banner(PTUSA_PAC_PORT, timeout=45)
                time.sleep(2)
                return
            except RuntimeError:
                if attempt == 2:
                    raise
                subprocess.run(["docker", "rm", "-f", PTUSA], capture_output=True)
                time.sleep(5)

    def stop_plc(self):
        subprocess.run(["docker", "rm", "-f", PTUSA], capture_output=True)

    def start(self, mode):
        self.start_plc()
        if mode == "opcua":
            self.facade = Proc([str(FACADE)], {"PAC_HOST": "127.0.0.1", "PAC_PORT": str(PTUSA_PAC_PORT),
                                               "STATION_CONFIG": str(STATIONS["opcua"]), "OPCUA_BIND": f"0.0.0.0:{FACADE_PORT}",
                                               "POLL_MS": FACADE_POLL_MS, "OPCUA_ENDPOINT": f"opc.tcp://127.0.0.1:{FACADE_PORT}"},
                               self.workdir / "facade.log")
            wait_port(FACADE_PORT)
            time.sleep(3)

    def stop(self):
        if self.facade:
            self.facade.stop()
            self.facade = None
        self.stop_plc()

    def plc_pids(self):
        """Процессы, чья загрузка — «нагрузка на контроллер и посредников»: ПЛК и, если есть, фасад."""
        out = {"plc": container_pid(PTUSA)}
        if self.facade:
            out["facade"] = self.facade.pid
        return out


class Simulator:
    """Rust-симулятор станции: все 2647 каналов только по OPC UA или только по PAC."""
    name = "sim"

    def __init__(self, workdir):
        self.workdir, self.proc = workdir, None
        self.pac_port, self.opc_port = SIM_PAC_PORT, SIM_OPC_PORT

    def start_plc(self):
        self.start(self.mode)

    def stop_plc(self):
        self.stop()

    def start(self, mode):
        self.mode = mode
        cfg = "config/station_pac.yaml" if mode == "easydriver" else "config/station_opcua.yaml"
        self.proc = Proc([str(SIMULATOR), cfg], {"PAC_PORT": str(SIM_PAC_PORT),
                                                 "OPCUA_ENDPOINT": f"opc.tcp://127.0.0.1:{SIM_OPC_PORT}"},
                         self.workdir / "simulator.log", cwd=ROOT / "simulator")
        wait_port(SIM_PAC_PORT if mode == "easydriver" else SIM_OPC_PORT)
        time.sleep(2)

    def stop(self):
        if self.proc:
            self.proc.stop()
            self.proc = None

    def plc_pids(self):
        return {"plc": self.proc.pid} if self.proc else {}


def wait_port(port, timeout=60):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), 1).close()
            return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"порт {port} не открылся")


def wait_banner(port, timeout=90):
    """Ждать, пока ПЛК не пришлёт приветствие PAC (прошивка поднимается несколько секунд)."""
    import socket
    end = time.time() + timeout
    while time.time() < end:
        try:
            s = socket.create_connection(("127.0.0.1", port), 1)
            s.settimeout(2)
            data = s.recv(10)
            s.close()
            if data.startswith(b"PAC accept"):
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError(f"PAC на порту {port} не отвечает")


# ---------------------------------------------------------------- шлюз
class Gateway:
    counter = 0

    def __init__(self, mode, src, workdir, poll_ms=1000, verify_ms=0, http_port=None, extra_env=None, tag=""):
        Gateway.counter += 1
        self.mode, self.n = mode, Gateway.counter
        self.port = http_port or 8900 + self.n
        self.prefix = f"cmp{self.n}-{mode}{tag}"
        station = STATIONS[mode].read_text(encoding="utf-8")
        station = station.replace("pollingRate: 1000", f"pollingRate: {poll_ms}")
        self.cfg = workdir / f"{self.prefix}.yaml"
        self.cfg.write_text(station, encoding="utf-8")
        env = {
            "CONTROLLERS_YAML": str(self.cfg), "DB_ENABLED": "false", "SERVER_PORT": str(self.port),
            "SPRING_KAFKA_BOOTSTRAP_SERVERS": f"localhost:{KAFKA_PORT}",
            "KAFKA_TOPICS_TELEMETRY": f"{self.prefix}.tags", "KAFKA_TOPICS_COMMANDS": f"{self.prefix}.cmd",
            "KAFKA_TOPICS_COMMAND_RESULTS": f"{self.prefix}.res", "KAFKA_TOPICS_EVENTS": f"{self.prefix}.ev",
            "KAFKA_TOPICS_ALARMS": f"{self.prefix}.al", "GATEWAY_SEND_BAD_FRAMES": "true",
            "GATEWAY_COMMANDS_VERIFY_MS": str(verify_ms),
        }
        if mode == "opcua":
            env.update({"PLC_HOST": "127.0.0.1", "PLC_OPCUA_PORT": str(src.opc_port)})
        else:
            env.update({"PAC_HOST": "127.0.0.1", "PAC_PORT": str(src.pac_port)})
        env.update(extra_env or {})
        self.started = time.time()
        self.proc = Proc([str(GATEWAY)], env, workdir / f"{self.prefix}.log")

    @property
    def topic(self):
        return f"{self.prefix}.tags"

    def wait_up(self, timeout=120):
        end = time.time() + timeout
        while time.time() < end:
            try:
                h = json.loads(http_get(f"http://127.0.0.1:{self.port}/actuator/health"))
                if all(v == "UP" for v in h["components"]["controllers"].values()):
                    return time.time() - self.started
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError("шлюз не поднялся")

    def wait_commands(self, timeout=90):
        """Ждать, пока шлюз начнёт принимать команды из Kafka (после старта брокера координатор группы готов не сразу)."""
        end = time.time() + timeout
        while time.time() < end:
            text = self.proc.log_path.read_text(encoding="utf-8", errors="replace")
            on, off = text.rfind("Приём команд включён"), text.rfind("Приём команд выключен")
            if on >= 0 and on > off:
                return
            time.sleep(0.5)
        raise RuntimeError("шлюз не начал принимать команды")

    def metrics(self):
        return prom(self.port)

    def stop(self):
        self.proc.stop()


def measure(gateways, src, seconds, warmup):
    """Окно замера: нагрузка шлюзов, источника и (у ПЛК) посредников, скорости публикации, цикл опроса."""
    time.sleep(warmup)
    t0 = time.time()
    m0 = [g.metrics() for g in gateways]
    g_cpu0 = [cpu_ticks(g.proc.pid) for g in gateways]
    s_pids = src.plc_pids()
    s_cpu0 = {k: cpu_ticks(v) for k, v in s_pids.items() if v}
    time.sleep(seconds)
    dt = time.time() - t0
    m1 = [g.metrics() for g in gateways]
    g_cpu1 = [cpu_ticks(g.proc.pid) for g in gateways]
    s_cpu1 = {k: cpu_ticks(s_pids[k]) for k in s_cpu0}
    res = {
        "window_s": round(dt, 1),
        "gateway_cores": round(sum(b - a for a, b in zip(g_cpu0, g_cpu1)) / CLK / dt, 3),
        "gateway_rss_mb": round(sum(rss_mb(g.proc.pid) for g in gateways)),
        "source_cores": {k: round((s_cpu1[k] - s_cpu0[k]) / CLK / dt, 3) for k in s_cpu0},
    }
    g0, g1 = m0[0], m1[0]
    sent = g1.get("scada_telemetry_sent_total", 0) - g0.get("scada_telemetry_sent_total", 0)
    sup = g1.get("scada_telemetry_suppressed_total", 0) - g0.get("scada_telemetry_suppressed_total", 0)
    res.update({
        "values_per_s": round((sent + sup) / dt), "kafka_per_s": round(sent / dt, 1),
        "poll_overruns": int(g1.get("scada_poll_overruns_total", 0) - g0.get("scada_poll_overruns_total", 0)),
        "send_errors": int(g1.get("scada_kafka_send_errors_total", 0) - g0.get("scada_kafka_send_errors_total", 0)),
    })
    for key, label in (("scada_poll_seconds", "cycle"), ("scada_process_seconds", "process")):
        b0, b1 = g0["_buckets"].get(key, {}), g1["_buckets"].get(key, {})
        cnt = g1.get(f"{key}_count", 0) - g0.get(f"{key}_count", 0)
        tot = g1.get(f"{key}_sum", 0) - g0.get(f"{key}_sum", 0)
        res[f"{label}_mean_ms"] = round(tot / cnt * 1000, 2) if cnt else None
        p99 = percentile(b0, b1, 0.99)
        res[f"{label}_p99_ms"] = round(p99 * 1000) if p99 is not None and p99 != float("inf") else None
    return res


# ---------------------------------------------------------------- сценарии
def sc_coverage(modes, src_factory, workdir, out):
    """Холодный старт: за сколько секунд все включённые каналы GOOD, сколько BAD/пропущено, чем отличаются значения."""
    result, first_values = {}, {}
    for mode in modes:
        log(f"coverage [{mode}]")
        src = src_factory()
        src.start(mode)
        gw = Gateway(mode, src, workdir, poll_ms=1000)
        try:
            up = gw.wait_up()
            tail = Kafka_.tail(gw.topic)  # после старта шлюза: топик создаёт он, а не потребитель
            time.sleep(12)
            rows = tail.since(0)  # первый цикл публикует все теги
            tail.close()
            first, quality, ts = {}, {}, {}
            for t, key, body in rows:
                if key not in first:
                    first[key] = body.get("value")
                    quality[key] = body.get("quality")
                    ts[key] = t
            good = [k for k, q in quality.items() if q == "GOOD"]
            bad = [k for k, q in quality.items() if q != "GOOD"]
            t0 = min(ts.values()) if ts else 0
            t_all = max(ts[k] for k in good) if good else 0
            result[mode] = {
                "enabled_channels": ENABLED_CHANNELS, "published": len(first), "good": len(good), "bad": len(bad),
                "link_up_s": round(up, 1), "first_to_last_good_ms": t_all - t0,
                "bad_examples": sorted(bad)[:8],
            }
            first_values[mode] = {k: first[k] for k in good}
            note(f"{mode}: GOOD {len(good)} из {ENABLED_CHANNELS}, BAD {len(bad)}, нет сообщений {ENABLED_CHANNELS - len(first)}")
        finally:
            gw.stop()
            src.stop()
    if len(modes) == 2:
        a, b = first_values[modes[0]], first_values[modes[1]]
        common = sorted(set(a) & set(b))
        diff = []
        for k in common:
            x, y = a[k], b[k]
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                same = abs(x - y) <= 1e-4 * max(abs(x), abs(y), 1.0)
            else:
                same = x == y
            if not same:
                diff.append((k, x, y))
        result["fidelity"] = {
            "compared": len(common), "equal": len(common) - len(diff), "different": len(diff),
            "only_" + modes[0]: len(set(a) - set(b)), "only_" + modes[1]: len(set(b) - set(a)),
            "different_examples": [{"tag": k, modes[0]: x, modes[1]: y} for k, x, y in diff[:12]],
        }
    out["coverage"] = result


def sc_load(modes, src_factory, workdir, out, seconds, warmup):
    result = {}
    for mode in modes:
        for poll in (1000, 100):
            log(f"load [{mode}, опрос {poll} мс]")
            src = src_factory()
            src.start(mode)
            gw = Gateway(mode, src, workdir, poll_ms=poll, tag=f"-load{poll}")
            try:
                gw.wait_up()
                res = measure([gw], src, seconds, warmup)
                result[f"{mode}@{poll}ms"] = res
                note(json.dumps(res, ensure_ascii=False))
            finally:
                gw.stop()
                src.stop()
    out["load"] = result


def sc_clients(modes, src_factory, workdir, out, seconds, warmup):
    result = {}
    for mode in modes:
        for n in (1, 2, 4, 8):
            log(f"clients [{mode}, шлюзов: {n}]")
            src = src_factory()
            src.start(mode)
            gws = [Gateway(mode, src, workdir, poll_ms=100, tag=f"-c{n}-{i}") for i in range(n)]
            try:
                for g in gws:
                    g.wait_up()
                res = measure(gws, src, seconds, warmup)
                res["gateways"] = n
                result[f"{mode}@{n}"] = res
                note(json.dumps({k: res[k] for k in ("gateways", "source_cores", "gateway_cores")}, ensure_ascii=False))
            finally:
                for g in gws:
                    g.stop()
                src.stop()
    out["clients"] = result


WRITE_TAGS = {
    # Держится: параметр, который прошивка принимает и хранит.
    "effective": "Барановичи-1.BN1_MCA1.LINE1.MAIN_CIP_MODULE.PAR_MAIN[7]",
}


def find_tag(device, field, station_text):
    """Имя канала по прибору и полю (для записи): первый включённый и записываемый."""
    for line in station_text.splitlines():
        if f'deviceName: "{device}", fieldName: "{field}"' in line and "enabled: true" in line and "writable: true" in line:
            m = re.search(r'name: "([^"]+)"', line)
            if m:
                return m.group(1)
    return None


def sc_writes(modes, src_factory, workdir, out, verify_ms, poll_ms=1000, samples=6, result_key="writes"):
    """Запись: статус, время до эффекта в телеметрии и поведение при «принял, но не применил» (сторожевой LINE1WATCHDOG1.P_T_ERR-подобные
    поля прошивка считает сама; берём заведомо игнорируемое поле LT1.CLEVEL — уровень в баке)."""
    result = {}
    text = STATIONS["opcua"].read_text(encoding="utf-8")
    effective = WRITE_TAGS["effective"]
    ignored = find_tag("LT1", "CLEVEL", text)  # уровень в баке: считает программа, прошивка принимает запись (код 0) и не меняет
    note(f"эффективный тег: {effective}; игнорируемый: {ignored}")
    for mode in modes:
        log(f"writes [{mode}]")
        src = src_factory()
        src.start(mode)
        gw = Gateway(mode, src, workdir, poll_ms=poll_ms, verify_ms=verify_ms, tag=f"-w{poll_ms}")
        res = {"effective": [], "ignored": []}
        tails = []
        try:
            gw.wait_up()
            gw.wait_commands()
            tags_t, res_t, cmd_t = (Kafka_.tail(x) for x in (gw.topic, f"{gw.prefix}.res", f"{gw.prefix}.cmd"))
            tails = [tags_t, res_t, cmd_t]
            time.sleep(4)
            for kind, tag in (("effective", effective), ("ignored", ignored)):
                if not tag:
                    continue
                for i in range(samples):
                    value = 10 + i * 3  # целое: у части полей прошивка хранит целое и дробную часть отбросила бы
                    cmd_id = f"cmp-{mode}-{kind}-{i}-{int(time.time() * 1000)}"
                    body = json.dumps({"commandId": cmd_id, "tagName": tag, "value": value, "requestedBy": "compare",
                                       "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())})
                    marks = (tags_t.mark(), res_t.mark(), cmd_t.mark())
                    Kafka_.produce(f"{gw.prefix}.cmd", [f"{tag}|{body}"])
                    status = t_result = t_effect = t_send = None
                    deadline = time.time() + 14
                    while time.time() < deadline and (status is None or (kind == "effective" and t_effect is None)):
                        if t_send is None:
                            for t, key, b in cmd_t.since(marks[2]):
                                if b.get("commandId") == cmd_id:
                                    t_send = t
                        for t, key, b in res_t.since(marks[1]):
                            if b.get("commandId") == cmd_id and status is None:
                                status, t_result = b.get("status"), t
                        if kind == "effective" and t_effect is None:
                            for t, key, b in tags_t.since(marks[0]):
                                if key == tag and isinstance(b.get("value"), (int, float)) and abs(b["value"] - value) < 1e-3:
                                    t_effect = t
                                    break
                        time.sleep(0.05)
                    base = t_send or 0
                    res[kind].append({"status": status, "result_ms": (t_result - base) if t_result and base else None,
                                      "effect_ms": (t_effect - base) if t_effect and base else None})
                    time.sleep(1.0)
            note(json.dumps({k: [(r["status"], r["result_ms"], r["effect_ms"]) for r in v] for k, v in res.items()}, ensure_ascii=False))
        finally:
            for t in tails:
                t.close()
            gw.stop()
            src.stop()
        result[mode] = res
    out[result_key] = result


def sc_recovery(modes, src_factory, workdir, out):
    result = {}
    for mode in modes:
        log(f"recovery [{mode}]")
        src = src_factory()
        src.start(mode)
        gw = Gateway(mode, src, workdir, poll_ms=1000, tag="-r")
        tail = None
        try:
            gw.wait_up()
            tail = Kafka_.tail(f"{gw.prefix}.tags")
            time.sleep(8)
            t_cut = time.time()
            mark = tail.mark()
            src.stop_plc()
            seen, bad_after = set(), None
            deadline = time.time() + 90
            while time.time() < deadline:
                seen |= {key for t, key, b in tail.since(mark) if t >= t_cut * 1000 and b.get("quality") == "BAD"}
                if len(seen) >= ENABLED_CHANNELS * 0.9:
                    bad_after = time.time() - t_cut
                    break
                time.sleep(0.2)
            note(f"{mode}: BAD у {len(seen)} каналов; обнаружено за {bad_after}")
            t_back = time.time()
            mark = tail.mark()
            src.start_plc()
            good, recovered = set(), None
            deadline = time.time() + 120
            while time.time() < deadline:
                good |= {key for t, key, b in tail.since(mark) if t >= t_back * 1000 and b.get("quality") == "GOOD"}
                if len(good) >= ENABLED_CHANNELS * 0.9:
                    recovered = time.time() - t_back
                    break
                time.sleep(0.2)
            note(f"{mode}: GOOD у {len(good)} каналов; восстановлено за {recovered}")
            result[mode] = {"detected_bad_s": round(bad_after, 1) if bad_after else None, "bad_channels": len(seen),
                            "recovered_good_s": round(recovered, 1) if recovered else None, "good_channels": len(good)}
        finally:
            if tail:
                tail.close()
            gw.stop()
            src.stop()
    out["recovery"] = result


# ---------------------------------------------------------------- таблицы
def tables(out):
    lines = []
    cov = out.get("coverage")
    if cov:
        lines += ["### Охват и точность", "", "| | включено каналов | получили GOOD | BAD | старт до связи, с |", "|---|---|---|---|---|"]
        for m in ("easydriver", "opcua"):
            if m in cov:
                c = cov[m]
                lines.append(f"| {m} | {c['enabled_channels']} | {c['good']} | {c['bad']} | {c['link_up_s']} |")
        if "fidelity" in cov:
            f = cov["fidelity"]
            lines += ["", f"Значения: сравнено {f['compared']}, совпало {f['equal']}, различаются {f['different']}.", ""]
    load = out.get("load")
    if load:
        lines += ["### Нагрузка", "", "| режим, опрос | ядер шлюза | память шлюза, МБ | ядер источника | цикл средний, мс | цикл p99, мс | пропущено циклов |",
                  "|---|---|---|---|---|---|---|"]
        for k, v in load.items():
            lines.append(f"| {k} | {v['gateway_cores']} | {v['gateway_rss_mb']} | {v['source_cores']} | {v['cycle_mean_ms']} | {v['cycle_p99_ms']} | {v['poll_overruns']} |")
        lines.append("")
    return "\n".join(lines)


Kafka_ = Kafka()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["firmware", "sim"], default="firmware")
    ap.add_argument("--scenarios", default="coverage,load,writes")
    ap.add_argument("--modes", default="easydriver,opcua")
    ap.add_argument("--out", default=str(ROOT / "loadtest/results/protocols/latest"))
    ap.add_argument("--project", default=os.environ.get("MOIKA_PROJECT", str(ROOT / "../Moika/BN1-CIP1")))
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--verify-ms", type=int, default=800)
    ap.add_argument("--facade-poll-ms", default="500", help="период опроса прошивки OPC UA-фасадом, мс (режим opcua, firmware)")
    ap.add_argument("--suffix", default="", help="добавить к ключам результата (например, _facade100), чтобы не затирать прежние")
    ap.add_argument("--merge", action="store_true", help="дополнить уже сохранённый результат источника новыми сценариями")
    a = ap.parse_args()
    global FACADE_POLL_MS
    FACADE_POLL_MS = a.facade_poll_ms

    for b in (GATEWAY, SIMULATOR, FACADE):
        if not b.exists():
            sys.exit(f"нет {b}: соберите release (cargo build --release; в simulator/ и ptusa-opcua/ — тоже)")
    outdir = Path(a.out)
    outdir.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="cmp-"))
    modes = a.modes.split(",")
    scenarios = a.scenarios.split(",")
    src_factory = (lambda: Firmware(a.project, workdir)) if a.source == "firmware" else (lambda: Simulator(workdir))
    out = {"source": a.source, "date": time.strftime("%Y-%m-%d"), "modes": modes}
    previous = outdir / f"{a.source}.json"
    if a.merge and previous.exists():
        out = {**json.loads(previous.read_text(encoding="utf-8")), "modes": modes}
    previous_values = {k: out[k] for k in scenarios if k in out}  # для --suffix: что было до этого прогона
    log("Kafka")
    Kafka_.start()
    try:
        if "coverage" in scenarios:
            sc_coverage(modes, src_factory, workdir, out)
        if "load" in scenarios:
            sc_load(modes, src_factory, workdir, out, a.seconds, a.warmup)
        if "clients" in scenarios:
            sc_clients(modes, src_factory, workdir, out, 30, 10)
        if "writes" in scenarios:
            sc_writes(modes, src_factory, workdir, out, a.verify_ms)
        if "writes_fast" in scenarios:  # опрос 100 мс: время до эффекта почти не зависит от периода опроса шлюза
            sc_writes(modes, src_factory, workdir, out, a.verify_ms, poll_ms=100, samples=12, result_key="writes_fast")
        if "recovery" in scenarios:
            sc_recovery(modes, src_factory, workdir, out)
        if a.suffix:
            # Новые результаты уходят под ключ с суффиксом, а прежние (из --merge) остаются на своих местах.
            for key in scenarios:
                if key in out:
                    out[key + a.suffix] = out[key]
                    if key in previous_values:
                        out[key] = previous_values[key]
                    else:
                        del out[key]
    finally:
        Kafka_.stop()
        if a.source == "firmware":
            subprocess.run(["docker", "rm", "-f", PTUSA], capture_output=True)
        (outdir / f"{a.source}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(tables(out))
    log(f"Результат: {outdir / (a.source + '.json')}")


if __name__ == "__main__":
    main()
