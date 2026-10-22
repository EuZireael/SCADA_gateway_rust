import com.example.channel.importer.CdbxChannel;
import com.example.channel.importer.CdbxFile;
import com.example.channel.importer.CdbxParser;
import com.example.channel.importer.DataTypeResolver;
import com.example.channel.importer.ObjectPathMapper;
import com.example.channel.importer.PlcProject;
import com.example.channel.importer.PlcProjectParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * controllers.yaml реальной станции из её базы каналов (.cdbx) и проекта ПЛК ptusa
 * (main.io.lua, main.objects.lua — выгрузка EasyEPLANner).
 *
 * <p>Имя канала (Kafka-key) строится КОДОМ МОНИТОРА: этот файл компилируется вместе с
 * исходниками импорта scada-editor-backend (CdbxParser, PlcProjectParser, ObjectPathMapper,
 * DataTypeResolver) и повторяет CdbxImportService — ключи совпадают с объектной базой каналов
 * монитора по построению. Адрес в ПЛК (deviceName.fieldName) — «Имя в ПЛК» из .cdbx, как у
 * GatewayExportService. Запуск — tools/station_config.sh.
 *
 * <p>Отличия от выгрузки монитора (GET /export/gateway):
 * <ul>
 *   <li>writable — по умолчанию (-Dwritable=all) ВСЕ числовые каналы: требование станции —
 *       любой тег можно менять; проверено на прошивке, что она принимает запись в каждый из них.
 *       Исключение — строки (set_cmd принимает только числа). -Dwritable=rules — по правилам
 *       writable-rules.tsv (команды и уставки), показания датчиков только чтение;</li>
 *   <li>enabled — из .cdbx (выключенные каналы не опрашиваются);</li>
 *   <li>nodeId — по имени в ПЛК: для OPC UA {@code ns=2;s=<прибор>.<поле>} (адресное
 *       пространство OPC UA-фасада эмулятора, ptusa-opcua/), для PAC {@code pac:<имя в ПЛК>};
 *       channelId не задаётся (это id узла в БД монитора, вне её не известен).</li>
 * </ul>
 *
 * <p>Протокол по умолчанию — OPC UA (все каналы станции через OPC UA-сервер); PAC — прямое
 * подключение шлюза к ptusa по driver-master.
 *
 * <p>args: cdbx, папка проекта ПЛК, площадка, проект, id контроллера, endpoint, rules.tsv,
 * выходной yaml, протокол (opcua|pac).
 */
public class StationConfig {

    /** Индекс массива; в новой базе каналов бывает с пробелами: ST_CH[ 1 ]. */
    private static final Pattern INDEX = Pattern.compile("\\[\\s*(\\d+)\\s*\\]");
    /** -Dwritable=all (по умолчанию): все числовые каналы на запись; rules — по writable-rules.tsv. */
    private static final boolean WRITE_ALL = !"rules".equals(System.getProperty("writable", "all"));
    private static final Pattern DEVICE_KIND = Pattern.compile("^[A-Z_]+");

    public static void main(String[] args) throws IOException {
        if (args.length != 9 || !List.of("opcua", "pac").contains(args[8])) {
            System.err.println("args: <cdbx> <project-dir> <site> <project> <controller-id> <endpoint> <rules.tsv> <out.yaml> <opcua|pac>");
            System.exit(2);
        }
        Path cdbx = Path.of(args[0]);
        Path projectDir = Path.of(args[1]);
        String site = args[2], project = args[3], controllerId = args[4], endpoint = args[5];
        Rules rules = Rules.load(Path.of(args[6]));
        Overrides overrides = Overrides.load(Path.of(args[6]).resolveSibling("overrides.tsv"));
        Path out = Path.of(args[7]);
        boolean opcua = "opcua".equals(args[8]);

        CdbxFile file = CdbxParser.parse(Files.readAllBytes(cdbx));
        PlcProject plc = PlcProjectParser.parse(read(projectDir.resolve("main.io.lua")),
                read(projectDir.resolve("main.objects.lua")));

        // --- Как CdbxImportService: путь = площадка.проект.<сегменты объекта>.поле ---
        String root = site + "." + project;
        Set<String> paths = new HashSet<>();
        List<String> merged = new ArrayList<>(), skipped = new ArrayList<>();
        Set<String> unmapped = new LinkedHashSet<>();
        Map<String, Integer> unknownRule = new TreeMap<>();
        StringBuilder tags = new StringBuilder();
        int total = 0, enabled = 0, writable = 0, limited = 0;
        Map<String, Integer> byType = new TreeMap<>();
        Map<String, Integer> offNotInProject = new TreeMap<>();
        Map<String, Integer> offByOverride = new TreeMap<>();
        for (CdbxChannel ch : file.channels()) {
            ObjectPathMapper.LegacyName legacy = ObjectPathMapper.split(ch.name()).orElse(null);
            if (legacy == null) {
                skipped.add(ch.name());
                continue;
            }
            if (!ObjectPathMapper.known(legacy.object(), plc)) unmapped.add(legacy.object());
            String parent = root;
            for (String segment : ObjectPathMapper.objectSegments(legacy.object(), plc)) parent += "." + segment;
            String path = parent + "." + legacy.field();
            if (!paths.add(path)) {
                merged.add(ch.name());
                continue;
            }
            String kind = kind(legacy.object(), plc);
            String dataType = overrides.type(kind, legacy.field())
                    .orElse(DataTypeResolver.resolve(legacy.field()).type());
            Boolean rw = overrides.rw(kind, legacy.field()).orElse(rules.writable(kind, legacy.field()));
            if (rw == null) {
                unknownRule.merge(kind + "\t" + Rules.fieldKey(legacy.field()), 1, Integer::sum);
                rw = false; // нет правила — только чтение (безопасно)
            }
            if (WRITE_ALL) rw = !"STRING".equals(dataType);
            String limits = overrides.limits(kind, legacy.field());
            if (limits != null && "STRING".equals(dataType)) {
                throw new IllegalArgumentException("overrides.tsv: пределы записи у строкового канала " + ch.name());
            }
            boolean on = !"0".equals(ch.enabled().trim());
            // Объекта нет в проекте ПЛК (OBJECT4 при трёх техобъектах, прибор со старым именем) —
            // значения не будет никогда: канал выключен. SYSTEM — таблица самой ptusa, есть всегда.
            if (on && !ObjectPathMapper.known(legacy.object(), plc) && !"SYSTEM".equals(legacy.object())) {
                on = false;
                offNotInProject.merge(legacy.object(), 1, Integer::sum);
            }
            String reason = overrides.disabled(ch.name());
            if (on && reason != null) {
                on = false;
                offByOverride.merge(reason, 1, Integer::sum);
            }
            total++;
            if (on) enabled++;
            if (rw) writable++;
            if (limits != null) limited++;
            byType.merge(dataType, 1, Integer::sum);
            String nodeId = opcua ? "ns=2;s=" + opcNodeName(legacy.object(), legacy.field()) : "pac:" + ch.name();
            tags.append("        - {name: \"").append(q(path))
                    .append("\", nodeId: \"").append(q(nodeId))
                    .append("\", deviceName: \"").append(q(legacy.object()))
                    .append("\", fieldName: \"").append(q(legacy.field()))
                    .append("\", deviceType: \"").append(q(kind))
                    .append("\", protocol: ").append(opcua ? "opcua" : "pac").append(", dataType: ").append(dataType)
                    .append(", pollingRate: 1000, enabled: ").append(on)
                    .append(", writable: ").append(rw).append(limits == null ? "" : limits).append("}\n");
        }

        String yaml = "# ============================================================================\n"
                + "# Реальная станция " + project + " (" + site + "): один контроллер ptusa, все каналы — "
                + (opcua ? "OPC UA\n#   (OPC UA-фасад эмулятора ptusa-opcua/: узел ns=2;s=<прибор>.<поле>).\n" : "PAC (driver-master).\n")
                + "# СГЕНЕРИРОВАНО tools/station_config.sh — не править руками.\n"
                + "#   база каналов: " + cdbx.getFileName() + ", проект ПЛК: " + projectDir.getFileName() + "\n"
                + "#   имена (Kafka-key) — как у объектной базы каналов монитора (его код импорта .cdbx);\n"
                + (WRITE_ALL ? "#   writable — все числовые каналы (строки — только чтение: set_cmd принимает числа).\n"
                             : "#   writable — tools/station-config/writable-rules.tsv.\n")
                + (opcua ? "# Адрес OPC UA-сервера — env PLC_HOST (IP объекта в публичный репозиторий не пишется).\n"
                         : "# Адрес контроллера — env PAC_HOST (в публичный репозиторий не пишется).\n")
                + "#   writeMin/writeMax — tools/station-config/overrides.tsv (limits).\n"
                + "#   каналов " + total + ", включено " + enabled + ", на запись " + writable + ", с пределами записи " + limited
                + "; типы " + byType + "\n"
                + "# ============================================================================\n"
                + "opcua:\n"
                + "  servers:\n"
                + "    - id: " + controllerId + "\n"
                + "      name: \"" + q(root) + "\"\n"
                + "      endpoint: \"" + q(endpoint) + "\"\n"
                + "      enabled: true\n"
                + "      tags:\n" + tags;
        Files.writeString(out, yaml);

        System.out.printf("Каналов %d (включено %d, на запись %d, с пределами записи %d), типы %s → %s%n",
                total, enabled, writable, limited, byType, out);
        if (!merged.isEmpty()) System.out.println("Слиты (одинаковый путь, как у монитора): " + merged.size() + " " + head(merged));
        if (!skipped.isEmpty()) System.out.println("Пропущены (имя без точки): " + skipped.size() + " " + head(skipped));
        if (!unmapped.isEmpty()) System.out.println("Нет в проекте ПЛК (путь по эвристике): " + unmapped.size() + " " + head(new ArrayList<>(unmapped)));
        if (!offNotInProject.isEmpty()) System.out.println("Выключено — объекта нет в проекте ПЛК: " + offNotInProject);
        if (!offByOverride.isEmpty()) System.out.println("Выключено по overrides.tsv: " + offByOverride);
        if (!unknownRule.isEmpty()) {
            System.out.println("Нет правила записи — только чтение (" + unknownRule.size() + " видов поля):");
            unknownRule.forEach((k, n) -> System.out.println("  " + k + "\t×" + n));
        }
    }

    /**
     * Имя узла OPC UA по имени в ПЛК: {@code LINE1V0.ST}, {@code OBJECT1.RT_PAR_F[12]},
     * {@code LINE1G1.ST_CH[1]} — пробелы внутри индекса (так в базе каналов) убираются. Так же
     * узлы называет OPC UA-фасад эмулятора (ptusa-opcua/).
     */
    static String opcNodeName(String object, String field) {
        return object + "." + INDEX.matcher(field).replaceAll("[$1]");
    }

    /**
     * Вид объекта для правил записи: буквенный тип прибора (V, VC, M, TE…), OBJECT —
     * технологический объект (OBJECTn), иначе имя целиком (SYSTEM).
     */
    static String kind(String object, PlcProject plc) {
        var device = plc.device(object);
        if (device.isPresent()) {
            Matcher m = DEVICE_KIND.matcher(device.get().device());
            return m.find() ? m.group() : object;
        }
        if (object.matches("OBJECT\\d+")) return "OBJECT";
        return ObjectPathMapper.deviceType(object, plc);
    }

    private static byte[] read(Path p) throws IOException {
        return Files.exists(p) ? Files.readAllBytes(p) : new byte[0];
    }

    private static String q(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String head(List<String> list) {
        return list.subList(0, Math.min(8, list.size())) + (list.size() > 8 ? "…" : "");
    }

    /**
     * overrides.tsv — точечные поправки с обоснованием, сверенные с настоящей прошивкой:
     * <pre>
     *   type     вид   поле        ТИП          # тип, который DataTypeResolver угадал неверно
     *   access   вид   поле        RW|RO        # доступ, которого нет в writable-rules.tsv
     *   limits   вид   поле        MIN|- MAX|-  # пределы значения команды (writeMin/writeMax), включительно
     *   disable  маска имени в ПЛК  причина…     # поля нет у прибора в прошивке
     * </pre>
     */
    record Overrides(Map<String, String> types, Map<String, Boolean> rw, Map<String, String> limits,
                     Map<Pattern, String> disabled) {
        static Overrides load(Path tsv) throws IOException {
            Map<String, String> types = new LinkedHashMap<>();
            Map<String, Boolean> rw = new LinkedHashMap<>();
            Map<String, String> limits = new LinkedHashMap<>();
            Map<Pattern, String> disabled = new LinkedHashMap<>();
            if (!Files.exists(tsv)) return new Overrides(types, rw, limits, disabled);
            for (String line : Files.readAllLines(tsv)) {
                String l = line.strip();
                if (l.isEmpty() || l.startsWith("#")) continue;
                String[] c = l.split("\\s+", 3);
                switch (c[0]) {
                    case "type" -> {
                        String[] t = l.split("\\s+");
                        types.put(t[1] + "\t" + t[2], t[3]);
                    }
                    case "access" -> {
                        String[] t = l.split("\\s+");
                        rw.put(t[1] + "\t" + t[2], "RW".equals(t[3]));
                    }
                    case "limits" -> {
                        String[] t = l.split("\\s+");
                        if (t.length < 5) throw new IllegalArgumentException("overrides.tsv: limits <вид> <поле> <MIN|-> <MAX|->: " + line);
                        Double lo = bound(t[3], line), hi = bound(t[4], line);
                        if (lo == null && hi == null || lo != null && hi != null && lo > hi) {
                            throw new IllegalArgumentException("overrides.tsv: пределы не заданы или MIN > MAX: " + line);
                        }
                        limits.put(t[1] + "\t" + t[2], (lo == null ? "" : ", writeMin: " + t[3]) + (hi == null ? "" : ", writeMax: " + t[4]));
                    }
                    case "disable" -> disabled.put(glob(c[1]), c.length > 2 ? c[2] : "overrides.tsv");
                    default -> throw new IllegalArgumentException("overrides.tsv: " + line);
                }
            }
            return new Overrides(types, rw, limits, disabled);
        }

        /** Предел из overrides.tsv: десятичное число (в YAML пишется как есть) или «-» (не задан). */
        private static Double bound(String text, String line) {
            if ("-".equals(text)) return null;
            if (!text.matches("-?\\d+(\\.\\d+)?")) throw new IllegalArgumentException("overrides.tsv: предел не число: " + line);
            return Double.parseDouble(text);
        }

        /** Фрагмент YAML {@code , writeMin: …, writeMax: …} для канала или null. */
        String limits(String kind, String field) {
            return limits.get(kind + "\t" + Rules.fieldKey(field));
        }

        java.util.Optional<Boolean> rw(String kind, String field) {
            return java.util.Optional.ofNullable(rw.get(kind + "\t" + Rules.fieldKey(field)));
        }

        java.util.Optional<String> type(String kind, String field) {
            return java.util.Optional.ofNullable(types.get(kind + "\t" + Rules.fieldKey(field)));
        }

        String disabled(String plcName) {
            for (var e : disabled.entrySet()) if (e.getKey().matcher(plcName).matches()) return e.getValue();
            return null;
        }

        /** Маска: * — любые символы, ? — один, остальное буквально (скобки массивов тоже). */
        private static Pattern glob(String g) {
            StringBuilder re = new StringBuilder();
            for (char ch : g.toCharArray()) {
                re.append(ch == '*' ? ".*" : ch == '?' ? "." : Pattern.quote(String.valueOf(ch)));
            }
            return Pattern.compile(re.toString());
        }
    }

    /** writable-rules.tsv: вид объекта, поле (массив — без индекса, RT_PAR_F — с индексом), RW|RO. */
    record Rules(Map<String, Boolean> byKey) {
        static Rules load(Path tsv) throws IOException {
            Map<String, Boolean> map = new LinkedHashMap<>();
            for (String line : Files.readAllLines(tsv)) {
                String l = line.strip();
                if (l.isEmpty() || l.startsWith("#")) continue;
                String[] c = l.split("\\s+");
                if (c.length < 3) throw new IllegalArgumentException("writable-rules.tsv: " + line);
                map.put(c[0] + "\t" + c[1], "RW".equals(c[2]));
            }
            return new Rules(map);
        }

        Boolean writable(String kind, String field) {
            return byKey.get(kind + "\t" + fieldKey(field));
        }

        /** RT_PAR_F[12] — индекс значим (у линии это разные параметры); PAR_MAIN[3] → PAR_MAIN. */
        static String fieldKey(String field) {
            Matcher m = INDEX.matcher(field);
            if (m.find()) {
                String base = field.substring(0, m.start());
                return base.equals("RT_PAR_F") ? base + "[" + m.group(1) + "]" : base;
            }
            return field;
        }
    }
}
