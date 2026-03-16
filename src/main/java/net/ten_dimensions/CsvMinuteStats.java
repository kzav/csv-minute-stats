package net.ten_dimensions;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.stream.*;
 
/**
 * 日時分割されたCSVファイルを読み込み、
 * 第2列目以降の各項目の1分間あたりの min / max / avg を集計し、
 * 時間帯（HH）ごとに別ファイルとして出力するプログラム
 *
 * 使い方:
 *   java CsvMinuteStats <入力ファイルまたはディレクトリ> [出力ディレクトリ]
 *
 * 例:
 *   java CsvMinuteStats ./split_csv/            (カレントディレクトリに出力)
 *   java CsvMinuteStats data_20240101.csv       (カレントディレクトリに出力)
 *   java CsvMinuteStats ./split_csv/ ./output/  (指定ディレクトリに出力)
 *
 * 出力ファイル名:
 *   minute_stats_yyyyMMdd_HH.csv  (例: minute_stats_20240101_09.csv)
 */
public class CsvMinuteStats {
 
    // ---- 設定 ---------------------------------------------------------------
    /** 入力CSVの日時フォーマット（必要に応じて変更してください） */
    private static final String[] DATE_FORMATS = {
        "MM/dd/yyyy HH:mm:ss.SSS",
        "MM/dd/yyyy HH:mm:ss",
        "MM/dd/yyyy HH:mm"
    };
 
    /** CSVの区切り文字 */
    private static final String DELIMITER = ",";
 
    /** 出力先デフォルトディレクトリ */
    private static final String DEFAULT_OUTPUT_DIR = ".";
 
    /** 出力ファイル名のプレフィックス */
    private static final String OUTPUT_PREFIX = "minute_stats_";
    // -------------------------------------------------------------------------
 
    public static void main(String[] args) throws Exception {
 
        if (args.length < 1) {
            System.out.println("使い方: java CsvMinuteStats <入力ファイルまたはディレクトリ> [出力ディレクトリ]");
            System.exit(1);
        }
 
        String inputPath = args[0];
        String outputDir = (args.length >= 2) ? args[1] : DEFAULT_OUTPUT_DIR;
 
        // 出力ディレクトリを作成
        Files.createDirectories(Paths.get(outputDir));
 
        // 入力CSVファイルの収集
        List<Path> csvFiles = collectCsvFiles(Paths.get(inputPath));
        if (csvFiles.isEmpty()) {
            System.out.println("CSVファイルが見つかりませんでした: " + inputPath);
            System.exit(1);
        }
        System.out.println("対象ファイル数: " + csvFiles.size());
 
        // データ読み込み・集計
        // hourKey  : "yyyyMMdd_HH"       時間帯ごとのファイル分割キー
        // minuteKey: "MM/dd/yyyy HH:mm"  分単位の集計キー
        // value    : 各列の値リスト
        TreeMap<String, TreeMap<String, List<List<Double>>>> hourMap = new TreeMap<>();
        List<String> headers = null; // 第2列以降のヘッダー
 
        for (Path file : csvFiles) {
            System.out.println("読み込み中: " + file.getFileName());
            try (BufferedReader br = Files.newBufferedReader(file)) {
                String line;
                boolean isFirstLine = true;
 
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
 
                    String[] cols = splitCsvLine(line);
                    if (cols.length < 2) continue;
 
                    // ヘッダー行の判定（数値でなければヘッダーとみなす）
                    if (isFirstLine && !isNumeric(cols[1])) {
                        if (headers == null) {
                            headers = new ArrayList<>();
                            for (int i = 1; i < cols.length; i++) {
                                headers.add(cols[i].trim());
                            }
                        }
                        isFirstLine = false;
                        continue;
                    }
                    isFirstLine = false;
 
                    // 日時を分単位キーと時間帯キーに変換
                    String[] keys = toKeys(cols[0].trim());
                    if (keys == null) {
                        System.err.println("  日時パース失敗 (スキップ): " + cols[0]);
                        continue;
                    }
                    String hourKey   = keys[0]; // "yyyyMMdd_HH"
                    String minuteKey = keys[1]; // "MM/dd/yyyy HH:mm"
 
                    // 時間帯マップ → 分マップ → 列値リスト に格納
                    TreeMap<String, List<List<Double>>> minuteMap =
                            hourMap.computeIfAbsent(hourKey, k -> new TreeMap<>());
 
                    List<List<Double>> colValues = minuteMap.computeIfAbsent(
                            minuteKey, k -> {
                                List<List<Double>> lists = new ArrayList<>();
                                for (int i = 1; i < cols.length; i++) lists.add(new ArrayList<>());
                                return lists;
                            });
 
                    for (int i = 1; i < cols.length; i++) {
                        int idx = i - 1;
                        while (colValues.size() <= idx) colValues.add(new ArrayList<>());
                        try {
                            colValues.get(idx).add(Double.parseDouble(cols[i].trim()));
                        } catch (NumberFormatException e) {
                            // 数値変換できない値はスキップ
                        }
                    }
                }
            }
        }
 
        if (hourMap.isEmpty()) {
            System.out.println("集計対象データがありませんでした。");
            System.exit(1);
        }
 
        // 全データから列数を確定
        int numDataCols = hourMap.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToInt(List::size).max().orElse(0);
 
        // ヘッダーが取得できなかった場合は Column1, Column2 … で補完
        if (headers == null) {
            headers = new ArrayList<>();
            for (int i = 0; i < numDataCols; i++) headers.add("Column" + (i + 1));
        }
        while (headers.size() < numDataCols) headers.add("Column" + (headers.size() + 1));
 
        // 時間帯ごとにファイル出力
        for (Map.Entry<String, TreeMap<String, List<List<Double>>>> entry : hourMap.entrySet()) {
            String hourKey    = entry.getKey(); // "yyyyMMdd_HH"
            String outputFile = Paths.get(outputDir, OUTPUT_PREFIX + hourKey + ".csv").toString();
            writeOutput(outputFile, headers, entry.getValue(), numDataCols);
            System.out.println("出力完了: " + outputFile);
        }
    }
 
    // =========================================================================
    //  ファイル収集
    // =========================================================================
    private static List<Path> collectCsvFiles(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            return Collections.singletonList(path);
        }
        if (Files.isDirectory(path)) {
            try (Stream<Path> stream = Files.list(path)) {
                return stream
                        .filter(p -> p.toString().toLowerCase().endsWith(".csv"))
                        .sorted()
                        .collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }
 
    // =========================================================================
    //  日時 → [時間帯キー "yyyyMMdd_HH", 分キー "MM/dd/yyyy HH:mm"] 変換
    // =========================================================================
    private static String[] toKeys(String dateStr) {
        for (String fmt : DATE_FORMATS) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fmt);
                LocalDateTime ldt = LocalDateTime.parse(dateStr, formatter);
                String hourKey   = ldt.format(DateTimeFormatter.ofPattern("yyyyMMdd_HH"));
                String minuteKey = ldt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm"));
                return new String[]{hourKey, minuteKey};
            } catch (DateTimeParseException ignored) {}
        }
        return null;
    }
 
    // =========================================================================
    //  出力（1時間帯 = 1ファイル）
    // =========================================================================
    private static void writeOutput(
            String outputPath,
            List<String> headers,
            TreeMap<String, List<List<Double>>> minuteMap,
            int numDataCols) throws IOException {
 
        try (PrintWriter pw = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputPath), "UTF-8")))) {
 
            // ---- ヘッダー行 ----
            StringBuilder headerLine = new StringBuilder("datetime");
            for (String h : headers.subList(0, numDataCols)) {
                headerLine.append(",").append(h).append("_min");
                headerLine.append(",").append(h).append("_max");
                headerLine.append(",").append(h).append("_avg");
            }
            pw.println(headerLine);
 
            // ---- データ行 ----
            for (Map.Entry<String, List<List<Double>>> row : minuteMap.entrySet()) {
                StringBuilder sb = new StringBuilder(row.getKey());
                List<List<Double>> colValues = row.getValue();
 
                for (int i = 0; i < numDataCols; i++) {
                    List<Double> vals = (i < colValues.size()) ? colValues.get(i) : Collections.emptyList();
                    if (vals.isEmpty()) {
                        sb.append(",,,");
                    } else {
                        double min = vals.stream().mapToDouble(Double::doubleValue).min().orElse(0);
                        double max = vals.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                        double avg = vals.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                        sb.append(String.format(",%.4f,%.4f,%.4f", min, max, avg));
                    }
                }
                pw.println(sb);
            }
        }
    }
 
    // =========================================================================
    //  ユーティリティ
    // =========================================================================
 
    /** クォートを考慮したCSV行分割 */
    private static String[] splitCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
            } else if (c == ',' && !inQuote) {
                result.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        result.add(cur.toString());
        return result.toArray(new String[0]);
    }
 
    /** 文字列が数値かどうか判定 */
    private static boolean isNumeric(String s) {
        try {
            Double.parseDouble(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}