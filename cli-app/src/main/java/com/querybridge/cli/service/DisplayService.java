package com.querybridge.cli.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DisplayService {

    public String formatSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "";
        }

        String[] lines = sql.trim().split("\\R");
        int maxWidth = 0;
        for (String line : lines) {
            maxWidth = Math.max(maxWidth, line.length());
        }
        maxWidth = Math.max(maxWidth, 3);

        StringBuilder sb = new StringBuilder();
        sb.append("┌").append("─".repeat(maxWidth + 2)).append("┐\n");

        for (String line : lines) {
            sb.append("│ ").append(padRight(line, maxWidth)).append(" │\n");
        }

        sb.append("└").append("─".repeat(maxWidth + 2)).append("┘");
        return sb.toString();
    }

    public String formatTable(List<String> columns, List<List<String>> rows) {
        if (columns == null || columns.isEmpty()) {
            return "(no columns)";
        }

        List<String> safeColumns = columns.stream()
                .map(c -> c == null ? "" : c)
                .toList();

        List<List<String>> safeRows = rows == null ? List.of() : rows.stream()
                .map(row -> {
                    List<String> normalized = new ArrayList<>();
                    for (int i = 0; i < safeColumns.size(); i++) {
                        if (row != null && i < row.size() && row.get(i) != null) {
                            normalized.add(row.get(i));
                        } else {
                            normalized.add("");
                        }
                    }
                    return normalized;
                })
                .toList();

        int[] widths = new int[safeColumns.size()];
        for (int i = 0; i < safeColumns.size(); i++) {
            widths[i] = safeColumns.get(i).length();
        }
        for (List<String> row : safeRows) {
            for (int i = 0; i < safeColumns.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(buildRow(safeColumns, widths)).append("\n");
        sb.append(buildSeparator(widths)).append("\n");

        for (List<String> row : safeRows) {
            sb.append(buildRow(row, widths)).append("\n");
        }

        if (safeRows.isEmpty()) {
            sb.append("(0 rows)");
        } else {
            sb.setLength(sb.length() - 1);
            sb.append("\n(").append(safeRows.size()).append(" row")
                    .append(safeRows.size() == 1 ? "" : "s").append(")");
        }

        return sb.toString();
    }

    public String formatExplanation(String explanation) {
        if (explanation == null || explanation.isBlank()) {
            return "";
        }

        int wrapWidth = 72;
        String[] words = explanation.trim().split("\\s+");
        StringBuilder sb = new StringBuilder();
        StringBuilder line = new StringBuilder();

        for (String word : words) {
            if (line.isEmpty()) {
                line.append(word);
            } else if (line.length() + 1 + word.length() <= wrapWidth) {
                line.append(' ').append(word);
            } else {
                sb.append(line).append('\n');
                line = new StringBuilder(word);
            }
        }

        if (!line.isEmpty()) {
            sb.append(line);
        }

        return sb.toString();
    }

    private String buildRow(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < cells.size(); i++) {
            sb.append(' ').append(padRight(cells.get(i), widths[i])).append(" |");
        }
        return sb.toString();
    }

    private String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append("|");
        }
        return sb.toString();
    }

    private String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }
}
