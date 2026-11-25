package com.querybridge.cli.service;   // CLI services

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

// ============================================================================
// DisplayService — pure text formatting for the terminal. (guide step 33)
//
// No Spring, Kafka or database logic at all: strings in, strings out.
// Three public methods:
//
//   formatSql(sql)               -> SQL inside a box drawn with Unicode
//                                   box-drawing characters:
//                                     ┌──────────────────────────┐
//                                     │ SELECT name              │
//                                     │ FROM products            │
//                                     └──────────────────────────┘
//
//   formatTable(columns, rows)   -> a Markdown-style ASCII table:
//                                     | name | price |
//                                     |------|-------|
//                                     | Lamp | 19.99 |
//                                     (1 row)
//
//   formatExplanation(text)      -> the LLM's explanation, word-wrapped at 72
//
// GENERAL TECHNIQUE (worth learning — it's how every text table is drawn)
//   1. Find the WIDTH of each column = the longest string in it.
//   2. Pad every cell with spaces on the right up to that width (padRight).
//   3. Glue cells together with separators.
//
// A LIMITATION: String.length() counts Java chars (UTF-16 code units), not
// how many terminal columns a character occupies. Emoji and CJK characters
// are typically 2 columns wide in a terminal, so tables containing them will
// look misaligned. Fine for typical data.
// ============================================================================
@Service
public class DisplayService {

    // ------------------------------------------------------------------------
    // formatSql(sql) — draw a box around (possibly multi-line) SQL.
    // ------------------------------------------------------------------------
    public String formatSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "";   // nothing to draw
        }

        // Split into lines. "\\R" is a regex meaning "any line break" (\n, \r\n,
        // \r, ...) — more robust than splitting on "\n" alone. (Written "\\R" in
        // Java source because the backslash itself must be escaped.)
        String[] lines = sql.trim().split("\\R");
        int maxWidth = 0;
        for (String line : lines) {
            maxWidth = Math.max(maxWidth, line.length());   // widest line decides the box width
        }
        maxWidth = Math.max(maxWidth, 3);                   // never draw a ridiculously thin box

        StringBuilder sb = new StringBuilder();
        // Top border: ┌ + (width + 2) dashes + ┐. The "+2" accounts for the one
        // space of padding on each side of the text.
        sb.append("┌").append("─".repeat(maxWidth + 2)).append("┐\n");

        for (String line : lines) {
            // Each line: "│ " + text padded to maxWidth + " │"
            sb.append("│ ").append(padRight(line, maxWidth)).append(" │\n");
        }

        sb.append("└").append("─".repeat(maxWidth + 2)).append("┘");   // bottom border (no trailing newline)
        return sb.toString();
        // Note: LLM SQL often comes back as ONE long line; then you get one very
        // wide box that may wrap in a narrow terminal. No pretty-printing is done.
    }

    // ------------------------------------------------------------------------
    // formatTable(columns, rows) — draw query results.
    //
    // Steps:
    //   1. Make the data "safe": no null column names, no null cells, and every
    //      row exactly as long as the header (pad short rows with "").
    //   2. Compute column widths (header and every cell).
    //   3. Print header, separator, rows, and a "(N rows)" footer.
    // ------------------------------------------------------------------------
    public String formatTable(List<String> columns, List<List<String>> rows) {
        if (columns == null || columns.isEmpty()) {
            return "(no columns)";
        }

        // Step 1a: null-safe column names.
        List<String> safeColumns = columns.stream()
                .map(c -> c == null ? "" : c)   // null -> ""
                .toList();

        // Step 1b: null-safe, rectangular rows.
        List<List<String>> safeRows = rows == null ? List.of() : rows.stream()
                .map(row -> {
                    List<String> normalized = new ArrayList<>();
                    for (int i = 0; i < safeColumns.size(); i++) {        // exactly one cell per column
                        if (row != null && i < row.size() && row.get(i) != null) {
                            normalized.add(row.get(i));                    // real value
                        } else {
                            normalized.add("");                            // missing/null -> empty cell
                        }
                    }
                    return normalized;
                })
                .toList();
        // (SQL NULLs already arrive as the TEXT "NULL" from the executor, so they
        //  print as NULL — only truly missing cells become blank.)

        // Step 2: column widths. Start with header widths...
        int[] widths = new int[safeColumns.size()];
        for (int i = 0; i < safeColumns.size(); i++) {
            widths[i] = safeColumns.get(i).length();
        }
        // ...then widen for any longer cell.
        for (List<String> row : safeRows) {
            for (int i = 0; i < safeColumns.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        // (A single 2000-character text cell makes that whole column 2000 wide.
        //  No truncation is done.)

        // Step 3: assemble.
        StringBuilder sb = new StringBuilder();
        sb.append(buildRow(safeColumns, widths)).append("\n");   // | name | price |
        sb.append(buildSeparator(widths)).append("\n");          // |------|-------|

        for (List<String> row : safeRows) {
            sb.append(buildRow(row, widths)).append("\n");       // | Lamp | 19.99 |
        }

        if (safeRows.isEmpty()) {
            sb.append("(0 rows)");   // header + separator + "(0 rows)"
        } else {
            // Remove the last character (the trailing "\n" after the last row)...
            sb.setLength(sb.length() - 1);
            // ...and immediately add "\n(" back. Net effect: identical to just
            // appending "(" — a harmless no-op round trip.
            sb.append("\n(").append(safeRows.size()).append(" row")
                    .append(safeRows.size() == 1 ? "" : "s").append(")");   // "(1 row)" vs "(5 rows)"
        }

        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // formatExplanation(explanation) — greedy word-wrap at 72 characters.
    //
    // Algorithm ("greedy" = keep adding words to the current line while they fit):
    //   for each word:
    //     - current line empty?      -> start it with this word
    //     - word fits (+1 for space)? -> append " word"
    //     - otherwise                -> finish the line, start a new one with the word
    //   at the end, flush the last line.
    // A single word longer than 72 chars gets its own (over-long) line.
    // ------------------------------------------------------------------------
    public String formatExplanation(String explanation) {
        if (explanation == null || explanation.isBlank()) {
            return "";
        }

        int wrapWidth = 72;                                   // classic terminal-friendly width
        String[] words = explanation.trim().split("\\s+");    // split on ANY run of whitespace (also collapses newlines)
        StringBuilder sb = new StringBuilder();               // the finished text
        StringBuilder line = new StringBuilder();             // the line being built

        for (String word : words) {
            if (line.isEmpty()) {                                       // first word of a line
                line.append(word);
            } else if (line.length() + 1 + word.length() <= wrapWidth) { // fits: current + space + word
                line.append(' ').append(word);
            } else {                                                    // doesn't fit: wrap
                sb.append(line).append('\n');
                line = new StringBuilder(word);
            }
        }

        if (!line.isEmpty()) {
            sb.append(line);   // flush the last line (no trailing newline)
        }

        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // buildRow(cells, widths) — "| a    | bb   |"
    // Each cell: space + padded text + " |".
    // ------------------------------------------------------------------------
    private String buildRow(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");   // leading pipe
        for (int i = 0; i < cells.size(); i++) {
            sb.append(' ').append(padRight(cells.get(i), widths[i])).append(" |");
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // buildSeparator(widths) — "|------|------|"
    // width + 2 dashes per column, to cover the padding spaces on both sides.
    // ------------------------------------------------------------------------
    private String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append("|");   // String.repeat is Java 11+
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // padRight(value, width) — append spaces until value is `width` long.
    // Already long enough? Returned unchanged (never truncated).
    // ------------------------------------------------------------------------
    private String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }
}
