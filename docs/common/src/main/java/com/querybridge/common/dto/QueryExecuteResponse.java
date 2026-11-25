package com.querybridge.common.dto;   // shared DTO package

import java.util.List;                 // for column names and rows

// ============================================================================
// QueryExecuteResponse — the result set of the SQL, flattened to strings.
// (guide step 9 — last DTO)
//
// DIRECTION:  query-executor  ──(Kafka topic "query.execute.response")──▶  CLI
//
// WHAT HAPPENS TO IT
//   The CLI's KafkaResponseHandler hands it to the waiting
//   QueryBridgeCommands.executeQuery(), which passes columnNames + rows to
//   DisplayService.formatTable() to draw an ASCII table like:
//
//       | category_name | product_count |
//       |---------------|---------------|
//       | Books         | 22            |
//       (1 row)
//
// WHY IS EVERYTHING A STRING?
//   A JDBC ResultSet can contain ints, BigDecimals, Timestamps, UUIDs, arrays,
//   JSON... Sending those faithfully through JSON would need type info for
//   every column and custom (de)serializers. But the CLI only ever PRINTS the
//   values. So the executor calls toString() on every value (and turns SQL
//   NULL into the literal text "NULL") and sends a simple grid of strings.
//   Simple and good enough for display — but it means the CLI can't, say,
//   sort numerically or format dates. Trade-off chosen deliberately.
//
// THE 1000-ROW CAP
//   QueryService stops reading after 1000 rows, so `rows` never has more
//   than 1000 entries and rowCount is at most 1000. Nothing in this message
//   tells the CLI that the result was TRUNCATED, so if you query a huge
//   table you'll see "(1000 rows)" with no warning that there were more.
// ============================================================================
public class QueryExecuteResponse {
    // Echo of the request's correlationId.
    private String correlationId;
    // Header row: column labels from the result set, e.g. ["category_name", "product_count"].
    // These are the SQL aliases (AS ...) when present — QueryService uses getColumnLabel().
    private List<String> columnNames;
    // The data: outer list = rows, inner list = cell values (as strings) in column order.
    private List<List<String>> rows;
    // Number of rows in `rows` (always equals rows.size(); max 1000). The CLI
    // doesn't actually read this field — DisplayService counts rows itself.
    private int rowCount;
    // true = the query ran; false = it failed (bad credentials, SQL error, timeout...).
    private boolean success;
    // The exception message when success == false, e.g.
    // 'ERROR: relation "prodcts" does not exist'.
    private String error;

    // No-arg constructor for Jackson (the CLI deserializes this).
    public QueryExecuteResponse() {}

    // All-fields constructor used by QueryService for both the success and the
    // failure case.
    public QueryExecuteResponse(String correlationId, List<String> columnNames,
                                List<List<String>> rows, int rowCount,
                                boolean success, String error) {
        this.correlationId = correlationId;
        this.columnNames = columnNames;
        this.rows = rows;
        this.rowCount = rowCount;
        this.success = success;
        this.error = error;
    }

    // Getters/setters.
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public List<String> getColumnNames() { return columnNames; }
    public void setColumnNames(List<String> columnNames) { this.columnNames = columnNames; }
    public List<List<String>> getRows() { return rows; }
    public void setRows(List<List<String>> rows) { this.rows = rows; }
    public int getRowCount() { return rowCount; }
    public void setRowCount(int rowCount) { this.rowCount = rowCount; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
