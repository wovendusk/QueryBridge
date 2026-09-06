package com.querybridge.common.dto;

import java.util.List;

public class QueryExecuteResponse {
    private String correlationId;
    private List<String> columnNames;
    private List<List<String>> rows;
    private int rowCount;
    private boolean success;
    private String error;

    public QueryExecuteResponse() {}

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
