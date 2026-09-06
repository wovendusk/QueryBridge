package com.querybridge.common.dto;

import java.util.List;

public class DescriptionRequest {
    private String correlationId;
    private String tableName;
    private List<String> columnNames;
    private List<String> columnTypes;
    private List<List<String>> sampleRows;

    public DescriptionRequest() {}

    public DescriptionRequest(String correlationId, String tableName,
                              List<String> columnNames, List<String> columnTypes,
                              List<List<String>> sampleRows) {
        this.correlationId = correlationId;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.sampleRows = sampleRows;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public List<String> getColumnNames() { return columnNames; }
    public void setColumnNames(List<String> columnNames) { this.columnNames = columnNames; }
    public List<String> getColumnTypes() { return columnTypes; }
    public void setColumnTypes(List<String> columnTypes) { this.columnTypes = columnTypes; }
    public List<List<String>> getSampleRows() { return sampleRows; }
    public void setSampleRows(List<List<String>> sampleRows) { this.sampleRows = sampleRows; }
}
