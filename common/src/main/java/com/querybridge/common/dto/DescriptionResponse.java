package com.querybridge.common.dto;

import java.util.Map;

public class DescriptionResponse {
    private String correlationId;
    private String tableName;
    private Map<String, String> columnDescriptions;
    private boolean success;
    private String error;

    public DescriptionResponse() {}

    public DescriptionResponse(String correlationId, String tableName,
                               Map<String, String> columnDescriptions,
                               boolean success, String error) {
        this.correlationId = correlationId;
        this.tableName = tableName;
        this.columnDescriptions = columnDescriptions;
        this.success = success;
        this.error = error;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public Map<String, String> getColumnDescriptions() { return columnDescriptions; }
    public void setColumnDescriptions(Map<String, String> columnDescriptions) { this.columnDescriptions = columnDescriptions; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
