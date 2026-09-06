package com.querybridge.common.dto;

public class SqlGenerateResponse {
    private String correlationId;
    private String sql;
    private String explanation;
    private boolean success;
    private String error;

    public SqlGenerateResponse() {}

    public SqlGenerateResponse(String correlationId, String sql,
                               String explanation, boolean success, String error) {
        this.correlationId = correlationId;
        this.sql = sql;
        this.explanation = explanation;
        this.success = success;
        this.error = error;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
    public String getExplanation() { return explanation; }
    public void setExplanation(String explanation) { this.explanation = explanation; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
