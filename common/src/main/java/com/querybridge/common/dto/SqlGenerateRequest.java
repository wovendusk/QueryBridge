package com.querybridge.common.dto;

import java.util.List;

public class SqlGenerateRequest {
    private String correlationId;
    private String question;
    private String schemaContext;
    private List<QueryHistory> previousQueries;
    private boolean repair;
    private String previousSql;
    private List<String> validationErrors;

    public SqlGenerateRequest() {}

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
    public String getSchemaContext() { return schemaContext; }
    public void setSchemaContext(String schemaContext) { this.schemaContext = schemaContext; }
    public List<QueryHistory> getPreviousQueries() { return previousQueries; }
    public void setPreviousQueries(List<QueryHistory> previousQueries) { this.previousQueries = previousQueries; }
    public boolean isRepair() { return repair; }
    public void setRepair(boolean repair) { this.repair = repair; }
    public String getPreviousSql() { return previousSql; }
    public void setPreviousSql(String previousSql) { this.previousSql = previousSql; }
    public List<String> getValidationErrors() { return validationErrors; }
    public void setValidationErrors(List<String> validationErrors) { this.validationErrors = validationErrors; }
}
