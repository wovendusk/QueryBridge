package com.querybridge.common.dto;

public class QueryExecuteRequest {
    private String correlationId;
    private String sql;
    private String jdbcUrl;
    private String username;
    private String password;

    public QueryExecuteRequest() {}

    public QueryExecuteRequest(String correlationId, String sql,
                               String jdbcUrl, String username, String password) {
        this.correlationId = correlationId;
        this.sql = sql;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
}
