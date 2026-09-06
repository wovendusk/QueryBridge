package com.querybridge.common.dto;

public class QueryHistory {
    private String question;
    private String sql;

    public QueryHistory() {}

    public QueryHistory(String question, String sql) {
        this.question = question;
        this.sql = sql;
    }

    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
}
