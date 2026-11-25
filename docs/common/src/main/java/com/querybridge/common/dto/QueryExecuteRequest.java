package com.querybridge.common.dto;   // shared DTO package

// ============================================================================
// QueryExecuteRequest — "Query executor, please run this SQL on this database."
// (guide step 8)
//
// DIRECTION:  CLI  ──(Kafka topic "query.execute.request")──▶  query-executor
// ANSWERED BY: QueryExecuteResponse on topic "query.execute.response"
//
// WHEN IS IT SENT?
//   Only after (1) the LLM produced SQL, (2) ValidatorService found no errors,
//   and (3) the user answered "Y" (or just pressed Enter) at
//   "Execute? [Y/n]".
//
// WHY DOES THE REQUEST CARRY THE DATABASE CREDENTIALS?
//   The query-executor is STATELESS. It has no idea which database you
//   `connect`ed to in the CLI — it's a separate program, possibly in a
//   separate container. So every execute request carries everything needed
//   to open a JDBC connection: the JDBC URL, username and password. The
//   executor opens a connection, runs the SQL, closes the connection, done.
//
//   ⚠ SECURITY NOTE (worth understanding, not just accepting):
//   This means your database PASSWORD travels in plain JSON through Kafka on
//   every single execute. The docker-compose Kafka uses PLAINTEXT listeners
//   (no TLS, no auth), and Kafka KEEPS messages on disk for its retention
//   period (7 days by default). Anyone who can read the
//   "query.execute.request" topic can read your password. For a local learning
//   project that's acceptable; in a real system you'd instead give the
//   executor its own credentials (or a short-lived token / a connection ID it
//   can look up), never ship raw passwords through a log-structured bus.
//
// ALSO NOTE: the executor will run WHATEVER sql is in here. All the "only
// SELECT queries" safety lives in the CLI's ValidatorService. The executor
// itself does no checking. (More on that in QueryService.java.)
// ============================================================================
public class QueryExecuteRequest {
    // Request/reply matching ID (see DescriptionRequest.java for the full story).
    private String correlationId;
    // The validated SQL to run, e.g. "SELECT name FROM products LIMIT 5".
    private String sql;
    // Full JDBC URL built by the CLI's ConnectionManager, e.g.
    // "jdbc:postgresql://db.example.com:5432/shop?sslmode=require".
    private String jdbcUrl;
    // Database username.
    private String username;
    // Database password — in plain text (see the security note above).
    private String password;

    // No-arg constructor for Jackson (the executor deserializes this).
    public QueryExecuteRequest() {}

    // All-fields constructor used by the CLI (QueryBridgeCommands.executeQuery).
    public QueryExecuteRequest(String correlationId, String sql,
                               String jdbcUrl, String username, String password) {
        this.correlationId = correlationId;
        this.sql = sql;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    // Getters/setters.
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
