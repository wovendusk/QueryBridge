package com.querybridge.common.dto;   // shared DTO package

// ============================================================================
// SqlGenerateResponse — the LLM's SQL (plus a short explanation).
// (guide step 7)
//
// DIRECTION:  LLM service  ──(Kafka topic "sql.generate.response")──▶  CLI
//
// Same response class for BOTH generate and repair requests — the CLI knows
// which one it asked for, so it doesn't need the response to say.
//
// HOW THESE FIELDS GET FILLED
//   The LLM is told to reply with ONLY a JSON object like:
//       {"sql": "SELECT ...", "explanation": "Counts products per category"}
//   LlmService parses that JSON and copies "sql" and "explanation" into this
//   object. If anything goes wrong (API down, LLM returned non-JSON, ...)
//   LlmService sets success=false and puts the exception message in error.
//
// IMPORTANT: success=true does NOT mean "the SQL is correct"!
//   It only means "the LLM replied and we could parse its JSON". Whether the
//   SQL is actually valid against your schema is decided afterwards, in the
//   CLI, by ValidatorService. Keep those two kinds of "success" separate in
//   your head.
// ============================================================================
public class SqlGenerateResponse {
    // Echo of the request's correlationId (for request/reply matching).
    private String correlationId;
    // The SQL text the LLM wrote. Can be null if the LLM's JSON had no "sql" key
    // (then ValidatorService will report "SQL is empty").
    private String sql;
    // One-or-two-sentence description of what the query does. The CLI word-wraps
    // it to 72 chars and prints it under the SQL box (DisplayService.formatExplanation).
    private String explanation;
    // true = the LLM answered with parseable JSON. NOT "the SQL is valid".
    private boolean success;
    // Failure reason when success == false.
    private String error;

    // No-arg constructor for Jackson (the CLI deserializes this) and for
    // LlmService, which builds the response with setters.
    public SqlGenerateResponse() {}

    // All-fields constructor (currently unused by the code, but handy).
    public SqlGenerateResponse(String correlationId, String sql,
                               String explanation, boolean success, String error) {
        this.correlationId = correlationId;
        this.sql = sql;
        this.explanation = explanation;
        this.success = success;
        this.error = error;
    }

    // Getters/setters.
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
