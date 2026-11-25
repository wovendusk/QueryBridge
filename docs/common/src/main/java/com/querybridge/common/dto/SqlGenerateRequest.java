package com.querybridge.common.dto;   // shared DTO package

import java.util.List;                 // for previousQueries and validationErrors

// ============================================================================
// SqlGenerateRequest — "LLM, turn this English question into SQL"
//                    OR  "LLM, your last SQL was wrong, here's why, fix it."
// (guide step 6 — the most important DTO; read slowly)
//
// DIRECTION:  CLI  ──(Kafka topic "sql.generate.request")──▶  LLM service
// ANSWERED BY: SqlGenerateResponse on topic "sql.generate.response"
//
// ONE MESSAGE TYPE, TWO MODES
//   This single class is used for two different jobs, switched by the
//   `repair` flag:
//
//   MODE 1 — GENERATE (repair = false)
//     Sent once when the user types `ask <question>`.
//     Fields that matter: question, schemaContext, previousQueries.
//     previousSql and validationErrors are null.
//     -> LlmService.buildGenerationPrompt() is used.
//
//   MODE 2 — REPAIR (repair = true)
//     Sent when the SQL the LLM produced FAILED the CLI's own validator
//     (ValidatorService). E.g. the LLM wrote `SELECT nme FROM products`
//     and the validator said "Unknown column 'nme' in referenced tables".
//     Fields that matter: question, schemaContext, previousSql (the broken
//     SQL), validationErrors (the list of problems the validator found).
//     -> LlmService.buildRepairPrompt() is used.
//     The CLI does this up to 3 times (MAX_REPAIR_ATTEMPTS) before giving up.
//
//   This "generate → validate → feed errors back → repair" loop is what the
//   README calls the "SQL Generation Harness". It's the core idea of the
//   project: don't blindly trust the LLM; CHECK its output against the real
//   schema and make it fix its own mistakes with precise feedback.
//
// WHY IS schemaContext A STRING, NOT A STRUCTURED OBJECT?
//   Because the LLM service never inspects the schema programmatically; it
//   only pastes it into a prompt. So the CLI pre-renders the schema to text
//   (SchemaContext.toSchemaText()), like:
//
//       Database: shop
//
//       Table: products
//         - id (integer): Unique product identifier
//         - name (text): Product display name
//
//   and the LLM service just drops that text into the prompt. Simple, and it
//   means the LLM service doesn't need the CLI's model classes at all.
//
// NOTE ON STATELESSNESS
//   The LLM service remembers NOTHING between requests. Every single request
//   carries the full schema text and the recent history. That makes the LLM
//   service trivially simple and restartable (no session storage), at the
//   cost of bigger messages. A good trade for a small project.
// ============================================================================
public class SqlGenerateRequest {
    // Random UUID chosen by the CLI per request; the response echoes it back.
    // A NEW correlationId is generated for each repair attempt too.
    private String correlationId;
    // The user's English question, e.g. "top 5 customers by total spend".
    // Sent in BOTH modes (in repair mode, the LLM needs to remember what the
    // SQL was supposed to do).
    private String question;
    // The whole database schema rendered as text (see SchemaContext.toSchemaText()).
    private String schemaContext;
    // Up to 5 earlier (question, sql) pairs from this session, oldest first,
    // for follow-up questions. Only used by the GENERATE prompt; the repair
    // prompt ignores it.
    private List<QueryHistory> previousQueries;
    // false = generate fresh SQL; true = repair previousSql.
    private boolean repair;
    // REPAIR MODE ONLY: the SQL that failed validation.
    private String previousSql;
    // REPAIR MODE ONLY: what was wrong with it, e.g.
    // ["Unknown column 'nme' in referenced tables", "Unknown table: prodcts"].
    private List<String> validationErrors;

    // Only a no-arg constructor here (no all-args one). The CLI builds this
    // object with setters one field at a time — see QueryBridgeCommands.generateSql().
    // Jackson also needs this constructor on the LLM-service side.
    public SqlGenerateRequest() {}

    // Getters/setters for every field (used by our code AND by Jackson).
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
    public String getSchemaContext() { return schemaContext; }
    public void setSchemaContext(String schemaContext) { this.schemaContext = schemaContext; }
    public List<QueryHistory> getPreviousQueries() { return previousQueries; }
    public void setPreviousQueries(List<QueryHistory> previousQueries) { this.previousQueries = previousQueries; }
    public boolean isRepair() { return repair; }               // boolean -> "is" getter, JSON key "repair"
    public void setRepair(boolean repair) { this.repair = repair; }
    public String getPreviousSql() { return previousSql; }
    public void setPreviousSql(String previousSql) { this.previousSql = previousSql; }
    public List<String> getValidationErrors() { return validationErrors; }
    public void setValidationErrors(List<String> validationErrors) { this.validationErrors = validationErrors; }
}
