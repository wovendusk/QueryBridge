package com.querybridge.common.dto;   // shared DTO package

import java.util.Map;                  // column name -> description

// ============================================================================
// DescriptionResponse — the LLM's answer to a DescriptionRequest.
// (guide step 5)
//
// DIRECTION:  LLM service  ──(Kafka topic "description.response")──▶  CLI
//
// WHAT HAPPENS TO IT
//   The CLI's KafkaResponseHandler receives it, reads correlationId, and hands
//   it to the code that's been waiting (QueryBridgeCommands.connect). If
//   success is true, connect() calls applyDescriptions(), which copies each
//   description onto the matching ColumnInfo object in the CLI's in-memory
//   schema.
//
// THE "success + error" PATTERN (used by every response DTO here)
//   Across a message bus you can't throw an exception "back" to the caller —
//   the caller is a different program. So failures are reported AS DATA:
//     success = true  -> the payload fields (here columnDescriptions) are valid
//     success = false -> payload is probably null; `error` has a message
//   The LLM service catches its own exceptions and turns them into a
//   success=false response, so the CLI gets an answer instead of waiting
//   until its timeout.
//
// EXAMPLE JSON:
//   {
//     "correlationId": "3f1c...e9",           <- same ID as the request
//     "tableName": "products",
//     "columnDescriptions": { "id": "Unique product identifier",
//                             "price": "Retail price in USD" },
//     "success": true,
//     "error": null
//   }
// ============================================================================
public class DescriptionResponse {
    // Copied verbatim from the request so the CLI can match reply to request.
    private String correlationId;
    // Copied from the request too. Handy for logs; the CLI doesn't actually
    // need it for matching (correlationId does that).
    private String tableName;
    // The useful payload: { "columnName": "one-line human description", ... }.
    // This is exactly the JSON object the LLM was asked to return, parsed
    // straight into a Map by LlmService.generateDescriptions().
    private Map<String, String> columnDescriptions;
    // true = it worked; false = look at `error`.
    // NOTE: for a boolean field, the JavaBean getter is named isSuccess() not
    // getSuccess(). Jackson understands that and still uses the key "success".
    private boolean success;
    // Human-readable failure reason when success == false; otherwise null.
    private String error;

    // No-arg constructor for Jackson (the CLI deserializes this class).
    public DescriptionResponse() {}

    // All-fields constructor — handy, though LlmKafkaListener actually builds the
    // response with the no-arg constructor + setters instead.
    public DescriptionResponse(String correlationId, String tableName,
                               Map<String, String> columnDescriptions,
                               boolean success, String error) {
        this.correlationId = correlationId;
        this.tableName = tableName;
        this.columnDescriptions = columnDescriptions;
        this.success = success;
        this.error = error;
    }

    // Getters/setters (Jackson uses them for JSON <-> object conversion).
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public Map<String, String> getColumnDescriptions() { return columnDescriptions; }
    public void setColumnDescriptions(Map<String, String> columnDescriptions) { this.columnDescriptions = columnDescriptions; }
    public boolean isSuccess() { return success; }                  // "is", not "get", for booleans
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
