// Every class in the "common" module lives in this package. All three apps
// import from com.querybridge.common.dto, which is also the package the Kafka
// JSON deserializers are told to "trust" (you'll see addTrustedPackages(...)
// with this exact string in every KafkaConfig).
package com.querybridge.common.dto;

// ============================================================================
// QueryHistory — ONE entry of "what the user asked, and what SQL we produced".
// (guide step 3 — the simplest DTO in the project, read it first)
//
// WHAT IS A DTO?
//   DTO = "Data Transfer Object". A class whose only job is to carry data
//   from one place to another — here, from one microservice to another via a
//   Kafka message. It has fields, getters, setters and constructors, and NO
//   business logic. Think of it as the "envelope format" of a message.
//
// WHERE IS THIS USED?
//   1. In the CLI, QueryBridgeCommands keeps a list of the last 5 of these
//      (the "session memory"). The `history` command prints them.
//   2. Every time the CLI asks the LLM service for new SQL, it puts that list
//      into SqlGenerateRequest.previousQueries. The LLM service then pastes
//      them into the prompt under "PREVIOUS QUERIES IN THIS SESSION", which is
//      what lets you ask follow-ups like "now only for last month" without
//      repeating the whole question.
//
//   So a QueryHistory never travels on its own; it always rides INSIDE a
//   SqlGenerateRequest.
//
// WHY A PLAIN CLASS WITH GETTERS/SETTERS INSTEAD OF A JAVA RECORD?
//   The project is on Java 21, so `record QueryHistory(String question,
//   String sql) {}` would have worked too (Jackson 2.12+ understands records).
//   The author chose the classic "JavaBean" style:
//     - a public NO-ARGUMENT constructor,
//     - private fields,
//     - getX()/setX() for each field.
//   That style is what Jackson has always understood out of the box: to READ
//   JSON into an object, Jackson calls the no-arg constructor to make an empty
//   object, then calls setQuestion(...), setSql(...) for each JSON key it
//   finds. To WRITE JSON, it calls every getX() and uses "x" as the key.
//   (So the JSON for this class is {"question": "...", "sql": "..."}.)
// ============================================================================
public class QueryHistory {
    // The natural-language question the user typed, e.g. "How many orders per city?"
    private String question;
    // The final, VALIDATED SQL that was produced for that question.
    // (Note: it's stored even if the user answered "n" at "Execute? [Y/n]" —
    // see addToHistory() in QueryBridgeCommands.)
    private String sql;

    // ------------------------------------------------------------------------
    // No-argument constructor.
    // REQUIRED BY JACKSON for deserialization: Jackson needs a way to create
    // an "empty" instance before it fills in the fields with setters. If you
    // delete this, receiving a SqlGenerateRequest with history entries would
    // fail at runtime with "Cannot construct instance of QueryHistory".
    // ------------------------------------------------------------------------
    public QueryHistory() {}

    // ------------------------------------------------------------------------
    // Convenience constructor used by OUR code (QueryBridgeCommands.addToHistory)
    // to build an entry in one line. Jackson does NOT use this one.
    // ------------------------------------------------------------------------
    public QueryHistory(String question, String sql) {
        this.question = question;   // copy the parameter into the field
        this.sql = sql;             // same
    }

    // Getters and setters. Jackson maps "getQuestion"/"setQuestion" to the JSON
    // key "question" (it strips "get"/"set" and lower-cases the first letter).
    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
}
