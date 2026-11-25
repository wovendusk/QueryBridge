package com.querybridge.cli.model;   // the CLI's in-memory picture of the database schema

// ============================================================================
// ColumnInfo — one column of one table. (guide step 29)
//
// THE CLI'S SCHEMA MODEL (three small classes, read them in this order)
//
//     SchemaContext            the whole connected database
//       ├─ jdbcUrl, username, password, databaseName
//       └─ List<TableInfo>     one per table
//             ├─ tableName
//             ├─ List<ColumnInfo>     one per column   <-- this file
//             │     ├─ name, dataType
//             │     └─ description    (filled in later by the LLM)
//             └─ sampleRows           up to 10 rows of real data
//
// These are NOT shared DTOs (they live in cli-app, not common) and they
// never travel over Kafka as objects. The CLI builds them at `connect`
// (ConnectionManager), enriches them with LLM descriptions, and then uses
// them for two things:
//   - rendering the schema as TEXT for prompts (SchemaContext.toSchemaText)
//   - looking up tables/columns while VALIDATING SQL (findTable/findColumn)
//
// LIFECYCLE OF A ColumnInfo
//   1. Created by ConnectionManager with (name, dataType), from
//      information_schema.columns. description is null.
//   2. QueryBridgeCommands.applyDescriptions() calls setDescription(...)
//      with the LLM's text, if the description request for its table
//      succeeded. Otherwise description stays null.
// ============================================================================
public class ColumnInfo {

    // Column name exactly as PostgreSQL reports it, e.g. "category_id".
    // (Unquoted identifiers are stored lower-case by PostgreSQL.)
    private String name;
    // PostgreSQL's information_schema "data_type" text, e.g. "integer",
    // "character varying", "timestamp without time zone", "ARRAY", "USER-DEFINED".
    // Note: for varchar this is just "character varying" — the length limit isn't included.
    private String dataType;
    // Human description from the LLM, e.g. "Foreign key to the product's category".
    // null until (and unless) descriptions arrive.
    private String description;

    // No-arg constructor (not used by the current code; JavaBean convention).
    public ColumnInfo() {
    }

    // The constructor ConnectionManager uses: name + type, description comes later.
    public ColumnInfo(String name, String dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    // Plain getters/setters — written in the expanded multi-line style here,
    // while the DTOs in `common` use one-liners. Same thing, different author habit.

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDescription() {
        return description;
    }

    // Called by QueryBridgeCommands.applyDescriptions() — the only setter that's
    // actually used after construction.
    public void setDescription(String description) {
        this.description = description;
    }
}
