package com.querybridge.cli.model;   // CLI schema model

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

// ============================================================================
// SchemaContext — everything the CLI knows about the database you're
// connected to. (guide step 31)
//
// One instance exists while you're connected: it's stored in the field
// QueryBridgeCommands.schemaContext. `null` there means "not connected".
//
// IT HOLDS TWO KINDS OF THINGS
//   1. CONNECTION DETAILS — jdbcUrl, username, password, databaseName.
//      The CLI itself only connects once (at `connect`, to read the schema).
//      After that, these are copied into every QueryExecuteRequest so the
//      query-executor service can open its own connection.
//      (So yes: the password sits in memory for the whole session, and is
//      sent over Kafka on every execute. See QueryExecuteRequest.java.)
//   2. THE SCHEMA — List<TableInfo>, each with columns (+ LLM descriptions)
//      and sample rows.
//
// IT RENDERS ITSELF TWO WAYS
//   toSchemaText()  -> compact text FOR THE LLM (goes into every SQL prompt)
//   toDisplayText() -> friendlier text FOR YOU (the `schema` command)
//
// AND IT ANSWERS LOOKUPS
//   hasTable(name) / findTable(name) -> used by ValidatorService
// ============================================================================
public class SchemaContext {

    private String databaseName;   // e.g. "shop" — shown in both renderings
    private String jdbcUrl;        // e.g. "jdbc:postgresql://host:5432/shop?sslmode=require"
    private String username;       // DB user
    private String password;       // DB password, plain text, kept for the session
    private List<TableInfo> tables = new ArrayList<>();   // every table in the "public" schema

    // ---- plain getters / setters (set by ConnectionManager.connect) -------

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<TableInfo> getTables() {
        return tables;
    }

    public void setTables(List<TableInfo> tables) {
        this.tables = tables;
    }

    // ------------------------------------------------------------------------
    // hasTable(tableName) — "does this database have a table called X?"
    // A convenience wrapper: just findTable(...).isPresent().
    // Used by ValidatorService for every table name found in the SQL.
    // ------------------------------------------------------------------------
    public boolean hasTable(String tableName) {
        return findTable(tableName).isPresent();
    }

    // ------------------------------------------------------------------------
    // findTable(tableName) — case- and quote-insensitive table lookup.
    // Exactly the same logic as TableInfo.findColumn (see there for why
    // case-insensitive, why Optional, and the small lower-case redundancy).
    //   findTable("Products")     -> found
    //   findTable("\"products\"") -> found
    //   findTable("p")            -> empty   (aliases are NOT understood — this
    //                                          is the root of a validator bug,
    //                                          see ValidatorService.java)
    // ------------------------------------------------------------------------
    public Optional<TableInfo> findTable(String tableName) {
        if (tableName == null) {
            return Optional.empty();
        }
        String normalized = tableName.replace("\"", "").toLowerCase();   // strip quotes, lower-case
        return tables.stream()
                .filter(t -> t.getTableName().equalsIgnoreCase(normalized))   // compare ignoring case
                .findFirst();
    }

    // ------------------------------------------------------------------------
    // toSchemaText() — the schema as text FOR THE LLM.
    //
    // Example output:
    //
    //   Database: shop
    //
    //   Table: categories
    //     - id (integer): Unique category identifier
    //     - name (text): Category display name
    //
    //   Table: products
    //     - id (integer): Unique product identifier
    //     - category_id (integer): The category this product belongs to
    //     - price (numeric)                   <- no description: none was generated
    //
    // This string goes into SqlGenerateRequest.schemaContext and gets pasted
    // into every generate AND repair prompt. Note what's NOT in it: primary
    // keys, foreign keys, indexes, row counts. The model has to infer joins
    // from column names and descriptions.
    //
    // Cost consideration: this full text is resent with EVERY request. For a
    // database with hundreds of tables it becomes a big (slow, expensive)
    // prompt. Fine for small schemas.
    // ------------------------------------------------------------------------
    public String toSchemaText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Database: ").append(databaseName).append("\n\n");   // header + blank line

        for (TableInfo table : tables) {
            sb.append("Table: ").append(table.getTableName()).append("\n");     // "Table: products"
            for (ColumnInfo column : table.getColumns()) {
                sb.append("  - ").append(column.getName())                     // "  - price"
                        .append(" (").append(column.getDataType()).append(")"); // " (numeric)"
                // Only append ": description" if there IS one.
                if (column.getDescription() != null && !column.getDescription().isBlank()) {
                    sb.append(": ").append(column.getDescription());
                }
                sb.append("\n");
            }
            sb.append("\n");   // blank line between tables
        }

        return sb.toString().trim();   // trim the trailing blank lines
    }

    // ------------------------------------------------------------------------
    // toDisplayText() — the schema as text FOR YOU (the `schema` command).
    //
    // Example output:
    //
    //   Database: shop
    //
    //   categories
    //     id (integer) — Unique category identifier
    //     name (text) — Category display name
    //
    //   products
    //     ...
    //
    // Same information as toSchemaText, laid out a bit more readably
    // (no "Table:" prefix, an em-dash before descriptions).
    // ------------------------------------------------------------------------
    public String toDisplayText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Database: ").append(databaseName).append("\n");

        for (TableInfo table : tables) {
            sb.append("\n").append(table.getTableName()).append("\n");          // blank line, then the table name
            for (ColumnInfo column : table.getColumns()) {
                sb.append("  ").append(column.getName())                        // indented column name
                        .append(" (").append(column.getDataType()).append(")"); // its type
                if (column.getDescription() != null && !column.getDescription().isBlank()) {
                    sb.append(" — ").append(column.getDescription());           // " — description"
                }
                sb.append("\n");
            }
        }

        return sb.toString();   // Spring Shell prints whatever String the command returns
    }
}
