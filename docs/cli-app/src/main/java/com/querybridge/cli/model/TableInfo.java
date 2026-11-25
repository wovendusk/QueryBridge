package com.querybridge.cli.model;   // CLI schema model

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;   // "a value that may or may not be there" — used instead of returning null

// ============================================================================
// TableInfo — one table: its name, its columns, and some sample rows.
// (guide step 30)
//
// See ColumnInfo.java for the big picture of the schema model.
//
// Two jobs:
//   1. DATA HOLDER — ConnectionManager fills it at `connect`; its columns and
//      sample rows are sent to the LLM in a DescriptionRequest.
//   2. LOOKUP — findColumn(name) answers "does this table have a column
//      called X?" for ValidatorService.
// ============================================================================
public class TableInfo {

    // Table name as PostgreSQL reports it, e.g. "products".
    private String tableName;
    // Columns in their table order (ordinal_position). Initialised to an empty
    // MUTABLE list so ConnectionManager can just call getColumns().add(...)
    // without null checks.
    private List<ColumnInfo> columns = new ArrayList<>();
    // Up to 10 rows (from "SELECT * ... LIMIT 10"), every value already a String.
    // Each inner list is in the same column order as `columns`.
    private List<List<String>> sampleRows = new ArrayList<>();

    // No-arg constructor (JavaBean convention; not used by current code).
    public TableInfo() {
    }

    // Used by ConnectionManager via a CONSTRUCTOR REFERENCE:
    //     tableMap.computeIfAbsent(tableName, TableInfo::new)
    // "TableInfo::new" means "a function that takes a String and calls
    // new TableInfo(thatString)" — this constructor.
    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    // Returns the LIVE list (not a copy) — callers can and do modify it.
    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public List<List<String>> getSampleRows() {
        return sampleRows;
    }

    // Called by ConnectionManager after it has fetched the sample rows.
    public void setSampleRows(List<List<String>> sampleRows) {
        this.sampleRows = sampleRows;
    }

    // ------------------------------------------------------------------------
    // findColumn(columnName)
    //
    // Case-insensitive, quote-insensitive lookup of a column by name.
    //   findColumn("price")    -> Optional[ColumnInfo price]
    //   findColumn("\"Price\"") -> also finds it (quotes stripped, case ignored)
    //   findColumn("nme")      -> Optional.empty()
    //
    // WHY OPTIONAL?
    //   Returning Optional<ColumnInfo> instead of "ColumnInfo or null" forces
    //   the caller to think about the "not found" case. ValidatorService uses
    //   .isEmpty() / .isPresent() / .flatMap(...) on it.
    //
    // WHY CASE-INSENSITIVE?
    //   In PostgreSQL, unquoted identifiers are folded to lower case
    //   (SELECT Price FROM products works on a column named price). The LLM
    //   might write either case, so the validator should accept both.
    //   (Strictly speaking, a QUOTED "Price" would be case-sensitive in
    //   PostgreSQL, so this lookup is more lenient than the real database.)
    //
    // Small redundancy: the name is lower-cased AND compared with
    // equalsIgnoreCase — either one alone would do.
    // ------------------------------------------------------------------------
    public Optional<ColumnInfo> findColumn(String columnName) {
        if (columnName == null) {
            return Optional.empty();                                       // nothing to look for
        }
        String normalized = columnName.replace("\"", "").toLowerCase();   // strip double quotes, lower-case
        return columns.stream()                                            // walk all columns...
                .filter(c -> c.getName().equalsIgnoreCase(normalized))     // ...keep the ones whose name matches...
                .findFirst();                                              // ...and return the first (or empty)
    }
}
