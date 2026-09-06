package com.querybridge.cli.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SchemaContext {

    private String databaseName;
    private String jdbcUrl;
    private String username;
    private String password;
    private List<TableInfo> tables = new ArrayList<>();

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

    public boolean hasTable(String tableName) {
        return findTable(tableName).isPresent();
    }

    public Optional<TableInfo> findTable(String tableName) {
        if (tableName == null) {
            return Optional.empty();
        }
        String normalized = tableName.replace("\"", "").toLowerCase();
        return tables.stream()
                .filter(t -> t.getTableName().equalsIgnoreCase(normalized))
                .findFirst();
    }

    public String toSchemaText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Database: ").append(databaseName).append("\n\n");

        for (TableInfo table : tables) {
            sb.append("Table: ").append(table.getTableName()).append("\n");
            for (ColumnInfo column : table.getColumns()) {
                sb.append("  - ").append(column.getName())
                        .append(" (").append(column.getDataType()).append(")");
                if (column.getDescription() != null && !column.getDescription().isBlank()) {
                    sb.append(": ").append(column.getDescription());
                }
                sb.append("\n");
            }
            sb.append("\n");
        }

        return sb.toString().trim();
    }

    public String toDisplayText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Database: ").append(databaseName).append("\n");

        for (TableInfo table : tables) {
            sb.append("\n").append(table.getTableName()).append("\n");
            for (ColumnInfo column : table.getColumns()) {
                sb.append("  ").append(column.getName())
                        .append(" (").append(column.getDataType()).append(")");
                if (column.getDescription() != null && !column.getDescription().isBlank()) {
                    sb.append(" — ").append(column.getDescription());
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }
}
