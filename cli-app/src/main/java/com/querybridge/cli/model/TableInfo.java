package com.querybridge.cli.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TableInfo {

    private String tableName;
    private List<ColumnInfo> columns = new ArrayList<>();
    private List<List<String>> sampleRows = new ArrayList<>();

    public TableInfo() {
    }

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public List<List<String>> getSampleRows() {
        return sampleRows;
    }

    public void setSampleRows(List<List<String>> sampleRows) {
        this.sampleRows = sampleRows;
    }

    public Optional<ColumnInfo> findColumn(String columnName) {
        if (columnName == null) {
            return Optional.empty();
        }
        String normalized = columnName.replace("\"", "").toLowerCase();
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(normalized))
                .findFirst();
    }
}
