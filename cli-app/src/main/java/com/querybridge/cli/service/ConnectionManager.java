package com.querybridge.cli.service;

import com.querybridge.cli.model.ColumnInfo;
import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConnectionManager {

    public SchemaContext connect(String host, int port, String database,
                                 String username, String password) throws SQLException {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?sslmode=require",
                host, port, database);

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            SchemaContext schemaContext = new SchemaContext();
            schemaContext.setDatabaseName(database);
            schemaContext.setJdbcUrl(jdbcUrl);
            schemaContext.setUsername(username);
            schemaContext.setPassword(password);

            Map<String, TableInfo> tableMap = fetchTablesFromMetadata(connection);
            for (TableInfo table : tableMap.values()) {
                table.setSampleRows(fetchSampleRows(connection, table));
            }

            schemaContext.setTables(new ArrayList<>(tableMap.values()));
            return schemaContext;
        }
    }

    private Map<String, TableInfo> fetchTablesFromMetadata(Connection connection) throws SQLException {
        Map<String, TableInfo> tableMap = new LinkedHashMap<>();

        String sql = """
                SELECT table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
                """;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");

                TableInfo table = tableMap.computeIfAbsent(tableName, TableInfo::new);
                table.getColumns().add(new ColumnInfo(columnName, dataType));
            }
        }

        return tableMap;
    }

    private List<List<String>> fetchSampleRows(Connection connection, TableInfo table) throws SQLException {
        List<String> columnNames = table.getColumns().stream()
                .map(ColumnInfo::getName)
                .toList();

        if (columnNames.isEmpty()) {
            return List.of();
        }

        String quotedTable = quoteIdentifier(table.getTableName());
        String sql = "SELECT * FROM " + quotedTable + " LIMIT 10";

        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    row.add(value == null ? "NULL" : value.toString());
                }
                rows.add(row);
            }
        }

        return rows;
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
