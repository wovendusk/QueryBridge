package com.querybridge.executor.service;

import com.querybridge.common.dto.QueryExecuteRequest;
import com.querybridge.common.dto.QueryExecuteResponse;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class QueryService {

    public QueryExecuteResponse execute(QueryExecuteRequest request) {
        String correlationId = request.getCorrelationId();

        try (Connection conn = DriverManager.getConnection(
                request.getJdbcUrl(), request.getUsername(), request.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.setQueryTimeout(30);

            ResultSet rs = stmt.executeQuery(request.getSql());
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) {
                columnNames.add(meta.getColumnLabel(i));
            }

            List<List<String>> rows = new ArrayList<>();
            int rowCount = 0;
            int maxRows = 1000;
            while (rs.next() && rowCount < maxRows) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    row.add(val == null ? "NULL" : val.toString());
                }
                rows.add(row);
                rowCount++;
            }

            return new QueryExecuteResponse(correlationId, columnNames, rows, rowCount, true, null);

        } catch (Exception e) {
            return new QueryExecuteResponse(correlationId, null, null, 0, false, e.getMessage());
        }
    }
}
