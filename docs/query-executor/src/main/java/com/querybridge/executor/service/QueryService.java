package com.querybridge.executor.service;   // business logic of the executor

import com.querybridge.common.dto.QueryExecuteRequest;    // input: SQL + credentials
import com.querybridge.common.dto.QueryExecuteResponse;   // output: rows as strings
import org.springframework.stereotype.Service;           // marks this class as a Spring bean

import java.sql.Connection;          // an open connection to a database
import java.sql.DriverManager;       // the classic JDBC entry point: URL -> Connection
import java.sql.ResultSet;           // a cursor over the rows a query returned
import java.sql.ResultSetMetaData;   // info ABOUT the result: column count, names, types
import java.sql.Statement;           // something you can execute SQL with
import java.util.ArrayList;
import java.util.List;

// ============================================================================
// QueryService — actually runs SQL on the user's PostgreSQL database using
// plain JDBC. (guide step 14)
//
// This is the ONLY place in the whole project where the user's generated SQL
// touches their real database.
//
// JDBC IN 30 SECONDS (in case it's rusty)
//   JDBC is Java's standard database API (package java.sql). The flow is
//   always the same:
//     1. Connection  conn = DriverManager.getConnection(url, user, pass);
//     2. Statement   stmt = conn.createStatement();
//     3. ResultSet   rs   = stmt.executeQuery("SELECT ...");
//     4. while (rs.next()) { rs.getObject(1); rs.getObject(2); ... }
//     5. close everything (rs, stmt, conn)
//   Column indexes in JDBC start at 1, not 0. You'll see `i = 1; i <= n` loops.
//
// DESIGN: ONE CONNECTION PER REQUEST, NO POOL
//   Each call opens a brand new connection and closes it at the end. Opening a
//   PostgreSQL connection costs a few ms to ~100ms (more with SSL / a remote
//   cloud DB). A production service would use a connection pool (HikariCP),
//   but a pool needs a FIXED database; here every request could target a
//   DIFFERENT database (the credentials come in the message), and a human is
//   typing the queries, so the simple approach is fine.
//
// ⚠ SAFETY: THIS CLASS RUNS WHATEVER SQL IT IS GIVEN
//   No check here that the SQL is a SELECT. The "SELECT only" rule is enforced
//   ONLY by the CLI's ValidatorService (and by the LLM prompt asking nicely).
//   Anyone who can publish to the "query.execute.request" topic can run
//   DELETE / DROP / anything with the credentials in the message.
//   Even a statement that passes the validator can have side effects —
//   e.g. `SELECT pg_terminate_backend(pid) FROM pg_stat_activity` is a valid
//   SELECT. Defence in depth would be (any of):
//     - connect with a READ-ONLY database role (the strongest option),
//     - conn.setAutoCommit(false) + conn.setReadOnly(true) so the driver runs
//       the transaction READ ONLY (as far as I know, the PostgreSQL driver only
//       applies setReadOnly when autocommit is off, by default — verify before
//       relying on it), and rollback at the end,
//     - re-validate the SQL here too.
// ============================================================================
@Service   // Spring bean; injected into ExecutorKafkaListener
public class QueryService {

    // ------------------------------------------------------------------------
    // execute(request)
    //
    // Runs request.getSql() against request.getJdbcUrl() and converts the
    // result into a QueryExecuteResponse.
    //
    // NEVER THROWS. Any problem at all (can't connect, wrong password, SQL
    // syntax error, timeout, ...) is caught and turned into
    // success=false + error message. That's essential in a Kafka service: if
    // this method threw, the CLI would get NO reply and would just sit there
    // until its 30s timeout. With this design it gets a clear error quickly.
    // ------------------------------------------------------------------------
    public QueryExecuteResponse execute(QueryExecuteRequest request) {
        String correlationId = request.getCorrelationId();   // keep it: every response must echo it

        // try-with-resources: Connection and Statement are AutoCloseable, so
        // they're closed automatically when the block exits — normally OR via an
        // exception — in reverse order (stmt first, then conn). No leaks.
        try (Connection conn = DriverManager.getConnection(
                request.getJdbcUrl(), request.getUsername(), request.getPassword());   // opens a TCP (+SSL) connection to Postgres
             Statement stmt = conn.createStatement()) {                                 // a plain (non-prepared) statement

            // If the query runs longer than 30 seconds, the driver asks the
            // server to cancel it and throws an SQLException. Protects against
            // accidentally expensive queries (e.g. an unintended cross join).
            // It lines up with the CLI's EXECUTE_TIMEOUT_SECONDS = 30 — though
            // note connection time + Kafka hops come on top, so a query that
            // takes ~29s may still make the CLI give up first.
            stmt.setQueryTimeout(30);

            // Run the SQL. executeQuery() is for statements that RETURN rows
            // (SELECT). For an UPDATE it would throw (no result set) — though
            // the server would already have executed it by then.
            //
            // Note: `rs` is not in the try-with-resources list. That's OK here:
            // by the JDBC spec, closing a Statement also closes its ResultSet.
            //
            // ⚠ MEMORY NOTE: by default the PostgreSQL JDBC driver downloads the
            // ENTIRE result set into memory on this line (it only streams rows in
            // chunks if you set a fetch size AND turn autocommit off). So the
            // 1000-row cap further down limits what we SEND, not what we LOAD.
            // `SELECT * FROM huge_table` would pull every row into this JVM first.
            // stmt.setMaxRows(1000) before executing would make the limit apply at
            // the source.
            ResultSet rs = stmt.executeQuery(request.getSql());
            ResultSetMetaData meta = rs.getMetaData();   // describes the columns of the result
            int colCount = meta.getColumnCount();        // how many columns the result has

            // Build the header row.
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) {        // JDBC columns are 1-based!
                // getColumnLabel vs getColumnName: label is the name as it appears
                // in the RESULT, i.e. the alias if you wrote `COUNT(*) AS total`
                // ("total"). getColumnName might give the underlying column name or
                // something like "count". Label is what a user wants to see.
                columnNames.add(meta.getColumnLabel(i));
            }

            List<List<String>> rows = new ArrayList<>();   // the data grid we'll send back
            int rowCount = 0;                              // how many rows we've copied so far
            int maxRows = 1000;                            // hard cap so a huge result doesn't make a giant Kafka message
            // rs.next() moves the cursor to the next row and returns false when
            // there are no more. Small quirk: because rs.next() is evaluated FIRST,
            // when rowCount hits 1000 the cursor still advances once more before
            // the `rowCount < maxRows` check stops the loop. Harmless, but writing
            // `rowCount < maxRows && rs.next()` would be the tidier order.
            //
            // Also note: nothing tells the CLI the result was cut off at 1000.
            while (rs.next() && rowCount < maxRows) {
                List<String> row = new ArrayList<>();      // one row's values
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);          // whatever Java type the driver maps it to (Integer, BigDecimal, Timestamp, String, ...)
                    // SQL NULL comes back as Java null. We turn it into the TEXT
                    // "NULL" so the CLI table shows something visible. Everything
                    // else is turned into text with toString().
                    row.add(val == null ? "NULL" : val.toString());
                }
                rows.add(row);                             // add the finished row to the grid
                rowCount++;
            }

            // Success response: header, data, count, success=true, no error.
            return new QueryExecuteResponse(correlationId, columnNames, rows, rowCount, true, null);

        } catch (Exception e) {
            // Catches SQLException (bad SQL, bad credentials, timeout, network...)
            // and anything unexpected. The message is sent back to the CLI and
            // printed as "Query execution failed: <message>".
            // Note: nothing is LOGGED here, so the executor's own logs won't show
            // why a query failed — only the CLI user sees it.
            return new QueryExecuteResponse(correlationId, null, null, 0, false, e.getMessage());
        }
    }   // <- here try-with-resources closes stmt (and with it rs), then conn
}
