package com.querybridge.cli.service;   // the CLI's service classes

import com.querybridge.cli.model.ColumnInfo;
import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import org.springframework.stereotype.Service;

import java.sql.Connection;      // JDBC connection
import java.sql.DriverManager;   // URL + credentials -> Connection
import java.sql.ResultSet;       // rows returned by a query
import java.sql.SQLException;    // JDBC's checked exception
import java.sql.Statement;       // executes SQL
import java.util.ArrayList;
import java.util.LinkedHashMap;  // a HashMap that remembers INSERTION ORDER
import java.util.List;
import java.util.Map;

// ============================================================================
// ConnectionManager — connects to your PostgreSQL ONCE, at `connect`, and
// reads (a) which tables and columns exist and (b) up to 10 sample rows per
// table. Returns all of it as a SchemaContext. (guide step 32)
//
// NOTE THE ARCHITECTURE: the CLI talks to your database DIRECTLY here (JDBC,
// no Kafka), but actual queries are run by the query-executor service. So
// BOTH the CLI and the executor must be able to reach your database host.
// See the "localhost trap" note in connect() below.
//
// HOW DO YOU ASK POSTGRESQL "WHAT TABLES DO YOU HAVE?"
//   Every SQL database exposes a standard read-only schema called
//   information_schema, full of views describing the database itself.
//   information_schema.columns has one row per column of every table/view:
//       table_schema | table_name | column_name | data_type | ordinal_position
//       public       | products   | id          | integer   | 1
//       public       | products   | name        | text      | 2
//       ...
//   One query on it gives us the whole schema.
// ============================================================================
@Service
public class ConnectionManager {

    // ------------------------------------------------------------------------
    // connect(host, port, database, username, password)
    //
    // 1. Build a JDBC URL.
    // 2. Open a connection (fails fast on bad host/credentials -> SQLException,
    //    which QueryBridgeCommands.connect turns into "Connection failed: ...").
    // 3. Read the schema, then sample rows for each table.
    // 4. Close the connection (try-with-resources) and return the SchemaContext.
    // ------------------------------------------------------------------------
    public SchemaContext connect(String host, int port, String database,
                                 String username, String password) throws SQLException {
        // JDBC URL format for PostgreSQL: jdbc:postgresql://HOST:PORT/DATABASE?params
        //
        // ⚠ sslmode=require is HARD-CODED. It means "use TLS or refuse to connect".
        //   - Cloud databases (Neon, Supabase, RDS, ...) usually REQUIRE TLS, so
        //     this works there out of the box.
        //   - A default local PostgreSQL (e.g. the official Docker image) does NOT
        //     have SSL enabled, so connecting to it fails with an error like
        //     "The server does not support SSL connections". To use a local DB
        //     you'd need to change this to sslmode=prefer / disable (or make it
        //     a command option).
        //   - Also: "require" encrypts but does NOT verify the server certificate
        //     (that's verify-ca / verify-full), so it doesn't protect against a
        //     man-in-the-middle. Fine for a dev tool.
        //
        // ⚠ THE LOCALHOST TRAP: this URL is reused by the query-executor. If you
        //   run the CLI on your machine but the executor inside Docker, then
        //   "localhost" means YOUR MACHINE here, but "the executor container
        //   itself" there. Schema loading would succeed and query execution would
        //   fail with "Connection refused". A cloud DB hostname avoids that (and
        //   on Docker Desktop, host.docker.internal can point back at the host).
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?sslmode=require",
                host, port, database);   // %s = string, %d = integer

        // try-with-resources: the connection is closed when this block ends.
        // The CLI does NOT keep a connection open during the session.
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            SchemaContext schemaContext = new SchemaContext();   // the object we'll fill and return
            schemaContext.setDatabaseName(database);
            schemaContext.setJdbcUrl(jdbcUrl);                    // kept so the executor can connect later
            schemaContext.setUsername(username);
            schemaContext.setPassword(password);

            // Step A: one query for all tables + columns.
            Map<String, TableInfo> tableMap = fetchTablesFromMetadata(connection);
            // Step B: one "SELECT * ... LIMIT 10" per table, run one after another.
            // With 50 tables that's 50 round-trips — noticeable on a remote DB, but OK.
            // Note: if ANY table's sample query fails (e.g. you lack SELECT
            // permission on one table), the SQLException escapes and the WHOLE
            // connect fails — not just that table.
            for (TableInfo table : tableMap.values()) {
                table.setSampleRows(fetchSampleRows(connection, table));
            }

            // Copy the map's values into a list (keeps the map's order).
            schemaContext.setTables(new ArrayList<>(tableMap.values()));
            return schemaContext;
        }   // <- connection closed here
    }

    // ------------------------------------------------------------------------
    // fetchTablesFromMetadata(connection)
    //
    // Reads information_schema.columns and groups rows by table, giving
    // { "categories" -> TableInfo(...), "products" -> TableInfo(...) }.
    //
    // DETAILS WORTH NOTICING
    //   - WHERE table_schema = 'public': only the default "public" schema.
    //     Tables in other schemas (e.g. sales.orders) are invisible to
    //     QueryBridge — the LLM won't know about them and the validator would
    //     reject them.
    //   - information_schema.columns also lists VIEWS, so views show up as
    //     "tables" too (which is actually handy: you can query them).
    //   - ORDER BY table_name, ordinal_position: alphabetical tables, columns
    //     in their real order (ordinal_position = 1, 2, 3...). That matters:
    //     fetchSampleRows relies on "SELECT *" returning columns in the SAME
    //     order as this list.
    //   - LinkedHashMap preserves that insertion order (a plain HashMap would
    //     scramble the tables).
    // ------------------------------------------------------------------------
    private Map<String, TableInfo> fetchTablesFromMetadata(Connection connection) throws SQLException {
        Map<String, TableInfo> tableMap = new LinkedHashMap<>();   // tableName -> TableInfo, in order

        // A Java TEXT BLOCK (""" ... """, Java 15+): a multi-line string literal.
        // Indentation common to all lines is stripped automatically.
        String sql = """
                SELECT table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
                """;

        try (Statement stmt = connection.createStatement();   // both closed automatically
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {                                 // one row per column of every table
                String tableName = rs.getString("table_name");  // columns can be read BY NAME too, not just index
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");

                // computeIfAbsent: "give me the TableInfo for this name; if there
                // isn't one yet, create it with new TableInfo(tableName), store it,
                // and give me that". Saves the classic get / null-check / put dance.
                TableInfo table = tableMap.computeIfAbsent(tableName, TableInfo::new);
                table.getColumns().add(new ColumnInfo(columnName, dataType));   // append this column
            }
        }

        return tableMap;
    }

    // ------------------------------------------------------------------------
    // fetchSampleRows(connection, table)
    //
    // Runs  SELECT * FROM "tableName" LIMIT 10  and converts each value to a
    // String (NULL -> "NULL"), same conversion as the executor's QueryService.
    //
    // There's no ORDER BY, so "the first 10 rows" is just whatever PostgreSQL
    // returns first — not necessarily the oldest or newest. That's fine for
    // giving the LLM a flavour of the data.
    // ------------------------------------------------------------------------
    private List<List<String>> fetchSampleRows(Connection connection, TableInfo table) throws SQLException {
        // Collect the column names (Stream.toList() returns an unmodifiable list, Java 16+).
        List<String> columnNames = table.getColumns().stream()
                .map(ColumnInfo::getName)   // method reference: column -> column.getName()
                .toList();

        if (columnNames.isEmpty()) {
            return List.of();   // defensive: nothing to sample
        }
        // (In practice this branch can't trigger: tables only get into tableMap
        //  by having at least one row in information_schema.columns. A zero-column
        //  table — legal in PostgreSQL — simply never shows up at all.)
        // (Small observation: columnNames isn't used after this check — the
        //  query below uses SELECT *, not the explicit column list.)

        // The table name is CONCATENATED into SQL here, which is normally a red flag
        // (SQL injection). It's safe-ish because (a) the name came from the
        // database's own metadata, not the user, and (b) it's properly QUOTED:
        String quotedTable = quoteIdentifier(table.getTableName());
        String sql = "SELECT * FROM " + quotedTable + " LIMIT 10";   // e.g. SELECT * FROM "products" LIMIT 10

        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();   // how many columns came back
            while (rs.next()) {                                    // up to 10 times
                List<String> row = new ArrayList<>(columnCount);   // pre-size the list
                for (int i = 1; i <= columnCount; i++) {           // JDBC: 1-based
                    Object value = rs.getObject(i);
                    row.add(value == null ? "NULL" : value.toString());   // everything becomes text
                }
                rows.add(row);
            }
        }

        return rows;
    }

    // ------------------------------------------------------------------------
    // quoteIdentifier(identifier)
    //
    // Wraps a name in double quotes, the SQL-standard way to quote an
    // IDENTIFIER (table/column name) — not to be confused with single quotes,
    // which are for STRING VALUES.
    //   products      -> "products"
    //   Order Items   -> "Order Items"      (spaces/caps are preserved when quoted)
    //   we"ird        -> "we""ird"          (an embedded quote is escaped by doubling it)
    // Quoting matters because a table created as "OrderItems" (mixed case) can
    // ONLY be referenced with quotes; without them PostgreSQL would look for
    // orderitems.
    // ------------------------------------------------------------------------
    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
