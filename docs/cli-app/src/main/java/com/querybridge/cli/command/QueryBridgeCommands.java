package com.querybridge.cli.command;   // the shell commands — the "controller" layer of the CLI

import com.querybridge.cli.kafka.KafkaProducerService;   // send-and-wait over Kafka
import com.querybridge.cli.model.ColumnInfo;
import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import com.querybridge.cli.service.ConnectionManager;    // JDBC schema reader
import com.querybridge.cli.service.DisplayService;       // pretty printing
import com.querybridge.cli.service.ValidatorService;     // SQL checker
import com.querybridge.common.dto.DescriptionRequest;
import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.QueryExecuteRequest;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.common.dto.QueryHistory;
import com.querybridge.common.dto.SqlGenerateRequest;
import com.querybridge.common.dto.SqlGenerateResponse;
import org.springframework.shell.standard.ShellComponent;   // "this bean contains shell commands"
import org.springframework.shell.standard.ShellMethod;      // "this method is a shell command"
import org.springframework.shell.standard.ShellOption;      // customise one command parameter

import java.util.ArrayList;
import java.util.LinkedList;   // a list that's cheap to add at the end / remove from the front
import java.util.List;
import java.util.Map;
import java.util.Scanner;      // reads a line from the keyboard (for "Execute? [Y/n]")
import java.util.UUID;         // random correlation IDs
import java.util.concurrent.CompletableFuture;   // (unused import — leftover; harmless)

// ============================================================================
// QueryBridgeCommands — every command you can type at `querybridge> `, and
// the orchestration of the whole pipeline. (guide step 38 — the TOP of the
// bottom-up path: everything you've read so far is used from here)
//
// THE COMMANDS
//   connect --host H [--port P] --database D --username U --password X
//   ask <question words...>
//   schema
//   history
//   quit / exit
//   (+ Spring Shell's built-ins: help, clear, history?, script, stacktrace, version)
//
// HOW SPRING SHELL TURNS A LINE INTO A METHOD CALL
//   @ShellComponent marks the class (it's a @Component, so it's a bean with
//   constructor injection like any other). Each @ShellMethod becomes a
//   command:
//     key   = the command word(s) you type
//     value = the help text shown by `help`
//   Method PARAMETERS become options. A parameter called `host` becomes
//   `--host`. (That only works because Spring Boot's parent POM compiles with
//   the `-parameters` javac flag, which keeps parameter names in the .class
//   file. Without it the names would be arg0, arg1...)
//   Whatever the method RETURNS (a String here) is printed to the terminal.
//
// THE STATE THIS CLASS HOLDS (it's a singleton bean, so this is per-process)
//   schemaContext   — the connected database (null = not connected)
//   sessionHistory  — the last 5 (question, sql) pairs
//
// THREADING
//   Every command runs on the shell's MAIN thread and BLOCKS while waiting
//   for Kafka replies (KafkaProducerService.sendAndWait). Replies are
//   delivered by Kafka listener threads. You can't type anything while a
//   command is running — which is exactly what you want for a REPL.
//
// NAME CLASH NOTE: Spring Shell ships built-in `quit`/`exit` AND `history`
// commands. This class defines its own `quit`/`exit` and `history`. From the
// Spring Shell 3.3.3 jar I can see the built-in quit is only skipped if you
// provide a bean of its special `Quit.Command` type (this class isn't one),
// so both definitions exist. I'm NOT sure which one wins at runtime (or
// whether Spring Shell complains) — it evidently works for the author. If you
// ever see odd behaviour with these three words, this is why.
// ============================================================================
@ShellComponent
public class QueryBridgeCommands {

    private static final int MAX_HISTORY = 5;                  // how many past queries to remember and send to the LLM
    private static final int MAX_REPAIR_ATTEMPTS = 3;          // max "fix your SQL" rounds before giving up
    private static final long LLM_TIMEOUT_SECONDS = 60;        // wait this long for generated/repaired SQL
    private static final long EXECUTE_TIMEOUT_SECONDS = 30;    // wait this long for query results
    private static final long DESCRIPTION_TIMEOUT_SECONDS = 30; // wait this long for ONE table's column descriptions

    private final ConnectionManager connectionManager;        // reads schema via JDBC
    private final KafkaProducerService kafkaProducerService;  // request/reply over Kafka
    private final ValidatorService validatorService;          // checks SQL against schema
    private final DisplayService displayService;              // formats output

    // Mutable session state. NOT final: replaced on every `connect`.
    private SchemaContext schemaContext;
    // LinkedList used as a fixed-size queue: addLast() new entries,
    // removeFirst() the oldest when there are more than MAX_HISTORY.
    private final LinkedList<QueryHistory> sessionHistory = new LinkedList<>();

    // Constructor injection of the four services.
    public QueryBridgeCommands(ConnectionManager connectionManager,
                               KafkaProducerService kafkaProducerService,
                               ValidatorService validatorService,
                               DisplayService displayService) {
        this.connectionManager = connectionManager;
        this.kafkaProducerService = kafkaProducerService;
        this.validatorService = validatorService;
        this.displayService = displayService;
    }

    // ========================================================================
    // COMMAND: connect
    // ========================================================================
    // Example:
    //   connect --host db.example.com --database shop --username me --password secret
    //
    // WHAT IT DOES
    //   1. ConnectionManager reads all tables/columns + 10 sample rows each.
    //   2. For EACH table, sequentially: send a DescriptionRequest to the LLM
    //      service and wait (≤30s) for the DescriptionResponse; copy the
    //      descriptions onto the columns.
    //   3. Clear the session history (new DB -> old queries are meaningless).
    //   4. Report how many tables got descriptions.
    //
    // PARAMETERS
    //   host, database, username, password: no @ShellOption -> REQUIRED options
    //     (--host etc.). Missing one -> Spring Shell shows an error.
    //   port: @ShellOption(defaultValue = "5432") -> OPTIONAL, defaults to
    //     PostgreSQL's standard port. defaultValue is always a String;
    //     Spring converts it to int.
    //
    // ⚠ The password is typed on the command line in clear text, so it's
    //   visible on screen and may end up in the shell's history file
    //   (Spring Shell keeps a command history — I believe in a file named
    //   spring-shell.log in the working directory by default; check yours).
    //
    // TIMING: with N tables this takes N sequential LLM calls (a few seconds
    // each). A 40-table DB can take minutes, and the prompt is blocked
    // meanwhile. Sending all requests first and then waiting for all replies
    // (in parallel) would be much faster — the LLM service has a single
    // consumer per topic, though, so it would still answer one by one.
    // ------------------------------------------------------------------------
    @ShellMethod(key = "connect", value = "Connect to a PostgreSQL database")
    public String connect(String host,
                          @ShellOption(defaultValue = "5432") int port,
                          String database,
                          String username,
                          String password) {
        try {
            // Step 1: schema + sample rows over JDBC. Throws SQLException on failure.
            schemaContext = connectionManager.connect(host, port, database, username, password);
            // Printed IMMEDIATELY with System.out (not returned), so you see progress
            // before the long description loop. %n = platform line separator.
            System.out.printf("✔ Connected to %s@%s:%d. %d tables found.%n",
                    database, host, port, schemaContext.getTables().size());
            System.out.println("  Generating column descriptions (this may take a moment)...");

            int described = 0;   // how many tables got descriptions successfully
            for (TableInfo table : schemaContext.getTables()) {
                try {
                    String correlationId = UUID.randomUUID().toString();   // fresh ID per request
                    DescriptionRequest request = new DescriptionRequest(
                            correlationId,
                            table.getTableName(),
                            table.getColumns().stream().map(ColumnInfo::getName).toList(),       // ["id", "name", ...]
                            table.getColumns().stream().map(ColumnInfo::getDataType).toList(),   // ["integer", "text", ...]
                            table.getSampleRows()                                                // up to 10 rows
                    );

                    // Send on "description.request" and block until the reply with the
                    // same correlationId arrives (or 30s pass -> RuntimeException).
                    // The generic <T> of sendAndWait is inferred as DescriptionResponse
                    // from the variable type on the left.
                    DescriptionResponse response = kafkaProducerService.sendAndWait(
                            "description.request", request, correlationId, DESCRIPTION_TIMEOUT_SECONDS);

                    // Only use it if the LLM call succeeded AND there's a map.
                    if (response != null && response.isSuccess() && response.getColumnDescriptions() != null) {
                        applyDescriptions(table, response.getColumnDescriptions());
                        described++;
                    }
                } catch (Exception e) {
                    // Description failed for this table — continue without it
                    // (Descriptions are a nice-to-have: SQL generation still works
                    //  without them, just with less context. Note the error itself is
                    //  swallowed silently — you only see it indirectly in the
                    //  "N/M tables analyzed" summary.)
                }
            }

            sessionHistory.clear();   // new database -> forget old queries
            // The returned String is printed by Spring Shell.
            if (described == schemaContext.getTables().size()) {
                return String.format("  All %d tables analyzed. Ready for questions.", described);
            } else if (described > 0) {
                return String.format("  %d/%d tables analyzed. Ready for questions.", described, schemaContext.getTables().size());
            } else {
                // Typical causes: LLM service not running, no API key, Kafka problems.
                return "  Descriptions unavailable, but schema loaded. Ready for questions.";
            }
        } catch (Exception e) {
            // JDBC failed (wrong host/password, SSL not supported, ...).
            schemaContext = null;   // make sure we're in the "not connected" state
            return "Connection failed: " + e.getMessage();
        }
    }

    // ========================================================================
    // COMMAND: ask
    // ========================================================================
    // Example:  ask How many products are there in each category?
    //
    // The question is typed as separate WORDS. `String[] words` +
    // @ShellOption(arity = -1) lets one parameter soak up all of them.
    // (-1 is Spring Shell's constant ARITY_USE_HEURISTICS — which is also the
    //  default, so writing it out changes nothing; for an array parameter the
    //  heuristic is to take all remaining words.) We then glue them back with
    //  spaces.
    //
    // Caveat: the shell's own parser sees your question first, so characters
    // it treats specially can misbehave. I'd expect an apostrophe (what's,
    // customer's) or a word starting with "--" to confuse it, since quotes and
    // "--" have meaning to the parser — I haven't tested it here.
    // ------------------------------------------------------------------------
    @ShellMethod(key = "ask", value = "Ask a natural language question")
    public String ask(@ShellOption(arity = -1) String[] words) {
        if (words == null || words.length == 0) {
            return "Please provide a question.";
        }
        return processQuestion(String.join(" ", words));   // ["How","many"] -> "How many"
    }

    // ------------------------------------------------------------------------
    // processQuestion(question) — THE PIPELINE. This is the "SQL Generation
    // Harness" from the README, start to finish:
    //
    //   ┌──────────┐   ┌──────────┐  errors?  ┌──────────┐
    //   │ GENERATE │──▶│ VALIDATE │──────────▶│  REPAIR  │──┐
    //   └──────────┘   └──────────┘   yes     └──────────┘  │
    //                       ▲  │ no (valid)                  │  up to 3 times
    //                       │  ▼                             │
    //                       │ ┌──────────┐                   │
    //                       │ │ CONFIRM  │ Execute? [Y/n]    │
    //                       │ └──────────┘                   │
    //                       │      │ y                       │
    //                       │      ▼                         │
    //                       │ ┌──────────┐                   │
    //                       │ │ EXECUTE  │ -> table          │
    //                       │ └──────────┘                   │
    //                       └────────────────────────────────┘
    //
    // Public (not private) and separate from ask() — see DefaultCommandConfig
    // for the likely reason.
    // ------------------------------------------------------------------------
    public String processQuestion(String question) {
        if (schemaContext == null) {
            return "Not connected. Use 'connect' first.";
        }
        if (question == null || question.isBlank()) {
            return "Please provide a question.";
        }

        try {
            // ---- GENERATE ---------------------------------------------------
            // repair=false, no previous SQL, no errors.
            SqlGenerateResponse response = generateSql(question, null, false, null);
            if (!response.isSuccess()) {
                // The LLM call itself failed (no key, API error, unparseable reply).
                return "SQL generation failed: " + nullSafe(response.getError());
            }

            // ---- VALIDATE ---------------------------------------------------
            String sql = response.getSql();
            List<String> validationErrors = validatorService.validate(sql, schemaContext);

            // ---- REPAIR LOOP ------------------------------------------------
            // While invalid and we have attempts left: send the broken SQL + the
            // exact errors back to the LLM, then validate its new attempt.
            int attempts = 0;
            while (!validationErrors.isEmpty() && attempts < MAX_REPAIR_ATTEMPTS) {
                attempts++;
                response = generateSql(question, sql, true, validationErrors);   // repair mode
                if (!response.isSuccess()) {
                    return "SQL repair failed: " + nullSafe(response.getError());
                }
                sql = response.getSql();                                  // the "fixed" SQL
                validationErrors = validatorService.validate(sql, schemaContext);   // check again
            }
            // Worst case: 1 generate + 3 repairs = 4 LLM calls for one question.

            if (!validationErrors.isEmpty()) {
                // Still invalid after all repairs: show the last errors and give up.
                // (Remember from ValidatorService: sometimes the SQL was actually
                //  fine and the VALIDATOR is wrong — e.g. aliases.)
                return "Could not generate valid SQL after " + MAX_REPAIR_ATTEMPTS
                        + " repair attempts. Please rephrase your question.\nErrors:\n"
                        + String.join("\n", validationErrors);
            }

            // ---- SHOW -------------------------------------------------------
            StringBuilder output = new StringBuilder();
            output.append(displayService.formatSql(sql)).append("\n\n");   // SQL in a box
            // `response` is the LAST response, so after a repair this is the
            // repair's explanation ("Fixed the column name ...").
            if (response.getExplanation() != null && !response.getExplanation().isBlank()) {
                output.append(displayService.formatExplanation(response.getExplanation())).append("\n\n");
            }
            // ⚠ ORDER BUG: so far `output` has only been BUILT, not printed. It is
            // RETURNED at the end of this method, and Spring Shell prints a
            // command's return value only after the method returns. But
            // confirmExecution() below prints "Execute? [Y/n]: " RIGHT NOW, with
            // System.out. Nothing prints the SQL before that. So you are asked to
            // confirm BEFORE you see the SQL, and the box shows up afterwards,
            // together with the results. That defeats the point of a review
            // step. (The README's sample output shows the box first; the code
            // as written can't produce that order.) The fix is one line:
            // System.out.println(output) before calling confirmExecution(), and
            // then don't return it again.

            // ---- CONFIRM ----------------------------------------------------
            if (!confirmExecution()) {
                addToHistory(question, sql);   // remembered even though not run, so follow-ups still work
                return output + "Query not executed.";   // String + StringBuilder -> calls output.toString()
            }

            // ---- EXECUTE ----------------------------------------------------
            String executeResult = executeQuery(sql);   // a table, or "Query execution failed: ..."
            addToHistory(question, sql);                // remembered even if execution failed
            return output + executeResult;
        } catch (Exception e) {
            // Mostly Kafka timeouts from sendAndWait, e.g.
            // "Kafka request timed out or failed for correlationId: ..." —
            // which usually means the LLM service or executor isn't running.
            return "Error processing question: " + e.getMessage();
        }
    }

    // ========================================================================
    // COMMAND: schema — print tables/columns/descriptions (SchemaContext.toDisplayText).
    // ========================================================================
    @ShellMethod(key = "schema", value = "Show the current database schema")
    public String schema() {
        if (schemaContext == null) {
            return "Not connected. Use 'connect' first.";
        }
        return schemaContext.toDisplayText();
    }

    // ========================================================================
    // COMMAND: history — print the session's last ≤5 questions and their SQL.
    // ========================================================================
    // Output:
    //   1. Q: how many products?
    //      SQL: SELECT COUNT(*) FROM products
    //
    //   2. Q: ...
    // ------------------------------------------------------------------------
    @ShellMethod(key = "history", value = "Show session query history")
    public String history() {
        if (sessionHistory.isEmpty()) {
            return "No queries in this session.";
        }

        StringBuilder sb = new StringBuilder();
        int index = 1;                                        // human numbering
        for (QueryHistory entry : sessionHistory) {           // oldest first
            sb.append(index++).append(". Q: ").append(entry.getQuestion()).append("\n");   // index++ : use, then increment
            sb.append("   SQL: ").append(entry.getSql()).append("\n\n");
        }
        return sb.toString().trim();   // drop the trailing blank line
    }

    // ========================================================================
    // COMMAND: quit / exit — key can be an ARRAY of aliases.
    // ========================================================================
    // System.exit(0) ends the JVM immediately with status 0 ("success").
    // It's less abrupt than it looks: System.exit runs JVM shutdown hooks, and
    // Spring Boot registers one that closes the application context (stopping
    // Kafka consumers, closing the producer) on the way out.
    // ------------------------------------------------------------------------
    @ShellMethod(key = {"quit", "exit"}, value = "Exit the application")
    public void quit() {
        System.exit(0);
    }

    // ========================================================================
    // PRIVATE HELPERS
    // ========================================================================

    // ------------------------------------------------------------------------
    // generateSql(question, previousSql, repair, validationErrors)
    //
    // Builds a SqlGenerateRequest and does a blocking send-and-wait on
    // "sql.generate.request". Used for BOTH modes:
    //   generate: generateSql(q, null, false, null)
    //   repair:   generateSql(q, badSql, true, errors)
    // The full schema text and recent history are attached EVERY time
    // (the LLM service is stateless).
    // Throws RuntimeException on timeout (caught in processQuestion).
    // ------------------------------------------------------------------------
    private SqlGenerateResponse generateSql(String question, String previousSql,
                                            boolean repair, List<String> validationErrors) {
        String correlationId = UUID.randomUUID().toString();   // new ID for every LLM round-trip

        SqlGenerateRequest request = new SqlGenerateRequest();
        request.setCorrelationId(correlationId);
        request.setQuestion(question);
        request.setSchemaContext(schemaContext.toSchemaText());   // whole schema as prompt text
        request.setPreviousQueries(getRecentHistory());           // ≤5 past Q/SQL pairs (a copy)
        request.setRepair(repair);
        request.setPreviousSql(previousSql);                      // null in generate mode
        request.setValidationErrors(validationErrors);            // null in generate mode

        return kafkaProducerService.sendAndWait(
                "sql.generate.request", request, correlationId, LLM_TIMEOUT_SECONDS);   // ≤60s
    }

    // ------------------------------------------------------------------------
    // executeQuery(sql)
    //
    // Sends the SQL + connection details to the query-executor and formats the
    // result as a table. Returns an error STRING (not an exception) if the
    // executor reports failure; a timeout still throws (from sendAndWait).
    // ------------------------------------------------------------------------
    private String executeQuery(String sql) {
        String correlationId = UUID.randomUUID().toString();

        QueryExecuteRequest request = new QueryExecuteRequest(
                correlationId,
                sql,
                schemaContext.getJdbcUrl(),    // the executor opens its OWN connection with these
                schemaContext.getUsername(),
                schemaContext.getPassword()    // plain text over Kafka (see QueryExecuteRequest.java)
        );

        QueryExecuteResponse response = kafkaProducerService.sendAndWait(
                "query.execute.request", request, correlationId, EXECUTE_TIMEOUT_SECONDS);   // ≤30s

        if (!response.isSuccess()) {
            return "Query execution failed: " + nullSafe(response.getError());   // e.g. a PostgreSQL error message
        }

        return displayService.formatTable(response.getColumnNames(), response.getRows());
    }

    // ------------------------------------------------------------------------
    // confirmExecution() — prompt "Execute? [Y/n]" and read one line.
    //
    // Convention: the CAPITAL letter in [Y/n] is the default, so just pressing
    // Enter means yes. Accepts "", "y", "yes" (any case); anything else is no.
    //
    // Reads System.in DIRECTLY with a Scanner, bypassing JLine (Spring Shell's
    // terminal library). That works because JLine only grabs the terminal while
    // it's reading a command line; while a command runs, the terminal is in
    // normal mode. You lose line-editing niceties for this one prompt.
    //
    // A new Scanner is created on each call and deliberately NOT closed —
    // closing a Scanner closes the stream underneath it, i.e. System.in, and
    // nothing could read the keyboard again.
    //
    // hasNextLine() false = input stream ended (e.g. stdin isn't a terminal /
    // Ctrl+D) -> treat as "no".
    // ------------------------------------------------------------------------
    private boolean confirmExecution() {
        System.out.print("Execute? [Y/n]: ");   // print (not println): the cursor stays on this line
        Scanner scanner = new Scanner(System.in);
        if (!scanner.hasNextLine()) {
            return false;
        }
        String input = scanner.nextLine().trim();
        return input.isEmpty() || input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes");
    }

    // ------------------------------------------------------------------------
    // applyDescriptions(table, descriptions)
    //
    // Copies the LLM's {column -> description} map onto the table's ColumnInfo
    // objects. Lookup strategy:
    //   1. exact key match ("category_id")
    //   2. otherwise case-insensitive match ("Category_ID"), since the model
    //      may change capitalisation
    //   3. otherwise null (no description for that column)
    // ------------------------------------------------------------------------
    private void applyDescriptions(TableInfo table, Map<String, String> descriptions) {
        for (ColumnInfo column : table.getColumns()) {
            String description = descriptions.get(column.getName());   // 1. exact
            if (description == null) {
                description = descriptions.entrySet().stream()          // 2. scan all entries...
                        .filter(e -> e.getKey().equalsIgnoreCase(column.getName()))   // ...for a case-insensitive key match
                        .map(Map.Entry::getValue)                       // take its value
                        .findFirst()
                        .orElse(null);                                  // 3. none -> null
            }
            column.setDescription(description);   // modifies the ColumnInfo inside schemaContext in place
        }
    }

    // ------------------------------------------------------------------------
    // getRecentHistory() — a COPY of the history list.
    // A defensive copy: the request object gets its own snapshot as an
    // ArrayList, rather than a live reference to our mutable LinkedList.
    // ------------------------------------------------------------------------
    private List<QueryHistory> getRecentHistory() {
        return new ArrayList<>(sessionHistory);
    }

    // ------------------------------------------------------------------------
    // addToHistory(question, sql) — append, then trim to the newest 5.
    // A tiny "bounded queue" / sliding window.
    // ------------------------------------------------------------------------
    private void addToHistory(String question, String sql) {
        sessionHistory.addLast(new QueryHistory(question, sql));   // newest at the end
        while (sessionHistory.size() > MAX_HISTORY) {
            sessionHistory.removeFirst();                          // drop the oldest
        }
    }

    // nullSafe(value) — never print "null" to the user; show "unknown error" instead.
    private String nullSafe(String value) {
        return value == null ? "unknown error" : value;
    }
}
