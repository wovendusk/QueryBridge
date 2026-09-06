package com.querybridge.cli.command;

import com.querybridge.cli.kafka.KafkaProducerService;
import com.querybridge.cli.model.ColumnInfo;
import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import com.querybridge.cli.service.ConnectionManager;
import com.querybridge.cli.service.DisplayService;
import com.querybridge.cli.service.ValidatorService;
import com.querybridge.common.dto.DescriptionRequest;
import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.QueryExecuteRequest;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.common.dto.QueryHistory;
import com.querybridge.common.dto.SqlGenerateRequest;
import com.querybridge.common.dto.SqlGenerateResponse;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@ShellComponent
public class QueryBridgeCommands {

    private static final int MAX_HISTORY = 5;
    private static final int MAX_REPAIR_ATTEMPTS = 3;
    private static final long LLM_TIMEOUT_SECONDS = 60;
    private static final long EXECUTE_TIMEOUT_SECONDS = 30;
    private static final long DESCRIPTION_TIMEOUT_SECONDS = 30;

    private final ConnectionManager connectionManager;
    private final KafkaProducerService kafkaProducerService;
    private final ValidatorService validatorService;
    private final DisplayService displayService;

    private SchemaContext schemaContext;
    private final LinkedList<QueryHistory> sessionHistory = new LinkedList<>();

    public QueryBridgeCommands(ConnectionManager connectionManager,
                               KafkaProducerService kafkaProducerService,
                               ValidatorService validatorService,
                               DisplayService displayService) {
        this.connectionManager = connectionManager;
        this.kafkaProducerService = kafkaProducerService;
        this.validatorService = validatorService;
        this.displayService = displayService;
    }

    @ShellMethod(key = "connect", value = "Connect to a PostgreSQL database")
    public String connect(String host,
                          @ShellOption(defaultValue = "5432") int port,
                          String database,
                          String username,
                          String password) {
        try {
            schemaContext = connectionManager.connect(host, port, database, username, password);
            System.out.printf("✔ Connected to %s@%s:%d. %d tables found.%n",
                    database, host, port, schemaContext.getTables().size());
            System.out.println("  Generating column descriptions (this may take a moment)...");

            int described = 0;
            for (TableInfo table : schemaContext.getTables()) {
                try {
                    String correlationId = UUID.randomUUID().toString();
                    DescriptionRequest request = new DescriptionRequest(
                            correlationId,
                            table.getTableName(),
                            table.getColumns().stream().map(ColumnInfo::getName).toList(),
                            table.getColumns().stream().map(ColumnInfo::getDataType).toList(),
                            table.getSampleRows()
                    );

                    DescriptionResponse response = kafkaProducerService.sendAndWait(
                            "description.request", request, correlationId, DESCRIPTION_TIMEOUT_SECONDS);

                    if (response != null && response.isSuccess() && response.getColumnDescriptions() != null) {
                        applyDescriptions(table, response.getColumnDescriptions());
                        described++;
                    }
                } catch (Exception e) {
                    // Description failed for this table — continue without it
                }
            }

            sessionHistory.clear();
            if (described == schemaContext.getTables().size()) {
                return String.format("  All %d tables analyzed. Ready for questions.", described);
            } else if (described > 0) {
                return String.format("  %d/%d tables analyzed. Ready for questions.", described, schemaContext.getTables().size());
            } else {
                return "  Descriptions unavailable, but schema loaded. Ready for questions.";
            }
        } catch (Exception e) {
            schemaContext = null;
            return "Connection failed: " + e.getMessage();
        }
    }

    @ShellMethod(key = "ask", value = "Ask a natural language question")
    public String ask(@ShellOption(arity = -1) String[] words) {
        if (words == null || words.length == 0) {
            return "Please provide a question.";
        }
        return processQuestion(String.join(" ", words));
    }

    public String processQuestion(String question) {
        if (schemaContext == null) {
            return "Not connected. Use 'connect' first.";
        }
        if (question == null || question.isBlank()) {
            return "Please provide a question.";
        }

        try {
            SqlGenerateResponse response = generateSql(question, null, false, null);
            if (!response.isSuccess()) {
                return "SQL generation failed: " + nullSafe(response.getError());
            }

            String sql = response.getSql();
            List<String> validationErrors = validatorService.validate(sql, schemaContext);

            int attempts = 0;
            while (!validationErrors.isEmpty() && attempts < MAX_REPAIR_ATTEMPTS) {
                attempts++;
                response = generateSql(question, sql, true, validationErrors);
                if (!response.isSuccess()) {
                    return "SQL repair failed: " + nullSafe(response.getError());
                }
                sql = response.getSql();
                validationErrors = validatorService.validate(sql, schemaContext);
            }

            if (!validationErrors.isEmpty()) {
                return "Could not generate valid SQL after " + MAX_REPAIR_ATTEMPTS
                        + " repair attempts. Please rephrase your question.\nErrors:\n"
                        + String.join("\n", validationErrors);
            }

            StringBuilder output = new StringBuilder();
            output.append(displayService.formatSql(sql)).append("\n\n");
            if (response.getExplanation() != null && !response.getExplanation().isBlank()) {
                output.append(displayService.formatExplanation(response.getExplanation())).append("\n\n");
            }

            if (!confirmExecution()) {
                addToHistory(question, sql);
                return output + "Query not executed.";
            }

            String executeResult = executeQuery(sql);
            addToHistory(question, sql);
            return output + executeResult;
        } catch (Exception e) {
            return "Error processing question: " + e.getMessage();
        }
    }

    @ShellMethod(key = "schema", value = "Show the current database schema")
    public String schema() {
        if (schemaContext == null) {
            return "Not connected. Use 'connect' first.";
        }
        return schemaContext.toDisplayText();
    }

    @ShellMethod(key = "history", value = "Show session query history")
    public String history() {
        if (sessionHistory.isEmpty()) {
            return "No queries in this session.";
        }

        StringBuilder sb = new StringBuilder();
        int index = 1;
        for (QueryHistory entry : sessionHistory) {
            sb.append(index++).append(". Q: ").append(entry.getQuestion()).append("\n");
            sb.append("   SQL: ").append(entry.getSql()).append("\n\n");
        }
        return sb.toString().trim();
    }

    @ShellMethod(key = {"quit", "exit"}, value = "Exit the application")
    public void quit() {
        System.exit(0);
    }

    private SqlGenerateResponse generateSql(String question, String previousSql,
                                            boolean repair, List<String> validationErrors) {
        String correlationId = UUID.randomUUID().toString();

        SqlGenerateRequest request = new SqlGenerateRequest();
        request.setCorrelationId(correlationId);
        request.setQuestion(question);
        request.setSchemaContext(schemaContext.toSchemaText());
        request.setPreviousQueries(getRecentHistory());
        request.setRepair(repair);
        request.setPreviousSql(previousSql);
        request.setValidationErrors(validationErrors);

        return kafkaProducerService.sendAndWait(
                "sql.generate.request", request, correlationId, LLM_TIMEOUT_SECONDS);
    }

    private String executeQuery(String sql) {
        String correlationId = UUID.randomUUID().toString();

        QueryExecuteRequest request = new QueryExecuteRequest(
                correlationId,
                sql,
                schemaContext.getJdbcUrl(),
                schemaContext.getUsername(),
                schemaContext.getPassword()
        );

        QueryExecuteResponse response = kafkaProducerService.sendAndWait(
                "query.execute.request", request, correlationId, EXECUTE_TIMEOUT_SECONDS);

        if (!response.isSuccess()) {
            return "Query execution failed: " + nullSafe(response.getError());
        }

        return displayService.formatTable(response.getColumnNames(), response.getRows());
    }

    private boolean confirmExecution() {
        System.out.print("Execute? [Y/n]: ");
        Scanner scanner = new Scanner(System.in);
        if (!scanner.hasNextLine()) {
            return false;
        }
        String input = scanner.nextLine().trim();
        return input.isEmpty() || input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes");
    }

    private void applyDescriptions(TableInfo table, Map<String, String> descriptions) {
        for (ColumnInfo column : table.getColumns()) {
            String description = descriptions.get(column.getName());
            if (description == null) {
                description = descriptions.entrySet().stream()
                        .filter(e -> e.getKey().equalsIgnoreCase(column.getName()))
                        .map(Map.Entry::getValue)
                        .findFirst()
                        .orElse(null);
            }
            column.setDescription(description);
        }
    }

    private List<QueryHistory> getRecentHistory() {
        return new ArrayList<>(sessionHistory);
    }

    private void addToHistory(String question, String sql) {
        sessionHistory.addLast(new QueryHistory(question, sql));
        while (sessionHistory.size() > MAX_HISTORY) {
            sessionHistory.removeFirst();
        }
    }

    private String nullSafe(String value) {
        return value == null ? "unknown error" : value;
    }
}
