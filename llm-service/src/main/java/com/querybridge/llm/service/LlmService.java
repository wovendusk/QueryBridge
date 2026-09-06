package com.querybridge.llm.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.querybridge.common.dto.DescriptionRequest;
import com.querybridge.common.dto.QueryHistory;
import com.querybridge.common.dto.SqlGenerateRequest;
import com.querybridge.common.dto.SqlGenerateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class LlmService {

    private static final Logger log = LoggerFactory.getLogger(LlmService.class);

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Value("${llm.api-key}")
    private String apiKey;

    @Value("${llm.api-url}")
    private String apiUrl;

    @Value("${llm.model}")
    private String model;

    public LlmService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public Map<String, String> generateDescriptions(DescriptionRequest request) {
        String prompt = buildDescriptionPrompt(request);
        log.debug("Description prompt for table {}: {}", request.getTableName(), prompt);

        String content = callChatCompletion(prompt);
        log.debug("Description LLM response for table {}: {}", request.getTableName(), content);

        String json = stripMarkdownCodeBlocks(content);
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse column descriptions from LLM response: " + e.getMessage(), e);
        }
    }

    public SqlGenerateResponse generateSql(SqlGenerateRequest request) {
        SqlGenerateResponse response = new SqlGenerateResponse();
        response.setCorrelationId(request.getCorrelationId());

        try {
            String prompt = request.isRepair()
                    ? buildRepairPrompt(request)
                    : buildGenerationPrompt(request);
            log.debug("SQL {} prompt for correlationId {}: {}",
                    request.isRepair() ? "repair" : "generation",
                    request.getCorrelationId(),
                    prompt);

            String content = callChatCompletion(prompt);
            log.debug("SQL LLM response for correlationId {}: {}", request.getCorrelationId(), content);

            String json = stripMarkdownCodeBlocks(content);
            JsonNode node = objectMapper.readTree(json);

            response.setSql(node.path("sql").asText(null));
            response.setExplanation(node.path("explanation").asText(null));
            response.setSuccess(true);
        } catch (Exception e) {
            log.error("Failed to generate SQL for correlationId {}", request.getCorrelationId(), e);
            response.setSuccess(false);
            response.setError(e.getMessage());
        }

        return response;
    }

    private String buildDescriptionPrompt(DescriptionRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a database analyst. Given the following table and sample data, ");
        sb.append("write a brief one-line description for each column explaining what it contains in human terms.\n\n");
        sb.append("Table: ").append(request.getTableName()).append('\n');

        List<String> columnNames = request.getColumnNames() != null ? request.getColumnNames() : List.of();
        List<String> columnTypes = request.getColumnTypes() != null ? request.getColumnTypes() : List.of();

        sb.append("Columns and types: ");
        sb.append(formatColumnsAndTypes(columnNames, columnTypes));
        sb.append("\n\nSample data (first 10 rows):\n");
        sb.append(formatSampleRows(columnNames, request.getSampleRows()));

        sb.append("\n\nReturn a JSON object mapping column names to descriptions. Example:\n");
        sb.append("{\"col1\": \"Customer's full name\", \"col2\": \"Total order amount in USD\"}\n\n");
        sb.append("Return ONLY the JSON object, nothing else.");
        return sb.toString();
    }

    private String buildGenerationPrompt(SqlGenerateRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a PostgreSQL SQL expert. Generate a SQL query for the following question.\n\n");
        sb.append("DATABASE SCHEMA (use ONLY these tables and columns):\n");
        sb.append(request.getSchemaContext()).append("\n\n");
        sb.append("RULES:\n");
        sb.append("- Use ONLY PostgreSQL syntax\n");
        sb.append("- Use ONLY the tables and columns listed above — do NOT invent any\n");
        sb.append("- Generate ONLY SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)\n");
        sb.append("- Use proper JOIN conditions based on foreign key relationships\n");
        sb.append("- Always qualify column names with table names/aliases when joining\n\n");

        List<QueryHistory> previousQueries = request.getPreviousQueries();
        if (previousQueries != null && !previousQueries.isEmpty()) {
            sb.append("PREVIOUS QUERIES IN THIS SESSION (for context):\n");
            for (QueryHistory history : previousQueries) {
                sb.append("Q: ").append(history.getQuestion()).append('\n');
                sb.append("SQL: ").append(history.getSql()).append("\n\n");
            }
        }

        sb.append("USER QUESTION: ").append(request.getQuestion()).append("\n\n");
        sb.append("Return a JSON object with exactly two keys:\n");
        sb.append("{\"sql\": \"YOUR SQL QUERY HERE\", \"explanation\": \"Brief explanation of what this query does\"}\n\n");
        sb.append("Return ONLY the JSON object, nothing else.");
        return sb.toString();
    }

    private String buildRepairPrompt(SqlGenerateRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a PostgreSQL SQL expert. The following SQL query has validation errors. Fix them.\n\n");
        sb.append("DATABASE SCHEMA:\n");
        sb.append(request.getSchemaContext()).append("\n\n");
        sb.append("ORIGINAL QUESTION: ").append(request.getQuestion()).append("\n\n");
        sb.append("PREVIOUS SQL (has errors):\n");
        sb.append(request.getPreviousSql()).append("\n\n");
        sb.append("VALIDATION ERRORS:\n");

        List<String> errors = request.getValidationErrors();
        if (errors != null) {
            for (int i = 0; i < errors.size(); i++) {
                sb.append(i + 1).append(". ").append(errors.get(i)).append('\n');
            }
        }

        sb.append("\nFix the SQL to resolve ALL errors. Use ONLY tables and columns from the schema above.\n\n");
        sb.append("Return a JSON object: {\"sql\": \"FIXED SQL\", \"explanation\": \"What was fixed and what the query does\"}\n");
        sb.append("Return ONLY the JSON object.");
        return sb.toString();
    }

    private String formatColumnsAndTypes(List<String> columnNames, List<String> columnTypes) {
        return IntStream.range(0, columnNames.size())
                .mapToObj(i -> {
                    String name = columnNames.get(i);
                    String type = i < columnTypes.size() ? columnTypes.get(i) : "unknown";
                    return name + " (" + type + ")";
                })
                .collect(Collectors.joining(", "));
    }

    private String formatSampleRows(List<String> columnNames, List<List<String>> sampleRows) {
        if (sampleRows == null || sampleRows.isEmpty()) {
            return "(no sample data)\n";
        }

        StringBuilder sb = new StringBuilder();
        for (int rowIndex = 0; rowIndex < sampleRows.size(); rowIndex++) {
            List<String> row = sampleRows.get(rowIndex);
            sb.append("Row ").append(rowIndex + 1).append(": ");
            List<String> pairs = new ArrayList<>();
            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                String colName = columnNames.get(colIndex);
                String value = colIndex < row.size() ? row.get(colIndex) : "";
                pairs.add(colName + "=" + value);
            }
            sb.append(String.join(", ", pairs)).append('\n');
        }
        return sb.toString();
    }

    private String callChatCompletion(String prompt) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("OpenAI API key is not configured (llm.api-key)");
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", model);
        requestBody.put("temperature", 0.1);
        requestBody.put("max_tokens", 2000);

        List<Map<String, String>> messages = List.of(
                Map.of("role", "system", "content", "You are a PostgreSQL database expert."),
                Map.of("role", "user", "content", prompt)
        );
        requestBody.put("messages", messages);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);

        String url = apiUrl.endsWith("/") ? apiUrl + "chat/completions" : apiUrl + "/chat/completions";
        log.debug("Calling LLM API at {} with model {}", url, model);

        try {
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);

            JsonNode root = objectMapper.readTree(response.getBody());
            JsonNode contentNode = root.path("choices").path(0).path("message").path("content");
            if (contentNode.isMissingNode() || contentNode.asText().isBlank()) {
                throw new IllegalStateException("LLM response did not contain message content");
            }
            return contentNode.asText();
        } catch (RestClientException e) {
            throw new IllegalStateException("LLM API call failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to process LLM API response: " + e.getMessage(), e);
        }
    }

    private String stripMarkdownCodeBlocks(String content) {
        if (content == null) {
            return "";
        }

        String trimmed = content.trim();
        if (!trimmed.startsWith("```")) {
            return trimmed;
        }

        int firstNewline = trimmed.indexOf('\n');
        if (firstNewline > 0) {
            trimmed = trimmed.substring(firstNewline + 1);
        }

        if (trimmed.endsWith("```")) {
            trimmed = trimmed.substring(0, trimmed.length() - 3);
        }

        return trimmed.trim();
    }
}
