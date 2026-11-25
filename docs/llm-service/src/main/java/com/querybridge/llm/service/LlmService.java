package com.querybridge.llm.service;   // the "brains" of the LLM service

import com.fasterxml.jackson.core.type.TypeReference;   // lets Jackson know a GENERIC target type (Map<String,String>)
import com.fasterxml.jackson.databind.JsonNode;         // a parsed JSON tree you can walk with .path("x")
import com.fasterxml.jackson.databind.ObjectMapper;     // Jackson's main class: JSON <-> Java
import com.querybridge.common.dto.DescriptionRequest;   // input for column descriptions
import com.querybridge.common.dto.QueryHistory;         // one (question, sql) history entry
import com.querybridge.common.dto.SqlGenerateRequest;   // input for SQL generation / repair
import com.querybridge.common.dto.SqlGenerateResponse;  // output for SQL generation / repair
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;   // inject llm.* settings
import org.springframework.http.HttpEntity;                  // an HTTP request = body + headers
import org.springframework.http.HttpHeaders;                 // HTTP headers container
import org.springframework.http.MediaType;                   // "application/json" etc.
import org.springframework.http.ResponseEntity;              // an HTTP response = status + headers + body
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;   // base class of RestTemplate's errors
import org.springframework.web.client.RestTemplate;          // HTTP client (bean from LlmApplication)

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;   // Collectors.joining(", ")
import java.util.stream.IntStream;    // IntStream.range(0, n) — a stream of indexes

// ============================================================================
// LlmService — builds prompts, calls the OpenAI-compatible Chat Completions
// API over HTTP, and parses the model's JSON answer.
// (guide step 21 — the second most important file in the project; read it
//  method by method, top to bottom)
//
// THE TWO PUBLIC JOBS
//   1. generateDescriptions(DescriptionRequest) -> Map<column, description>
//   2. generateSql(SqlGenerateRequest)          -> SqlGenerateResponse
//      (which itself has two modes: generate fresh SQL, or repair broken SQL)
//
// THE SHAPE OF EVERY JOB
//   build a prompt string  ->  callChatCompletion(prompt)  ->  get text back
//   ->  stripMarkdownCodeBlocks(text)  ->  parse that text as JSON
//
// HOW THE OPENAI CHAT COMPLETIONS API WORKS (what callChatCompletion sends)
//   POST https://api.openai.com/v1/chat/completions
//   Headers: Authorization: Bearer <api key>, Content-Type: application/json
//   Body:
//     {
//       "model": "gpt-4o-mini",
//       "temperature": 0.1,
//       "max_tokens": 2000,
//       "messages": [
//         {"role": "system", "content": "You are a PostgreSQL database expert."},
//         {"role": "user",   "content": "<the big prompt we built>"}
//       ]
//     }
//   Response (trimmed):
//     { "choices": [ { "message": { "role": "assistant",
//                                   "content": "{\"sql\": \"SELECT ...\", ...}" } } ],
//       ... }
//   The model's reply is the STRING at choices[0].message.content. We asked
//   the model to make that string itself be JSON, so we parse it a second time.
//   Many other providers (DeepSeek, Groq, local servers like Ollama/vLLM in
//   OpenAI mode, ...) copy this exact API shape, which is why the README says
//   "any OpenAI-compatible API" works.
//
// "PROMPT ENGINEERING" PATTERNS YOU'LL SEE HERE
//   - Give the model a role ("You are a PostgreSQL SQL expert").
//   - Give it ALL the context it needs (schema text, history, errors).
//   - State hard rules in CAPS ("use ONLY these tables", "ONLY SELECT").
//   - Specify the exact output format with an example, and say "Return ONLY
//     the JSON object, nothing else" — because models love to add chatter
//     like "Sure! Here's your query:" which would break JSON parsing.
//   - Low temperature (0.1) so the output is near-deterministic.
//
// ⚠ PRIVACY NOTE
//   generateDescriptions sends up to 10 REAL ROWS of every table to the LLM
//   provider. If your database has personal data (emails, phone numbers...),
//   it leaves your machine and goes to OpenAI/whoever. Fine for a demo DB;
//   think twice for a real one.
// ============================================================================
@Service
public class LlmService {

    private static final Logger log = LoggerFactory.getLogger(LlmService.class);

    private final RestTemplate restTemplate;   // HTTP client (bean defined in LlmApplication)
    private final ObjectMapper objectMapper;   // JSON mapper (auto-configured by Spring Boot's Jackson support)

    // FIELD injection with @Value: Spring sets these right after the
    // constructor runs. They can't be `final` because of that. (They're only
    // used inside methods called later, so they're always set by then.)
    @Value("${llm.api-key}")    // from application.yml -> env OPENAI_API_KEY
    private String apiKey;

    @Value("${llm.api-url}")    // e.g. https://api.openai.com/v1
    private String apiUrl;

    @Value("${llm.model}")      // e.g. gpt-4o-mini
    private String model;

    // CONSTRUCTOR injection for the two beans (best practice: final fields,
    // easy to pass fakes in a unit test).
    public LlmService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    // ========================================================================
    // PUBLIC JOB #1: generateDescriptions
    // ========================================================================
    // Asks the LLM for a one-line description of every column of ONE table.
    //
    // Returns a Map like {"id": "Unique identifier", "price": "Price in USD"}.
    //
    // THROWS on failure (API down, unparseable answer...). Unlike generateSql
    // below, this method does NOT catch its own errors — its caller
    // (LlmKafkaListener.onDescriptionRequest) has a try/catch that turns the
    // exception into a success=false DescriptionResponse. Two different error
    // styles in one class; both work, but it's worth noticing the inconsistency.
    // ------------------------------------------------------------------------
    public Map<String, String> generateDescriptions(DescriptionRequest request) {
        String prompt = buildDescriptionPrompt(request);   // step 1: build the text we send
        // DEBUG level: NOT printed by default (Spring Boot's default level is INFO).
        // Enable with logging.level.com.querybridge.llm=DEBUG to see full prompts.
        log.debug("Description prompt for table {}: {}", request.getTableName(), prompt);

        String content = callChatCompletion(prompt);       // step 2: HTTP call, get the model's text reply
        log.debug("Description LLM response for table {}: {}", request.getTableName(), content);

        String json = stripMarkdownCodeBlocks(content);    // step 3: remove ```json ... ``` fences if present
        try {
            // step 4: parse the JSON text into a Map<String, String>.
            //
            // WHY `new TypeReference<Map<String, String>>() {}`?
            // Because of type erasure you can't write Map<String,String>.class.
            // Creating an anonymous subclass of TypeReference (note the `{}`)
            // captures the full generic type in the class's metadata, where
            // Jackson can read it. It's the standard Jackson idiom for
            // "deserialize into a generic type".
            //
            // If the model returned something like {"id": {"desc": "..."}} (a
            // nested object instead of a string), this fails -> exception below.
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            // Wrap with a clearer message; keep the original as the "cause" (2nd arg)
            // so the full stack trace is preserved in logs.
            throw new IllegalStateException("Failed to parse column descriptions from LLM response: " + e.getMessage(), e);
        }
    }

    // ========================================================================
    // PUBLIC JOB #2: generateSql
    // ========================================================================
    // Turns a question into SQL (repair=false) or fixes broken SQL (repair=true).
    //
    // NEVER THROWS: every failure becomes success=false + error inside the
    // returned response. That's why LlmKafkaListener.onSqlGenerateRequest has
    // no try/catch of its own.
    //
    // Remember (see SqlGenerateResponse): success=true only means "we got and
    // parsed a JSON answer". Whether the SQL is any good is checked later by
    // the CLI's ValidatorService.
    // ------------------------------------------------------------------------
    public SqlGenerateResponse generateSql(SqlGenerateRequest request) {
        SqlGenerateResponse response = new SqlGenerateResponse();   // start with an empty response
        response.setCorrelationId(request.getCorrelationId());      // ALWAYS echo the ID, even on failure

        try {
            // Pick the prompt for the mode we're in (ternary operator: cond ? a : b).
            String prompt = request.isRepair()
                    ? buildRepairPrompt(request)       // "here's broken SQL + errors, fix it"
                    : buildGenerationPrompt(request);  // "here's a question, write SQL"
            log.debug("SQL {} prompt for correlationId {}: {}",
                    request.isRepair() ? "repair" : "generation",
                    request.getCorrelationId(),
                    prompt);

            String content = callChatCompletion(prompt);   // HTTP call -> model's text
            log.debug("SQL LLM response for correlationId {}: {}", request.getCorrelationId(), content);

            String json = stripMarkdownCodeBlocks(content);   // remove ``` fences
            // readTree = parse JSON into a generic tree (JsonNode) instead of a
            // specific class. Good when you only want a couple of fields.
            JsonNode node = objectMapper.readTree(json);

            // node.path("sql") returns the child node, or a "MissingNode" if the key
            // doesn't exist (path() never returns null — unlike get(), which does).
            // .asText(null) then gives the text, or our default (null) if the node
            // is missing/null. So a model answer without "sql" just yields sql=null,
            // which the CLI's validator later reports as "SQL is empty".
            response.setSql(node.path("sql").asText(null));
            response.setExplanation(node.path("explanation").asText(null));
            response.setSuccess(true);   // "we got a parseable answer"
        } catch (Exception e) {
            // Anything: no API key, HTTP 401/429/500, network error, non-JSON reply...
            // Logged at ERROR with the stack trace (last argument `e`), and reported
            // to the CLI, which prints "SQL generation failed: <error>".
            log.error("Failed to generate SQL for correlationId {}", request.getCorrelationId(), e);
            response.setSuccess(false);
            response.setError(e.getMessage());
        }

        return response;
    }

    // ========================================================================
    // PROMPT BUILDERS (private)
    // ========================================================================

    // ------------------------------------------------------------------------
    // buildDescriptionPrompt(request)
    //
    // Produces a prompt like:
    //
    //   You are a database analyst. Given the following table and sample data,
    //   write a brief one-line description for each column explaining what it
    //   contains in human terms.
    //
    //   Table: products
    //   Columns and types: id (integer), name (text), price (numeric)
    //
    //   Sample data (first 10 rows):
    //   Row 1: id=1, name=Lamp, price=19.99
    //   Row 2: id=2, name=Desk, price=120.00
    //
    //   Return a JSON object mapping column names to descriptions. Example:
    //   {"col1": "Customer's full name", "col2": "Total order amount in USD"}
    //
    //   Return ONLY the JSON object, nothing else.
    //
    // StringBuilder is used because building a long string with many `+`
    // in a loop would create lots of temporary String objects.
    // ------------------------------------------------------------------------
    private String buildDescriptionPrompt(DescriptionRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a database analyst. Given the following table and sample data, ");   // role
        sb.append("write a brief one-line description for each column explaining what it contains in human terms.\n\n");   // task
        sb.append("Table: ").append(request.getTableName()).append('\n');   // which table

        // Defensive null handling: if the lists are missing from the JSON, use
        // empty immutable lists instead of crashing with a NullPointerException.
        List<String> columnNames = request.getColumnNames() != null ? request.getColumnNames() : List.of();
        List<String> columnTypes = request.getColumnTypes() != null ? request.getColumnTypes() : List.of();

        sb.append("Columns and types: ");
        sb.append(formatColumnsAndTypes(columnNames, columnTypes));   // "id (integer), name (text), ..."
        sb.append("\n\nSample data (first 10 rows):\n");
        sb.append(formatSampleRows(columnNames, request.getSampleRows()));   // "Row 1: id=1, name=Lamp ..."

        // The output contract, with an example — this is what makes the reply parseable.
        sb.append("\n\nReturn a JSON object mapping column names to descriptions. Example:\n");
        sb.append("{\"col1\": \"Customer's full name\", \"col2\": \"Total order amount in USD\"}\n\n");   // \" = a literal quote inside a Java string
        sb.append("Return ONLY the JSON object, nothing else.");
        return sb.toString();
        // Side note on "prompt injection": the sample VALUES are pasted straight into
        // the prompt. A cell containing text like "ignore previous instructions..."
        // is, to the model, just more prompt. Worst case here is a bad description,
        // not a security hole — but it's the same class of problem every LLM app has.
    }

    // ------------------------------------------------------------------------
    // buildGenerationPrompt(request)   — mode 1: question -> SQL
    //
    // Produces a prompt like:
    //
    //   You are a PostgreSQL SQL expert. Generate a SQL query for the following question.
    //
    //   DATABASE SCHEMA (use ONLY these tables and columns):
    //   Database: shop
    //
    //   Table: products
    //     - id (integer): Unique product identifier
    //     - name (text): Product display name
    //   ...
    //
    //   RULES:
    //   - Use ONLY PostgreSQL syntax
    //   - ...
    //
    //   PREVIOUS QUERIES IN THIS SESSION (for context):
    //   Q: how many products?
    //   SQL: SELECT COUNT(*) FROM products
    //
    //   USER QUESTION: now only the expensive ones
    //
    //   Return a JSON object with exactly two keys:
    //   {"sql": "YOUR SQL QUERY HERE", "explanation": "Brief explanation ..."}
    //
    //   Return ONLY the JSON object, nothing else.
    // ------------------------------------------------------------------------
    private String buildGenerationPrompt(SqlGenerateRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a PostgreSQL SQL expert. Generate a SQL query for the following question.\n\n");
        sb.append("DATABASE SCHEMA (use ONLY these tables and columns):\n");
        sb.append(request.getSchemaContext()).append("\n\n");   // pre-rendered by the CLI (SchemaContext.toSchemaText())
        sb.append("RULES:\n");
        sb.append("- Use ONLY PostgreSQL syntax\n");
        sb.append("- Use ONLY the tables and columns listed above — do NOT invent any\n");   // fights "hallucinated" columns
        sb.append("- Generate ONLY SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)\n");   // a request, not a guarantee — the CLI validator enforces it
        sb.append("- Use proper JOIN conditions based on foreign key relationships\n");
        // ⚠ Interesting conflict: this rule invites the model to use ALIASES
        // ("FROM products p ... p.name"), but the CLI's ValidatorService does NOT
        // understand aliases and reports "Unknown table in column reference: p".
        // So aliased SQL always burns at least one repair round. See
        // ValidatorService.java for the details.
        // (Also note: the schema text the CLI sends contains no foreign-key info,
        // so the model has to GUESS join columns from names like category_id.)
        sb.append("- Always qualify column names with table names/aliases when joining\n\n");

        // Session memory: the last <=5 (question, sql) pairs, so follow-ups work.
        List<QueryHistory> previousQueries = request.getPreviousQueries();
        if (previousQueries != null && !previousQueries.isEmpty()) {
            sb.append("PREVIOUS QUERIES IN THIS SESSION (for context):\n");
            for (QueryHistory history : previousQueries) {   // oldest first
                sb.append("Q: ").append(history.getQuestion()).append('\n');
                sb.append("SQL: ").append(history.getSql()).append("\n\n");
            }
        }

        sb.append("USER QUESTION: ").append(request.getQuestion()).append("\n\n");   // the actual question, LAST, so it's freshest in the model's "mind"
        sb.append("Return a JSON object with exactly two keys:\n");
        sb.append("{\"sql\": \"YOUR SQL QUERY HERE\", \"explanation\": \"Brief explanation of what this query does\"}\n\n");
        sb.append("Return ONLY the JSON object, nothing else.");
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // buildRepairPrompt(request)   — mode 2: broken SQL + errors -> fixed SQL
    //
    // Produces a prompt like:
    //
    //   You are a PostgreSQL SQL expert. The following SQL query has validation errors. Fix them.
    //
    //   DATABASE SCHEMA:
    //   ...
    //
    //   ORIGINAL QUESTION: top 5 products by price
    //
    //   PREVIOUS SQL (has errors):
    //   SELECT nme FROM products ORDER BY price DESC LIMIT 5
    //
    //   VALIDATION ERRORS:
    //   1. Unknown column 'nme' in referenced tables
    //
    //   Fix the SQL to resolve ALL errors. Use ONLY tables and columns from the schema above.
    //
    //   Return a JSON object: {"sql": "FIXED SQL", "explanation": "..."}
    //   Return ONLY the JSON object.
    //
    // This is the clever bit of the harness: the model gets the EXACT,
    // machine-checked reason its SQL was rejected, which is far more useful
    // than "try again". Note the repair prompt does NOT include session history.
    // ------------------------------------------------------------------------
    private String buildRepairPrompt(SqlGenerateRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a PostgreSQL SQL expert. The following SQL query has validation errors. Fix them.\n\n");
        sb.append("DATABASE SCHEMA:\n");
        sb.append(request.getSchemaContext()).append("\n\n");
        sb.append("ORIGINAL QUESTION: ").append(request.getQuestion()).append("\n\n");   // what the SQL is supposed to do
        sb.append("PREVIOUS SQL (has errors):\n");
        sb.append(request.getPreviousSql()).append("\n\n");                                 // the rejected SQL
        sb.append("VALIDATION ERRORS:\n");

        List<String> errors = request.getValidationErrors();
        if (errors != null) {
            for (int i = 0; i < errors.size(); i++) {
                sb.append(i + 1).append(". ").append(errors.get(i)).append('\n');   // numbered list "1. ...", "2. ..."
            }
        }

        sb.append("\nFix the SQL to resolve ALL errors. Use ONLY tables and columns from the schema above.\n\n");
        sb.append("Return a JSON object: {\"sql\": \"FIXED SQL\", \"explanation\": \"What was fixed and what the query does\"}\n");
        sb.append("Return ONLY the JSON object.");
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    // formatColumnsAndTypes(names, types)
    //
    // ["id","name"] + ["integer","text"]  ->  "id (integer), name (text)"
    //
    // IntStream.range(0, n) produces 0, 1, ..., n-1 — a stream of INDEXES, so
    // we can walk the two parallel lists together. mapToObj turns each index
    // into a String, and Collectors.joining(", ") glues them with commas.
    // If the types list is shorter than the names list, "unknown" is used.
    // ------------------------------------------------------------------------
    private String formatColumnsAndTypes(List<String> columnNames, List<String> columnTypes) {
        return IntStream.range(0, columnNames.size())
                .mapToObj(i -> {
                    String name = columnNames.get(i);                                      // i-th column name
                    String type = i < columnTypes.size() ? columnTypes.get(i) : "unknown"; // matching type, if any
                    return name + " (" + type + ")";                                       // "name (type)"
                })
                .collect(Collectors.joining(", "));
    }

    // ------------------------------------------------------------------------
    // formatSampleRows(names, rows)
    //
    // Turns the grid of sample values into readable "key=value" lines:
    //     Row 1: id=1, name=Lamp, price=19.99
    //     Row 2: id=2, name=Desk, price=120.00
    // Putting the column name next to every value makes it much easier for the
    // model to see which value belongs to which column.
    // ------------------------------------------------------------------------
    private String formatSampleRows(List<String> columnNames, List<List<String>> sampleRows) {
        if (sampleRows == null || sampleRows.isEmpty()) {
            return "(no sample data)\n";   // empty table: the model has to guess from names/types alone
        }

        StringBuilder sb = new StringBuilder();
        for (int rowIndex = 0; rowIndex < sampleRows.size(); rowIndex++) {
            List<String> row = sampleRows.get(rowIndex);
            sb.append("Row ").append(rowIndex + 1).append(": ");   // human numbering starts at 1
            List<String> pairs = new ArrayList<>();
            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                String colName = columnNames.get(colIndex);
                String value = colIndex < row.size() ? row.get(colIndex) : "";   // guard against short rows
                pairs.add(colName + "=" + value);                                  // "price=19.99"
            }
            sb.append(String.join(", ", pairs)).append('\n');   // join the pairs with ", "
        }
        return sb.toString();
        // Note: long values (e.g. a 5 KB text column) are pasted in full. Ten rows
        // of a wide table with big text fields can make a very large prompt
        // (slower, more expensive, may exceed the model's context window).
    }

    // ========================================================================
    // THE HTTP CALL
    // ========================================================================
    // callChatCompletion(prompt)
    //
    // Sends ONE chat-completions request and returns the model's reply text
    // (choices[0].message.content). Throws IllegalStateException on any error.
    // ------------------------------------------------------------------------
    private String callChatCompletion(String prompt) {
        // Fail fast with a clear message if no key is configured (default in
        // application.yml is an empty string).
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("OpenAI API key is not configured (llm.api-key)");
        }

        // Build the JSON body as a plain Map. RestTemplate will turn it into JSON
        // using Jackson (because we'll set Content-Type: application/json).
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", model);          // which model to use
        // temperature: 0 = always pick the most likely next token (near
        // deterministic), ~1 = creative/random. 0.1 = "be consistent" — what you
        // want for code generation.
        requestBody.put("temperature", 0.1);
        // Upper bound on the reply length (in tokens, ~3/4 of a word each).
        // ⚠ Some newer OpenAI reasoning models reject "max_tokens" (they want
        // "max_completion_tokens") and/or a custom temperature. With gpt-4o-mini
        // (the default) and most compatible providers this works. If you switch
        // OPENAI_MODEL and get a 400 error, this is the first thing to check.
        requestBody.put("max_tokens", 2000);

        // The conversation: a "system" message (sets behaviour/role) and a "user"
        // message (our big prompt). List.of / Map.of create IMMUTABLE collections.
        List<Map<String, String>> messages = List.of(
                Map.of("role", "system", "content", "You are a PostgreSQL database expert."),
                Map.of("role", "user", "content", prompt)
        );
        requestBody.put("messages", messages);
        // (Possible improvement: OpenAI and several compatible APIs accept
        //  "response_format": {"type": "json_object"}, which forces the reply to be
        //  valid JSON. That would make stripMarkdownCodeBlocks mostly unnecessary.)

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);   // "Content-Type: application/json"
        headers.setBearerAuth(apiKey);                        // "Authorization: Bearer sk-..."

        // Build the full URL, tolerating a trailing slash in the configured base URL:
        //   "https://api.openai.com/v1"  -> ".../v1/chat/completions"
        //   "https://api.openai.com/v1/" -> ".../v1/chat/completions"
        String url = apiUrl.endsWith("/") ? apiUrl + "chat/completions" : apiUrl + "/chat/completions";
        log.debug("Calling LLM API at {} with model {}", url, model);

        try {
            // HttpEntity = body + headers bundled together.
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);
            // POST it. String.class = "give me the response body as a raw String";
            // we parse it ourselves on the next line. Throws:
            //   HttpClientErrorException (4xx: bad key 401, rate limit 429, bad request 400)
            //   HttpServerErrorException (5xx), ResourceAccessException (network/IO)
            // — all subclasses of RestClientException.
            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);

            JsonNode root = objectMapper.readTree(response.getBody());   // parse the API's JSON envelope
            // Walk to choices[0].message.content. path(0) = first array element.
            // Every path() call is null-safe (MissingNode if absent).
            JsonNode contentNode = root.path("choices").path(0).path("message").path("content");
            if (contentNode.isMissingNode() || contentNode.asText().isBlank()) {
                // Quirk: this exception is thrown INSIDE the try, so the generic
                // `catch (Exception e)` below catches it and wraps it again. The
                // final message becomes
                //   "Failed to process LLM API response: LLM response did not contain message content"
                // Not wrong, just double-wrapped.
                throw new IllegalStateException("LLM response did not contain message content");
            }
            return contentNode.asText();   // the model's reply text (hopefully JSON)
        } catch (RestClientException e) {
            // HTTP-level failure. e.getMessage() usually includes the status code and
            // the body, e.g. "401 Unauthorized: {"error": {"message": "Incorrect API key..."}}".
            throw new IllegalStateException("LLM API call failed: " + e.getMessage(), e);
        } catch (Exception e) {
            // Anything else (e.g. the envelope wasn't JSON).
            throw new IllegalStateException("Failed to process LLM API response: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------------
    // stripMarkdownCodeBlocks(content)
    //
    // Even when told "return ONLY the JSON object", chat models very often wrap
    // their answer in a Markdown code fence:
    //
    //     ```json
    //     {"sql": "SELECT ...", "explanation": "..."}
    //     ```
    //
    // which is not valid JSON. This method removes such a fence:
    //   1. trim whitespace
    //   2. if it doesn't START with ```, return it as-is (already plain JSON)
    //   3. drop the first line (the "```json" line)
    //   4. drop a trailing ``` if present
    //   5. trim again
    //
    // LIMITATIONS: it only handles a fence at the very START. A reply like
    // "Here you go:\n```json\n{...}\n```" doesn't start with ```, is returned
    // unchanged, and JSON parsing then fails (-> success=false for SQL,
    // exception for descriptions). A more forgiving approach would be to
    // extract the substring from the first '{' to the last '}'.
    // ------------------------------------------------------------------------
    private String stripMarkdownCodeBlocks(String content) {
        if (content == null) {
            return "";   // nothing to strip; parsing "" will then fail with a clear error
        }

        String trimmed = content.trim();
        if (!trimmed.startsWith("```")) {
            return trimmed;   // no fence -> assume it's already the JSON
        }

        int firstNewline = trimmed.indexOf('\n');           // end of the "```json" line
        if (firstNewline > 0) {
            trimmed = trimmed.substring(firstNewline + 1);  // keep everything after it
        }
        // (If there's no newline at all, e.g. "```{...}```", the opening fence is
        //  NOT removed and parsing will fail. Rare in practice.)

        if (trimmed.endsWith("```")) {
            trimmed = trimmed.substring(0, trimmed.length() - 3);   // chop the closing fence
        }

        return trimmed.trim();
    }
}
