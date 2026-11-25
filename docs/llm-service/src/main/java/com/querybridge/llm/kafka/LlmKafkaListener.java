package com.querybridge.llm.kafka;   // Kafka-facing classes of the LLM service

import com.querybridge.common.dto.DescriptionRequest;    // in  (topic description.request)
import com.querybridge.common.dto.DescriptionResponse;   // out (topic description.response)
import com.querybridge.common.dto.SqlGenerateRequest;    // in  (topic sql.generate.request)
import com.querybridge.common.dto.SqlGenerateResponse;   // out (topic sql.generate.response)
import com.querybridge.llm.service.LlmService;           // does the real work
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

// ============================================================================
// LlmKafkaListener — the front door of the LLM service. (guide step 22)
//
// Same role as ExecutorKafkaListener (read that one first), but with TWO
// listener methods, one per incoming topic:
//
//   "description.request"  --> onDescriptionRequest  --> "description.response"
//   "sql.generate.request" --> onSqlGenerateRequest  --> "sql.generate.response"
//
// Each @KafkaListener gets its OWN listener container (its own consumer and
// thread), so a description request and an SQL request can be handled at the
// same time. Within one topic, requests are handled one at a time.
//
// Unlike the executor, replies here are sent WITH a Kafka key
// (key = correlationId). The CLI doesn't use the key (it reads the
// correlationId from the JSON body), so this is just a nicer habit: keys show
// up in Kafka tooling and would keep related messages on the same partition
// if topics had several.
// ============================================================================
@Component
public class LlmKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(LlmKafkaListener.class);

    private final LlmService llmService;                         // prompt + HTTP + parsing
    private final KafkaTemplate<String, Object> kafkaTemplate;   // for sending replies

    // Constructor injection (see ExecutorKafkaListener for the explanation).
    public LlmKafkaListener(LlmService llmService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.llmService = llmService;
        this.kafkaTemplate = kafkaTemplate;
    }

    // ------------------------------------------------------------------------
    // onDescriptionRequest(request)
    //
    // One call per table, during the CLI's `connect`. Builds a
    // DescriptionResponse and ALWAYS sends it — on success with the
    // descriptions, on failure with success=false + error. Always replying is
    // what lets the CLI move on to the next table immediately instead of
    // waiting out its 30-second timeout.
    //
    // The try/catch lives HERE because LlmService.generateDescriptions throws
    // on failure (unlike generateSql, which catches internally).
    // ------------------------------------------------------------------------
    @KafkaListener(topics = "description.request", containerFactory = "descriptionRequestFactory")
    public void onDescriptionRequest(DescriptionRequest request) {
        log.info("Received description request for table {} (correlationId={})",
                request.getTableName(), request.getCorrelationId());

        DescriptionResponse response = new DescriptionResponse();   // empty response...
        response.setCorrelationId(request.getCorrelationId());      // ...echo the ID (critical for matching)
        response.setTableName(request.getTableName());              // ...and the table name (for readability)

        try {
            Map<String, String> descriptions = llmService.generateDescriptions(request);   // may take seconds (HTTP to the LLM)
            response.setColumnDescriptions(descriptions);
            response.setSuccess(true);
        } catch (Exception e) {
            // SLF4J trick: when the LAST argument is a Throwable and there's no {}
            // for it, it's logged as the exception with full stack trace.
            log.error("Failed to generate descriptions for table {} (correlationId={})",
                    request.getTableName(), request.getCorrelationId(), e);
            response.setSuccess(false);
            response.setError(e.getMessage());
        }

        // send(topic, KEY, value). Asynchronous, not awaited (same caveat as in the
        // executor: a failed send is silently lost and the CLI times out).
        kafkaTemplate.send("description.response", request.getCorrelationId(), response);
        log.info("Sent description response for correlationId={}", request.getCorrelationId());
    }

    // ------------------------------------------------------------------------
    // onSqlGenerateRequest(request)
    //
    // Handles BOTH fresh generation and repair — the `repair` flag inside the
    // request decides which prompt LlmService builds. No try/catch needed:
    // generateSql never throws (it returns success=false instead).
    // ------------------------------------------------------------------------
    @KafkaListener(topics = "sql.generate.request", containerFactory = "sqlRequestFactory")
    public void onSqlGenerateRequest(SqlGenerateRequest request) {
        log.info("Received SQL generate request (correlationId={}, repair={})",
                request.getCorrelationId(), request.isRepair());

        SqlGenerateResponse response = llmService.generateSql(request);                 // prompt -> LLM -> parsed response
        kafkaTemplate.send("sql.generate.response", request.getCorrelationId(), response); // reply, keyed by correlationId
        log.info("Sent SQL generate response for correlationId={}", request.getCorrelationId());
    }
}
