package com.querybridge.executor.kafka;   // Kafka-facing classes of the executor

import com.querybridge.common.dto.QueryExecuteRequest;    // what we receive
import com.querybridge.common.dto.QueryExecuteResponse;   // what we send back
import com.querybridge.executor.service.QueryService;     // does the actual JDBC work
import org.slf4j.Logger;                                  // logging API (Spring Boot backs it with Logback)
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;   // "call this method for each message on a topic"
import org.springframework.kafka.core.KafkaTemplate;         // for sending the reply
import org.springframework.stereotype.Component;             // makes this class a Spring bean

// ============================================================================
// ExecutorKafkaListener — the "front door" of the query-executor service.
// (guide step 15)
//
// THE WHOLE SERVICE IN ONE PICTURE
//
//   CLI ──"query.execute.request"──▶ [ onExecuteRequest() ]
//                                          │
//                                          ▼
//                                   QueryService.execute()   (JDBC to your DB)
//                                          │
//                                          ▼
//   CLI ◀──"query.execute.response"── kafkaTemplate.send(...)
//
// This class is the "SERVER half" of the request-reply-over-Kafka pattern:
// take a request, do the work, publish a reply that carries the SAME
// correlationId. (The "CLIENT half" — sending a request and waiting for the
// matching reply — is in cli-app/kafka/KafkaProducerService.java.)
//
// SEPARATION OF CONCERNS
//   Notice this class has NO JDBC code and QueryService has NO Kafka code.
//   The listener only translates "Kafka message" <-> "method call". That makes
//   QueryService testable without Kafka, and lets you swap the transport
//   (e.g. to REST) without touching the query logic.
// ============================================================================
@Component   // Spring bean; Spring Kafka scans beans for @KafkaListener methods
public class ExecutorKafkaListener {

    // One logger per class is the SLF4J convention. "static final" = shared, created once.
    private static final Logger log = LoggerFactory.getLogger(ExecutorKafkaListener.class);

    private final QueryService queryService;                     // runs the SQL
    private final KafkaTemplate<String, Object> kafkaTemplate;   // sends the response (bean from KafkaConfig)

    // CONSTRUCTOR INJECTION: Spring sees a single constructor and automatically
    // passes in the matching beans (no @Autowired needed since Spring 4.3).
    // Fields are final -> they can never be null or reassigned after construction.
    public ExecutorKafkaListener(QueryService queryService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.queryService = queryService;
        this.kafkaTemplate = kafkaTemplate;
    }

    // ------------------------------------------------------------------------
    // onExecuteRequest(request)
    //
    // Called BY SPRING KAFKA (on its consumer thread, never by our code) once
    // per message on "query.execute.request".
    //
    //   topics = "query.execute.request"
    //       Which topic to subscribe to.
    //   containerFactory = "executeRequestFactory"
    //       Which listener container factory bean to use (by bean name) — the
    //       one in KafkaConfig whose consumer deserializes the JSON value into a
    //       QueryExecuteRequest. That's why the parameter can be typed directly
    //       as QueryExecuteRequest: by the time this method is called, the JSON
    //       has already been turned into the object.
    //   (no groupId attribute)
    //       So the group.id from the consumer factory is used: "query-executor".
    //
    // OFFSETS / "did we process it?"
    //   After this method returns normally, the container commits the offset
    //   (Spring Kafka's default is to commit after each batch of records returned
    //   by poll), meaning "this group has processed up to here". If the service
    //   crashes mid-way, the request would be re-delivered on restart.
    //
    // THREADING
    //   One consumer thread -> requests are handled strictly one after another.
    //   A slow query (up to 30s) blocks every other user's query. Fine for one
    //   person at a terminal; a bottleneck for many users.
    // ------------------------------------------------------------------------
    @KafkaListener(topics = "query.execute.request", containerFactory = "executeRequestFactory")
    public void onExecuteRequest(QueryExecuteRequest request) {
        // {} placeholders are SLF4J's cheap string formatting.
        log.info("Received execute request: {}", request.getCorrelationId());
        // Do the work. Never throws (QueryService catches everything and returns
        // a success=false response instead).
        QueryExecuteResponse response = queryService.execute(request);
        // Publish the reply. Two things to notice:
        //  1. send(topic, value) — NO KEY. The LLM service sends its replies with
        //     key = correlationId; this one doesn't. It still works because the
        //     CLI matches on the correlationId INSIDE the JSON value, not on the
        //     Kafka key. Just an inconsistency between the services.
        //  2. send() is ASYNCHRONOUS: it returns a CompletableFuture right away and
        //     the actual network send happens in the background. We don't wait on
        //     it or check it, so if the send fails, it's silently lost (the CLI
        //     would time out after 30s). The "Sent ..." log below only means
        //     "handed to the producer", not "delivered".
        kafkaTemplate.send("query.execute.response", response);
        log.info("Sent execute response: {}", request.getCorrelationId());
    }
}
