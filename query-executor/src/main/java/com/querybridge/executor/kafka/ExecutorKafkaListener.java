package com.querybridge.executor.kafka;

import com.querybridge.common.dto.QueryExecuteRequest;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.executor.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ExecutorKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(ExecutorKafkaListener.class);

    private final QueryService queryService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ExecutorKafkaListener(QueryService queryService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.queryService = queryService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "query.execute.request", containerFactory = "executeRequestFactory")
    public void onExecuteRequest(QueryExecuteRequest request) {
        log.info("Received execute request: {}", request.getCorrelationId());
        QueryExecuteResponse response = queryService.execute(request);
        kafkaTemplate.send("query.execute.response", response);
        log.info("Sent execute response: {}", request.getCorrelationId());
    }
}
