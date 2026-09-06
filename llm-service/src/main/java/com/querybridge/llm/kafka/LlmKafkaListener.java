package com.querybridge.llm.kafka;

import com.querybridge.common.dto.DescriptionRequest;
import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.SqlGenerateRequest;
import com.querybridge.common.dto.SqlGenerateResponse;
import com.querybridge.llm.service.LlmService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class LlmKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(LlmKafkaListener.class);

    private final LlmService llmService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public LlmKafkaListener(LlmService llmService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.llmService = llmService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "description.request", containerFactory = "descriptionRequestFactory")
    public void onDescriptionRequest(DescriptionRequest request) {
        log.info("Received description request for table {} (correlationId={})",
                request.getTableName(), request.getCorrelationId());

        DescriptionResponse response = new DescriptionResponse();
        response.setCorrelationId(request.getCorrelationId());
        response.setTableName(request.getTableName());

        try {
            Map<String, String> descriptions = llmService.generateDescriptions(request);
            response.setColumnDescriptions(descriptions);
            response.setSuccess(true);
        } catch (Exception e) {
            log.error("Failed to generate descriptions for table {} (correlationId={})",
                    request.getTableName(), request.getCorrelationId(), e);
            response.setSuccess(false);
            response.setError(e.getMessage());
        }

        kafkaTemplate.send("description.response", request.getCorrelationId(), response);
        log.info("Sent description response for correlationId={}", request.getCorrelationId());
    }

    @KafkaListener(topics = "sql.generate.request", containerFactory = "sqlRequestFactory")
    public void onSqlGenerateRequest(SqlGenerateRequest request) {
        log.info("Received SQL generate request (correlationId={}, repair={})",
                request.getCorrelationId(), request.isRepair());

        SqlGenerateResponse response = llmService.generateSql(request);
        kafkaTemplate.send("sql.generate.response", request.getCorrelationId(), response);
        log.info("Sent SQL generate response for correlationId={}", request.getCorrelationId());
    }
}
