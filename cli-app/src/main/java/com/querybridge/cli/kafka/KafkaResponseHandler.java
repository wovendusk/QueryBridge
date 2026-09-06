package com.querybridge.cli.kafka;

import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.common.dto.SqlGenerateResponse;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaResponseHandler {

    private final KafkaProducerService producerService;

    public KafkaResponseHandler(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @KafkaListener(topics = "description.response", containerFactory = "descriptionResponseFactory",
                   groupId = "cli-desc-response")
    public void onDescriptionResponse(DescriptionResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);
    }

    @KafkaListener(topics = "sql.generate.response", containerFactory = "sqlResponseFactory",
                   groupId = "cli-sql-response")
    public void onSqlResponse(SqlGenerateResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);
    }

    @KafkaListener(topics = "query.execute.response", containerFactory = "executeResponseFactory",
                   groupId = "cli-exec-response")
    public void onExecuteResponse(QueryExecuteResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);
    }
}
