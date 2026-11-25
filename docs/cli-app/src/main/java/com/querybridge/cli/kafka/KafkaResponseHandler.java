package com.querybridge.cli.kafka;

import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.common.dto.SqlGenerateResponse;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// ============================================================================
// KafkaResponseHandler — receives ALL replies coming back to the CLI and
// hands each one to the command that's waiting for it. (guide step 36)
//
// This is the "listener thread" side of the picture in
// KafkaProducerService.java. Every method here does the same one thing:
//     producerService.completeRequest(response.getCorrelationId(), response);
// i.e. "find whoever is waiting for this ID and give them this object".
// Three methods only because there are three reply TYPES on three topics,
// each needing its own typed deserializer (container factory).
//
// THE groupId ATTRIBUTE
//   Each @KafkaListener sets its own consumer group, which OVERRIDES the
//   group.id in the consumer factory ("cli-app" from application.yml). So the
//   CLI has three consumer groups: cli-desc-response, cli-sql-response,
//   cli-exec-response.
//
// ⚠ LIMITATION: ONLY ONE CLI AT A TIME
//   Consumer groups SHARE work: within a group, each partition is read by
//   exactly one consumer. These group names are fixed, so if you run TWO CLI
//   instances, both join e.g. "cli-sql-response" and Kafka gives the topic's
//   single partition to only ONE of them. The other CLI never sees ANY
//   replies and every request it makes times out. (Even with more partitions
//   it'd be broken: a reply could land at the CLI that didn't ask.)
//   For "every instance must see every reply", each CLI instance needs its
//   OWN group, e.g. groupId = "cli-sql-" + a random UUID — or use Spring
//   Kafka's ReplyingKafkaTemplate. For a single-user tool, one CLI is fine.
// ============================================================================
@Component
public class KafkaResponseHandler {

    private final KafkaProducerService producerService;   // where the pending-request map lives

    public KafkaResponseHandler(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    // Replies to column-description requests (sent during `connect`).
    @KafkaListener(topics = "description.response", containerFactory = "descriptionResponseFactory",
                   groupId = "cli-desc-response")
    public void onDescriptionResponse(DescriptionResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);   // wake up QueryBridgeCommands.connect
    }

    // Replies to SQL generate/repair requests (sent during `ask`).
    @KafkaListener(topics = "sql.generate.response", containerFactory = "sqlResponseFactory",
                   groupId = "cli-sql-response")
    public void onSqlResponse(SqlGenerateResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);   // wake up QueryBridgeCommands.generateSql
    }

    // Replies to execute requests (sent after you confirm "Execute? [Y/n]").
    @KafkaListener(topics = "query.execute.response", containerFactory = "executeResponseFactory",
                   groupId = "cli-exec-response")
    public void onExecuteResponse(QueryExecuteResponse response) {
        producerService.completeRequest(response.getCorrelationId(), response);   // wake up QueryBridgeCommands.executeQuery
    }
}
