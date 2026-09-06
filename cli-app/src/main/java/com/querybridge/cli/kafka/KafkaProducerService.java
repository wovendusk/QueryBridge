package com.querybridge.cli.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @SuppressWarnings("unchecked")
    public <T> T sendAndWait(String topic, Object payload, String correlationId, long timeoutSeconds) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(correlationId, future);

        try {
            kafkaTemplate.send(topic, correlationId, payload).get(10, TimeUnit.SECONDS);
            Object result = future.get(timeoutSeconds, TimeUnit.SECONDS);
            return (T) result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka request interrupted for correlationId: " + correlationId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Kafka request timed out or failed for correlationId: " + correlationId, e);
        } finally {
            pendingRequests.remove(correlationId);
        }
    }

    public void completeRequest(String correlationId, Object response) {
        CompletableFuture<Object> future = pendingRequests.get(correlationId);
        if (future != null) {
            future.complete(response);
        }
    }
}
