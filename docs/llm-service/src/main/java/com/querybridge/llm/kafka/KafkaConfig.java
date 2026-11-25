package com.querybridge.llm.kafka;   // Kafka classes of the LLM service

import com.querybridge.common.dto.DescriptionRequest;    // incoming message type #1
import com.querybridge.common.dto.SqlGenerateRequest;    // incoming message type #2
import org.apache.kafka.clients.consumer.ConsumerConfig;            // consumer setting names
import org.apache.kafka.clients.producer.ProducerConfig;            // producer setting names
import org.apache.kafka.common.serialization.StringDeserializer;    // key: bytes -> String
import org.apache.kafka.common.serialization.StringSerializer;      // key: String -> bytes
import org.springframework.beans.factory.annotation.Value;          // read application.yml values
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;   // value: JSON -> DTO
import org.springframework.kafka.support.serializer.JsonSerializer;     // value: DTO -> JSON

import java.util.HashMap;
import java.util.Map;

// ============================================================================
// KafkaConfig (llm-service) — producer + TWO typed consumers.
// (guide step 20)
//
// PREREQUISITE: read query-executor/kafka/KafkaConfig.java first. It explains
// ProducerFactory / KafkaTemplate / ConsumerFactory / ListenerContainerFactory,
// auto.offset.reset, the JSON (de)serializers, type headers and trusted
// packages in detail. This file is the same thing, with one twist:
//
// THE TWIST: TWO INCOMING MESSAGE TYPES
//   This service receives two different DTOs on two different topics:
//       "description.request"  -> DescriptionRequest
//       "sql.generate.request" -> SqlGenerateRequest
//   Because each JsonDeserializer is created for ONE fixed target class (and
//   is told to ignore type headers), we need one ConsumerFactory per DTO, and
//   one ListenerContainerFactory per ConsumerFactory. The two
//   @KafkaListener methods in LlmKafkaListener each pick theirs by bean name:
//       containerFactory = "descriptionRequestFactory"
//       containerFactory = "sqlRequestFactory"
//
//   To avoid copy-pasting the consumer setup twice, it's written once as a
//   GENERIC helper method, createConsumerFactory(Class<T> type).
//
// (An alternative design: one consumer with type headers enabled, which
// would figure out the class from "__TypeId__" automatically. The explicit
// per-type approach used here is more verbose but very predictable.)
//
// CONCURRENCY CONSEQUENCE
//   Two listener containers = two separate consumer threads. So a long SQL
//   generation and a column-description request CAN run in parallel, but two
//   SQL requests can't (each container has concurrency 1).
// ============================================================================
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")   // e.g. kafka:9092
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")    // "llm-service" — shared by both consumers
    private String groupId;

    // ------------------------------------------------------------------------
    // producerFactory() + kafkaTemplate()
    // Identical to the executor's: String keys, JSON values, value type Object
    // so the one template can send both DescriptionResponse and
    // SqlGenerateResponse.
    // ------------------------------------------------------------------------
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);           // broker address
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);  // keys as plain strings
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);  // values as JSON
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());   // singleton factory (see @Configuration proxy note in the executor's file)
    }

    // ------------------------------------------------------------------------
    // One ConsumerFactory per incoming DTO type. Both delegate to the generic
    // helper at the bottom; only the target class differs.
    // ------------------------------------------------------------------------
    @Bean
    public ConsumerFactory<String, DescriptionRequest> descriptionRequestConsumerFactory() {
        return createConsumerFactory(DescriptionRequest.class);   // JSON -> DescriptionRequest
    }

    @Bean
    public ConsumerFactory<String, SqlGenerateRequest> sqlRequestConsumerFactory() {
        return createConsumerFactory(SqlGenerateRequest.class);   // JSON -> SqlGenerateRequest
    }

    // ------------------------------------------------------------------------
    // One ListenerContainerFactory per ConsumerFactory. The bean NAMES
    // ("descriptionRequestFactory", "sqlRequestFactory") are what the
    // @KafkaListener annotations reference.
    // ------------------------------------------------------------------------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DescriptionRequest> descriptionRequestFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DescriptionRequest> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(descriptionRequestConsumerFactory());   // use the DescriptionRequest consumer
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SqlGenerateRequest> sqlRequestFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SqlGenerateRequest> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sqlRequestConsumerFactory());           // use the SqlGenerateRequest consumer
        return factory;
    }

    // ------------------------------------------------------------------------
    // createConsumerFactory(type)  — a GENERIC helper.
    //
    // <T> is a type parameter: call it with DescriptionRequest.class and you
    // get a ConsumerFactory<String, DescriptionRequest>; call it with
    // SqlGenerateRequest.class and you get ConsumerFactory<String,
    // SqlGenerateRequest>. The Class<T> argument exists because of Java's
    // "type erasure": at runtime generics are gone, so JsonDeserializer needs
    // a real Class object to know what to instantiate.
    //
    // Not a @Bean itself (it's private and has a parameter) — just a helper
    // called by the two @Bean methods above.
    // ------------------------------------------------------------------------
    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> type) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);   // broker address
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);                     // "llm-service"
        // "earliest": on first ever start, process requests already waiting in
        // the topic. ⚠ For THIS service that has a real cost: stale requests mean
        // real (billed) LLM API calls whose answers nobody is waiting for any more.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type, false);   // fixed target class, ignore type headers
        deserializer.addTrustedPackages("com.querybridge.common.dto");             // only relevant when headers are used

        // Same poison-pill caveat as the executor: not wrapped in an ErrorHandlingDeserializer.
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }
}
