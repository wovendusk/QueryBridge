package com.querybridge.cli.kafka;   // the CLI's Kafka classes

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.querybridge.common.dto.DescriptionResponse;    // reply type #1
import com.querybridge.common.dto.QueryExecuteResponse;   // reply type #3
import com.querybridge.common.dto.SqlGenerateResponse;    // reply type #2

import org.apache.kafka.clients.admin.NewTopic;           // "please make sure this topic exists"

import java.util.HashMap;
import java.util.Map;

// ============================================================================
// KafkaConfig (cli-app) — topics, one producer, THREE typed consumers.
// (guide step 34)
//
// PREREQUISITES: query-executor/kafka/KafkaConfig.java (the building blocks)
// and llm-service/kafka/KafkaConfig.java (the generic createConsumerFactory
// helper). This file is the mirror image of the LLM service's: that one
// RECEIVES requests and SENDS responses; this one SENDS requests and
// RECEIVES responses — three kinds:
//     "description.response"   -> DescriptionResponse
//     "sql.generate.response"  -> SqlGenerateResponse
//     "query.execute.response" -> QueryExecuteResponse
//
// TWO THINGS ARE NEW HERE
//   1. NewTopic beans — the CLI is the one that CREATES all six topics.
//   2. auto.offset.reset = "latest" (the services use "earliest"). Explained
//      on createConsumerFactory below.
// ============================================================================
@Configuration
@EnableKafka
public class KafkaConfig {

    // ------------------------------------------------------------------------
    // TOPIC DECLARATIONS
    //
    // A NewTopic bean doesn't create anything by itself. Spring Boot
    // auto-configures a KafkaAdmin bean (an admin client), and at startup
    // KafkaAdmin collects every NewTopic bean and creates the ones that don't
    // exist yet. Topics that already exist are left alone.
    //
    // new NewTopic(name, numPartitions, replicationFactor):
    //   numPartitions = 1      -> one ordered log per topic. Simple; it also
    //                             means at most ONE consumer per group can read
    //                             it at a time (no parallelism).
    //   replicationFactor = 1  -> one copy of the data. Required here because
    //                             docker-compose runs a SINGLE broker (you can't
    //                             have more replicas than brokers). In production
    //                             you'd use 3.
    //   (short) 1              -> the constructor's parameter type is `short`,
    //                             so the int literal 1 must be cast.
    //
    // Only the CLI declares these. If the LLM service or executor start BEFORE
    // the CLI has ever run, the topics may not exist yet. They will most likely
    // still work, because Kafka brokers by default auto-create a topic the first
    // time a client asks for it (auto.create.topics.enable defaults to true) —
    // but then it's created with the broker's defaults rather than these
    // settings. With one broker the result is the same anyway.
    //
    // Declaration order below: request/response pairs, in pipeline order.
    // ------------------------------------------------------------------------
    @Bean
    public NewTopic descriptionRequestTopic() { return new NewTopic("description.request", 1, (short) 1); }     // CLI -> LLM
    @Bean
    public NewTopic descriptionResponseTopic() { return new NewTopic("description.response", 1, (short) 1); }   // LLM -> CLI
    @Bean
    public NewTopic sqlGenerateRequestTopic() { return new NewTopic("sql.generate.request", 1, (short) 1); }    // CLI -> LLM
    @Bean
    public NewTopic sqlGenerateResponseTopic() { return new NewTopic("sql.generate.response", 1, (short) 1); }  // LLM -> CLI
    @Bean
    public NewTopic queryExecuteRequestTopic() { return new NewTopic("query.execute.request", 1, (short) 1); }  // CLI -> executor
    @Bean
    public NewTopic queryExecuteResponseTopic() { return new NewTopic("query.execute.response", 1, (short) 1); } // executor -> CLI


    @Value("${spring.kafka.bootstrap-servers}")   // kafka:9092 in Docker; set localhost:29092 when running on the host
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")    // "cli-app" — but overridden per listener, see KafkaResponseHandler
    private String groupId;

    // ------------------------------------------------------------------------
    // producerFactory() + kafkaTemplate() — the same as in both services.
    // Used by KafkaProducerService to send all three request types.
    // ------------------------------------------------------------------------
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);           // broker
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);  // key = correlationId
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);  // value = DTO as JSON
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ------------------------------------------------------------------------
    // One ConsumerFactory per reply type (generic helper at the bottom).
    // ------------------------------------------------------------------------
    @Bean
    public ConsumerFactory<String, DescriptionResponse> descriptionConsumerFactory() {
        return createConsumerFactory(DescriptionResponse.class);
    }

    @Bean
    public ConsumerFactory<String, SqlGenerateResponse> sqlResponseConsumerFactory() {
        return createConsumerFactory(SqlGenerateResponse.class);
    }

    @Bean
    public ConsumerFactory<String, QueryExecuteResponse> executeResponseConsumerFactory() {
        return createConsumerFactory(QueryExecuteResponse.class);
    }

    // ------------------------------------------------------------------------
    // One ListenerContainerFactory per ConsumerFactory. Bean names are what
    // KafkaResponseHandler's @KafkaListener(containerFactory = "...") refer to:
    //   "descriptionResponseFactory", "sqlResponseFactory", "executeResponseFactory".
    // ------------------------------------------------------------------------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DescriptionResponse> descriptionResponseFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DescriptionResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(descriptionConsumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SqlGenerateResponse> sqlResponseFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SqlGenerateResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sqlResponseConsumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, QueryExecuteResponse> executeResponseFactory() {
        ConcurrentKafkaListenerContainerFactory<String, QueryExecuteResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(executeResponseConsumerFactory());
        return factory;
    }

    // ------------------------------------------------------------------------
    // createConsumerFactory(type) — same generic helper as in the LLM service,
    // with ONE important difference: auto.offset.reset = "latest".
    //
    // WHY "latest" FOR THE CLI?
    //   The CLI only cares about replies to requests it sends FROM NOW ON. Old
    //   replies in the topic (from previous sessions) are useless: nobody is
    //   waiting for them. "latest" means: the first time this consumer group
    //   reads a partition (no committed offset yet), start at the END and skip
    //   history.
    //
    //   Remember auto.offset.reset only applies when there's NO committed offset.
    //   The CLI's groups have FIXED names ("cli-sql-response" etc.), so from the
    //   second run on, offsets ARE committed and the CLI resumes where it left
    //   off — it will read replies that arrived while it was closed. That's
    //   harmless: KafkaProducerService.completeRequest finds no waiting future
    //   for those old correlationIds and silently ignores them.
    //
    //   (A subtle timing edge: with "latest", if a reply were published BEFORE
    //   the consumer had finished joining its group and getting its partition
    //   assigned, it would be skipped. In practice you type `connect` long after
    //   startup, so the consumers are ready.)
    // ------------------------------------------------------------------------
    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> type) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);                  // fallback group; listeners override it
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");        // skip old replies on first read

        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type, false);   // fixed class, ignore type headers
        deserializer.addTrustedPackages("com.querybridge.common.dto");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }
}
