package com.querybridge.executor.kafka;   // Kafka-related classes of the executor

import com.querybridge.common.dto.QueryExecuteRequest;                       // the message type we RECEIVE
import org.apache.kafka.clients.consumer.ConsumerConfig;                     // constants for consumer settings (raw Kafka client)
import org.apache.kafka.clients.producer.ProducerConfig;                     // constants for producer settings (raw Kafka client)
import org.apache.kafka.common.serialization.StringDeserializer;             // bytes -> String (for message KEYS)
import org.apache.kafka.common.serialization.StringSerializer;               // String -> bytes (for message KEYS)
import org.springframework.beans.factory.annotation.Value;                   // inject values from application.yml
import org.springframework.context.annotation.Bean;                          // "this method creates a bean"
import org.springframework.context.annotation.Configuration;                 // "this class holds @Bean methods"
import org.springframework.kafka.annotation.EnableKafka;                     // turn on @KafkaListener processing
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory; // builds listener containers
import org.springframework.kafka.core.ConsumerFactory;                       // makes KafkaConsumer instances
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;           // the standard ConsumerFactory impl
import org.springframework.kafka.core.DefaultKafkaProducerFactory;           // the standard ProducerFactory impl
import org.springframework.kafka.core.KafkaTemplate;                         // Spring's "send a message" helper
import org.springframework.kafka.core.ProducerFactory;                       // makes KafkaProducer instances
import org.springframework.kafka.support.serializer.JsonDeserializer;        // JSON bytes -> Java object (uses Jackson)
import org.springframework.kafka.support.serializer.JsonSerializer;          // Java object -> JSON bytes (uses Jackson)

import java.util.HashMap;   // config maps
import java.util.Map;

// ============================================================================
// KafkaConfig (query-executor) — how this service sends and receives Kafka
// messages. (guide step 13 — THE Kafka config to study; the other two
// services' KafkaConfig classes are variations of this one.)
//
// THE FOUR SPRING-KAFKA BUILDING BLOCKS (learn these names, they're everywhere)
//
//   SENDING side:
//     ProducerFactory  — a factory that knows HOW to create a raw Kafka
//                        producer: which broker, how to serialize keys/values.
//     KafkaTemplate    — the friendly API you actually call:
//                        kafkaTemplate.send(topic, key, value). It uses the
//                        ProducerFactory internally (and reuses one producer).
//
//   RECEIVING side:
//     ConsumerFactory  — knows HOW to create a raw Kafka consumer: broker,
//                        group id, where to start reading, how to deserialize.
//     ListenerContainerFactory
//                      — builds the "listener container" for each
//                        @KafkaListener method. The container is the
//                        machinery that runs a background thread doing
//                        consumer.poll() in a loop, deserializes each record,
//                        calls your method, and commits offsets. You never
//                        write that loop yourself; that's the whole point.
//
// WHY CONFIGURE THIS BY HAND?
//   Spring Boot can auto-configure ALL of this from application.yml alone
//   (spring.kafka.producer.value-serializer=..., etc.). As soon as you define
//   your own ProducerFactory / ConsumerFactory / KafkaTemplate beans, Boot's
//   auto-configuration "backs off" (its beans are @ConditionalOnMissingBean)
//   and uses yours. The author chose the explicit Java way so that each
//   listener can have a consumer that deserializes straight into ONE specific
//   DTO class — here, QueryExecuteRequest.
//
// KEY vs VALUE
//   Every Kafka record has an optional KEY and a VALUE. Here keys are Strings
//   (the correlationId, when the sender sets one) and values are our DTOs as
//   JSON. The key matters for partitioning (same key -> same partition) but
//   with 1 partition per topic in this project, it hardly matters.
// ============================================================================
@Configuration   // Spring: run the @Bean methods below and register what they return
@EnableKafka     // Turn on detection of @KafkaListener methods. (Spring Boot's Kafka
                 // auto-configuration would enable this anyway; declaring it is harmless
                 // and makes the intent obvious.)
public class KafkaConfig {

    // Injected from application.yml: spring.kafka.bootstrap-servers.
    // e.g. "kafka:9092" in Docker, "localhost:9092" by default.
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Injected from application.yml: spring.kafka.consumer.group-id = "query-executor".
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // ------------------------------------------------------------------------
    // producerFactory()
    //
    // Describes how to build a Kafka PRODUCER (the thing that sends messages).
    // The executor needs one because after running a query it SENDS a
    // QueryExecuteResponse to "query.execute.response".
    //
    // The generic type <String, Object> means: keys are Strings, values can be
    // ANY object — JsonSerializer will turn whatever we pass into JSON. Using
    // Object (not QueryExecuteResponse) lets one producer send any DTO.
    // ------------------------------------------------------------------------
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();                                   // raw Kafka client properties
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);          // "bootstrap.servers" — where the broker is
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // keys: String -> UTF-8 bytes
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // values: object -> JSON bytes (Jackson)
        // Side effect worth knowing: Spring's JsonSerializer by default also adds a
        // Kafka HEADER called "__TypeId__" containing the Java class name of the
        // value (e.g. com.querybridge.common.dto.QueryExecuteResponse). The
        // receiving side here is configured to IGNORE that header (see below).
        return new DefaultKafkaProducerFactory<>(config);                               // the standard implementation
    }

    // ------------------------------------------------------------------------
    // kafkaTemplate()
    //
    // The object our code actually uses to send: kafkaTemplate.send(topic, value).
    // ExecutorKafkaListener gets this injected through its constructor.
    // KafkaTemplate is thread-safe and reuses a single underlying producer.
    // ------------------------------------------------------------------------
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        // Calling producerFactory() here does NOT create a second factory: in a
        // @Configuration class, Spring intercepts calls between @Bean methods and
        // returns the already-created singleton bean. (This is the "CGLIB proxy"
        // trick of @Configuration classes.)
        return new KafkaTemplate<>(producerFactory());
    }

    // ------------------------------------------------------------------------
    // executeRequestConsumerFactory()
    //
    // Describes how to build a Kafka CONSUMER that reads QueryExecuteRequest
    // messages. Typed <String, QueryExecuteRequest>: key is a String, value is
    // deserialized straight into our DTO.
    // ------------------------------------------------------------------------
    @Bean
    public ConsumerFactory<String, QueryExecuteRequest> executeRequestConsumerFactory() {
        Map<String, Object> config = new HashMap<>();                          // raw Kafka consumer properties
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // where the broker is
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);                   // "query-executor" — our consumer group
        // auto.offset.reset = what to do when this group has NO committed offset
        // yet for a partition (i.e. the very first time this group ever reads the
        // topic, or after offsets expire).
        //   "earliest" -> start from the oldest message still kept in the topic
        //   "latest"   -> ignore everything already there, only read new messages
        // "earliest" means: if requests were sent before the executor ever
        // started, it will still process them. Downside: those may be stale (the
        // CLI that sent them has probably timed out and moved on), so the work is
        // wasted — but harmless, because the CLI ignores replies nobody waits for.
        // Once offsets ARE committed, this setting no longer applies: the group
        // simply resumes where it left off.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // The value deserializer: JSON bytes -> QueryExecuteRequest.
        //   arg 1: the target class to build.
        //   arg 2: useHeadersIfPresent = false -> IGNORE the "__TypeId__" header
        //          the sender's JsonSerializer added; always build a
        //          QueryExecuteRequest. This makes the consumer robust: it doesn't
        //          care what class name the producer wrote in the header.
        JsonDeserializer<QueryExecuteRequest> deserializer = new JsonDeserializer<>(QueryExecuteRequest.class, false);
        // "Trusted packages": a security feature. When the deserializer DOES use
        // type headers, it refuses to instantiate classes outside trusted packages
        // (otherwise a malicious message could make it instantiate arbitrary
        // classes). Since we've told it to ignore headers and use a fixed class,
        // this line is mostly belt-and-braces here.
        deserializer.addTrustedPackages("com.querybridge.common.dto");

        // Pass deserializer INSTANCES (instead of class names in the config map).
        // Key deserializer = plain String; value deserializer = our JSON one.
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
        // ⚠ ROBUSTNESS NOTE: this JsonDeserializer is not wrapped in Spring's
        // ErrorHandlingDeserializer. If someone ever puts a non-JSON or malformed
        // message on "query.execute.request", deserialization fails INSIDE the
        // poll loop, before your listener method is reached. Spring Kafka's docs
        // explicitly recommend ErrorHandlingDeserializer for exactly this case
        // (a "poison pill" message); without it the container can keep failing
        // on the same record. I'm fairly — not 100% — sure of the exact
        // behaviour in this spring-kafka version (3.2.4), but the recommended fix
        // is well established. Our own producers always send valid JSON, so it
        // doesn't bite in normal use.
    }

    // ------------------------------------------------------------------------
    // executeRequestFactory()
    //
    // The listener CONTAINER factory. ExecutorKafkaListener's @KafkaListener
    // refers to it BY BEAN NAME:
    //     @KafkaListener(topics = "query.execute.request",
    //                    containerFactory = "executeRequestFactory")
    // The bean name is the METHOD NAME — that's how @Bean names work by default.
    //
    // "Concurrent" = it can run N consumer threads (setConcurrency(n)). We don't
    // call setConcurrency, so it's 1 thread: requests are processed ONE AT A TIME,
    // in order. With 1 partition per topic, more threads wouldn't help anyway
    // (a partition is read by at most one consumer in a group).
    // ------------------------------------------------------------------------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, QueryExecuteRequest> executeRequestFactory() {
        ConcurrentKafkaListenerContainerFactory<String, QueryExecuteRequest> factory =
                new ConcurrentKafkaListenerContainerFactory<>();          // create the container factory
        factory.setConsumerFactory(executeRequestConsumerFactory());      // containers it builds use OUR consumer config
        return factory;
    }
}
