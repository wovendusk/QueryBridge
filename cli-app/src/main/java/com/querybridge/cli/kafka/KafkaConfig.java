package com.querybridge.cli.kafka;

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

import com.querybridge.common.dto.DescriptionResponse;
import com.querybridge.common.dto.QueryExecuteResponse;
import com.querybridge.common.dto.SqlGenerateResponse;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Bean
    public NewTopic descriptionRequestTopic() { return new NewTopic("description.request", 1, (short) 1); }
    @Bean
    public NewTopic descriptionResponseTopic() { return new NewTopic("description.response", 1, (short) 1); }
    @Bean
    public NewTopic sqlGenerateRequestTopic() { return new NewTopic("sql.generate.request", 1, (short) 1); }
    @Bean
    public NewTopic sqlGenerateResponseTopic() { return new NewTopic("sql.generate.response", 1, (short) 1); }
    @Bean
    public NewTopic queryExecuteRequestTopic() { return new NewTopic("query.execute.request", 1, (short) 1); }
    @Bean
    public NewTopic queryExecuteResponseTopic() { return new NewTopic("query.execute.response", 1, (short) 1); }


    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

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

    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> type) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type, false);
        deserializer.addTrustedPackages("com.querybridge.common.dto");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }
}
