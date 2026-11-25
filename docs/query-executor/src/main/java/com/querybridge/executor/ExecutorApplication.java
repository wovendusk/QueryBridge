// The root package of this service. Important: @SpringBootApplication scans
// THIS package and everything below it (…executor.kafka, …executor.service)
// for @Component/@Service/@Configuration classes. That's why all the other
// classes in this service live in sub-packages of com.querybridge.executor.
package com.querybridge.executor;

import org.springframework.boot.SpringApplication;                          // the class that boots the app
import org.springframework.boot.autoconfigure.SpringBootApplication;        // the "do everything" annotation

// ============================================================================
// ExecutorApplication — the main() entry point of the query-executor service.
// (guide step 12)
//
// WHAT @SpringBootApplication REALLY MEANS
//   It's a shortcut for three annotations at once:
//     @Configuration            - this class may declare @Bean methods
//     @EnableAutoConfiguration  - let Spring Boot look at the classpath and
//                                 auto-create beans (e.g. it sees spring-kafka
//                                 and sets up Kafka infrastructure; it sees
//                                 Jackson and creates an ObjectMapper ...)
//     @ComponentScan            - find every @Component, @Service,
//                                 @Configuration ... in this package and its
//                                 sub-packages and register them as beans
//
// WHAT HAPPENS WHEN main() RUNS, FOR THIS SERVICE SPECIFICALLY
//   1. Spring reads application.yml.
//   2. Component scan finds:
//        kafka/KafkaConfig.java            (@Configuration -> Kafka beans)
//        kafka/ExecutorKafkaListener.java  (@Component with a @KafkaListener)
//        service/QueryService.java         (@Service)
//   3. Spring wires them together via constructor injection
//      (ExecutorKafkaListener gets QueryService + KafkaTemplate).
//   4. Spring Kafka sees the @KafkaListener and starts a consumer thread that
//      subscribes to "query.execute.request".
//   5. main() returns, but the JVM stays alive because that consumer thread
//      is running. The service now just waits for messages.
// ============================================================================
@SpringBootApplication
public class ExecutorApplication {

    // Standard Java entry point. Everything interesting is inside
    // SpringApplication.run: it creates the Spring container ("application
    // context"), creates all the beans, and starts the Kafka listeners.
    public static void main(String[] args) {
        SpringApplication.run(ExecutorApplication.class, args);   // args lets you override config, e.g. --spring.kafka.bootstrap-servers=...
    }
}
