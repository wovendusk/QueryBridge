// Root package of the LLM service; component scanning covers
// com.querybridge.llm.kafka and com.querybridge.llm.service below it.
package com.querybridge.llm;

import org.springframework.boot.SpringApplication;                     // boots the app
import org.springframework.boot.autoconfigure.SpringBootApplication;   // config + auto-config + component scan
import org.springframework.context.annotation.Bean;                    // declare a bean from a method
import org.springframework.web.client.RestTemplate;                    // Spring's classic HTTP client

// ============================================================================
// LlmApplication — entry point of the LLM service + one bean definition.
// (guide step 19)
//
// For what @SpringBootApplication and SpringApplication.run do, see
// query-executor/ExecutorApplication.java — it's the same here. In this
// service, component scanning finds:
//    kafka/KafkaConfig.java        (Kafka producer/consumer beans)
//    kafka/LlmKafkaListener.java   (two @KafkaListener methods)
//    service/LlmService.java       (prompt building + HTTP call to the LLM)
//
// WHY IS THERE A @Bean HERE?
//   @SpringBootApplication includes @Configuration, so the main class is also
//   allowed to declare beans. This one declares the RestTemplate that
//   LlmService needs. Spring Boot does NOT auto-create a RestTemplate bean
//   (it auto-creates a RestTemplateBuilder, deliberately, so that you decide
//   timeouts etc.), so if you want to inject one, you must define it yourself.
// ============================================================================
@SpringBootApplication
public class LlmApplication {

    // Java entry point -> start Spring, which starts Tomcat (port 8081, unused)
    // and the two Kafka listener containers.
    public static void main(String[] args) {
        SpringApplication.run(LlmApplication.class, args);
    }

    // ------------------------------------------------------------------------
    // restTemplate()
    //
    // Creates the single, shared RestTemplate (an HTTP client) for the whole
    // app. RestTemplate is thread-safe, so one instance is fine.
    //
    // ⚠ NO TIMEOUTS: `new RestTemplate()` uses the JDK's basic HTTP connection
    // with NO connect or read timeout by default (as far as I know they default
    // to "infinite"). If the LLM API hangs, this service's consumer thread hangs
    // with it. The CLI gives up after 60s and says "timed out", but the LLM
    // service is still stuck and can't process the NEXT request either.
    // Using `new RestTemplateBuilder().setConnectTimeout(...).setReadTimeout(...)
    // .build()` (or inject Boot's RestTemplateBuilder bean) would fix that.
    //
    // (Side note: Spring 6.1+ also offers RestClient, a newer fluent API that
    // replaces RestTemplate for new code. RestTemplate still works fine.)
    // ------------------------------------------------------------------------
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
