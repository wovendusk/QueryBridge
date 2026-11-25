// Root package of the CLI. Component scanning covers everything below it:
// cli.command, cli.config, cli.kafka, cli.model, cli.service.
package com.querybridge.cli;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// ============================================================================
// CliApplication — entry point of the terminal app. (guide step 26)
//
// Looks identical to the other two main classes, but what happens after
// SpringApplication.run is quite different, because Spring Shell is on the
// classpath:
//
//   1. Spring creates all beans:
//        config/ShellConfig          -> the custom "querybridge> " prompt
//        config/DefaultCommandConfig -> (empty)
//        kafka/KafkaConfig           -> topics, producer, 3 typed consumers
//        kafka/KafkaProducerService  -> send-and-wait request/reply helper
//        kafka/KafkaResponseHandler  -> 3 @KafkaListeners for the replies
//        service/ConnectionManager   -> JDBC schema reader
//        service/ValidatorService    -> JSqlParser-based SQL checker
//        service/DisplayService      -> ASCII box/table formatting
//        command/QueryBridgeCommands -> the shell commands (connect, ask, ...)
//   2. Spring Boot's Kafka auto-config creates a KafkaAdmin, which creates the
//      6 topics declared as NewTopic beans (if they don't exist yet).
//   3. Spring Kafka starts the 3 reply-listener containers (3 background threads).
//   4. Spring Shell takes over the terminal on the MAIN thread: prints the
//      prompt, reads a line, finds the matching @ShellMethod, calls it, prints
//      the returned String, repeat.
//
// So the CLI is a small CONCURRENT program: the main thread runs your
// commands, and 3 Kafka threads deliver replies to it. KafkaProducerService
// is where those threads meet.
// ============================================================================
@SpringBootApplication
public class CliApplication {

    public static void main(String[] args) {
        SpringApplication.run(CliApplication.class, args);   // boots Spring; Spring Shell then runs the REPL loop
    }
}
