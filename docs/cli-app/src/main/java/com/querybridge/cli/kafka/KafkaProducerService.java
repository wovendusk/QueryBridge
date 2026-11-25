package com.querybridge.cli.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;    // a "box" that will contain a value LATER
import java.util.concurrent.ConcurrentHashMap;    // a thread-safe HashMap
import java.util.concurrent.ExecutionException;   // thrown by future.get() if the future failed
import java.util.concurrent.TimeUnit;             // SECONDS, MILLISECONDS, ...
import java.util.concurrent.TimeoutException;     // thrown by future.get(timeout) if time runs out

// ============================================================================
// KafkaProducerService — makes Kafka feel like a normal method call:
//     Response r = sendAndWait(topic, request, correlationId, timeout);
// (guide step 35 — THE key concurrency idea of the project; read slowly)
//
// THE PROBLEM
//   Kafka is fire-and-forget. kafkaTemplate.send(...) delivers a message to a
//   topic and that's it. The answer will arrive LATER, on a DIFFERENT topic,
//   on a DIFFERENT thread (a Kafka listener thread in KafkaResponseHandler).
//   But the shell command (on the main thread) wants to write simple code:
//   "send request, get response, print it".
//
// THE SOLUTION: a map of pending requests + CompletableFuture
//
//   main thread (shell command)                Kafka listener thread
//   ───────────────────────────                ─────────────────────
//   1. future = new CompletableFuture()
//   2. pending.put(corrId, future)
//   3. kafkaTemplate.send(request)  ──────▶ (LLM service / executor works...)
//   4. future.get(60s)   ← BLOCKS here
//                                               5. reply arrives on *.response
//                                               6. KafkaResponseHandler calls
//                                                  completeRequest(corrId, reply)
//                                               7. pending.get(corrId)
//                                                       .complete(reply)
//   8. get() returns the reply  ◀──────────────────────┘
//   9. pending.remove(corrId)   (in finally)
//
//   A CompletableFuture is a one-shot container: one thread waits on it with
//   get(), another thread fills it with complete(value), and the waiter wakes
//   up with that value. The correlationId is how step 7 finds the right
//   future out of possibly many.
//
// WHY ConcurrentHashMap?
//   Two different threads touch the map (main thread puts/removes, listener
//   threads get). A plain HashMap is not safe for concurrent use;
//   ConcurrentHashMap is.
//
// A FOOTNOTE: Spring Kafka has a built-in class for exactly this pattern,
// ReplyingKafkaTemplate (it even adds correlation headers for you). This
// hand-written version is simpler to read and understand, which is a fine
// reason to write it yourself while learning.
// ============================================================================
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;   // from KafkaConfig
    // correlationId -> the future the waiting command is blocked on.
    // Entries live only for the duration of one sendAndWait call.
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // ------------------------------------------------------------------------
    // sendAndWait(topic, payload, correlationId, timeoutSeconds)
    //
    // Sends `payload` to `topic` and BLOCKS the calling thread until the
    // reply with the same correlationId arrives, or timeoutSeconds pass.
    //
    // GENERIC RETURN TYPE <T>
    //   The caller decides the type by what it assigns to:
    //       SqlGenerateResponse r = sendAndWait(...);   // T = SqlGenerateResponse
    //   Inside, we only have an Object, so we cast with (T). Because of type
    //   erasure that cast is NOT checked here — if the wrong type ever arrived,
    //   the ClassCastException would appear in the CALLER, at the assignment.
    //   @SuppressWarnings("unchecked") silences the compiler warning about that.
    //
    // ERRORS: always a RuntimeException with a message mentioning the
    // correlationId. QueryBridgeCommands catches these and prints them.
    // ------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    public <T> T sendAndWait(String topic, Object payload, String correlationId, long timeoutSeconds) {
        CompletableFuture<Object> future = new CompletableFuture<>();   // the empty box
        // REGISTER FIRST, SEND SECOND. If we sent first, a very fast reply could
        // arrive before the future is in the map — completeRequest would find
        // nothing and the reply would be lost. Registering first avoids that race.
        pendingRequests.put(correlationId, future);

        try {
            // send(topic, key, value) returns a CompletableFuture<SendResult> that
            // completes when the BROKER has acknowledged the message. Calling
            // .get(10, SECONDS) waits for that acknowledgement. So if Kafka is
            // down, we fail after ~10s here with a clear error, instead of waiting
            // the full reply timeout.
            kafkaTemplate.send(topic, correlationId, payload).get(10, TimeUnit.SECONDS);
            // Now wait for the REPLY (completed by completeRequest on a listener thread).
            Object result = future.get(timeoutSeconds, TimeUnit.SECONDS);
            return (T) result;   // unchecked cast to whatever the caller expects
        } catch (InterruptedException e) {
            // Someone interrupted this thread while it was waiting (e.g. Ctrl+C
            // handling / shutdown). Best practice: RESTORE the interrupt flag so
            // code further up can see that an interrupt happened, then bail out.
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka request interrupted for correlationId: " + correlationId, e);
        } catch (ExecutionException | TimeoutException e) {
            // Multi-catch (Java 7+): one handler for both.
            //   ExecutionException -> the send to the broker failed.
            //   TimeoutException   -> either the broker didn't ack within 10s, or
            //                         no reply arrived within timeoutSeconds
            //                         (service down, LLM slow, reply lost...).
            throw new RuntimeException("Kafka request timed out or failed for correlationId: " + correlationId, e);
        } finally {
            // ALWAYS clean up — success, error or timeout — so the map doesn't
            // grow forever. If a reply arrives after this (a "late" reply), it
            // finds no future and is ignored.
            pendingRequests.remove(correlationId);
        }
    }

    // ------------------------------------------------------------------------
    // completeRequest(correlationId, response)
    //
    // Called by KafkaResponseHandler, ON A KAFKA LISTENER THREAD, for every
    // reply on every *.response topic. If someone is waiting for this
    // correlationId, hand them the response (which wakes up their
    // future.get()). If nobody is (late reply, or a reply from an old session),
    // do nothing.
    // ------------------------------------------------------------------------
    public void completeRequest(String correlationId, Object response) {
        CompletableFuture<Object> future = pendingRequests.get(correlationId);   // is anyone waiting?
        if (future != null) {
            future.complete(response);   // fill the box -> the waiting thread wakes up
        }
    }
}
