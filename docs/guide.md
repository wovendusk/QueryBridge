# Reading Guide — QueryBridge

This guide tells you **which file to read next, and why**. The order is
bottom-up: you start with the simplest building blocks (build files, plain
data classes) and climb toward the class that orchestrates everything. Each
file only uses ideas from files you've already read.

Everything under `docs/` is a **copy of the real project**, with the same
folder layout, where every file has been annotated:

- a big block comment at the top of each class explaining what it is, where
  it sits in the system, and why it exists,
- a block comment above every method explaining what it does and how,
- a short comment on almost every line.

**The code in these copies is identical to the real code.** I checked this
mechanically by stripping all comments from both versions and diffing them
(all 41 files match). The `common` and `cli-app` copies also compile. So
you can trust that what you read here is exactly what runs.

The comments are written to **teach**, not as professional dev comments. They
assume you know basic Java, Spring Boot and Kafka, and explain everything
beyond that (Spring Kafka's factories, request-reply over Kafka,
CompletableFuture, JSqlParser's visitor, JDBC, Kafka listeners in Docker,
etc.). Where the code has a bug or a sharp edge, the comment says so plainly.
Where I wasn't sure about something, the comment says that too.

Each file header carries a **"guide step N"** number that matches the steps
below.

---

## Phase 0: The Big Picture (read this before any code)

### What QueryBridge does

You type a question in English at a terminal prompt:

```
querybridge> ask How many products are there in each category?
```

and QueryBridge:

1. asks an LLM (OpenAI or any compatible API) to write PostgreSQL for it,
2. **checks** that SQL against your database's real schema with a SQL parser,
3. if the check fails, sends the exact errors back to the LLM and asks it to
   fix the query (up to 3 times),
4. asks you to confirm, then
5. runs the SQL on your database and prints the result as a table.

Steps 2 and 3 are what the README calls the **SQL Generation Harness**. They
are the core idea of the project: *never blindly trust the LLM's output; verify
it mechanically and feed precise errors back.*

Two extra tricks improve the LLM's accuracy:

- **Column descriptions.** On `connect`, 10 sample rows from every table are
  sent to the LLM, which writes a one-line human description of each column.
  Those descriptions are included in every later prompt, so a cryptic `vl_cnt`
  column becomes "Number of villagers".
- **Session memory.** The last 5 (question, SQL) pairs go into every prompt,
  so follow-ups like "now only last month" work.

### The architecture: 3 programs + Kafka

```
                         ┌──────────────────────────┐
   you ── keyboard ────▶ │         cli-app          │ ── JDBC (schema + samples, at `connect`) ──┐
                         │  Spring Shell terminal   │                                             │
                         │  ValidatorService (SQL)  │                                             ▼
                         └───────────┬──────────────┘                                    ┌──────────────────┐
                                     │  Kafka (6 topics)                                 │ YOUR PostgreSQL  │
                   ┌─────────────────┴──────────────────┐                                └──────────────────┘
                   ▼                                    ▼                                         ▲
        ┌────────────────────┐               ┌────────────────────┐                               │
        │    llm-service     │               │   query-executor   │ ── JDBC (runs the SQL) ──────┘
        │ prompts + HTTP to  │               │                    │
        │ OpenAI-compatible  │               └────────────────────┘
        │       API          │
        └────────────────────┘
```

- **cli-app**: the only part a human touches. Reads your schema directly via
  JDBC, sends requests over Kafka, validates SQL, prints results.
- **llm-service**: stateless. Turns requests into prompts, calls the LLM API,
  parses the JSON answer, replies.
- **query-executor**: stateless. Opens a JDBC connection with the credentials
  *inside the request*, runs the SQL, replies with the rows as strings.
- **common**: not a program. It's a shared library of the message classes
  (DTOs) that all three use.

### The 6 Kafka topics: 3 request/reply pairs

| Request topic          | From → To            | Reply topic              | DTOs                                              |
|------------------------|----------------------|--------------------------|---------------------------------------------------|
| `description.request`  | CLI → llm-service    | `description.response`   | `DescriptionRequest` / `DescriptionResponse`      |
| `sql.generate.request` | CLI → llm-service    | `sql.generate.response`  | `SqlGenerateRequest` / `SqlGenerateResponse`      |
| `query.execute.request`| CLI → query-executor | `query.execute.response` | `QueryExecuteRequest` / `QueryExecuteResponse`    |

### The one pattern to have in your head: request-reply over Kafka

Kafka has no built-in "reply to this message". So the CLI invents a random
**correlationId** (a UUID) for every request, the service copies it into its
reply, and the CLI uses it to match the reply to whatever is waiting for it.
On the CLI side a map of `correlationId → CompletableFuture` makes this feel
like a blocking method call. You'll meet it in the DTOs (step 4), see the
server half in the executor (step 15) and the client half in
`KafkaProducerService` (step 35).

---

## Phase 1: The Build (Maven multi-module)

### 1. `pom.xml` (root)
**What it is:** The parent POM. It builds nothing itself (`<packaging>pom`), but
lists the 4 modules and pins versions for them.

**Key concepts:**
- the inheritance chain `spring-boot-starter-parent → querybridge → modules`
- `<dependencyManagement>` (a version table) vs `<dependencies>` (actually used)
- importing a BOM (`spring-shell-dependencies`)
- why `common` must *not* use the Spring Boot plugin

### 2. `common/pom.xml`
**What it is:** The shared DTO library. Short. Note that it's a plain jar and
that its Jackson dependency isn't actually needed to compile.

---

## Phase 2: The Messages (common DTOs)

These are plain data classes: fields, getters, setters, constructors, no logic.
Read them to learn the vocabulary the whole system speaks. Each one says which
topic it travels on and who sends and receives it.

### 3. `common/.../dto/QueryHistory.java`
The simplest class. **Read the block comment fully**: it explains what a DTO is,
and why these classes are JavaBeans (a no-arg constructor plus getters and
setters) rather than records. Every other DTO relies on that explanation.

### 4. `common/.../dto/DescriptionRequest.java`
**Key concept: the correlationId**, the field that makes request-reply over
Kafka possible. Also shows *why* column descriptions exist at all.

### 5. `common/.../dto/DescriptionResponse.java`
**Key concept: the `success` + `error` pattern.** You can't throw an exception
across a message bus, so failures travel as data.

### 6. `common/.../dto/SqlGenerateRequest.java`
**The most important DTO. Read slowly.** One class, two modes: *generate*
(`repair=false`) and *repair* (`repair=true`, carrying the broken SQL and the
validator's errors). It also explains why the schema travels as pre-rendered
text and why the LLM service can be stateless.

### 7. `common/.../dto/SqlGenerateResponse.java`
**Key concept:** `success=true` only means "the LLM replied with parseable
JSON", *not* "the SQL is correct". Validation is a separate, later step.

### 8. `common/.../dto/QueryExecuteRequest.java`
**Key concept:** the executor is stateless, so the request carries the DB
credentials. Read the **security note**: your DB password travels in plain JSON
through an unauthenticated Kafka on every execute.

### 9. `common/.../dto/QueryExecuteResponse.java`
Why every value is flattened to a `String`, and the silent 1000-row cap.

---

## Phase 3: The Simplest Service — query-executor

Start with the smallest app. It introduces every Spring Kafka concept that the
other two services reuse.

### 10. `query-executor/pom.xml`
The dependencies of a headless Kafka service. **Key concept:** why the
PostgreSQL driver can be `runtime` scope (JDBC's `DriverManager` and
`ServiceLoader`).

### 11. `query-executor/src/main/resources/application.yml`
`${ENV_VAR:default}` placeholders, `web-application-type: none`, the meaning of
`bootstrap-servers` and `group-id`, and the first mention of the
**localhost:9092 vs localhost:29092** trap.

### 12. `query-executor/.../ExecutorApplication.java`
What `@SpringBootApplication` really is (three annotations in one), what
happens at startup, and why the JVM stays alive with no web server.

### 13. `query-executor/.../kafka/KafkaConfig.java`
**THE Kafka config to study. The other two KafkaConfigs refer back here.**
Understand:
- the four building blocks: `ProducerFactory`, `KafkaTemplate`,
  `ConsumerFactory`, `ListenerContainerFactory`
- why Spring Boot's auto-configuration backs off when you define your own beans
- `auto.offset.reset = earliest`, and when it applies at all
- `JsonSerializer`'s `__TypeId__` header, and `JsonDeserializer(type, false)`
  ignoring it
- trusted packages
- how `@Configuration` makes `producerFactory()` return the singleton
- the poison-pill caveat (no `ErrorHandlingDeserializer`)

### 14. `query-executor/.../service/QueryService.java`
Plain JDBC: connect, execute, read the metadata, loop over rows, try-with-resources.
**Read the safety and memory notes:**
- this class runs **any** SQL it is given; the "SELECT only" rule lives in the CLI,
- the PostgreSQL driver loads the whole result into memory before the 1000-row cap applies.

### 15. `query-executor/.../kafka/ExecutorKafkaListener.java`
**The server half of request-reply.** Receive a request, call the service,
send a reply with the same correlationId. Also covers what `@KafkaListener`'s
attributes mean, when offsets get committed, the single-threaded consumer,
and why the reply has no key.

### 16. `query-executor/Dockerfile`
Four lines. **Key gotcha:** it copies a *prebuilt* jar, so run `mvn package`
before building the image, every time.

---

## Phase 4: The LLM Service

Same skeleton as the executor. The new material is prompt building and the HTTP call.

### 17. `llm-service/pom.xml`
Why `spring-boot-starter-web` is here (for `RestTemplate` and Jackson), and the
side effect: a Tomcat server with no endpoints.

### 18. `llm-service/src/main/resources/application.yml`
The custom `llm:` section, and why an empty API key still lets the app start.

### 19. `llm-service/.../LlmApplication.java`
Declaring a `@Bean` in the main class. Why Boot doesn't give you a
`RestTemplate` for free, and the **missing timeouts** problem.

### 20. `llm-service/.../kafka/KafkaConfig.java`
Read it after step 13. **The twist:** two incoming DTO types, so two consumer
factories and two container factories, built by a **generic helper**
`createConsumerFactory(Class<T>)`. Also shows what type erasure has to do with
it, and what having two containers means for concurrency.

### 21. `llm-service/.../service/LlmService.java`
**The second most important file. Read it method by method.**
- the OpenAI Chat Completions request and response shapes (with examples)
- the three prompts (description, generation, repair), each shown as example output
- prompt-engineering patterns: role, full context, rules in CAPS, exact output
  format, "Return ONLY the JSON", low temperature
- Jackson: `TypeReference`, `readTree`, `path()` vs `get()`, `asText(null)`
- `stripMarkdownCodeBlocks` and its limits
- the inconsistency between `generateDescriptions` (throws) and `generateSql`
  (never throws)
- the conflict between the prompt asking for **aliases** and the validator
  rejecting them
- the **privacy** note: real sample rows are sent to the LLM provider

### 22. `llm-service/.../kafka/LlmKafkaListener.java`
Two listeners, two topics, always reply (even on failure), replies keyed by
correlationId.

### 23. `llm-service/Dockerfile`
Identical to the executor's. Keep secrets out of images.

---

## Phase 5: The CLI — from the bottom up

The CLI is the biggest module. Read the plumbing first (config, model, services,
Kafka) and the orchestrator `QueryBridgeCommands` **last**.

### 24. `cli-app/pom.xml`
Spring Shell (via the BOM) and JSqlParser (pinned by a property).

### 25. `cli-app/src/main/resources/application.yml`
`banner-mode: off`, interactive shell mode, and why `group-id: cli-app` is
effectively unused.

### 26. `cli-app/.../CliApplication.java`
What starts up in the CLI: beans, topic creation, three listener threads, and
the shell's read-eval-print loop on the main thread. **Key concept:** the CLI
is a small concurrent program.

### 27. `cli-app/.../config/ShellConfig.java`
JLine, the `PromptProvider` lambda, and the *"define a bean of the same type
to replace Boot's default"* pattern.

### 28. `cli-app/.../config/DefaultCommandConfig.java`
An empty leftover. Ten seconds.

### 29. `cli-app/.../model/ColumnInfo.java`
**Read the block comment's diagram of the CLI's schema model.**
`SchemaContext → TableInfo → ColumnInfo`, and how a column's description gets filled in.

### 30. `cli-app/.../model/TableInfo.java`
`findColumn`: case- and quote-insensitive lookup, `Optional`, and the
`TableInfo::new` constructor reference.

### 31. `cli-app/.../model/SchemaContext.java`
Connection details plus the schema. Two renderings: `toSchemaText()` (for the
LLM, shown as example output) and `toDisplayText()` (for you). `findTable`
doesn't understand aliases, which is the root of the validator bug in step 37.

### 32. `cli-app/.../service/ConnectionManager.java`
`information_schema.columns`, `LinkedHashMap` and `computeIfAbsent`, text
blocks, sample rows, quoting identifiers. **Read the two warnings in
`connect()`:**
- `sslmode=require` is hard-coded, so a plain local PostgreSQL fails,
- **the localhost trap**: the CLI and the executor resolve "localhost" differently in Docker.

### 33. `cli-app/.../service/DisplayService.java`
Text tables: compute the column widths, pad, join. Also the Unicode box and
greedy word-wrap. Pure string code, a nice breather.

### 34. `cli-app/.../kafka/KafkaConfig.java`
Read it after steps 13 and 20. What's new: the **`NewTopic` beans** (the CLI
creates all six topics, through Boot's `KafkaAdmin`) and **`auto.offset.reset =
latest`**, with why it suits a reply consumer and what happens on the second run.

### 35. `cli-app/.../kafka/KafkaProducerService.java`
**THE key concurrency idea in the project. Read slowly, twice.**
`sendAndWait`: register a `CompletableFuture` under the correlationId, send,
block on `future.get(timeout)`, and clean up in `finally`. A listener thread
calls `completeRequest` to wake the waiter. The block comment has a
two-thread timeline diagram. Also covered: why it registers *before* sending,
`ConcurrentHashMap`, the unchecked generic cast, restoring the interrupt flag,
and Spring's built-in `ReplyingKafkaTemplate`.

### 36. `cli-app/.../kafka/KafkaResponseHandler.java`
The listener-thread side: three one-line methods. **Key concept:** the
per-listener `groupId` override, and the **only one CLI at a time** limitation
that comes from fixed group names.

### 37. `cli-app/.../service/ValidatorService.java`
**The heart of the harness. Take your time.**
- what it checks: empty, parse, SELECT-only, tables, columns
- what an AST is, and the **visitor pattern** (`ExpressionVisitorAdapter` +
  overriding `visit(Column)`)
- how the walk covers each SELECT shape (parenthesised, UNION, plain) and each clause
- **a table of observed edge cases.** I ran the real `ValidatorService` from your
  built jar against test SQL, so these are measured results:
  - *false alarms*: table aliases (`p.name`), SELECT-list aliases in ORDER BY,
    `WHERE true`, subquery aliases, CTE output columns
  - *missed errors*: columns inside sub-selects in WHERE
  - *safety gaps*: `SELECT ... INTO newtable` **passes** (it creates a table in
    PostgreSQL); `FOR UPDATE` and side-effect functions pass too

### 38. `cli-app/.../command/QueryBridgeCommands.java`
**The top of the climb. Everything you've read is used from here.**
- how Spring Shell maps `@ShellMethod` and parameters to commands and `--options`
  (and why that needs the `-parameters` compiler flag)
- `connect`: the schema read, then one blocking description request per table
- `ask` / `processQuestion`: the full pipeline diagram
  (generate → validate → repair ×3 → confirm → execute)
- **the order bug**: the code asks "Execute? [Y/n]" *before* printing the SQL
  box, because the box is only in the return value, which Spring Shell prints
  after the method returns
- `confirmExecution` reading `System.in` directly, and why that Scanner is never closed
- the bounded history queue, and the name clash with Spring Shell's built-in
  `quit`/`exit`/`history`

---

## Phase 6: Running It

### 39. `cli-app/Dockerfile`
The same four lines as the other two Dockerfiles. Running an interactive app
in a container needs `stdin_open`, `tty` and `docker compose run`.

### 40. `docker-compose.yml`
**Read it after all the Java, so every name in it means something.**
- Kafka in KRaft mode (no ZooKeeper), as one combined broker and controller
- **listeners vs advertised listeners**, explained slowly. This is *why* host
  processes must use `localhost:29092`.
- why the internal topics need replication factor 1 here
- healthcheck plus `depends_on: service_healthy` vs `service_started`
- no volumes (a fresh Kafka every `down`), and no database service

### 41. `.gitignore`
Thirty seconds. Never commit `.env`. (Small heads-up: since this copy sits in
`docs/`, git treats it as a real `.gitignore` for the `docs/` folder. Its
patterns match nothing in `docs/`, so it's harmless.)

---

## Phase 7: End-to-End Traces (read after Phase 5; the payoff)

Now replay the two flows with every piece in your head.

### Trace A: `connect --host db.example.com --database shop --username me --password pw`

```
CLI main thread                                   Kafka / other services
───────────────                                   ──────────────────────
QueryBridgeCommands.connect
 └─ ConnectionManager.connect
     ├─ JDBC: open jdbc:postgresql://db.example.com:5432/shop?sslmode=require
     ├─ SELECT ... FROM information_schema.columns WHERE table_schema='public'
     │     -> LinkedHashMap<tableName, TableInfo{columns}>
     ├─ for each table: SELECT * FROM "t" LIMIT 10 -> sampleRows
     └─ close connection, return SchemaContext
 print "✔ Connected to shop@db.example.com:5432. N tables found."
 for each table (one after another):
   ├─ id = UUID
   ├─ KafkaProducerService.sendAndWait("description.request", req, id, 30s)
   │    ├─ pending.put(id, future)
   │    ├─ kafkaTemplate.send(...).get(10s) ───────▶ topic description.request
   │    │                                             └─▶ LlmKafkaListener.onDescriptionRequest
   │    │                                                   └─ LlmService.generateDescriptions
   │    │                                                        ├─ buildDescriptionPrompt
   │    │                                                        ├─ POST /chat/completions
   │    │                                                        └─ parse JSON -> Map
   │    │                                                 send("description.response", id, resp)
   │    │  KafkaResponseHandler.onDescriptionResponse ◀───────┘   (CLI listener thread)
   │    │    └─ completeRequest(id, resp) -> future.complete(resp)
   │    └─ future.get() returns resp; pending.remove(id)
   └─ applyDescriptions(table, resp.columnDescriptions)
 sessionHistory.clear()
 return "All N tables analyzed. Ready for questions."
```

### Trace B: `ask top 5 products by price` (with one repair)

```
processQuestion
 ├─ generateSql(q, null, false, null)  → sql.generate.request → LlmService.buildGenerationPrompt → LLM
 │     ← "SELECT p.name, p.price FROM products p ORDER BY p.price DESC LIMIT 5"
 ├─ ValidatorService.validate → ["Unknown table in column reference: p", ...]   (the alias false alarm)
 ├─ attempt 1: generateSql(q, badSql, true, errors) → LlmService.buildRepairPrompt → LLM
 │     ← "SELECT products.name, products.price FROM products ORDER BY products.price DESC LIMIT 5"
 ├─ validate → []   ✓
 ├─ build output (SQL box + explanation)      ← built, NOT yet printed
 ├─ confirmExecution(): prints "Execute? [Y/n]: ", reads a line   ← the order bug
 ├─ executeQuery(sql) → query.execute.request → QueryService.execute (JDBC) → query.execute.response
 │     ← columns + rows (as strings, ≤1000)
 ├─ DisplayService.formatTable
 ├─ addToHistory(q, sql)
 └─ return output + table   → Spring Shell prints it
```

---

## Phase 8: Sharp Edges and Bugs — Consolidated

Every item is explained in detail at the spot in the code where it happens.
**"Verified"** means I tested it against the real built jar; the others come
from reading the code (or from library docs, where noted as such).

### Correctness / UX

| # | Issue | Where | Status |
|---|-------|-------|--------|
| 1 | The validator rejects **table aliases** (`p.name`), even though the prompt asks the model to use aliases. That costs at least one repair round. | ValidatorService, LlmService | Verified |
| 2 | The validator rejects **SELECT-list aliases** in ORDER BY (`ORDER BY cnt`), `WHERE true`, subquery aliases and CTE output columns. | ValidatorService | Verified |
| 3 | The validator **misses** bad columns inside sub-selects in WHERE/SELECT. | ValidatorService | Verified |
| 4 | "Execute? [Y/n]" appears **before** the SQL is shown. | QueryBridgeCommands.processQuestion | From code (deterministic) |
| 5 | `sslmode=require` is hard-coded, so a default local PostgreSQL without SSL fails to connect. | ConnectionManager | From code |
| 6 | **Localhost trap:** with the CLI on the host and the executor in Docker, "localhost" points at different machines. The schema loads, then execution fails. | ConnectionManager / QueryService | From code |
| 7 | Running the CLI on the host needs `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`. The default `localhost:9092` doesn't match this compose file. | application.yml / docker-compose | From config (README does mention it) |
| 8 | Results are silently cut off at 1000 rows. | QueryService | From code |
| 9 | One failing sample query (e.g. no SELECT permission on one table) makes the whole `connect` fail. | ConnectionManager | From code |
| 10 | Only one CLI instance can work at a time, because of the fixed consumer-group names. | KafkaResponseHandler | From Kafka semantics |
| 11 | Spring Shell's built-in `quit`/`exit`/`history` clash with this app's commands. Which one wins is unclear. | QueryBridgeCommands | Partially checked (jar inspected) |

### Safety / security

| # | Issue | Where |
|---|-------|-------|
| 12 | `SELECT ... INTO newtable` passes the "SELECT only" check, and in PostgreSQL it **creates a table**. *(Verified that it passes.)* | ValidatorService |
| 13 | The executor runs **any** SQL from the topic. Real protection would be a read-only DB role. | QueryService |
| 14 | DB passwords travel in plain JSON through PLAINTEXT Kafka and stay on disk for the retention period. | QueryExecuteRequest |
| 15 | Real sample rows (possibly personal data) are sent to the LLM provider. | LlmService |

### Robustness

| # | Issue | Where |
|---|-------|-------|
| 16 | `RestTemplate` has no timeouts, so a hung LLM API blocks that consumer thread. | LlmApplication |
| 17 | No `ErrorHandlingDeserializer`, so a malformed message can jam a consumer (Spring Kafka docs recommend it; I'm fairly but not fully sure of the exact behaviour in 3.2.4). | all KafkaConfigs |
| 18 | `kafkaTemplate.send()` results in the services are never checked, so a failed reply is lost silently and the CLI times out. | both listeners |
| 19 | `stripMarkdownCodeBlocks` only handles a code fence at the very start of the reply. | LlmService |
| 20 | `max_tokens`/`temperature` may be rejected by some newer reasoning models. | LlmService |
| 21 | The PostgreSQL driver loads the full result into memory before the row cap applies. | QueryService |

### Leftovers (harmless)

- `DefaultCommandConfig` is empty.
- `CompletableFuture` is imported but unused in QueryBridgeCommands.
- `server.port` in the executor's config is never used.
- Tomcat starts in llm-service but serves nothing.
- `common` depends on Jackson but doesn't use it.
- The `setLength(-1)` + `"\n("` round trip in DisplayService does nothing.

---

## Phase 9: How to Run It (with the gotchas applied)

```bash
cd ~/Projects/querybridge
mvn clean package -DskipTests
```

Create `.env` in the project root:

```
OPENAI_API_KEY=sk-...
OPENAI_API_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4o-mini
```

Start Kafka and the two services (`--build` so images pick up the fresh jars):

```bash
docker compose up -d --build kafka llm-service query-executor
```

Then either run the CLI in Docker:

```bash
docker compose run --rm cli
```

or on your host (note the **29092** port):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 java -jar cli-app/target/cli-app-1.0-SNAPSHOT.jar
```

Then, at the prompt, connect to a database that **both** your CLI and the
executor container can reach, and that accepts SSL. A cloud PostgreSQL is the
easiest fit:

```
querybridge> connect --host <db-host> --database <db> --username <user> --password <pw>
querybridge> ask how many rows are in each table?
```

Debugging tips:
- `docker compose logs -f llm-service` shows prompt failures and API errors.
- Set `logging.level.com.querybridge.llm=DEBUG` (for example
  `-Dlogging.level.com.querybridge.llm=DEBUG`) to see the full prompts and LLM replies.

---

## Phase 10: Check Your Understanding

If you can answer these without looking, you know the project.

1. Why does every request DTO have a `correlationId`, and what would break without it?
2. In `sendAndWait`, why is the future put into the map *before* the message is sent?
3. Why does the CLI use `auto.offset.reset = latest` while the services use `earliest`?
   When does that setting stop mattering?
4. What does `new JsonDeserializer<>(X.class, false)`'s `false` do, and why does that
   make `addTrustedPackages` mostly irrelevant here?
5. A user types `ask top customers`. List every Kafka topic touched, in order, if the
   first SQL has one bad column name.
6. Why does `SELECT p.name FROM products p` take a repair round, and what is the
   smallest change to ValidatorService that would fix it? (Hint: `PlainSelect.getFromItem()`
   and `Join.getFromItem()` have `getAlias()`.)
7. Why does the host-run CLI need port 29092 while containers use 9092? What would go
   wrong if `EXTERNAL` advertised `kafka:29092`?
8. Name two ways a statement that passes ValidatorService can still change your database,
   and the one change that would block all of them.
9. Why does the SQL box show up only after you answer "Execute?"? Fix it in one line.
10. What happens if you start two CLIs at once? Why?

---

## Quick Reference: File Count

| Area | Files | Steps |
|------|-------|-------|
| Build (root + common POM) | 2 | 1–2 |
| Shared DTOs (common) | 7 | 3–9 |
| query-executor (pom, yml, 4 Java, Dockerfile) | 7 | 10–16 |
| llm-service (pom, yml, 4 Java, Dockerfile) | 7 | 17–23 |
| cli-app (pom, yml, 13 Java, Dockerfile) | 16 | 24–39 |
| Infra (docker-compose, .gitignore) | 2 | 40–41 |
| **Total annotated** | **41** | |

`README.md` isn't copied; this guide replaces it as the entry point. There are
no tests in the project.
