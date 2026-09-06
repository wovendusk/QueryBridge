# QueryBridge

A natural-language-to-SQL CLI tool built with Java 21, Spring Boot, and Apache Kafka. QueryBridge lets you connect to any PostgreSQL database, ask questions in plain English, and get validated SQL queries — all from your terminal.

## Architecture

QueryBridge runs as **three microservices** communicating over Apache Kafka:

```
┌─────────────────────────┐
│       CLI App           │  Interactive terminal (Spring Shell)
│  connect, ask, schema   │  Validates SQL with JSqlParser
└──────────┬──────────────┘
           │ Kafka
     ┌─────┴──────┐
     ▼            ▼
┌─────────┐  ┌──────────┐
│   LLM   │  │  Query   │
│ Service  │  │ Executor │
└─────────┘  └──────────┘
     │             │
     ▼             ▼
 OpenAI API    User's PostgreSQL
```

| Service | Role |
|---------|------|
| **CLI App** | Interactive terminal — accepts DB credentials, takes NL questions, validates generated SQL, displays results as tables |
| **LLM Service** | Generates column descriptions, converts natural language to SQL, repairs invalid queries |
| **Query Executor** | Runs SQL on the user's PostgreSQL database via JDBC, returns results |

## Key Features

**SQL Generation Harness** — The core of QueryBridge. Instead of naively passing questions to an LLM, queries go through a multi-stage pipeline:

1. **Generate** — LLM produces SQL using the full schema + column descriptions as context
2. **Validate** — JSqlParser parses the SQL and checks every table/column name against the real schema
3. **Repair** — If validation fails, specific errors are fed back to the LLM for correction (up to 3 attempts)
4. **Confirm** — Valid SQL is shown to the user for review before execution

**Column Descriptions** — At connect time, 10 sample rows from each table are sent to the LLM via Kafka. The LLM generates human-readable descriptions for each column (e.g., `col_nm` → "Full name of the user"). These descriptions help the model map informal user terms ("villagers") to actual column names (`col_vl`).

**Session Memory** — The last 5 queries are included as context in every new request, so follow-up questions like "now filter that by last month" work without restating the full question.

## Tech Stack

- **Java 21**, **Spring Boot 3.3**, **Spring Shell 3.3**
- **Apache Kafka** (KRaft mode, no Zookeeper)
- **PostgreSQL** (target databases)
- **JSqlParser 4.9** (SQL validation)
- **OpenAI API** / any OpenAI-compatible API (LLM)
- **Docker & Docker Compose**
- **Maven** (multi-module build)

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker and Docker Compose

## Quick Start

### 1. Clone and build

```bash
git clone <repo-url>
cd querybridge
mvn clean package -DskipTests
```

### 2. Configure

Create a `.env` file in the project root:

```bash
OPENAI_API_KEY=sk-your-key-here
OPENAI_API_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4o-mini
```

### 3. Start everything

```bash
docker compose up -d kafka llm-service query-executor
```

Wait ~15 seconds for Kafka to become healthy.

### 4. Run the CLI

**Option A — Via Docker:**

```bash
docker compose run --rm cli
```

**Option B — Directly (for development):**

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 java -jar cli-app/target/cli-app-1.0-SNAPSHOT.jar
```

### 5. Use it

```
querybridge> connect --host localhost --port 5432 --database mydb --username user --password pass
✔ Connected to mydb@localhost:5432. 6 tables found.
  All 6 tables analyzed. Ready for questions.

querybridge> ask How many products are there in each category?

┌──────────────────────────────────────────────────────────────────────┐
│ SELECT categories.name AS category_name,                             │
│   COUNT(products.id) AS product_count                                │
│ FROM categories LEFT JOIN products                                   │
│   ON categories.id = products.category_id                            │
│ GROUP BY categories.id, categories.name                              │
│ ORDER BY categories.name;                                            │
└──────────────────────────────────────────────────────────────────────┘

Execute? [Y/n]: y

| category_name     | product_count |
|-------------------|---------------|
| Automotive        | 26            |
| Books             | 22            |
| Clothing          | 25            |
| Electronics       | 20            |
(10 rows)
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `connect --host H --port P --database D --username U --password P` | Connect to a PostgreSQL database |
| `ask <question>` | Ask a natural language question |
| `schema` | Show the current database schema with column descriptions |
| `history` | Show the session query history |
| `quit` / `exit` | Exit the application |

## Project Structure

```
querybridge/
├── pom.xml                  Parent POM
├── docker-compose.yml
├── .env                     API keys (gitignored)
├── common/                  Shared DTOs for Kafka messages
├── cli-app/                 Spring Shell interactive terminal
├── llm-service/             LLM integration (OpenAI API)
└── query-executor/          SQL execution via JDBC
```

## Kafka Topics

```
description.request    CLI → LLM Service      Column description requests
description.response   LLM Service → CLI      Column descriptions
sql.generate.request   CLI → LLM Service      NL-to-SQL and repair requests
sql.generate.response  LLM Service → CLI      Generated/repaired SQL
query.execute.request  CLI → Query Executor    SQL execution requests
query.execute.response Query Executor → CLI    Query results
```

## Using a Different LLM

QueryBridge works with any OpenAI-compatible API. To use DeepSeek, for example:

```bash
OPENAI_API_KEY=your-deepseek-key
OPENAI_API_URL=https://api.deepseek.com
OPENAI_MODEL=deepseek-chat
```

## License

MIT
