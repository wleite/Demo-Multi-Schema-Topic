# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a demonstration project showing how to produce and consume **multiple event types** on a **single Kafka topic** in Confluent Cloud using **JSON Schema** with the **TopicRecordNameStrategy**.

The demo implements three event types representing an order lifecycle:
- **OrderStatus** — tracks order state (PENDING → CONFIRMED → COMPLETED)
- **OrderPayment** — records payment transactions
- **OrderShipment** — tracks shipping and delivery

### Key Architecture Concept: TopicRecordNameStrategy

This project demonstrates the **TopicRecordNameStrategy** pattern, where:
- Multiple schemas share a single Kafka topic (`orders-multi-schema`)
- Each schema is registered under its own subject: `<topic>-<record_name>-value`
- Schemas evolve independently without affecting each other
- The consumer auto-resolves which schema to use via the schema ID embedded in each message

Subject naming examples:
- `orders-multi-schema-OrderStatus-value`
- `orders-multi-schema-OrderPayment-value`
- `orders-multi-schema-OrderShipment-value`

## Project Structure

```
├── src/
│   ├── config.py        # Confluent Cloud credentials loader (.env → config dicts)
│   ├── schemas.py       # Schema file paths, display metadata, event type detection
│   ├── producer.py      # Produces 3 event types to orders-multi-schema topic
│   └── consumer.py      # Consumes & auto-resolves all event types from topic
├── schemas/             # JSON Schema definitions
│   ├── order_status.json
│   ├── order_payment.json
│   └── order_shipment.json
├── .env.example         # Template for Confluent Cloud credentials
└── requirements.txt     # Python dependencies
```

### Code Organization

**`config.py`** — Loads credentials from `.env` and provides two config builders:
- `get_kafka_config()` → dict for Producer/Consumer (bootstrap servers, SASL auth)
- `get_schema_registry_config()` → dict for SchemaRegistryClient

**`schemas.py`** — Central registry for:
- Schema file paths and loading utilities
- Display metadata (emoji, colors) per event type
- `identify_event_type()` — inspects deserialized message fields to determine type

**`producer.py`** — Creates three JSONSerializers (one per schema), produces messages round-robin

**`consumer.py`** — Uses a single JSONDeserializer that auto-resolves schemas by embedded schema ID

## Environment Setup

### Prerequisites
- Python 3.8+
- `uv` package manager (or standard venv)
- Confluent Cloud account with:
  - Kafka cluster + API keys
  - Schema Registry + API keys

### Setup Commands

```bash
# Create and activate virtual environment
uv venv demo-multi-schema
source demo-multi-schema/bin/activate  # macOS/Linux
# .\demo-multi-schema\Scripts\activate  # Windows

# Install dependencies
uv pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env with your Confluent Cloud credentials
```

### Required .env Variables
```
BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret
TOPIC_NAME=orders-multi-schema
```

## Running the Demo

### Producer
```bash
cd src
python producer.py
```
Produces 15 messages (5 per event type) to the `orders-multi-schema` topic.

### Consumer
```bash
# In a separate terminal (with venv activated)
cd src
python consumer.py
```
Continuously polls the topic and displays messages grouped by event type. Press Ctrl+C to stop.

**Pro Tip:** Split your terminal vertically to run producer (left) and consumer (right) side by side.

## Key Implementation Details

### Producer Pattern
- Creates **three separate JSONSerializer instances**, one per schema
- Each serializer uses `TopicRecordNameStrategy` via the `name.strategy` config
- Schemas are auto-registered on first produce
- Messages are produced round-robin across the three event types

### Consumer Pattern
- Uses a **single JSONDeserializer** with no schema specified
- The deserializer reads the schema ID from bytes 1–4 of each message
- Automatically fetches the corresponding schema from Schema Registry
- `identify_event_type()` inspects the deserialized dict to determine display formatting

### Wire Format
Each Kafka message follows the Confluent Schema Registry wire format:
```
[Magic Byte (0x00)] [Schema ID (4 bytes)] [JSON Payload]
```

### Schema Evolution
Because each event type has its own subject, you can:
- Add fields to OrderPayment without affecting OrderStatus
- Change enums in OrderShipment independently
- Version each schema separately

## Confluent Cloud Management

### Creating the Topic (CLI)
```bash
confluent kafka topic create orders-multi-schema --partitions 6
```

### Manually Registering Schemas (Optional — producer auto-registers)
```bash
confluent schema-registry schema create \
  --subject orders-multi-schema-OrderStatus-value \
  --schema schemas/order_status.json \
  --type JSON
```

### Cleanup
```bash
# Delete topic
confluent kafka topic delete orders-multi-schema

# Delete schemas (soft delete)
confluent schema-registry schema delete \
  --subject orders-multi-schema-OrderStatus-value --version all

# Hard delete (permanent)
confluent schema-registry schema delete \
  --subject orders-multi-schema-OrderStatus-value --version all --permanent
```

## Dependencies

- `confluent-kafka[json,avro,schema-registry]` — Kafka client with Schema Registry support
- `python-dotenv` — Load .env files
- `rich` — Terminal UI (panels, tables, colored output)
- `pyfiglet` — ASCII art banners

## Development Notes

- The project uses `rich.Console` for all terminal output (colored panels, tables, emoji)
- Both producer and consumer include graceful shutdown handlers (SIGINT)
- Event type identification is field-based (see `schemas.py:identify_event_type()`)
- Schema files in `schemas/` are JSON Schema Draft-07 format
- The virtual environment (`demo-multi-schema/`) is excluded from git via `.gitignore`

## Git Workflow

### Commit Authorship

Different authorship rules apply based on the type of change:

**Code changes** (implementation, bug fixes, refactoring, tests):
- Author: Claude Sonnet 4.5
- Include `Co-Authored-By` trailer

**User decisions** (documentation, architecture decisions, configuration):
- Author: User (from git config)
- Use `git commit --author="User Name <email>"`

This separation allows tracking what decisions were made by the user vs. what code was implemented by Claude.

### Pushing to Remote

**IMPORTANT:** When pushing commits to external repositories, you MUST use `git push-external` instead of `git push`.

This is a company-internal requirement. Use `git push-external` with the same arguments you would normally pass to `git push`:

```bash
# Instead of: git push origin main
git push-external origin main

# Instead of: git push -u origin feature-branch  
git push-external -u origin feature-branch
```
