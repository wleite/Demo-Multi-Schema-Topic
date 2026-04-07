# Architecture — Multi-Schema Topic

## High-Level Architecture

```mermaid
graph LR
    subgraph Producer["🟣 Producer Application"]
        P1["OrderStatus<br/>Serializer"]
        P2["OrderPayment<br/>Serializer"]
        P3["OrderShipment<br/>Serializer"]
    end

    subgraph SR["📋 Schema Registry"]
        S1["orders-multi-schema<br/>-OrderStatus-value"]
        S2["orders-multi-schema<br/>-OrderPayment-value"]
        S3["orders-multi-schema<br/>-OrderShipment-value"]
    end

    subgraph Kafka["⚡ Kafka Topic"]
        T["orders-multi-schema"]
    end

    subgraph Consumer["🟢 Consumer Application"]
        C1["JSONDeserializer<br/>(auto schema resolution)"]
    end

    P1 -->|register/fetch| S1
    P2 -->|register/fetch| S2
    P3 -->|register/fetch| S3

    P1 -->|produce| T
    P2 -->|produce| T
    P3 -->|produce| T

    T -->|consume| C1
    C1 -->|fetch schema by ID| SR
```

## Subject Naming with TopicRecordNameStrategy

```
Subject Name = <topic>-<record_name>-value
```

| Event Type    | Subject Name                              |
| ------------- | ----------------------------------------- |
| OrderStatus   | `orders-multi-schema-OrderStatus-value`   |
| OrderPayment  | `orders-multi-schema-OrderPayment-value`  |
| OrderShipment | `orders-multi-schema-OrderShipment-value` |

## Message Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant SR as Schema Registry
    participant K as Kafka Broker
    participant C as Consumer

    Note over P: Round-robin: Status → Payment → Shipment

    P->>SR: Register/Fetch OrderStatus schema
    SR-->>P: Schema ID = 1
    P->>K: Produce [Schema ID=1 | OrderStatus JSON]

    P->>SR: Register/Fetch OrderPayment schema
    SR-->>P: Schema ID = 2
    P->>K: Produce [Schema ID=2 | OrderPayment JSON]

    P->>SR: Register/Fetch OrderShipment schema
    SR-->>P: Schema ID = 3
    P->>K: Produce [Schema ID=3 | OrderShipment JSON]

    K->>C: Poll messages
    C->>SR: Fetch schema for Schema ID in message
    SR-->>C: Return schema definition
    C->>C: Deserialize & display by event type
```

## Wire Format

Each message on the wire follows the Confluent Schema Registry wire format:

```
[Magic Byte (0x00)] [Schema ID (4 bytes)] [JSON Payload]
```

The consumer reads the Schema ID from bytes 1–4, fetches the corresponding
schema from Schema Registry, and uses it to deserialize/validate the JSON payload.
