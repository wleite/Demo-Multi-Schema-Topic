"""
Shared configuration loader for Confluent Cloud credentials.
Reads from .env file and provides config dicts for Kafka and Schema Registry.
"""

import os
import sys
from dotenv import load_dotenv
from rich.console import Console

console = Console()

# Load .env from the project root (one level up from src/)
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# Required environment variables
REQUIRED_VARS = [
    "BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
    "TOPIC_NAME",
]

# Optional environment variables with defaults
OPTIONAL_VARS = {
    "NUM_MESSAGES": "15",
    "PRODUCER_FLUSH_TIMEOUT_SEC": "30",
    "PRODUCER_ACKS": "all",
    "PRODUCER_RETRIES": "3",
    "CONSUMER_GROUP_ID": "multi-schema-demo-consumer",
    "AUTO_OFFSET_RESET": "earliest",
    "CONSUMER_POLL_TIMEOUT_SEC": "1.0",
    "LOG_LEVEL": "INFO",
    "LOG_FILE": "",
}


def validate_env():
    """Validate that all required environment variables are set."""
    missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing:
        console.print(
            "\n[bold red]❌ Missing required environment variables:[/bold red]"
        )
        for var in missing:
            console.print(f"   • [yellow]{var}[/yellow]")
        console.print(
            "\n[dim]💡 Copy .env.example to .env and fill in your Confluent Cloud credentials.[/dim]"
        )
        console.print("[dim]   cp .env.example .env[/dim]\n")
        sys.exit(1)


def get_kafka_config(producer=True):
    """Return Kafka producer/consumer configuration dict."""
    validate_env()
    config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
    }

    if producer:
        # Producer-specific configs
        config.update({
            "acks": os.getenv("PRODUCER_ACKS", OPTIONAL_VARS["PRODUCER_ACKS"]),
            "retries": int(os.getenv("PRODUCER_RETRIES", OPTIONAL_VARS["PRODUCER_RETRIES"])),
            "compression.type": "snappy",
            "linger.ms": 100,
        })

    return config


def get_schema_registry_config():
    """Return Schema Registry configuration dict."""
    validate_env()
    return {
        "url": os.getenv("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}",
    }


def get_topic_name():
    """Return the configured topic name."""
    validate_env()
    return os.getenv("TOPIC_NAME", "orders-multi-schema")


def get_num_messages():
    """Return the number of messages to produce."""
    return int(os.getenv("NUM_MESSAGES", OPTIONAL_VARS["NUM_MESSAGES"]))


def get_producer_flush_timeout():
    """Return the producer flush timeout in seconds."""
    return int(os.getenv("PRODUCER_FLUSH_TIMEOUT_SEC", OPTIONAL_VARS["PRODUCER_FLUSH_TIMEOUT_SEC"]))


def get_consumer_group_id():
    """Return the consumer group ID."""
    return os.getenv("CONSUMER_GROUP_ID", OPTIONAL_VARS["CONSUMER_GROUP_ID"])


def get_auto_offset_reset():
    """Return the auto offset reset policy."""
    return os.getenv("AUTO_OFFSET_RESET", OPTIONAL_VARS["AUTO_OFFSET_RESET"])


def get_consumer_poll_timeout():
    """Return the consumer poll timeout in seconds."""
    return float(os.getenv("CONSUMER_POLL_TIMEOUT_SEC", OPTIONAL_VARS["CONSUMER_POLL_TIMEOUT_SEC"]))


def get_log_level():
    """Return the logging level."""
    return os.getenv("LOG_LEVEL", OPTIONAL_VARS["LOG_LEVEL"])


def get_log_file():
    """Return the log file path (empty string for no file logging)."""
    return os.getenv("LOG_FILE", OPTIONAL_VARS["LOG_FILE"])
