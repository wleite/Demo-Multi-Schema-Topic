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


def get_kafka_config():
    """Return Kafka producer/consumer configuration dict."""
    validate_env()
    return {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
    }


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
