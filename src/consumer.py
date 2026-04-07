"""
Multi-Schema Consumer for Confluent Cloud.

Consumes OrderStatus, OrderPayment, and OrderShipment events from a single
Kafka topic. Uses JSONDeserializer which automatically resolves the correct
schema from Schema Registry using the schema ID embedded in each message.
"""

import json
import signal
import sys

import pyfiglet
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config import get_kafka_config, get_schema_registry_config, get_topic_name
from schemas import identify_event_type, get_display_info

console = Console()

# ── Graceful shutdown ──────────────────────────────────────────────────────────

running = True
consumed_count = {"OrderStatus": 0, "OrderPayment": 0, "OrderShipment": 0, "Unknown": 0}


def signal_handler(sig, frame):
    """Handle CTRL+C for graceful shutdown."""
    global running
    running = False
    console.print("\n\n[bold yellow]⚠ Shutdown signal received. Closing consumer...[/bold yellow]")


signal.signal(signal.SIGINT, signal_handler)


# ── Banner ─────────────────────────────────────────────────────────────────────

def show_banner():
    """Display a colorful ASCII art banner."""
    banner_text = pyfiglet.figlet_format("Multi-Schema", font="slant")
    subtitle = pyfiglet.figlet_format("Consumer", font="slant")

    console.print(f"[bold cyan]{banner_text}[/bold cyan]", end="")
    console.print(f"[bold magenta]{subtitle}[/bold magenta]")
    console.print(
        Panel(
            "[bold white]Confluent Cloud · Multi-Schema Topic Demo[/bold white]\n"
            "[dim]Consuming OrderStatus, OrderPayment & OrderShipment events[/dim]",
            border_style="bright_cyan",
            padding=(1, 2),
        )
    )
    console.print()


# ── Message display ────────────────────────────────────────────────────────────

def display_message(data: dict, partition: int, offset: int):
    """Display a consumed message in a color-coded Rich panel."""
    event_type = identify_event_type(data)
    display = get_display_info(event_type)
    consumed_count[event_type] = consumed_count.get(event_type, 0) + 1

    # Build a table of fields inside the panel
    fields_table = Table(show_header=False, box=None, padding=(0, 1))
    fields_table.add_column("Field", style="bold white", min_width=20)
    fields_table.add_column("Value", style="white")

    for key, value in data.items():
        fields_table.add_row(f"  {key}", str(value))

    # Create a styled panel for this event
    panel = Panel(
        fields_table,
        title=f"{display['emoji']} {display['label']}",
        subtitle=f"[dim]partition={partition} · offset={offset}[/dim]",
        border_style=display["color"],
        padding=(0, 1),
    )

    console.print(panel)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    show_banner()

    topic = get_topic_name()

    console.print(f"[bold]📋 Topic:[/bold]       [cyan]{topic}[/cyan]")
    console.print(f"[bold]📐 Strategy:[/bold]    [cyan]TopicRecordNameStrategy[/cyan]")
    console.print(f"[bold]🔄 Group ID:[/bold]    [cyan]multi-schema-demo-consumer[/cyan]")
    console.print()
    console.print(
        "[bold yellow]💡 Press CTRL+C to stop the consumer[/bold yellow]\n"
    )

    # ── Initialize clients ─────────────────────────────────────────────────
    console.print("[dim]Connecting to Confluent Cloud...[/dim]")

    kafka_config = get_kafka_config()
    kafka_config.update(
        {
            "group.id": "multi-schema-demo-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        }
    )

    sr_config = get_schema_registry_config()
    schema_registry_client = SchemaRegistryClient(sr_config)

    # Create a generic JSONDeserializer — it will use the schema ID in each
    # message to fetch the correct schema from Schema Registry automatically.
    json_deserializer = JSONDeserializer(
        schema_str=None,
        schema_registry_client=schema_registry_client,
    )

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    console.print(f"[dim]Subscribed to topic: {topic}[/dim]")
    console.print(
        Panel(
            "[bold]Waiting for messages...[/bold]",
            border_style="bright_green",
            padding=(0, 2),
        )
    )
    console.print()

    # ── Consume loop ───────────────────────────────────────────────────────
    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    console.print(
                        f"[dim]  ⏸ Reached end of partition {msg.partition()} "
                        f"at offset {msg.offset()}[/dim]"
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
                continue

            # Deserialize the message value
            try:
                serialization_ctx = SerializationContext(
                    msg.topic(), MessageField.VALUE
                )
                data = json_deserializer(msg.value(), serialization_ctx)

                if data is not None:
                    display_message(data, msg.partition(), msg.offset())
            except Exception as e:
                # Fallback: try plain JSON if the message wasn't serialized with SR
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    display_message(data, msg.partition(), msg.offset())
                except Exception:
                    console.print(
                        f"[bold red]✗ Failed to deserialize message at "
                        f"offset {msg.offset()}: {e}[/bold red]"
                    )

    except KeyboardInterrupt:
        pass
    finally:
        # ── Shutdown ───────────────────────────────────────────────────────
        consumer.close()
        console.print()

        # Summary table
        total = sum(consumed_count.values())
        if total > 0:
            summary_table = Table(
                title="📊 Consumption Summary",
                show_header=True,
                header_style="bold white",
                border_style="bright_cyan",
            )
            summary_table.add_column("Event Type", style="bold")
            summary_table.add_column("Count", justify="center")

            for event_type, count in consumed_count.items():
                if count > 0:
                    display = get_display_info(event_type)
                    summary_table.add_row(
                        f"{display['emoji']} {display['label']}",
                        f"[{display['rich_color']}]{count}[/{display['rich_color']}]",
                    )

            console.print(summary_table)

        console.print(
            f"\n[bold green]✅ Consumer closed gracefully. "
            f"Total messages consumed: {total}[/bold green]\n"
        )


if __name__ == "__main__":
    main()
