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

from config import (
    get_kafka_config,
    get_schema_registry_config,
    get_topic_name,
    get_consumer_group_id,
    get_auto_offset_reset,
    get_consumer_poll_timeout,
)
from schemas import identify_event_type, get_display_info
from logger import setup_logging

logger = setup_logging(__name__)

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
    group_id = get_consumer_group_id()
    poll_timeout = get_consumer_poll_timeout()

    logger.info(f"Starting consumer - topic={topic}, group_id={group_id}")

    console.print(f"[bold]📋 Topic:[/bold]       [cyan]{topic}[/cyan]")
    console.print(f"[bold]📐 Strategy:[/bold]    [cyan]TopicRecordNameStrategy[/cyan]")
    console.print(f"[bold]🔄 Group ID:[/bold]    [cyan]{group_id}[/cyan]")
    console.print()
    console.print(
        "[bold yellow]💡 Press CTRL+C to stop the consumer[/bold yellow]\n"
    )

    # ── Initialize clients ─────────────────────────────────────────────────
    console.print("[dim]Connecting to Confluent Cloud...[/dim]")

    try:
        kafka_config = get_kafka_config(producer=False)
        kafka_config.update(
            {
                "group.id": group_id,
                "auto.offset.reset": get_auto_offset_reset(),
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
        logger.info(f"Successfully connected and subscribed to {topic}")

    except Exception as e:
        logger.error(f"Failed to initialize consumer: {e}")
        console.print(f"\n[bold red]✗ Connection failed: {e}[/bold red]\n")
        return 1

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
            msg = consumer.poll(timeout=poll_timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                    console.print(
                        f"[dim]  ⏸ Reached end of partition {msg.partition()} "
                        f"at offset {msg.offset()}[/dim]"
                    )
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())
                continue

            # Deserialize the message value
            try:
                serialization_ctx = SerializationContext(
                    msg.topic(), MessageField.VALUE
                )
                data = json_deserializer(msg.value(), serialization_ctx)

                if data is not None:
                    logger.debug(f"Consumed message from partition {msg.partition()}, offset {msg.offset()}")
                    display_message(data, msg.partition(), msg.offset())

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error at offset {msg.offset()}: {e}")
                console.print(
                    f"[bold red]✗ Invalid JSON at offset {msg.offset()}: {e}[/bold red]"
                )

            except Exception as e:
                # Try fallback: plain JSON if message wasn't serialized with SR
                logger.warning(f"Schema Registry deserialization failed, trying plain JSON: {e}")
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    display_message(data, msg.partition(), msg.offset())
                except Exception as fallback_error:
                    logger.error(f"Failed to deserialize message at offset {msg.offset()}: {fallback_error}")
                    console.print(
                        f"[bold red]✗ Failed to deserialize message at "
                        f"offset {msg.offset()}: {e}[/bold red]"
                    )

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
        pass
    except KafkaException as e:
        logger.error(f"Kafka error in consumer loop: {e}")
        console.print(f"\n[bold red]✗ Kafka error: {e}[/bold red]\n")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error in consumer: {e}")
        console.print(f"\n[bold red]✗ Unexpected error: {e}[/bold red]\n")
        return 1
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

        logger.info(f"Consumer finished - {total} messages consumed")
        console.print(
            f"\n[bold green]✅ Consumer closed gracefully. "
            f"Total messages consumed: {total}[/bold green]\n"
        )

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code if exit_code is not None else 0)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
        console.print("\n[yellow]⚠ Interrupted by user[/yellow]\n")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        console.print(f"\n[bold red]✗ Unexpected error: {e}[/bold red]\n")
        sys.exit(1)
