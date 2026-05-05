"""
Multi-Schema Producer for Confluent Cloud.

Produces OrderStatus, OrderPayment, and OrderShipment events to a single Kafka
topic using TopicRecordNameStrategy, so each schema is registered under its own
subject (e.g. orders-multi-schema-OrderStatus-value).
"""

import random
import string
import uuid
from datetime import datetime, timedelta, timezone

import pyfiglet
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
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
    get_num_messages,
    get_producer_flush_timeout,
)
from schemas import (
    ORDER_STATUS_SCHEMA_PATH,
    ORDER_PAYMENT_SCHEMA_PATH,
    ORDER_SHIPMENT_SCHEMA_PATH,
    get_schema_str,
    get_display_info,
)
from logger import setup_logging

logger = setup_logging(__name__)

console = Console()

# ── Banner ─────────────────────────────────────────────────────────────────────

def show_banner():
    """Display a colorful ASCII art banner."""
    banner_text = pyfiglet.figlet_format("Multi-Schema", font="slant")
    subtitle = pyfiglet.figlet_format("Producer", font="slant")

    console.print(f"[bold cyan]{banner_text}[/bold cyan]", end="")
    console.print(f"[bold magenta]{subtitle}[/bold magenta]")
    console.print(
        Panel(
            "[bold white]Confluent Cloud · Multi-Schema Topic Demo[/bold white]\n"
            "[dim]Producing OrderStatus, OrderPayment & OrderShipment events[/dim]",
            border_style="bright_cyan",
            padding=(1, 2),
        )
    )
    console.print()


# ── Data generators ────────────────────────────────────────────────────────────

def generate_order_id() -> str:
    return f"ORD-{uuid.uuid4().hex[:8].upper()}"


def generate_customer_id() -> str:
    return f"CUST-{random.randint(1000, 9999)}"


def generate_tracking_number() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=12))


def generate_order_status() -> dict:
    """Generate a random OrderStatus event."""
    return {
        "order_id": generate_order_id(),
        "status": random.choice(
            ["PENDING", "CONFIRMED", "PROCESSING", "COMPLETED", "CANCELLED"]
        ),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "customer_id": generate_customer_id(),
    }


def generate_order_payment() -> dict:
    """Generate a random OrderPayment event."""
    return {
        "order_id": generate_order_id(),
        "payment_method": random.choice(
            ["CREDIT_CARD", "DEBIT_CARD", "PIX", "PAYPAL", "BANK_TRANSFER"]
        ),
        "amount": round(random.uniform(9.99, 999.99), 2),
        "currency": random.choice(["USD", "EUR", "BRL", "GBP"]),
        "payment_status": random.choice(
            ["INITIATED", "AUTHORIZED", "CAPTURED", "REFUNDED", "FAILED"]
        ),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_order_shipment() -> dict:
    """Generate a random OrderShipment event."""
    now = datetime.now(timezone.utc)
    return {
        "order_id": generate_order_id(),
        "tracking_number": generate_tracking_number(),
        "carrier": random.choice(["FEDEX", "UPS", "DHL", "USPS", "CORREIOS"]),
        "shipment_status": random.choice(
            ["LABEL_CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]
        ),
        "estimated_delivery": (now + timedelta(days=random.randint(1, 10))).isoformat(),
        "shipped_at": now.isoformat(),
    }


# ── Subject name strategy ─────────────────────────────────────────────────────

def topic_record_subject_name_strategy(ctx, record_name):
    """
    TopicRecordNameStrategy: subject = <topic>-<record_name>-value
    This allows each schema to evolve independently under its own subject.
    """
    return f"{ctx.topic}-{record_name}-value"


# ── Serializer factory ────────────────────────────────────────────────────────

def create_json_serializer(schema_registry_client, schema_path, record_name):
    """Create a JSONSerializer for a given schema with TopicRecordNameStrategy."""
    schema_str = get_schema_str(schema_path)

    def subject_name_strategy(ctx, _record_name):
        return f"{ctx.topic}-{record_name}-value"

    return JSONSerializer(
        schema_str=schema_str,
        schema_registry_client=schema_registry_client,
        conf={"subject.name.strategy": subject_name_strategy},
    )


# ── Delivery callback ─────────────────────────────────────────────────────────

produced_count = {"OrderStatus": 0, "OrderPayment": 0, "OrderShipment": 0}


def delivery_report(err, msg, event_type):
    """Callback for message delivery reports."""
    display = get_display_info(event_type)
    if err is not None:
        console.print(
            f"  [bold red]✗ FAILED[/bold red] {display['emoji']} {display['label']}: {err}"
        )
    else:
        produced_count[event_type] += 1
        console.print(
            f"  [{display['rich_color']}]✓ Delivered[/{display['rich_color']}] "
            f"{display['emoji']} {display['label']} → "
            f"[dim]partition={msg.partition()}, offset={msg.offset()}[/dim]"
        )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    show_banner()

    topic = get_topic_name()
    num_messages = get_num_messages()
    flush_timeout = get_producer_flush_timeout()

    logger.info(f"Starting producer - topic={topic}, messages={num_messages}")

    console.print(f"[bold]📋 Topic:[/bold]    [cyan]{topic}[/cyan]")
    console.print(f"[bold]📦 Messages:[/bold] [cyan]{num_messages}[/cyan] ({num_messages // 3} per schema type)")
    console.print(f"[bold]📐 Strategy:[/bold] [cyan]TopicRecordNameStrategy[/cyan]")
    console.print()

    # ── Initialize clients ─────────────────────────────────────────────────
    console.print("[dim]Connecting to Confluent Cloud...[/dim]")

    try:
        kafka_config = get_kafka_config(producer=True)
        sr_config = get_schema_registry_config()
        schema_registry_client = SchemaRegistryClient(sr_config)
        producer = Producer(kafka_config)
        logger.info("Successfully connected to Kafka and Schema Registry")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka clients: {e}")
        console.print(f"\n[bold red]✗ Connection failed: {e}[/bold red]\n")
        return 1

    # ── Create serializers ─────────────────────────────────────────────────
    console.print("[dim]Initializing JSON serializers...[/dim]\n")

    try:
        serializers = {
            "OrderStatus": create_json_serializer(
                schema_registry_client, ORDER_STATUS_SCHEMA_PATH, "OrderStatus"
            ),
            "OrderPayment": create_json_serializer(
                schema_registry_client, ORDER_PAYMENT_SCHEMA_PATH, "OrderPayment"
            ),
            "OrderShipment": create_json_serializer(
                schema_registry_client, ORDER_SHIPMENT_SCHEMA_PATH, "OrderShipment"
            ),
        }
        logger.info("Successfully initialized serializers for all event types")
    except Exception as e:
        logger.error(f"Failed to create serializers: {e}")
        console.print(f"\n[bold red]✗ Serializer creation failed: {e}[/bold red]\n")
        return 1

    generators = {
        "OrderStatus": generate_order_status,
        "OrderPayment": generate_order_payment,
        "OrderShipment": generate_order_shipment,
    }

    # Event types cycle: Status → Payment → Shipment → ...
    event_types = ["OrderStatus", "OrderPayment", "OrderShipment"]

    # ── Produce messages ───────────────────────────────────────────────────
    console.print(
        Panel(
            "[bold]Producing messages...[/bold]",
            border_style="bright_green",
            padding=(0, 2),
        )
    )
    console.print()

    for i in range(num_messages):
        event_type = event_types[i % 3]
        data = generators[event_type]()
        display = get_display_info(event_type)

        # Show what we're producing
        console.print(
            f"  [dim]#{i + 1:02d}[/dim] Producing {display['emoji']} "
            f"[{display['rich_color']}]{display['label']}[/{display['rich_color']}] "
            f"[dim]order_id={data['order_id']}[/dim]"
        )

        try:
            # Serialize and produce
            serialization_ctx = SerializationContext(topic, MessageField.VALUE)
            serialized_value = serializers[event_type](data, serialization_ctx)

            producer.produce(
                topic=topic,
                key=data["order_id"],
                value=serialized_value,
                on_delivery=lambda err, msg, et=event_type: delivery_report(err, msg, et),
            )

            producer.poll(0)
            logger.debug(f"Queued {event_type} message: {data['order_id']}")

        except BufferError as e:
            # Local queue is full, wait for messages to be sent
            logger.warning(f"Producer queue full, waiting... {e}")
            console.print(f"  [yellow]⚠ Queue full, flushing...[/yellow]")
            producer.flush(timeout=10)
            # Retry this message
            i -= 1
            continue

        except Exception as e:
            logger.error(f"Failed to produce {event_type} message: {e}")
            console.print(
                f"  [bold red]✗ Failed to produce {display['label']}: {e}[/bold red]"
            )

    # ── Flush and summary ──────────────────────────────────────────────────
    console.print("\n[dim]Flushing producer...[/dim]")
    remaining = producer.flush(timeout=flush_timeout)
    if remaining > 0:
        logger.warning(f"{remaining} messages failed to deliver after flush")
        console.print(f"[yellow]⚠ {remaining} messages not delivered[/yellow]\n")
    else:
        logger.info("All messages flushed successfully")
    console.print()

    # Summary table
    summary_table = Table(
        title="📊 Production Summary",
        show_header=True,
        header_style="bold white",
        border_style="bright_cyan",
    )
    summary_table.add_column("Event Type", style="bold")
    summary_table.add_column("Count", justify="center")
    summary_table.add_column("Subject Name", style="dim")

    for event_type, count in produced_count.items():
        display = get_display_info(event_type)
        summary_table.add_row(
            f"{display['emoji']} {display['label']}",
            f"[{display['rich_color']}]{count}[/{display['rich_color']}]",
            f"{topic}-{event_type}-value",
        )

    console.print(summary_table)
    total_produced = sum(produced_count.values())
    logger.info(f"Producer finished - {total_produced}/{num_messages} messages delivered")
    console.print(
        f"\n[bold green]✅ Successfully produced {total_produced} messages "
        f"to topic [cyan]{topic}[/cyan][/bold green]\n"
    )

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code if exit_code is not None else 0)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
        console.print("\n[yellow]⚠ Interrupted by user[/yellow]\n")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unexpected error in producer: {e}")
        console.print(f"\n[bold red]✗ Unexpected error: {e}[/bold red]\n")
        sys.exit(1)
