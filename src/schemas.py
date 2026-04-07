"""
Schema definitions and helpers for the three event types:
OrderStatus, OrderPayment, and OrderShipment.
"""

import json
import os

# ── Schema file paths ──────────────────────────────────────────────────────────
SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schemas")

ORDER_STATUS_SCHEMA_PATH = os.path.join(SCHEMAS_DIR, "order_status.json")
ORDER_PAYMENT_SCHEMA_PATH = os.path.join(SCHEMAS_DIR, "order_payment.json")
ORDER_SHIPMENT_SCHEMA_PATH = os.path.join(SCHEMAS_DIR, "order_shipment.json")


def _load_schema(path: str) -> str:
    """Load a JSON Schema file and return it as a string."""
    with open(path, "r") as f:
        return f.read()


def get_schema_str(schema_path: str) -> str:
    """Load and return a schema as a JSON string."""
    return _load_schema(schema_path)


def get_schema_dict(schema_path: str) -> dict:
    """Load and return a schema as a Python dict."""
    with open(schema_path, "r") as f:
        return json.load(f)


# ── Display metadata per event type ────────────────────────────────────────────
EVENT_TYPE_DISPLAY = {
    "OrderStatus": {
        "emoji": "🟢",
        "color": "green",
        "rich_color": "bold green",
        "label": "Order Status",
    },
    "OrderPayment": {
        "emoji": "🔵",
        "color": "blue",
        "rich_color": "bold blue",
        "label": "Order Payment",
    },
    "OrderShipment": {
        "emoji": "🟠",
        "color": "yellow",
        "rich_color": "bold yellow",
        "label": "Order Shipment",
    },
}


def identify_event_type(data: dict) -> str:
    """
    Identify the event type from a deserialized message by inspecting its fields.
    Returns the schema title string: 'OrderStatus', 'OrderPayment', or 'OrderShipment'.
    """
    if "tracking_number" in data or "carrier" in data:
        return "OrderShipment"
    elif "payment_method" in data or "amount" in data:
        return "OrderPayment"
    elif "status" in data or "customer_id" in data:
        return "OrderStatus"
    else:
        return "Unknown"


def get_display_info(event_type: str) -> dict:
    """Return display metadata (emoji, color, label) for a given event type."""
    return EVENT_TYPE_DISPLAY.get(
        event_type,
        {"emoji": "⚪", "color": "white", "rich_color": "white", "label": event_type},
    )
