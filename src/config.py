"""All paths, mappings and thresholds in one place."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"

BATCH_PATTERN = "bookings_batch*.csv"
PROPERTIES_FILE = DATA_DIR / "properties.csv"
FX_FILE = DATA_DIR / "fx_rates.csv"

STATE_FILE = OUTPUT_DIR / "state_current_bookings.parquet"  # pipeline state between runs
CURRENT_FILE = OUTPUT_DIR / "bookings_analysis_ready.csv"
REJECTS_FILE = OUTPUT_DIR / "rejects.csv"
DQ_FILE = OUTPUT_DIR / "dq_summary.csv"
AUDIT_FILE = OUTPUT_DIR / "merge_audit.csv"

# Slash dates are day-first (e.g. 24/04/2025).
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"]

# Keys are lower case, trimmed, with repeated spaces collapsed.
CURRENCY_MAP = {
    "zar": "ZAR", "r": "ZAR",
    "usd": "USD", "us$": "USD", "$": "USD",   # assumption: "$" means US dollars
    "eur": "EUR", "€": "EUR",
    "gbp": "GBP", "£": "GBP",
}
CHANNEL_MAP = {
    "direct": "Direct",
    "ota": "OTA", "o.t.a": "OTA",
    "website": "Website",
    "travel agent": "Travel Agent",
}
STATUSES = ["confirmed", "pending", "cancelled"]
COUNTRY_MAP = {"S. Africa": "South Africa"}

MIN_GUESTS, MAX_GUESTS = 1, 12

# Columns that define a booking's content. Used to tell an update from a re-send.
COMPARE_COLS = [
    "property_id", "check_in", "check_out", "num_guests", "room_rate", "currency",
    "revenue", "booking_channel", "booking_status", "created_at", "updated_at",
]
# Newest version wins. batch_date and source_row break ties on updated_at.
VERSION_ORDER = ["updated_at", "batch_date", "source_row"]
