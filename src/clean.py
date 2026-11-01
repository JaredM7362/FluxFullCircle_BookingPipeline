"""Clean, enrich and validate one batch of bookings."""
import pandas as pd

from src import config
from src.helpers import clean_text, map_values, parse_dates, point_in_time_join, to_number


def clean_bookings(raw: pd.DataFrame, batch_date: str, source_file: str) -> pd.DataFrame:
    guests = to_number(raw["num_guests"])
    guests = guests.where(guests % 1 == 0).astype("Int64")  # whole numbers only; 2.5 -> blank

    df = pd.DataFrame({
        "booking_id": clean_text(raw["booking_id"]).str.upper(),
        "property_id": clean_text(raw["property_id"]).str.upper(),
        "check_in": parse_dates(raw["check_in"], config.DATE_FORMATS),
        "check_out": parse_dates(raw["check_out"], config.DATE_FORMATS),
        "num_guests": guests,
        "room_rate": to_number(raw["room_rate"]),
        "currency": map_values(raw["currency"], config.CURRENCY_MAP),
        "revenue": to_number(raw["revenue"]),
        "booking_channel": map_values(raw["booking_channel"], config.CHANNEL_MAP),
        "booking_status": clean_text(raw["booking_status"]).str.lower(),
        "created_at": pd.to_datetime(clean_text(raw["created_at"]), errors="coerce"),
        "updated_at": pd.to_datetime(clean_text(raw["updated_at"]), errors="coerce"),
    })
    # Lineage: where each row came from. Also used to break updated_at ties.
    df["batch_date"] = pd.Timestamp(batch_date)
    df["source_file"] = source_file
    df["source_row"] = range(2, len(df) + 2)  # line number in the file (header is line 1)
    df["raw_currency"] = raw["currency"]      # kept so rejects show what arrived
    return df


def clean_properties(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.apply(clean_text)
    df["property_id"] = df["property_id"].str.upper()
    df["country"] = df["country"].replace(config.COUNTRY_MAP)
    return df


def clean_fx(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "currency": map_values(raw["currency"], config.CURRENCY_MAP),
        "fx_rate_to_zar": to_number(raw["rate_to_zar"]),
        "fx_effective_from": pd.to_datetime(raw["effective_from"]),
    })


def enrich(df: pd.DataFrame, properties: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    """Add property attributes and the FX rate effective on the created_at date."""
    df = df.merge(properties[["property_id", "property_name", "country", "region"]],
                  on="property_id", how="left")
    df["created_date"] = df["created_at"].dt.normalize()
    df = point_in_time_join(df, fx, "created_date", "fx_effective_from", by="currency")
    df["revenue_zar"] = (df["revenue"] * df["fx_rate_to_zar"]).round(2)
    return df


def validate(df: pd.DataFrame):
    """Split rows into good and rejected. Each reject lists every rule it broke.
    Returns (good, rejects, counts per rule)."""
    nights = (df["check_out"] - df["check_in"]).dt.days
    checks = {
        "missing_booking_id": df["booking_id"].isna(),
        "bad_timestamps": df["created_at"].isna() | df["updated_at"].isna(),
        "updated_before_created": df["updated_at"] < df["created_at"],
        "bad_dates": df["check_in"].isna() | df["check_out"].isna(),
        "check_out_not_after_check_in": nights <= 0,
        "bad_num_guests": ~df["num_guests"].between(config.MIN_GUESTS, config.MAX_GUESTS),
        "missing_or_non_positive_revenue": ~(df["revenue"] > 0),
        "unknown_currency": df["currency"].isna(),
        "unknown_channel": df["booking_channel"].isna(),
        "unknown_status": ~df["booking_status"].isin(config.STATUSES),
        "orphan_property": df["property_name"].isna(),
        "no_fx_rate": df["currency"].notna() & df["fx_rate_to_zar"].isna(),
    }
    reasons = pd.Series("", index=df.index)
    for name, failed in checks.items():
        reasons = reasons.where(~failed, reasons + name + ";")

    is_bad = reasons != ""
    rejects = df[is_bad].assign(reject_reasons=reasons[is_bad].str.rstrip(";"))
    counts = {name: int(failed.sum()) for name, failed in checks.items()}
    return df[~is_bad].copy(), rejects, counts
