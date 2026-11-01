"""Incremental bookings pipeline.

For each batch file (oldest first):
    read -> clean -> enrich -> validate -> merge good rows into the saved state
Then write the analysis-ready table, rejects, DQ summary and merge audit.

Run:  python -m src.pipeline              (all batch files in data/)
      python -m src.pipeline --batch bookings_batch2_2025-09-02.csv
Re-running is safe: the merge keeps the newest version per booking, so
processing the same file twice gives the same result.
"""
import argparse
import logging
import re
from datetime import datetime

import pandas as pd

from src import config
from src.clean import clean_bookings, clean_fx, clean_properties, enrich, validate
from src.helpers import latest_per_key

log = logging.getLogger("pipeline")


def load_state() -> pd.DataFrame:
    return pd.read_parquet(config.STATE_FILE) if config.STATE_FILE.exists() else pd.DataFrame()


def merge_batch(state: pd.DataFrame, batch: pd.DataFrame):
    """Merge a cleaned batch into the current state.

    Returns the new state and an audit dict: inserted / updated / unchanged /
    stale_ignored / duplicate_in_batch.
    """
    new_state = latest_per_key(pd.concat([state, batch]), "booking_id", config.VERSION_ORDER)

    batch_latest = latest_per_key(batch, "booking_id", config.VERSION_ORDER).set_index("booking_id")
    audit = {"rows_in": len(batch), "duplicate_in_batch": len(batch) - len(batch_latest)}

    # First run: no state yet, so use an empty table with the same columns.
    old = (state if len(state) else batch.iloc[0:0]).set_index("booking_id")
    is_new = ~batch_latest.index.isin(old.index)
    existing = batch_latest.index[~is_new]

    # For bookings we already had: older than what we hold, or a real change?
    # .to_numpy() compares values row by row in the same order (both are looked up
    # by `existing`), without pandas checking that the two indexes have the same type.
    is_stale = (batch_latest.loc[existing, "updated_at"].to_numpy()
                < old.loc[existing, "updated_at"].to_numpy())
    after = new_state.set_index("booking_id").loc[existing, config.COMPARE_COLS].astype(str).to_numpy()
    before = old.loc[existing, config.COMPARE_COLS].astype(str).to_numpy()
    changed = (after != before).any(axis=1)

    audit["inserted"] = int(is_new.sum())
    audit["stale_ignored"] = int(is_stale.sum())
    audit["updated"] = int((changed & ~is_stale).sum())
    audit["unchanged"] = int((~changed & ~is_stale).sum())
    return new_state.reset_index(drop=True), audit


def analysis_ready(state: pd.DataFrame) -> pd.DataFrame:
    """Status decision: every booking stays in the table with its current status.
    Only confirmed bookings count as revenue (recognised_revenue_zar);
    cancelled earn nothing (no fee data), pending are not revenue yet."""
    df = state.copy()
    df["nights"] = (df["check_out"] - df["check_in"]).dt.days
    df["is_cancelled"] = df["booking_status"] == "cancelled"
    df["recognised_revenue_zar"] = df["revenue_zar"].where(df["booking_status"] == "confirmed", 0.0)
    return df.drop(columns=["raw_currency"]).sort_values("booking_id")


def warnings(batch: pd.DataFrame, batch_file: str, rows: int) -> pd.DataFrame:
    """Suspicious but not invalid: counted in the DQ summary, rows still kept."""
    nights = (batch["check_out"] - batch["check_in"]).dt.days
    content = pd.util.hash_pandas_object(batch[config.COMPARE_COLS].astype("string").fillna(""), index=False)
    same_time = batch.assign(content=content.to_numpy()).groupby(["booking_id", "updated_at"])["content"]
    checks = {
        "warn_created_after_batch_date": batch["created_at"].dt.normalize() > batch["batch_date"],
        # e.g. B1021: two different rows with the same updated_at; file order decides
        "warn_conflicting_rows_same_updated_at": same_time.transform("nunique") > 1,
        "warn_revenue_not_rate_x_nights": (batch["revenue"] - batch["room_rate"] * nights).abs() > 1,
        "warn_pending_after_stay_ended": (batch["booking_status"] == "pending")
                                         & (batch["check_out"] < batch["batch_date"]),
    }
    return pd.DataFrame({"batch_file": batch_file, "check": list(checks), "rows_checked": rows,
                         "rows_failed": [int(m.sum()) for m in checks.values()]})


def append_csv(path, new: pd.DataFrame, unique_on: list = None) -> None:
    """Append rows to a CSV; optionally drop repeats so re-runs don't duplicate."""
    if path.exists():
        new = pd.concat([pd.read_csv(path, encoding="utf-8-sig"), new.astype(object)])
    if unique_on:
        new = new.drop_duplicates(unique_on, keep="last")
    new.to_csv(path, index=False, encoding="utf-8-sig")  # BOM: Excel shows € and £ correctly


def run(batch_name: str = None) -> pd.DataFrame:
    config.OUTPUT_DIR.mkdir(exist_ok=True)
    properties = clean_properties(pd.read_csv(config.PROPERTIES_FILE, dtype=str))
    fx = clean_fx(pd.read_csv(config.FX_FILE, dtype=str))
    state = load_state()
    run_at = datetime.now().isoformat(timespec="seconds")

    files = sorted(config.DATA_DIR.glob(config.BATCH_PATTERN))
    if batch_name:
        files = [f for f in files if f.name == batch_name]

    for path in files:
        batch_date = re.search(r"\d{4}-\d{2}-\d{2}", path.name).group()
        raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        batch = enrich(clean_bookings(raw, batch_date, path.name), properties, fx)
        good, rejects, counts = validate(batch)

        state, audit = merge_batch(state, good)
        log.info("%s: %d rows, %d rejected, merge %s", path.name, len(raw), len(rejects), audit)

        append_csv(config.AUDIT_FILE, pd.DataFrame([{"run_at": run_at, "batch_file": path.name,
                                                     "rejected": len(rejects), **audit}]))
        rejects = rejects.assign(check_in=rejects["check_in"].dt.date, check_out=rejects["check_out"].dt.date)
        append_csv(config.REJECTS_FILE,
                   rejects[["source_file", "source_row", "booking_id", "reject_reasons", "raw_currency",
                            "revenue", "check_in", "check_out", "num_guests", "property_id"]],
                   unique_on=["source_file", "source_row"])
        dq = pd.DataFrame({"batch_file": path.name, "check": list(counts), "rows_checked": len(raw),
                           "rows_failed": list(counts.values())})
        dq = pd.concat([dq, warnings(batch, path.name, len(raw))])
        append_csv(config.DQ_FILE, dq, unique_on=["batch_file", "check"])

    state.to_parquet(config.STATE_FILE, index=False)
    result = analysis_ready(state)
    result.to_csv(config.CURRENT_FILE, index=False, encoding="utf-8-sig")
    log.info("Done: %d current bookings written to %s", len(result), config.CURRENT_FILE.name)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", help="process only this batch file")
    run(parser.parse_args().batch)
