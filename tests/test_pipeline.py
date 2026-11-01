import pandas as pd

from src import config
from src.clean import validate
from src.helpers import map_values, parse_dates, point_in_time_join, to_number
from src.pipeline import merge_batch


def booking(booking_id, revenue, updated_at, batch_date="2025-09-01", source_row=2):
    return {"booking_id": booking_id, "property_id": "P001", "check_in": pd.Timestamp("2025-07-01"),
            "check_out": pd.Timestamp("2025-07-03"), "num_guests": 2, "room_rate": 100.0,
            "currency": "USD", "revenue": revenue, "booking_channel": "OTA",
            "booking_status": "confirmed", "created_at": pd.Timestamp("2025-01-01"),
            "updated_at": pd.Timestamp(updated_at), "batch_date": pd.Timestamp(batch_date),
            "source_row": source_row}


def test_merge_inserts_updates_and_rerun_changes_nothing():
    batch1 = pd.DataFrame([booking("B1", 200.0, "2025-01-01"), booking("B2", 300.0, "2025-01-01", source_row=3)])
    state, _ = merge_batch(pd.DataFrame(), batch1)

    batch2 = pd.DataFrame([
        booking("B1", 200.0, "2025-01-01", "2025-09-02"),          # re-sent unchanged
        booking("B2", 350.0, "2025-02-01", "2025-09-02", 3),       # amended
        booking("B3", 400.0, "2025-02-01", "2025-09-02", 4),       # new
    ])
    state, audit = merge_batch(state, batch2)
    assert (audit["inserted"], audit["updated"], audit["unchanged"]) == (1, 1, 1)
    assert state.set_index("booking_id").loc["B2", "revenue"] == 350.0

    rerun, audit = merge_batch(state, batch2)                       # process batch 2 again
    assert rerun.equals(state)
    assert (audit["inserted"], audit["updated"], audit["unchanged"]) == (0, 0, 3)


def test_older_update_arriving_late_does_not_overwrite():
    state, _ = merge_batch(pd.DataFrame(), pd.DataFrame([booking("B1", 500.0, "2025-03-01")]))
    late = pd.DataFrame([booking("B1", 999.0, "2025-02-01", "2025-09-02")])
    state, audit = merge_batch(state, late)
    assert state.loc[0, "revenue"] == 500.0
    assert audit["stale_ignored"] == 1


def test_fx_rate_is_the_one_effective_on_created_date():
    fx = pd.DataFrame({"currency": ["USD", "USD", "USD"], "fx_rate_to_zar": [17.5, 18.5, 19.0],
                       "fx_effective_from": pd.to_datetime(["2024-01-01", "2025-01-01", "2025-07-01"])})
    df = pd.DataFrame({"created_date": pd.to_datetime(["2024-12-31", "2025-01-01", "2025-08-15", "2023-06-01"]),
                       "currency": ["USD"] * 4})
    out = point_in_time_join(df, fx, "created_date", "fx_effective_from", by="currency")
    assert out["fx_rate_to_zar"].tolist()[:3] == [17.5, 18.5, 19.0]
    assert pd.isna(out["fx_rate_to_zar"].iloc[3])                 # before the first rate


def test_cleaning_dates_currencies_and_numbers():
    dates = parse_dates(pd.Series(["2025-02-22", "24/04/2025", "22 Jul 2025", " 2025-06-10 ", "rubbish"]),
                        config.DATE_FORMATS)
    assert dates.dt.strftime("%Y-%m-%d").tolist()[:4] == ["2025-02-22", "2025-04-24", "2025-07-22", "2025-06-10"]
    assert pd.isna(dates.iloc[4])
    currencies = map_values(pd.Series(["€", "us$", " gbp", "R", "XYZ"]), config.CURRENCY_MAP)
    assert currencies.tolist()[:4] == ["EUR", "USD", "GBP", "ZAR"] and pd.isna(currencies.iloc[4])
    assert to_number(pd.Series(["12,000.00", "", "-5"])).tolist()[::2] == [12000.0, -5.0]


def test_validation_rejects_bad_rows_with_all_reasons():
    good = {**booking("B1", 200.0, "2025-01-01"), "property_name": "Lodge", "fx_rate_to_zar": 18.5}
    bad = {**good, "booking_id": "B2", "revenue": -1.0, "check_out": pd.Timestamp("2025-06-01"),
           "property_name": None}
    kept, rejects, counts = validate(pd.DataFrame([good, bad]))
    assert kept["booking_id"].tolist() == ["B1"]
    assert rejects["reject_reasons"].item() == \
        "check_out_not_after_check_in;missing_or_non_positive_revenue;orphan_property"
    assert counts["orphan_property"] == 1
