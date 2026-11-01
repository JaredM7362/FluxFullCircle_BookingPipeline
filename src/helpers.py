import pandas as pd


def clean_text(s: pd.Series) -> pd.Series:
    """Trim and collapse repeated spaces: '  Travel  Agent ' -> 'Travel Agent'."""
    return s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)


def map_values(s: pd.Series, mapping: dict) -> pd.Series:
    """Map messy spellings to standard values (case-insensitive). Unknown -> NA."""
    return clean_text(s).str.lower().map(mapping).astype("string")


def parse_dates(s: pd.Series, formats: list) -> pd.Series:
    """Parse a column with mixed date formats, trying each format in turn."""
    s = clean_text(s)
    result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formats:
        result = result.fillna(pd.to_datetime(s, format=fmt, errors="coerce"))
    return result


def to_number(s: pd.Series) -> pd.Series:
    """'12,000.00' -> 12000.0. Blank or text -> NaN."""
    return pd.to_numeric(clean_text(s).str.replace(",", ""), errors="coerce")


def latest_per_key(df: pd.DataFrame, key: str, order_by: list) -> pd.DataFrame:
    """Keep the newest row per key (like SQL QUALIFY ROW_NUMBER() ... = 1)."""
    return df.sort_values([key, *order_by]).drop_duplicates(key, keep="last")


def point_in_time_join(df, rates, date_col, rate_date_col, by):
    """Attach the rate effective on each row's date: the latest rate_date_col
    on or before date_col, for the same `by` value. No match -> NaN."""
    df = df.copy()
    df["_order"] = range(len(df))
    has_date = df[date_col].notna()
    joined = pd.merge_asof(
        df[has_date].sort_values(date_col),
        rates.sort_values(rate_date_col),
        left_on=date_col, right_on=rate_date_col, by=by, direction="backward",
    )
    return pd.concat([joined, df[~has_date]]).sort_values("_order").drop(columns="_order")
