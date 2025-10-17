from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

YEAR_COLUMNS: Iterable[str] = [str(year) for year in range(2000, 2025)]


def read_csv_records(csv_path: Path) -> list[dict]:
    """Read a CSV into a list of dictionaries, normalising data for MongoDB."""
    df = pd.read_csv(csv_path)

    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.dropna(axis=1, how="all")

    if "country_serial" in df.columns:
        df["country_serial"] = pd.to_numeric(df["country_serial"], errors="coerce")

    for column in YEAR_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.astype(object).where(pd.notnull(df), None)

    if "country_serial" in df.columns:
        df["country_serial"] = df["country_serial"].apply(
            lambda value: int(value) if isinstance(value, (int, float)) and value is not None else value
        )

    year_columns_present = [col for col in YEAR_COLUMNS if col in df.columns]
    if year_columns_present:
        df = df[df[year_columns_present].apply(lambda row: any(val is not None for val in row), axis=1)]

    records: list[dict] = df.to_dict("records")
    return records
