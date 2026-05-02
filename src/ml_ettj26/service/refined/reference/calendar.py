from __future__ import annotations

import pandas as pd

def build_refined_dim_calendar_br_market(df_trusted : pd.DataFrame) -> pd.DataFrame:
    return (
        df_trusted.copy()
        .pipe(rename_calendar_columns)
        .pipe(add_act_index)
        .pipe(add_date_parts)
        .pipe(add_calendar_flags)
        .pipe(finalize_calendar_schema)
        .pipe(validate_refined_calendar)
    )

def rename_calendar_columns(df : pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"cal_id":"calendar_id"})

def add_act_index(df : pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()
    df_out["act_index"] = range(len(df_out))
    return df_out

def add_date_parts(df : pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()
    
    df_out["year"] = df_out["date"].dt.year.astype("int32")
    df_out["month"] = df_out["date"].dt.month.astype("int32")
    df_out["day"] = df_out["date"].dt.day.astype("int32")
    df_out["date"] = df_out["date"].dt.date
    
    return df_out

def add_calendar_flags(df : pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()

    df_out["is_weekend"] = df_out["weekday"].isin([5,6])
    df_out["is_holiday"] = df_out["holiday_name"].notna()

    return df_out

def finalize_calendar_schema(df : pd.DataFrame) -> pd.DataFrame:
    cols = [
        "calendar_id",
        "date",
        "year",
        "month",
        "day",
        "weekday",
        "is_weekend",
        "is_holiday",
        "is_business_day",
        "act_index",
        "bd_index",
        "holiday_name",
        "source_file_hash"]
    
    df_out = df[cols].sort_values(by=["calendar_id", "date"]).reset_index(drop=True)
    
    return df_out

def validate_refined_calendar(df: pd.DataFrame) -> pd   .DataFrame:
    if df.duplicated(["calendar_id", "date"]).any():
        raise ValueError("Duplicated key in refined calendar: ['calendar_id', 'date'].")
    
    invalid_business_day = df.loc[
        (df["is_business_day"]) & (df["is_weekend"] | df["is_holiday"])
    ]

    if not invalid_business_day.empty:
        raise ValueError("Found invalid business day flagged on weekend/holiday")
    
    if not df["bd_index"].is_monotonic_increasing:
        raise ValueError("bd_index must be monotonic increasing")
    
    return df
