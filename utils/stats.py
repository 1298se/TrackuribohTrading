import datetime
from datetime import timedelta

import pandas as pd
from matplotlib import dates
from pandas import DataFrame


def remove_outliers_iqr(df: DataFrame, axis: str) -> DataFrame:
    q3 = df[axis].quantile(0.75)
    q1 = df[axis].quantile(0.25)
    iqr = q3 - q1

    upper_bound = q3 + 1.5 * iqr
    lower_bound = q1 - 1.5 * iqr

    filtered_df = df[
        (df[axis] < upper_bound) & (df[axis] > lower_bound)
        ]

    return filtered_df


def get_past_week_data(df: DataFrame, axis: str) -> DataFrame:
    return df[df[axis] > (datetime.datetime.now() - timedelta(days=7))].sort_values(axis)


def shift_series_by_time_delta(series, time_delta) -> list:
    return dates.date2num(series - (datetime.datetime.now() - time_delta) + pd.to_datetime(dates.get_epoch()))
