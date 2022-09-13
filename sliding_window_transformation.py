import datetime
import pytimeparse

from pyspark.sql.types import ArrayType
from pyspark.sql.types import TimestampType

from tecton import transformation, materialization_context


# Constant used for unbounded window period.
WINDOW_UNBOUNDED_PRECEDING = "unbounded_preceding"


def _validate_and_parse_time(duration: str, field_name: str, allow_unbounded: bool):
    """
        :param duration: time period string being validated
        :param field_name: transformation input being validated
        :param allow_unbounded: Allow unbounded duration string

        Validate duration strings for sliding_interval and window.
    """
    if allow_unbounded and duration.lower() == WINDOW_UNBOUNDED_PRECEDING:
        return None

    parsed_duration = pytimeparse.parse(duration)
    if parsed_duration is None:
        raise ValueError(f'Could not parse time string "{duration}"')

    duration_td = datetime.timedelta(seconds=parsed_duration)
    if duration_td is None:
        raise ValueError(f'Could not parse time string "{duration}"')
    elif duration_td.total_seconds() <= 0:
        raise ValueError(f"Duration {duration} provided for field {field_name} must be positive.")

    return duration_td


def _validate_sliding_window_duration(
        window_size: str, slide_interval: str
):
    """
        :param window_size: Window size
        :param slide_interval: How often a window is produced

        Validate that window size is a multiple of slide_interval.
    """
    slide_interval_td = _validate_and_parse_time(slide_interval, "slide_interval", allow_unbounded=False)
    window_size_td = _validate_and_parse_time(window_size, "window_size", allow_unbounded=True)
    if window_size_td is not None:
        # note this also confirms window >= slide since a>0, b>0, a % b = 0 implies a >= b
        if window_size_td.total_seconds() % slide_interval_td.total_seconds() != 0:
            raise ValueError(
                f"Window size {window_size} must be a multiple of slide interval {slide_interval}"
            )
    return window_size_td, slide_interval_td


def _parse_time(duration: str, allow_unbounded: bool):
    """
        :param duration: time period string converted to timedelta
        :param allow_unbouded: Allow unbounded time periods

        Convert string to timedelta.
    """
    if allow_unbounded and duration.lower() == WINDOW_UNBOUNDED_PRECEDING:
        return None
    return datetime.timedelta(seconds=pytimeparse.parse(duration))



def _align_time_downwards(time: datetime.datetime, alignment: datetime.timedelta) -> datetime.datetime:
    """
        :param time: timestamp
        :param alignment: How often window is produced

        Align timestamps down so that each window contains equivalent time 
        period of data

        Ex: If start_time = 3:30:00 pm and alignment is 1h then align down to 3:00:00 pm
    """
    excess_seconds = time.timestamp() % alignment.total_seconds()
    return datetime.datetime.utcfromtimestamp(time.timestamp() - excess_seconds)


def sliding_windows(
    timestamp: datetime.datetime,
    window_size: str,
    slide_interval: str,
    feature_start: datetime.datetime,
    feature_end: datetime.datetime,
):
    """
        :param timestamp: timestamp being exploded
        :param window_size: Window size
        :param slide_interval: How often a window is produced
        :param feature_start: Start of time window of data being materialized
        :param feature_end: End of time window of data being materialized

        Explode a single timestamp by the number of windows it will
        be included aggregated into.
    """
    window_size_td = _parse_time(window_size, allow_unbounded=True)
    slide_interval_td = _parse_time(slide_interval, allow_unbounded=False)

    aligned_feature_start = _align_time_downwards(feature_start, slide_interval_td)
    earliest_possible_window_start = _align_time_downwards(timestamp, slide_interval_td)
    window_end_cursor = max(aligned_feature_start, earliest_possible_window_start) + slide_interval_td

    # Create a new window for each 
    windows = []
    while window_end_cursor <= feature_end:
        ts_after_window_start = window_size_td is None or timestamp >= window_end_cursor - window_size_td
        ts_before_window_end = timestamp < window_end_cursor
        if ts_after_window_start and ts_before_window_end:
            windows.append(window_end_cursor - datetime.timedelta(microseconds=1))
            window_end_cursor = window_end_cursor + slide_interval_td
        else:
            break
    return windows

@transformation(mode="pyspark")
def sliding_window_transformation(
        df,
        timestamp_key: str,
        window_size: str,
        slide_interval: str = None,
        window_column_name="window_end",
        context=materialization_context()
):
    """
        :param df: Spark DataFrame
        :param timestamp_key: The name of the timestamp columns for the event times in `df`
        :param window_size: How long each sliding window is, as a string in the format "[QUANTITY] [UNIT]".
            Ex: "2 days". See https://pypi.org/project/pytimeparse/ for more details.
        :param slide_interval: [optional] How often window is produced, as a string in the format "[QUANTITY] [UNIT]".
            Ex: "2 days". See https://pypi.org/project/pytimeparse/ for more details.
            Note this must be less than or equal to window_size, and window_size must be a multiple of slide_interval.
            If not provided, this defaults to the batch schedule of the FeatureView.
        :param window_column_name: [optional] The output column name for the timestamp of the end of each window
        :return: An exploded Spark DataFrame with an added column according to window_column_name.

        Tecton transformation to allow defining custom aggregations. 
    """

    from pyspark.sql import functions as F
    import pytimeparse

    slide_interval = slide_interval or f"{context.batch_schedule.total_seconds()} seconds"
    _validate_sliding_window_duration(window_size, slide_interval)
    sliding_window_udf = F.udf(sliding_windows, ArrayType(TimestampType()))

    return df.withColumn(
        window_column_name,
        F.explode(
            sliding_window_udf(
                F.col(timestamp_key),
                F.lit(window_size),
                F.lit(slide_interval),
                F.lit(context.feature_start_time),
                F.lit(context.feature_end_time),
            )
        ),
    )
