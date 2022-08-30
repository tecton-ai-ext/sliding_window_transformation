import datetime

from pyspark.sql.types import ArrayType
from pyspark.sql.types import TimestampType

from tecton import transformation, materialization_context


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
        Ex:
            sliding_window_transformation(
                [
                    (user_id=1, timestamp = '2021-01-10 10:14:14'),
                    (user_id=2, timestamp = '2021-01-11 23:10:10'),
                    (user_id=3, timestamp = '2021-01-12 01:01:01')
                ],
                window_size = '2 days')
            with context(
                feature_start_time='2021-01-10 00:00:00',
                feature_end_time='2021-01-12 00:00:00',
                batch_schedule='1 day'
            )
            =>
            [
                (user_id=1, timestamp='2021-01-10 10:14:14', window_end='2021-01-10 23:59:59.999999'),
                (user_id=1, timestamp='2021-01-10 10:14:14', window_end='2021-01-11 23:59:59.999999'),
                (user_id=2, timestamp='2021-01-11 23:10:10', window_end='2021-01-11 23:59:59.999999')
            ]
            Note that each input row can produce from 0 to (window_size / slide_interval) output rows, depending on how many
            windows it is present in.
            To do an aggregation on these rows, you could write a query like below, which computes a count for each window.
            ```
            SELECT
                user_id,
                COUNT(1) as occurrences,
                window_end
            FROM {df}
            GROUP BY
                user_id,
                window_end
            ```
    """

    from pyspark.sql import functions as F
    import pytimeparse

    WINDOW_UNBOUNDED_PRECEDING = "unbounded_preceding"

    def _parse_time(duration: str, allow_unbounded: bool):
        if allow_unbounded and duration.lower() == WINDOW_UNBOUNDED_PRECEDING:
            return None
        return datetime.timedelta(seconds=pytimeparse.parse(duration))


    def _align_time_downwards(time: datetime.datetime, alignment: datetime.timedelta) -> datetime.datetime:
        excess_seconds = time.timestamp() % alignment.total_seconds()
        return datetime.datetime.utcfromtimestamp(time.timestamp() - excess_seconds)


    def _validate_and_parse_time(duration: str, field_name: str, allow_unbounded: bool):
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
        slide_interval_td = _validate_and_parse_time(slide_interval, "slide_interval", allow_unbounded=False)
        window_size_td = _validate_and_parse_time(window_size, "window_size", allow_unbounded=True)
        if window_size_td is not None:
            # note this also confirms window >= slide since a>0, b>0, a % b = 0 implies a >= b
            if window_size_td.total_seconds() % slide_interval_td.total_seconds() != 0:
                raise ValueError(
                    f"Window size {window_size} must be a multiple of slide interval {slide_interval}"
                )
        return window_size_td, slide_interval_td


    @F.udf(returnType=ArrayType(TimestampType()))
    def sliding_window_udf(
            timestamp: datetime.datetime,
            window_size: str,
            slide_interval: str,
            feature_start: datetime.datetime,
            feature_end: datetime.datetime,
    ):
        window_size_td = _parse_time(window_size, allow_unbounded=True)
        slide_interval_td = _parse_time(slide_interval, allow_unbounded=False)

        aligned_feature_start = _align_time_downwards(feature_start, slide_interval_td)
        earliest_possible_window_start = _align_time_downwards(timestamp, slide_interval_td)
        window_end_cursor = max(aligned_feature_start, earliest_possible_window_start) + slide_interval_td

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

    slide_interval = slide_interval or f"{context.batch_schedule.total_seconds()} seconds"
    _validate_sliding_window_duration(window_size, slide_interval)

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
