from sliding_window_transformation import sliding_window_transformation
from datetime import timedelta, datetime
from tecton import (
    batch_feature_view,
    FilteredSource,
    transformation,
    const,
)
from entities import user
from data_source import transactions_batch


@transformation(mode="spark_sql")
def user_distinct_merchant_transaction_count_transformation(window_input_df):
    return f"""
        SELECT
            user_id,
            COUNT(DISTINCT merchant) AS distinct_merchant_count,
            window_end AS timestamp
        FROM {window_input_df}
        GROUP BY
            user_id,
            window_end
    """


@batch_feature_view(
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-29))],
    entities=[user],
    mode="pipeline",
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    tags={"release": "production"},
    owner="user@tecton.ai",
    description="How many transactions the user has made to distinct merchants in the last 30 days.",
)
def user_distinct_merchant_transaction_count_30d(transactions_batch):
    return user_distinct_merchant_transaction_count_transformation(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        sliding_window_transformation(
            transactions_batch,
            timestamp_key=const("timestamp"),
            window_size=const("30d"),
        )
    )
