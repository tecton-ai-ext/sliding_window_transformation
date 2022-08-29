from sliding_window_transformation import sliding_window_transformation
from datetime import timedelta
from tecton import batch_feature_view, FilteredSource, transformation, const, materialization_context
from entities import user
from data_source import transactions_batch
from datetime import datetime

# Counts distinct merchant names for each user and window. The timestamp
# for the feature is the end of the window.
# window_input_df is created by passing the original input through
# tecton_sliding_window transformation.
@transformation(mode='spark_sql')
def user_distinct_merchant_transaction_count_transformation(window_input_df):
    return f'''
        SELECT
            nameorig AS user_id,
            COUNT(DISTINCT namedest) AS distinct_merchant_count,
            window_end AS timestamp
        FROM {window_input_df}
        GROUP BY
            nameorig,
            window_end
    '''

@batch_feature_view(
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-29))],
    entities=[user],
    mode='pipeline',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='How many transactions the user has made to distinct merchants in the last 30 days.',
    alert_email="derek@tecton.ai",
    monitor_freshness=True
)
def user_distinct_merchant_transaction_count_30d(transactions_batch, context=materialization_context()):
    return user_distinct_merchant_transaction_count_transformation(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        sliding_window_transformation(transactions_batch,
            timestamp_key=const('timestamp'),
            window_size=const('30d'),
            start_time=context.start_time,
            context=context))
