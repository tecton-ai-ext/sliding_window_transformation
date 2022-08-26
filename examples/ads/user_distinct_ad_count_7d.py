from transformations.tecton_sliding_window import tecton_sliding_window
from datetime import timedelta
from tecton import batch_feature_view, FilteredSource, transformation, const, materialization_context
from ads.entities import user
from ads.data_source import ad_impressions_batch
from datetime import datetime

# Counts distinct ad ids for each user and window. The timestamp
# for the feature is the end of the window, which is set by using
# the tecton_sliding_window transformation
@transformation(mode='spark_sql')
def user_distinct_ad_count_transformation(window_input_df):
    return f'''
        SELECT
            user_uuid as user_id,
            approx_count_distinct(ad_id) as distinct_ad_count,
            window_end as timestamp
        FROM
            {window_input_df}
        GROUP BY
            user_uuid, window_end
        '''

@batch_feature_view(
    sources=[ FilteredSource(ad_impressions_batch, start_time_offset=timedelta(days=-6))],
    entities=[user],
    mode='pipeline',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 4, 1),
    tags={'release': 'production'},
    owner='david@tecton.ai',
    description='How many distinct advertisements a user has been shown in the last week'
)
def user_distinct_ad_count_7d(ad_impressions, context=materialization_context()):
    return user_distinct_ad_count_transformation(
        # Use tecton_sliding_transformation to create trailing 7 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        tecton_sliding_window(ad_impressions,
            timestamp_key=const('timestamp'),
            window_size=const('7d'),
            context=context))
