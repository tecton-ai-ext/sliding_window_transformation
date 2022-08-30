# Sliding Window Transformation

`tecton_sliding_window` is transformation that was built-into older versions of Tecton SDK. It enables running backfills 
of historical data for custom time-windowed aggregations in a single job. This repository contains the equivalent transformation.

>Warning:
>As of 0.4, it is deprecated and no longer managed by Tecton. In newer
versions of the Tecton SDK, you can use [incremental backfills](https://docs.tecton.ai/latest/overviews/framework/feature_views/batch/incremental_backfills.html)

In steady-state, Tecton will schedule a job to materialize data based on `batch_schedule` defined in your feature views. Prior to 0.4, historical
data could only be backfilled multiple `batch_schedule` periods at a time, which would cause issues with custom time-windowed aggregations (e.g. a rolling 30 day aggregation implemented with SQL aggregation). 
To ensure they are backfilled correctly, you could use `tecton_sliding_window` to duplicate each data point for each window it is included in, and then group by windows to do 
custom aggregations.

## Example
Given a series of transactions, we want a list of merchants visited over the last 3 days with a `slide_interval` of 1 day.

| Timestamp  | user_id | merchant_id |
|------------|---------|-------------|
| 2021-05-01 | 1       | a           |
| 2021-05-02 | 1       | b           |
| 2021-05-03 | 1       | c           |
| 2021-05-04 | 1       | d           |

After running this data through `tecton_sliding_window` each data point is repeated (`window_size`/`slide_interval`) times
with each window_end that the data point would be included in.

| Timestamp  | user_id | merchant_id | window_end |
|------------|---------|-------------|------------|
| 2021-05-01 | 1       | a           | 2021-05-01 |
|||||
| 2021-05-01 | 1       | a           | 2021-05-02 |
| 2021-05-02 | 1       | b           | 2021-05-02 |
|||||
| 2021-05-01 | 1       | a           | 2021-05-03 |
| 2021-05-02 | 1       | b           | 2021-05-03 |
| 2021-05-03 | 1       | c           | 2021-05-03 |
|||||
| 2021-05-02 | 1       | b           | 2021-05-04 |
| 2021-05-03 | 1       | c           | 2021-05-04 |
| 2021-05-04 | 1       | d           | 2021-05-04 |
|||||
| 2021-05-03 | 1       | c           | 2021-05-05 |
| 2021-05-04 | 1       | d           | 2021-05-05 |
|||||
| 2021-05-04 | 1       | d           | 2021-05-06 |

The output of tecton_sliding_window can now be grouped by window_end, to get the desired window aggregation. If we want a list
of merchants, we can use the transformation below on the output of  `tecton_sliding_window`:

```python
@transformation(mode='spark_sql')
def user_merchant_list_transformation(window_input_df):
    return f'''
        SELECT
            user_id,
            COLLECT_LIST(merchant_id) AS recent_merchants,
            window_end
        FROM {window_input_df}
        GROUP BY
            user_id,
            window_end
    '''
```

The final output dataframe is:

| window_end | recent_merchants | user_id|
|------------|-------------------------|---------|
| 2021-05-01 | a                       | 1       | 
| 2021-05-02 | a, b                    | 1       |
| 2021-05-03 | a, b, c                 | 1       |
| 2021-05-04 | b, c, d                 | 1       |
| 2021-05-05 | c, d                    | 1       |
| 2021-05-06 | d                       | 1       |


The feature view definition would be the following:

```python
from datetime import timedelta
from tecton import const

@batch_feature_view(
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-2))],
    entities=[user],
    mode='pipeline',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    description='List of merchants user visited in the last 3 days.'
)
def user_merchant_list_3d(transactions_batch, context=materialization_context()):
    return user_merchant_list_transformation(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        sliding_window_transformation(transactions_batch,
            timestamp_key=const('timestamp'),
            window_size=const('3d')))
```
## Usage
The tecton_sliding_window() has 3 primary inputs:

`df`: the input Spark dataframe.

`timestamp_key`: the timestamp column in your input data that represents the time of the event.

`window_size`(**str**): The time period for the sliding window. To include all data up to current time, set to `unbounded_preceding`. For example, if the feature is the number of distinct values in the last week, then the window size is 7 days. Format window_size as "[QUANTITY] [UNIT]".
            Ex: "2 days". See https://pypi.org/project/pytimeparse/ for more details.

`slide_interval`(**Optional**): How often window is produced, as a string in the format "[QUANTITY] [UNIT]".
            Ex: "2 days".g See https://pypi.org/project/pytimeparse/ for more details.
            Note this must be less than or equal to window_size, and window_size must be a multiple of slide_interval.
            If not provided, this defaults to the batch schedule of the FeatureView.

`window_column_name`(**Optional**): name of added column with the exploded window ends. Default: `window_end`
