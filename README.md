# Tecton Sliding Window

`tecton_sliding_window` is transformation that was built-into older versions of Tecton SDK. It enables running backfills 
of historical data for custom time-windowed aggregations in a single job.

>Warning:
>As of 0.4, it is deprecated and no longer managed by Tecton.

>Note:
>In newer
versions of the Tecton SDK, you can use [incremental backfills](https://docs.tecton.ai/latest/overviews/framework/feature_views/batch/incremental_backfills.html)

In steady-state Tecton runs jobs periodically based on the `batch_schedule` defined in your feature views. Prior to 0.4, historical
data could only be backfilled multiple `batch_schedule` periods at a time, which would cause issues with time-windowed aggregations.  To ensure they are backfilled
correctly, you could use `tecton_sliding_window` to duplicate each data point for each window it is included in, and then group by windows to do 
custom aggregations.

## Example
Given a series of transactions, we want to calculate a time_window average over 3 days with a `batch_schedule` of 1 day.

| Timestamp  | Amount | user_id | merchant_id |
|------------|--------|---------|-------------|
| 2021-05-01 | 100    | 1       | a           |
| 2021-05-02 | 200    | 1       | a           |
| 2021-05-03 | 300    | 1       | b           |
| 2021-05-04 | 400    | 1       | a           |

After running this data through `tecton_sliding_window` each data point is repeated (`window_size`/`batch_schedule`) times
with each window_end that the data point would be included in.

| Timestamp  | Amount | user_id | merchant_id | window_end |
|------------|--------|---------|-------------|------------|
| 2021-05-01 | 100    | 1       | a           | 2021-05-01 |
| 2021-05-01 | 100    | 1       | a           | 2021-05-02 |
| 2021-05-01 | 100    | 1       | a           | 2021-05-03 |
| 2021-05-02 | 200    | 1       | a           | 2021-05-02 |
| 2021-05-02 | 200    | 1       | a           | 2021-05-03 |
| 2021-05-02 | 200    | 1       | a           | 2021-05-04 |
| 2021-05-03 | 300    | 1       | b           | 2021-05-03 |
| 2021-05-03 | 300    | 1       | b           | 2021-05-04 |
| 2021-05-03 | 300    | 1       | b           | 2021-05-05 |
| 2021-05-04 | 400    | 1       | a           | 2021-05-04 |
| 2021-05-04 | 400    | 1       | a           | 2021-05-05 |
| 2021-05-04 | 400    | 1       | a           | 2021-05-06 |

The output of tecton_sliding_window can now be grouped by window_end, to get the desired window aggregation. If we want to
count number of distinct transactions, we can use the transformation below on our `tecton_sliding_window` output:
```python
@transformation(mode='spark_sql')
def user_distinct_merchant_transaction_count_transformation(window_input_df):
    return f'''
        SELECT
            user_id,
            COUNT(DISTINCT merchant_id) AS distinct_merchant_count,
            window_end AS timestamp
        FROM {window_input_df}
        GROUP BY
            merchant_id,
            window_end
    '''
```

The final output dataframe is:

| timestamp  | distinct_merchant_count | user_id |
|------------|-------------------------|---------|
| 2021-05-01 | 1                       | 1       | 
| 2021-05-02 | 1                       | 1       |
| 2021-05-03 | 2                       | 1       |
| 2021-05-04 | 2                       | 1       |
| 2021-05-05 | 2                       | 1       |
| 2021-05-06 | 1                       | 1       |


The feature view definition would be the following:

```python
import datetime
from tecton.compat import const

@batch_feature_view(
    inputs={'transactions_batch': Input(transactions_batch, window='3d')},
    entities=[user],
    mode='pipeline',
    ttl='1d',
    batch_schedule='1d',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 4, 1),
    tags={'release': 'production'},
    owner='user@tecton.ai',
    description='How many transactions the user has made to distinct merchants in the last 3 days.'
)
def user_distinct_merchant_transaction_count_3d(transactions_batch):
    return user_distinct_merchant_transaction_count_transformation(
        tecton_sliding_window(transactions_batch,
            timestamp_key=const('timestamp'),
            window_size=const('3d')))
```
## Usage
The tecton_sliding_window() has 3 primary inputs:

`df`: the input data.

`timestamp_key`: the timestamp column in your input data that represents the time of the event.

`window_size`(**str**): how far back in time the window should go. For example, if my feature is the number of distinct IDs in the last week, then the window size is 7 days.

`window_column_name`(**Optional**): name of added column with the exploded window ends. Default: `window_end`
