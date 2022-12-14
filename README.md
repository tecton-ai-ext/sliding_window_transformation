# Sliding Window Transformation

`tecton_sliding_window` is a transformation that was built-into older versions of the Tecton SDK. It enabled running backfills 
of historical data for custom time-windowed aggregations in a single job. This repository contains the equivalent transformation.

As of 0.4, tecton_sliding_window is deprecated and no longer managed by Tecton. In newer
versions of the Tecton SDK, you can use incremental backfills to create custom sliding window transformations, however this will require rematerializing your features.

If you wish to maintain your existing Feature View logic and avoid rematerialization, you can use the
`sliding_window_transformation` implemented this repo in place of tecton_sliding_window. This transformation must be included in your feature repo and managed by you.

Follow these steps to migrate a Feature View from `tecton_sliding_window` to `sliding_window_transformation` without rematerializing data:


1. Upgrade your Feature View to 0.4 (non-compat) definition, but keep the `tecton_sliding_window` from tecton.compat package. Run `tecton apply`, and you should see your feature views being upgraded in the plan output.

2. Copy the `sliding_window_transformation` transformation from this repo into your feature repo. Replace the `tecton_sliding_window` imported from tecton.compat with `sliding_window_transformation`. We recommend testing your feature view in a development workspace using Feature View `run()`. You can compare the output features with the old Feature View over the same time period to check the Feature Views
are equivalent.

```python
import tecton
from datetime import datetime

fv = tecton.get_workspace('my_dev_workspace').get_feature_view('fv_with_sliding_window_transformation.')

# Run the feature view over multiple materialization periods. This should produce features for each period in the range.
df = fv.run(start_time=datetime(2022, 5, 10), end_time=datetime(2022, 5, 13)).to_spark()

df.show()
```

3. You can now safely run `tecton plan` and `tecton apply` with the [`--suppress-recreates`](https://docs.tecton.ai/latest/examples/cost-management-best-practices.html#suppressing-rematerialization) flag to avoid re-materializing the feature data. When you have removed all usages of `tecton_sliding_window` from your feature views, you will see that the transformation is deleted in the plan output.

Detailed information on sliding_window_transformation can be found below.

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
from datetime import timedelta, datetime
from tecton import const, batch_feature_view, FilteredSource
from sliding_window_transformation import sliding_window_transformation

@batch_feature_view(
    # Use a FilteredSource to efficiently pre-filter the input data before the sliding window explosion. 
    # This function will filter the data to `[start_time + start_time_offset, end_time)`
    # where start_time and end_time are for the period being materialized. During a backfill this 
    # may be multiple months. In steady state, jobs will have a period of a single `batch_schedule`.
    # To compute a 3-day aggregate, we'll always need the two full days preceeding the start time,
    # so we use -2 days for the start_time_offset.
    sources=[FilteredSource(transactions_batch, start_time_offset=timedelta(days=-2))],
    entities=[user],
    mode='pipeline',
    ttl=timedelta(days=1),
    batch_schedule=timedelta(days=1),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 5, 1),
    description='List of merchants user visited in the last 3 days.'
)
def user_merchant_list_3d(transactions_batch):
    return user_merchant_list_transformation(
        # Use tecton_sliding_transformation to create trailing 3 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        sliding_window_transformation(transactions_batch,
            timestamp_key=const('timestamp'),
            window_size=const('3d')))
```
## Usage
The sliding_window_transformation() has the following inputs:

`df`: the input Spark dataframe.

`timestamp_key`: the timestamp column in your input data that represents the time of the event.

`window_size`(**str**): The time period for the sliding window. To include all data up to current time, set to `unbounded_preceding`. For example, if the feature is the number of distinct values in the last week, then the window size is 7 days. Format window_size as "[QUANTITY] [UNIT]".
            Ex: "2 days". See https://pypi.org/project/pytimeparse/ for more details.

`slide_interval`(**Optional**): How often window is produced, as a string in the format "[QUANTITY] [UNIT]".
            Ex: "2 days".g See https://pypi.org/project/pytimeparse/ for more details.
            Note this must be less than or equal to window_size, and window_size must be a multiple of slide_interval.
            If not provided, this defaults to the batch schedule of the FeatureView.

`window_column_name`(**Optional**): name of added column with the exploded window ends. Default: `window_end`
