from tecton import HiveConfig, BatchSource, DatetimePartitionColumn

partition_columns = [
    DatetimePartitionColumn(
        column_name="partition_0", datepart="year", zero_padded=True
    ),
    DatetimePartitionColumn(
        column_name="partition_1", datepart="month", zero_padded=True
    ),
    DatetimePartitionColumn(
        column_name="partition_2", datepart="day", zero_padded=True
    ),
]

transactions_batch = BatchSource(
    name="transactions_batch",
    batch_config=HiveConfig(
        database="demo_fraud_v2",
        table="transactions",
        timestamp_field="timestamp",
        # Setting the datetime partition columns significantly speeds up queries from Hive tables.
        datetime_partition_columns=partition_columns,
    ),
    owner="matt@tecton.ai",
    tags={"release": "production"},
)
