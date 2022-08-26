from tecton import HiveConfig, BatchSource, DatetimePartitionColumn


ad_impressions_hiveds = HiveConfig(
        database='demo_ads',
        table='impressions_batch',
        timestamp_field='timestamp',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="datestr", datepart="date", zero_padded=True)
        ]
    )


ad_impressions_batch = BatchSource(
    name='ad_impressions_batch',
    batch_config=ad_impressions_hiveds,
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
