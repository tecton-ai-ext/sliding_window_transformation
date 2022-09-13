from sliding_window_transformation import sliding_windows
from user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d
from datetime import datetime

def test_sliding_window_transformation_7d():
	timestamp = datetime(2021, 5, 12)
	window_size = "3d"
	slide_interval = "1d"
	feature_start = datetime(2021, 5, 1)
	feature_end = datetime(2021, 5, 30)

	windows = sliding_windows(timestamp, window_size, slide_interval, feature_start, feature_end)
	
	expected_windows = [datetime(2021, 5, 12, 23, 59, 59, 999999),
						datetime(2021, 5, 13, 23, 59, 59, 999999),
						datetime(2021, 5, 14, 23, 59, 59, 999999)]

	assert windows == expected_windows


def test_sliding_window_transformation_unbounded_window():
	timestamp = datetime(2021, 5, 12)
	window_size = "unbounded_preceding"
	slide_interval = "2d"
	feature_start = datetime(2021, 5, 1)
	feature_end = datetime(2021, 5, 30)

	windows = sliding_windows(timestamp, window_size, slide_interval, feature_start, feature_end)
	
	expected_windows = [datetime(2021, 5, 12, 23, 59, 59, 999999),
						datetime(2021, 5, 14, 23, 59, 59, 999999),
						datetime(2021, 5, 16, 23, 59, 59, 999999),
						datetime(2021, 5, 18, 23, 59, 59, 999999),
						datetime(2021, 5, 20, 23, 59, 59, 999999),
						datetime(2021, 5, 22, 23, 59, 59, 999999),
						datetime(2021, 5, 24, 23, 59, 59, 999999),
						datetime(2021, 5, 26, 23, 59, 59, 999999),
						datetime(2021, 5, 28, 23, 59, 59, 999999)]
	
	assert windows == expected_windows