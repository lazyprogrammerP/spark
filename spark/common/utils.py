from datetime import datetime

import pandas as pd


def check_sample_rate(sample_rate):
    assert sample_rate <= 1 and sample_rate >= 0


def is_empty(data):
    if isinstance(data, pd.DataFrame):
        return data.empty
    return True if not data else False


def is_not_empty(data):
    return not is_empty(data)


def get_current_timestamp():
    return int(datetime.now().timestamp() * 1000)
