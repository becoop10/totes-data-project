from dim_date import format_date, write_to_db
import pytest
import datetime
import logging
import math

def test_format_date_returns_list_of_dicts_containing_dates_starting_from_date_one_ending_at_date_two():
    result = format_date('11/3/2022', '11/4/2022')
    assert result[0]['year'] == 2022
    assert result[0]['month'] == 11
    assert result[0]['day'] == 3
    assert result[0]['day_of_week'] == 4
    assert result[0]['day_name'] == 'Thursday'
    assert result[0]['month_name'] == 'November'
    assert result[0]['quarter'] == 4

    i = len(result) - 1

    assert result[i]['year'] == 2022
    assert result[i]['month'] == 11
    assert result[i]['day'] == 4
    assert result[i]['day_of_week'] == 5
    assert result[i]['day_name'] == 'Friday'
    assert result[i]['month_name'] == 'November'
    assert result[i]['quarter'] == 4

def test_write_to_db_writes_table_of_dates_to_data_warehouse():
    pass