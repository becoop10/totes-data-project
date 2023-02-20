from star_schema.src.utils.utils import format_date
import pytest
import datetime
import logging
import math

def test_returns_list_of_dicts_containing_dates_starting_from_3_nov_2022():
    result = format_date()
    assert result[0]['year'] == 2022
    assert result[0]['month'] == 11
    assert result[0]['day'] == 3
    assert result[0]['day_of_week'] == 4
    assert result[0]['day_name'] == 'Thursday'
    assert result[0]['month_name'] == 'November'
    assert result[0]['quarter'] == 4

def test_returns_list_of_dicts_ending_with_today():
    result = format_date()
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    d = datetime.datetime.now()
    i = len(result) - 1
    assert result[i]['year'] == d.year
    assert result[i]['month'] == d.month
    assert result[i]['day'] == d.day
    assert result[i]['day_of_week'] == d.isoweekday()
    assert result[i]['day_name'] == days[d.weekday()]
    assert result[i]['month_name'] == months[d.month - 1]
    assert result[i]['quarter'] == math.ceil(d.month / 3 )