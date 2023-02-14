from star_schema.src.format_currency import format_currency


def test_format_currency_formats_single_item_list():

    test_currency = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]

    expected = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "currency_name": "Great British Pound"
        }
    ]

    assert format_currency(test_currency) == expected


def test_format_currency_formats_multiple_currency_data():

    test_currency = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000"
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000"
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]
    expected = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "currency_name": "Great British Pound"
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "currency_name": "United States Dollar"
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "currency_name": "Euro"
        }
    ]
    assert format_currency(test_currency) == expected
