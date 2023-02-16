from star_schema.src.utils.utils import format_payment_type


def test_format_payment_type_formats_list_containing_one_payment_entity():

    test_payment_data = [{
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT",
        "created_at": "2022-11-03T14:20:49.962Z",
        "last_updated": "2022-11-03T14:20:49.962Z"
    }]

    expected = [{
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT"
    }]

    assert format_payment_type(test_payment_data) == expected


def test_format_payment_type_formats_list_containing_multiple_payment_entities():

    test_payment_data = [{
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT",
        "created_at": "2022-11-03T14:20:49.962Z",
        "last_updated": "2022-11-03T14:20:49.962Z"
    },
        {
        "payment_type_id": 2,
        "payment_type_name": "SALES_REFUND",
        "created_at": "2022-11-03T14:20:49.962Z",
        "last_updated": "2022-11-03T14:20:49.962Z"
    },
        {
        "payment_type_id": 3,
        "payment_type_name": "PURCHASE_PAYMENT",
        "created_at": "2022-11-03T14:20:49.962Z",
        "last_updated": "2022-11-03T14:20:49.962Z"
    },
        {
        "payment_type_id": 4,
        "payment_type_name": "PURCHASE_REFUND",
        "created_at": "2022-11-03T14:20:49.962Z",
        "last_updated": "2022-11-03T14:20:49.962Z"
    }]

    expected = [{
                "payment_type_id": 1,
                "payment_type_name": "SALES_RECEIPT",

                },
                {
                "payment_type_id": 2,
                "payment_type_name": "SALES_REFUND",
                },
                {
                "payment_type_id": 3,
                "payment_type_name": "PURCHASE_PAYMENT",
                },
                {
                "payment_type_id": 4,
                "payment_type_name": "PURCHASE_REFUND",
                }]

    assert format_payment_type(test_payment_data) == expected
