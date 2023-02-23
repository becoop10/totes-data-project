from src.transform_package.myutils import format_purchase


def test_format_purchases_correctly_formats_list_containing_one_sale():
    test_sales_data = [
        {"purchase_order_id": 854,
         "created_at": "2023-02-15 08:38:10.121000",
         "last_updated": "2023-02-15 08:38:10.121000",
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "item_code": "test",
                         "item_quantity": 82523,
                         "item_unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21}
    ]

    expected = [
        {"purchase_order_id": 854,
         "created_date": "2023-02-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-02-15",
         "last_updated_time": "08:38:10.121000",
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "item_code": "test",
                         "item_quantity": 82523,
                         "item_unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21
         }
    ]

    assert format_purchase(test_sales_data) == expected


def test_format_purchas_correctly_formats_list_containing_mutilple_purchases():
    test_sales_data = [
        {"purchase_order_id": 854,
         "created_at": "2023-02-15 08:38:10.121000",
         "last_updated": "2023-02-15 08:38:10.121000",
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "item_code": "test",
                         "item_quantity": 82523,
                         "item_unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21},
        {"purchase_order_id": 859,
         "created_at": "2023-02-15 08:38:10.121000",
         "last_updated": "2023-02-15 08:38:10.121000",
         "staff_id": 126,
         "counterparty_id": 144,
         "item_code": "test",
         "item_quantity": 8252,
         "item_unit_price": 3.29,
         "currency_id": 1,
         "agreed_delivery_date":
         "2023-02-19",
         "agreed_payment_date":
         "2023-02-19",
         "agreed_delivery_location_id": 2}

    ]

    expected = [
        {"purchase_order_id": 854,
         "created_date": "2023-02-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-02-15",
         "last_updated_time": "08:38:10.121000",
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "item_code": "test",
                         "item_quantity": 82523,
                         "item_unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21
         },
        {"purchase_order_id": 859,
         "created_date": "2023-02-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-02-15",
         "last_updated_time": "08:38:10.121000",
         "staff_id": 126,
                         "counterparty_id": 144,
                         "item_code": "test",
                         "item_quantity": 8252,
                         "item_unit_price": 3.29,
                         "currency_id": 1,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 2
         }
    ]

    assert format_purchase(test_sales_data) == expected
