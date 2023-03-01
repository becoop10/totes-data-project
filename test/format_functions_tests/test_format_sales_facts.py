from src.utils.myutils import format_sales_facts


def test_format_sales_correctly_formats_list_containing_one_sale():
    test_sales_data = [
        {"sales_order_id": 854,
         "created_at": "2023-02-15 08:38:10.121000",
         "last_updated": "2023-02-15 08:38:10.121000",
                         "design_id": 68,
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "units_sold": 82523,
                         "unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21}
    ]

    expected = [
        {"sales_order_id": 854,
         "created_date": "2023-02-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-02-15",
         "last_updated_time": "08:38:10.121000",
                         "sales_staff_id": 12,
                         "counterparty_id": 14,
                         "units_sold": 82523,
                         "unit_price": 3.29,
                         "currency_id": 2,
                         "design_id": 68,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21
         }
    ]

    assert format_sales_facts(test_sales_data) == expected


def test_format_sales_formats_list_containing_multiple_sales():
    test_sales_data = [
        {"sales_order_id": 854,
         "created_at": "2023-02-15 08:38:10.121000",
         "last_updated": "2023-02-15 08:38:10.121000",
                         "design_id": 68,
                         "staff_id": 12,
                         "counterparty_id": 14,
                         "units_sold": 82523,
                         "unit_price": 3.29,
                         "currency_id": 2,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21},
        {"sales_order_id": 855,
         "created_at": "2023-03-15 08:38:10.121000",
         "last_updated": "2023-03-15 08:38:10.121000",
         "design_id": 68,
         "staff_id": 12,
         "counterparty_id": 14,
         "units_sold": 82523,
         "unit_price": 3.29,
         "currency_id": 2,
         "agreed_delivery_date":
         "2023-03-19",
         "agreed_payment_date":
         "2023-03-19",
         "agreed_delivery_location_id": 21}
    ]

    expected = [
        {"sales_order_id": 854,
         "created_date": "2023-02-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-02-15",
         "last_updated_time": "08:38:10.121000",
                         "sales_staff_id": 12,
                         "counterparty_id": 14,
                         "units_sold": 82523,
                         "unit_price": 3.29,
                         "currency_id": 2,
                         "design_id": 68,
                         "agreed_delivery_date":
                         "2023-02-19",
                         "agreed_payment_date":
                         "2023-02-19",
                         "agreed_delivery_location_id": 21
         },
        {"sales_order_id": 855,
         "created_date": "2023-03-15",
         "created_time": "08:38:10.121000",
         "last_updated_date": "2023-03-15",
         "last_updated_time": "08:38:10.121000",
         "sales_staff_id": 12,
                         "counterparty_id": 14,
                         "units_sold": 82523,
                         "unit_price": 3.29,
                         "currency_id": 2,
                         "design_id": 68,
                         "agreed_delivery_date":
                         "2023-03-19",
                         "agreed_payment_date":
                         "2023-03-19",
                         "agreed_delivery_location_id": 21
         }

    ]

    assert format_sales_facts(test_sales_data) == expected
