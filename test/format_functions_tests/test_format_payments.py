from utils.utils import format_payments


def test_format_payments_correctly_formats_single_payment_input():

    test_payment_data = [
        {
            "payment_id": 2,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "transaction_id": 2,
            "counterparty_id": 15,
            "payment_amount": 552548.62,
            "currency_id": 2,
            "payment_type_id": 3,
            "paid": False,
            "payment_date": "2022-11-04",
            "company_ac_number": 67305075,
            "counterparty_ac_number": 31622269
        }
    ]

    expected = [
        {
            "payment_id": 2,
            "created_date": "2022-11-03",
            "created_time": "14:20:52.187000",
            "last_updated_date": "2022-11-03",
            "last_updated_time": "14:20:52.187000",
            "transaction_id": 2,
            "counterparty_id": 15,
            "payment_amount": 552548.62,
            "currency_id": 2,
            "payment_type_id": 3,
            "paid": False,
            "payment_date": "2022-11-04",
        }
    ]

    assert format_payments(test_payment_data) == expected
