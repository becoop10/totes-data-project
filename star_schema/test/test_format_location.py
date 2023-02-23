from src.transform_package.myutils import format_location


def test_format_location_for_a_single_item():
    unformatted_address = [{
        "address_id": 3,
        "address_line_1": "42 Hitchhiker's Guide",
        "address_line_2": "To the Galaxy",
        "district": "Adams",
        "city": "Douglas",
        "postal_code": "DA42 C3PO",
        "country": "Magarathea",
        "phone": "0118999",
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }]

    expected = [{
        "location_id": 3,
        "address_line_1": "42 Hitchhiker's Guide",
        "address_line_2": "To the Galaxy",
        "district": "Adams",
        "city": "Douglas",
        "postal_code": "DA42 C3PO",
        "country": "Magarathea",
        "phone": "0118999",
    }]

    assert format_location(unformatted_address) == expected


def test_format_location_doesnt_mutate():
    unformatted_address = [{
        "address_id": 3,
        "address_line_1": "42 Hitchhiker's Guide",
        "address_line_2": "To the Galaxy",
        "district": "Adams",
        "city": "Douglas",
        "postal_code": "DA42 C3PO",
        "country": "Magarathea",
        "phone": "0118999",
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }]
    copy_address = [{
        "address_id": 3,
        "address_line_1": "42 Hitchhiker's Guide",
        "address_line_2": "To the Galaxy",
        "district": "Adams",
        "city": "Douglas",
        "postal_code": "DA42 C3PO",
        "country": "Magarathea",
        "phone": "0118999",
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }]

    format_location(unformatted_address)
    assert unformatted_address == copy_address


def test_location_formats_for_multiple_in_list():
    unformatted = [{
        "address_id": 3,
        "address_line_1": "42 Hitchhiker's Guide",
        "address_line_2": "To the Galaxy",
        "district": "Adams",
        "city": "Douglas",
        "postal_code": "DA42 C3PO",
        "country": "Magarathea",
        "phone": "0118999",
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }, {
        "address_id": 5,
        "address_line_1": "1337 Computers",
        "address_line_2": "Microsoft Ave",
        "district": "Gates",
        "city": "Bill",
        "postal_code": "BG13 C3PO",
        "country": "America",
        "phone": "0118999",
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }]
    expected = [
        {
            "location_id": 3,
            "address_line_1": "42 Hitchhiker's Guide",
            "address_line_2": "To the Galaxy",
            "district": "Adams",
            "city": "Douglas",
            "postal_code": "DA42 C3PO",
            "country": "Magarathea",
            "phone": "0118999",

        }, {
            "location_id": 5,
            "address_line_1": "1337 Computers",
            "address_line_2": "Microsoft Ave",
            "district": "Gates",
            "city": "Bill",
            "postal_code": "BG13 C3PO",
            "country": "America",
            "phone": "0118999",

        },
    ]

    assert expected == format_location(unformatted)
