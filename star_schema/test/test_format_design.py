from star_schema.src.utils.utils import format_design


def test_format_design_formats_for_single_item_input():

    test_design_data = [
        {
            "design_id": 1,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]

    expected = [
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
        }
    ]

    assert format_design(test_design_data) == expected


def test_format_design_on_list_of_items():
    test_design_data = [
        {
            "design_id": 1,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
            "last_updated": "2022-11-03 14:20:49.962000"
        },
        {
            "design_id": 2,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Steel",
            "file_location": "/home/user",
            "file_name": "testnumber2",
            "last_updated": "2022-11-03 14:20:49.962000"
        }, {
            "design_id": 3,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Water",
            "file_location": "/home/user/Mark",
            "file_name": "testnumber3",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]
    expected = [
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
        }, {
            "design_id": 2,
            "design_name": "Steel",
            "file_location": "/home/user",
            "file_name": "testnumber2",
        }, {
            "design_id": 3,
            "design_name": "Water",
            "file_location": "/home/user/Mark",
            "file_name": "testnumber3",
        }
    ]

    assert format_design(test_design_data) == expected


def test_format_design_does_mutate():
    test_design_data = [
        {
            "design_id": 1,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
            "last_updated": "2022-11-03 14:20:49.962000"
        },
        {
            "design_id": 2,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Steel",
            "file_location": "/home/user",
            "file_name": "testnumber2",
            "last_updated": "2022-11-03 14:20:49.962000"
        }, {
            "design_id": 3,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Water",
            "file_location": "/home/user/Mark",
            "file_name": "testnumber3",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]
    copied = [
        {
            "design_id": 1,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/home/user",
            "file_name": "test",
            "last_updated": "2022-11-03 14:20:49.962000"
        },
        {
            "design_id": 2,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Steel",
            "file_location": "/home/user",
            "file_name": "testnumber2",
            "last_updated": "2022-11-03 14:20:49.962000"
        }, {
            "design_id": 3,
            "created_at": "2022-11-03 14:20:49.962000",
            "design_name": "Water",
            "file_location": "/home/user/Mark",
            "file_name": "testnumber3",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]

    format_design(test_design_data)

    assert test_design_data == copied
