from star_schema.src.format_design import format_design


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
