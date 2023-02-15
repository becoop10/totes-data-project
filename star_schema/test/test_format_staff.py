from star_schema.src.utils.format_staff import format_staff
import pytest

dummy_staff = [{
    "staff_id": 1,
    "first_name": 'Jeremie',
    "last_name": 'Franey',
    "department_id": 2,
    "email_address": 'jeremie.franey@terrifictotes.com',
    "created_at": '2022-11-03 14:20:51.563000',
    "last_updated": '2022-11-03 14:20:51.563000'
},
{
    "staff_id": 2,
    "first_name": 'Deron',
    "last_name": 'Beier',
    "department_id": 3,
    "email_address": 'deron.beier@terrifictotes.com',
    "created_at": '2022-11-03 14:20:51.563000',
    "last_updated": '2022-11-03 14:20:51.563000'
}]

dummy_department = [{
    "department_id": 2,
    "department_name": "sales",
    "location": "Manchester",
    "manager": "Raphael",
    "created_at": "2022-10-01 14:00:59.456000",
    "last_updated": "2022-10-01 14:00:59.456000",
},
{
    "department_id": 3,
    "department_name": "human resources",
    "location": "Manchester",
    "manager": "Raphael",
    "created_at": "2022-10-01 14:00:59.456000",
    "last_updated": "2022-10-01 14:00:59.456000",
}]

def test_empty_list_returns_empty_list():

    assert format_staff([], dummy_department) == []


def test_format_staff_returns_one_staff_list_formatted():

    test_staff = [{
    "staff_id": 1,
    "first_name": 'Jeremie',
    "last_name": 'Franey',
    "department_id": 2,
    "email_address": 'jeremie.franey@terrifictotes.com',
    "created_at": '2022-11-03 14:20:51.563000',
    "last_updated": '2022-11-03 14:20:51.563000'
}]

    expected = [{
    "staff_id": 1,
    "first_name": 'Jeremie',
    "last_name": 'Franey',
    "department_name": "sales",
    "location": "Manchester",
    "email_address": 'jeremie.franey@terrifictotes.com'
}]

    assert format_staff(test_staff, dummy_department) == expected


def test_format_staff_returns_multiple_staff_matched_against_multiple_sized_list():
    expected=[{
    "staff_id": 1,
    "first_name": 'Jeremie',
    "last_name": 'Franey',
    "department_name": "sales",
    "location": "Manchester",
    "email_address": 'jeremie.franey@terrifictotes.com'
},{
    "staff_id": 2,
    "first_name": 'Deron',
    "last_name": 'Beier',
    "department_name": "human resources",
    "location":"Manchester",
    "email_address": 'deron.beier@terrifictotes.com',
    
}
    ]

    assert format_staff(dummy_staff,dummy_department)==expected


def test_raises_error_when_no_match():
    test_staff=[{
    "staff_id": 1,
    "first_name": 'Jeremie',
    "last_name": 'Franey',
    "department_id": 7,
    "email_address": 'jeremie.franey@terrifictotes.com',
    "created_at": '2022-11-03 14:20:51.563000',
    "last_updated": '2022-11-03 14:20:51.563000'
}]
    with pytest.raises(IndexError):
        format_staff(test_staff,dummy_department)
