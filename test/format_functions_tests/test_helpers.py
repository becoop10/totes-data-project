from src.utils.myutils import find_match, time_splitter, remove_keys
import pytest


def test_returns_dictionary():
    expected = dict
    result = find_match("key", "key", {"key": 1}, [
                        {"key": 1, "wanted": "test"}])
    assert type(result) == expected


def test_matches_key_when_only_item():
    expected = {"key": 1, "wanted": "test"}
    result = find_match("key", "key", {"key": 1}, [
                        {"key": 1, "wanted": "test"}])
    assert result == expected


def test_matches_key_into_list():
    expected = {"key": 2, "wanted": "test"}

    result = [{"key": 1, "wanted": "wrong"}, {
        "key": 2, "wanted": "test"}, {"key": 3, "wanted": "wrong"}]

    assert find_match("key", "key", {"key": 2}, result) == expected


def test_matches_with_long_target():
    expected = {
        "department_id": 2,
        "department_name": "sales",
        "location": "Manchester",
        "manager": "Raphael",
        "created_at": "2022-10-01 14:00:59.456000",
        "last_updated": "2022-10-01 14:00:59.456000",
    }
    target = {
        "staff_id": 1,
        "first_name": 'Jeremie',
        "last_name": 'Franey',
        "department_id": 2,
        "email_address": 'jeremie.franey@terrifictotes.com',
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }
    list = [{
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
    assert find_match("department_id", "department_id",
                      target, list) == expected


def test_returns_error_when_no_match():
    target = {
        "staff_id": 1,
        "first_name": 'Jeremie',
        "last_name": 'Franey',
        "department_id": 7,
        "email_address": 'jeremie.franey@terrifictotes.com',
        "created_at": '2022-11-03 14:20:51.563000',
        "last_updated": '2022-11-03 14:20:51.563000'
    }
    list = [{
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
    with pytest.raises(IndexError):
        find_match("department_id", "department_id", target, list)


def test_finds_match_when_keys_are_different_strings():
    expected = {"secondkey": 1, "wanted": "test"}

    target = {"key": 1}
    list = [{"secondkey": 1, "wanted": "test"}]

    assert find_match("key", "secondkey", target, list) == expected


def test_remove_keys_doesnt_mutate():
    testlist = [{"hello": "Mark", "created_at": "2009-12-25"}]
    testclone = [{"hello": "Mark", "created_at": "2009-12-25"}]
    remove_keys(testlist)
    assert testlist == testclone


def test_removes_keys_from_single_dictionary():
    testlist = [{"hello": "Mark", "created_at": "2009-12-25"}]
    expected = [{"hello": "Mark"}]
    assert remove_keys(testlist) == expected


def test_removes_keys_from_list():
    testlist = [{"hello": "Mark", "created_at": "2009-12-25"},
                {"hello": "Ben", "created_at": "2009-12-25"}]
    expected = [{"hello": "Mark"}, {"hello": "Ben"}]
    assert remove_keys(testlist) == expected


def test_time_splitter_splits_single_dict():
    expected = {"created_date": "2023-02-15",
                "created_time": "08:38:10.121000", "random": "test"}
    input = {"created_at": "2023-02-15 08:38:10.121000", "random": "test"}

    assert time_splitter(input) == expected


def test_time_splitter_no_mutation():
    input = {"created_at": "2023-02-15 08:38:10.121000", "random": "test"}
    clone = {"created_at": "2023-02-15 08:38:10.121000", "random": "test"}
    time_splitter(input)
    assert input == clone
