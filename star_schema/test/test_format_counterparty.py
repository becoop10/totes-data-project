from star_schema.src.utils.format_counterparty import format_counterparty
import pytest
from unittest.mock import patch
import logging
def test_returns_empty_list_with_empty_inputs():
    assert format_counterparty([],[])==[]

def test_formats_single_counterparty_with_single_list_address():
    formatted=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "counterparty_legal_address_line_1":"42 Hitchhiker's Guide",
        "counterparty_legal_address_line_2":"To the Galaxy",
        "counterparty_legal_district":"Adams",
        "counterparty_legal_city":"Douglas",
        "counterparty_legal_postal_code":"DA42 C3PO",
        "counterparty_legal_country":"Magarathea",
        "counterparty_legal_phone_number":"0118999"

    }]
    unformatted_counterparty=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "legal_address_id":3,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]
    unformatted_address=[{
        "address_id":3,
        "address_line_1":"42 Hitchhiker's Guide",
        "address_line_2":"To the Galaxy",
        "district":"Adams",
        "city":"Douglas",
        "postal_code":"DA42 C3PO",
        "country":"Magarathea",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]

    assert format_counterparty(unformatted_counterparty,unformatted_address)==formatted

def test_formats_counterparty_with_multiple_addresses():
    formatted=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "counterparty_legal_address_line_1":"42 Hitchhiker's Guide",
        "counterparty_legal_address_line_2":"To the Galaxy",
        "counterparty_legal_district":"Adams",
        "counterparty_legal_city":"Douglas",
        "counterparty_legal_postal_code":"DA42 C3PO",
        "counterparty_legal_country":"Magarathea",
        "counterparty_legal_phone_number":"0118999"

    }]
    unformatted_counterparty=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "legal_address_id":3,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]

    unformatted_address=[{
        "address_id":1,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":3,
        "address_line_1":"42 Hitchhiker's Guide",
        "address_line_2":"To the Galaxy",
        "district":"Adams",
        "city":"Douglas",
        "postal_code":"DA42 C3PO",
        "country":"Magarathea",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":6,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }
    ]

    assert format_counterparty(unformatted_counterparty,unformatted_address)==formatted


def test_formats_multiple_counters():
    formatted=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "counterparty_legal_address_line_1":"42 Hitchhiker's Guide",
        "counterparty_legal_address_line_2":"To the Galaxy",
        "counterparty_legal_district":"Adams",
        "counterparty_legal_city":"Douglas",
        "counterparty_legal_postal_code":"DA42 C3PO",
        "counterparty_legal_country":"Magarathea",
        "counterparty_legal_phone_number":"0118999"

    },{
        "counterparty_id":2,
        "counterparty_legal_name":"Ben",
        "counterparty_legal_address_line_1":"1337 Computers",
        "counterparty_legal_address_line_2":"Microsoft Ave",
        "counterparty_legal_district":"Gates",
        "counterparty_legal_city":"Bill",
        "counterparty_legal_postal_code":"BG13 C3PO",
        "counterparty_legal_country":"America",
        "counterparty_legal_phone_number":"0118999"

    }]

    unformatted_counter=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "legal_address_id":3,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "counterparty_id":2,
        "counterparty_legal_name":"Ben",
        "legal_address_id":5,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]

    unformatted_address=[{
        "address_id":1,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":3,
        "address_line_1":"42 Hitchhiker's Guide",
        "address_line_2":"To the Galaxy",
        "district":"Adams",
        "city":"Douglas",
        "postal_code":"DA42 C3PO",
        "country":"Magarathea",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":5,
        "address_line_1":"1337 Computers",
        "address_line_2":"Microsoft Ave",
        "district":"Gates",
        "city":"Bill",
        "postal_code":"BG13 C3PO",
        "country":"America",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },
    {
        "address_id":6,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }
    ]

    assert format_counterparty(unformatted_counter,unformatted_address) == formatted


def test_error_when_no_matching_address_and_continues():
    unformatted_counter=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "legal_address_id":3,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "counterparty_id":2,
        "counterparty_legal_name":"Ben",
        "legal_address_id":5,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]

    unformatted_address=[{
        "address_id":1,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":2,
        "address_line_1":"42 Hitchhiker's Guide",
        "address_line_2":"To the Galaxy",
        "district":"Adams",
        "city":"Douglas",
        "postal_code":"DA42 C3PO",
        "country":"Magarathea",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":5,
        "address_line_1":"1337 Computers",
        "address_line_2":"Microsoft Ave",
        "district":"Gates",
        "city":"Bill",
        "postal_code":"BG13 C3PO",
        "country":"America",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },
    {
        "address_id":6,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }
    ]

    formatted=[{
        "counterparty_id":2,
        "counterparty_legal_name":"Ben",
        "counterparty_legal_address_line_1":"1337 Computers",
        "counterparty_legal_address_line_2":"Microsoft Ave",
        "counterparty_legal_district":"Gates",
        "counterparty_legal_city":"Bill",
        "counterparty_legal_postal_code":"BG13 C3PO",
        "counterparty_legal_country":"America",
        "counterparty_legal_phone_number":"0118999"

    }]
    
    assert format_counterparty(unformatted_counter,unformatted_address) == formatted

LOGGER=logging.getLogger(__name__)

def test_raises_log(caplog):
    unformatted_counter=[{
        "counterparty_id":1,
        "counterparty_legal_name":"Mark",
        "legal_address_id":3,
        "commercial_contact":"Dave Mate",
        "delivery_contact":"Alan Partridge",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }]
    unformatted_address=[{
        "address_id":1,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":2,
        "address_line_1":"42 Hitchhiker's Guide",
        "address_line_2":"To the Galaxy",
        "district":"Adams",
        "city":"Douglas",
        "postal_code":"DA42 C3PO",
        "country":"Magarathea",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },{
        "address_id":5,
        "address_line_1":"1337 Computers",
        "address_line_2":"Microsoft Ave",
        "district":"Gates",
        "city":"Bill",
        "postal_code":"BG13 C3PO",
        "country":"America",
        "phone":"0118999",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    },
    {
        "address_id":6,
        "address_line_1":"DONT WANT",
        "address_line_2":"DONT WANT",
        "district":"DONT WANT",
        "city":"DONT WANT",
        "postal_code":"DONT WANT",
        "country":"DONT WANT",
        "phone":"DONT WANT",
        "created_at":'2022-11-03 14:20:51.563000',
        "last_updated":'2022-11-03 14:20:51.563000'
    }
    ]

    
    with caplog.at_level(logging.WARNING):
        format_counterparty(unformatted_counter,unformatted_address)

        assert "{'counterparty_id': 1, 'counterparty_legal_name': 'Mark', 'legal_address_id': 3, 'commercial_contact': 'Dave Mate', 'delivery_contact': 'Alan Partridge', 'created_at': '2022-11-03 14:20:51.563000', 'last_updated': '2022-11-03 14:20:51.563000'} no matching table found" in caplog.text

