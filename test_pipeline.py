import pytest
from pipeline import normalize_phone, parse_date, parse_decimal, normalize_header

def test_normalize_phone():
    assert normalize_phone("917718750036") == "+917718750036"
    assert normalize_phone("07718750036") == "+917718750036"
    assert normalize_phone("7718750036") == "+917718750036"
    assert normalize_phone("+91-77187-50036") == "+917718750036"
    assert normalize_phone("12345") is None

def test_parse_date():
    assert parse_date("2026-10-05") == "2026-10-05"
    assert parse_date("05/10/2026") == "2026-10-05"
    assert parse_date("05-Oct-2026") == "2026-10-05"
    assert parse_date("31/02/2026") is None

def test_parse_decimal():
    assert parse_decimal("₹5,000.50") == 5000.50
    assert parse_decimal("Rs. 1,200") == 1200.0
    assert parse_decimal("-100") is None

def test_normalize_header():
    assert normalize_header("Customer_Name") == "customer name"