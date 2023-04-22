from src.utils.transaction_category import parse_single_value_config, parse_list_of_values, parse_not_operator, create_sub_query_for_single_condition

def test_parse_not_operator():
    assert parse_not_operator("contain") == (False, "contain")
    assert parse_not_operator("not contain") == (True, "contain")
    assert parse_not_operator("notcontain") == (True, "contain")
    assert parse_not_operator("not contain") == (True, "contain")

def test_parse_single_value_config():
    assert parse_single_value_config("date(2020-01-01)") == ("2020-01-01", "date")
    assert parse_single_value_config("datetime(2020-01-01 00:00:00)") == ("2020-01-01 00:00:00", "datetime")
    assert parse_single_value_config("abc") == ("abc", "str")
    assert parse_single_value_config(123) == (123, "int")
    assert parse_single_value_config("123") == ("123", "str")
    assert parse_single_value_config(123.45) == (123.45, "float")
    assert parse_single_value_config("123.45") == ("123.45", "str")

def test_parse_list_of_values():
    assert parse_list_of_values(["date(2020-01-01)","date(2020-01-02)"]) == (["2020-01-01", "2020-01-02"], "date")
    assert parse_list_of_values(["datetime(2020-01-01 00:00:00)","datetime(2020-01-02 00:00:00)"]) == (["2020-01-01 00:00:00", "2020-01-02 00:00:00"], "datetime")
    assert parse_list_of_values(["abc","def"]) == (["abc", "def"], "str")
    assert parse_list_of_values([123,456]) == ([123, 456], "int")
    assert parse_list_of_values(["123","456"]) == (["123", "456"], "str")
    assert parse_list_of_values([123.45, 456.78]) == ([123.45, 456.78], "float")
    assert parse_list_of_values(["123.45","456.78"]) == (["123.45", "456.78"], "str")
    assert parse_list_of_values("123.45") == ("123.45", "str")
    assert parse_list_of_values(123.45) == (123.45, "float")


def test_create_sub_query_for_single_condition():
    assert create_sub_query_for_single_condition("start_date", ">=", "date", "2020-01-01") == "start_date >= to_date('2020-01-01', 'yyyy-MM-dd')"
    assert create_sub_query_for_single_condition("timestamp", ">", "datetime", "2020-01-01 00:00:00") == "timestamp > to_timestamp('2020-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')"
    assert create_sub_query_for_single_condition("tran_desc", "contain", "str", "abc") == "tran_desc rlike 'abc'"
    assert create_sub_query_for_single_condition("tran_desc", "start_with", "str", "abc") == "tran_desc rlike '^(abc)'"
    assert create_sub_query_for_single_condition("tran_desc", "end_with", "str", "abc") == "tran_desc rlike '(abc)$'"
    assert create_sub_query_for_single_condition("tran_desc", "=", "str", "abc") == '''tran_desc = "abc"'''