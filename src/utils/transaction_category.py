# code for reading transaction categorization logic from YAML file and construct a CASE WEHN spark sql query to categorize the transaction
from collections import OrderedDict
import re
from pathlib import Path
import yaml
from pyspark.sql import DataFrame, SparkSession
# utils for reading yaml file
def read_yaml_file(file_path: str) -> dict:
    with open(file_path, "r") as f:
        data = yaml.safe_load(f.read())
    return data

# define supported operators
simple_operators = ["=", ">", "<", ">=", "<=", "!=", "==", "in", "not in", "<=>", "%", "^"]
other_operators = ["isnull", "not isnull", "start_with", "end_with", "contain"]

# function to parse single value config from yaml file, it will return the value and the type of the value
def parse_single_value_config(value: any) -> "tuple[any, str]":
    res_value = None
    res_type = ''
    if isinstance(value, str):
        value = value.strip()
        if re.match(r"^date\(\d{4}-\d{2}-\d{2}\)$", value):
            res_value = re.search(r"\((\d{4}-\d{2}-\d{2})\)", value).group(0)[1:-1]
            res_type = "date"
        elif re.match(r"^datetime\(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\)$", value):
            res_value = re.search(r"\((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\)", value).group(0)[1:-1]
            res_type = "datetime"
        else:
            res_value = value
            res_type = "str"
    elif isinstance(value, int):
        res_value = value
        res_type = 'int'
    elif isinstance(value, float):
        res_value = value
        res_type = "float"
    else:
        raise ValueError("Unsupported value type: {}; each single value hs to be a string or number".format(type(value)))
    return (res_value, res_type)

# function to parse list of values, it will return a list of values and the type of the values
def parse_list_of_values(values: any) -> "tuple[list, str]":
    res = None
    data_type = ''
    if isinstance(values, list):
        data_type = parse_single_value_config(values[0])[1]
        res = [parse_single_value_config(value)[0] for value in values]
    else:
        res, data_type = parse_single_value_config(values)
    return (res, data_type)

# function to create sub-qeury of a single condition from the yaml config
def create_sub_query_for_single_condition(field_name: str, operator: str, field_type: str, value: any) -> str:
    sub_query = ""
    value_string = ""
    if field_type == "date":
        if isinstance(value, str):
            value_string = "to_date('{}', 'yyyy-MM-dd')".format(value)
        elif isinstance(value, list):
            values = ["to_date('{}', 'yyyy-MM-dd')".format(item) for item in value]
            value_string = "( {} )".format(",".join(values))
    elif field_type == "datetime":
        if isinstance(value, str):
            value_string = "to_timestamp('{}', 'yyyy-MM-dd HH:mm:ss')".format(value)
        elif isinstance(value, list):
            values = ["to_timestamp('{}', 'yyyy-MM-dd HH:mm:ss')".format(item) for item in value]
            value_string = "( {} )".format(",".join(values))
    elif field_type == "str":
        if isinstance(value, list):
            values = ['"{}"'.format(item) for item in value]
            value_string = "( {} )".format(",".join(values))
        else:
            if operator in simple_operators:
                value_string = '"{}"'.format(value)
            else:
                value_string = value
    else:
        if isinstance(value, list):
            values = [str(item) for item in value]
            value_string = "( {} )".format(",".join(values))
        else:
            value_string = str(value)
    
    if operator in simple_operators:
        sub_query = "{} {} {}".format(field_name, operator, value_string)
    elif (operator == "isnull") or (operator == "not isnull"):
        sub_query = "{}({})".format(operator, field_name)
    elif field_type != "str":
        raise ValueError("Operators contain/start_with/end_with only support string type")
    else:
        if operator == "contain":
            sub_query = "{} rlike '{}'".format(field_name, value_string)
        elif operator == "start_with":
            sub_query = "{} rlike '^({})'".format(field_name, value_string)
        elif operator == "end_with":
            sub_query = "{} rlike '({})$'".format(field_name, value_string)
        else:
            raise ValueError("Unsupported operator: {}. Now we only support contain/start_with/end_with for rlike operators".format(operator))
    return sub_query


# function to parse & split 'not' in operator
def parse_not_operator(operator: str) -> "tuple[bool, str]":
    if "not" in operator:
        return (True, operator.replace("not", "").strip())
    else:
        return (False, operator)
    
# function to create single WHEN sub-query
def create_single_when_query(category_config_dict: dict) -> str:
    tran_type = category_config_dict["tran_type"]
    conditions = category_config_dict["conditions"]
    when_conditions_list = []
    
    # in one when sub-query, there could be multiple conditions connected by AND
    for condition in conditions:
        condition_dict = OrderedDict(condition["condition"])
        when_condition_list = []
        for key, value in condition_dict.items():
            # for one single column/field, we support multiple pairs of operator and value connected by AND, 
            # i.e. "field_name": {"operator1": "contain", value1: "aa", "operator2": "not contain", "value2": "bb"}
            for i in range(1, 1 + len(value.keys()//2)):
                operator_not, operator = parse_not_operator(value["operator{}".format(i)])
                data_value, data_type  = parse_list_of_values(value["value{}".format(i)])
                single_field_condition = create_sub_query_for_single_condition(key, operator, data_type, data_value)
                if operator_not:
                    when_condition_list.append("!({})".format(single_field_condition))
                else:
                    when_condition_list.append("{}".format(single_field_condition))
        when_conditions_list.append(" AND ".join(when_condition_list))
    when_query = "WHEN ({0}) THEN '{1}'".format(" OR ".join(when_conditions_list), tran_type)
    return when_query

# function to create the whole query
def create_query(category_config_dict: dict, default_tran_type: str, category_col_name: str) -> str:
    when_query_list = []
    for category_config in category_config_dict:
        when_query_list.append(create_single_when_query(category_config))
    query = "SELECT *, CASE {0} ELSE '{1}' END AS {2}".format(" ".join(when_query_list), default_tran_type, category_col_name)
    return query

# function to load the yaml config, create the whole query and run it
def create_query_and_run(source_df: DataFrame, categorization_config_path: str, default_tran_type: str, category_col_name: str, spark: SparkSession) -> DataFrame:
    # load the yaml config
    categorization_config = read_yaml_file(categorization_config_path)

    # create tmp view from dataframe
    source_df.createOrReplaceTempView("table_txn_source")

    # create the whole query
    query = create_query(categorization_config, default_tran_type, "category") + "from table_txn_source"
    print("The query is: {}".format(query))

    return spark.sql(query)
