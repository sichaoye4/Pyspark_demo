'''code for preprocess the loan, account & transaction table from Czech Bank Dataset'''
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import lit, col, date_format, to_date, when, row_number, add_months, expr
from pyspark.sql.types import StringType, IntegerType

# the preprocessing function for transaction table
def preprocess_transaction(df):
    # format the date column
    df = df.withColumn("date", date_format(to_date(col("date").cast(StringType()), "yyMMdd"), "yyyy-MM-dd"))

    # translate the type column
    df = df.withColumn("type", when(col("type") == "PRIJEM", "credit").otherwise("debit"))

    # translate the operation column
    df = df.withColumn("operation", when(col("operation") == "VYBER KARTOU", "Credit Card Withdrawal")\
                                    .when(col("operation") == "VKLAD", "Credit in Cash")\
                                    .when(col("operation") == "PREVOD Z UCTU", "Collection from another bank")\
                                    .when(col("operation") == "VYBER", "Withdrawal in Cash")\
                                    .when(col("operation") == "PREVOD NA UCET", "Remittance to another bank"))
    
    # translate the k_symbol column
    df = df.withColumn("tran_desc", when(col("k_symbol") == "POJISTNE", "Insurance Payment")\
                                    .when(col("k_symbol") == "SLUZBY", "Payment on Statement")\
                                    .when(col("k_symbol") == "UROK", "Interest Credited")\
                                    .when(col("k_symbol") == "SANKC. UROK", "Sanction interest")\
                                    .when(col("k_symbol") == "SIPO", "Household Payment")\
                                    .when(col("k_symbol") == "DUCHOD", "Old-age Pension")\
                                    .when(col("k_symbol") == "UVER", "Loan Payment"))
    
    # create a month_id column
    df = df.withColumn("month_id", date_format(col("date"), "yyyyMM").cast(IntegerType()))
    return df

# the preprocessing function of account table
def preprocess_account(df):
    # dedup account by account_id using window function, and keep the record with the latest date
    window = Window.partitionBy("account_id").orderBy(col("type").desc())
    df = df.withColumn("rn", row_number().over(window)).where(col("rn") == 1).select("account_id", "client_id")
    return df

# the preprocessing function of loan table
def preprocess_loan(df):
    # format the date column
    df = df.withColumn("date", to_date(col("date").cast(StringType()), "yyMMdd"))

    # create a month_id column
    df = df.withColumn("month_id", date_format(col("date"), "yyyyMM").cast(IntegerType()))

    # create a due date column by adding duration as month to the date column
    df = df.withColumn("duration", col("duration").cast(IntegerType()))\
            .withColumn("due_date", date_format(expr("add_months(date, duration)"), "yyyy-MM-dd"))
    
    # format date into yyyy-MM-dd string
    df = df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
    return df