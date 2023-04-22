'''code for preprocess the demographics, account & transaction table from Czech Bank Dataset'''
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import lit, col, regexp_replace, regexp_extract, date_format, to_date, when

# the preprocessing function for transaction table
def preprocess_transaction(df):
    # format the date column
    df = df.withColumn("date", date_format(to_date(col("date"), "yyMMdd"), "yyyy-MM-dd"))

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
    return df

# the preprocessing function of account table
def preprocess_account(df):
    # translate the Frequency column
    df = df.withColumn("frequency", when(col("frequency") == "POPLATEK MESICNE", "Monthly Issuance")\
                                    .when(col("frequency") == "POPLATEK TYDNE", "Weekly Issuance")\
                                    .otherwise("ISSUANCE AFTER TRANSACTION"))
    return df
