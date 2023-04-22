from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import lit, col, regexp_replace, regexp_extract, date_format, to_date, when
import pyspark.sql.functions as f
from preprocessing_tables import preprocess_transaction, preprocess_account, preprocess_loan
from utils.utils import create_spark_session, read_csv_file, save_single_csv_file
from utils.transaction_category import create_query_and_run
from pathlib import Path
import sys
# create Spark session
spark = create_spark_session()

# function for 
# 1. preprocess transaction table
# 2. preprocess account table
# 3. left join transaction with account table on account_id
def join_account_transaction(account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    # preprocess transaction table
    transaction_df = preprocess_transaction(transaction_df)

    # preprocess account table
    account_df = preprocess_account(account_df)

    # join transaction with account table on account_id
    df = transaction_df.join(account_df, ["account_id"], how="left")
    return df

# function for 
# 1. preprocess account table
# 2. left join loan table with account table on account_id
def join_account_loan(account_df: DataFrame, loan_df: DataFrame) -> DataFrame:
    # preprocess account table
    account_df = preprocess_account(account_df).cache()

    # preprocess loan table
    loan_df = preprocess_loan(loan_df)

    # join loan with account table on account_id
    df = loan_df.join(account_df, ["account_id"], how="left")
    return df


# main flow of the program
if __name__ == "__main__":
    # find main.py file path from sys.argv[0] using pathlib.Path
    rootPath = Path(sys.argv[0]).parent.parent.absolute()

    # define path pf account, loan & transaction csv files
    account_file_path = Path.joinpath(rootPath, "data", "disp.csv")
    loan_file_path = Path.joinpath(rootPath, "data", "loan.csv")
    transaction_file_path = Path.joinpath(rootPath, "data", "trans.csv")

    # read account, loan & transaction table from csv file
    account_df = read_csv_file(str(account_file_path), spark, delimeter=",")
    loan_df = read_csv_file(str(loan_file_path), spark, delimeter=",")
    transaction_df = read_csv_file(str(transaction_file_path), spark)

    # join account & transaction table
    df_trans = join_account_transaction(account_df, transaction_df)

    # join account & loan table
    df_loan = join_account_loan(account_df, loan_df)

    # aggregate df_trans by client_id & month_id to get the total transaction amount & number of transactions by counting trans_id for each client & month
    df_trans_agg = df_trans.groupBy("client_id", "month_id", "operation").agg(f.count(col("trans_id")).alias("number_of_trans"), f.sum(col("amount")).alias("total_trans_amount"))
    df_trans_agg_path = str(Path.joinpath(rootPath, "data", "client_trans_summary.csv"))
    save_single_csv_file(df_trans_agg, df_trans_agg_path)


    # aggregate df_loan by client_id & month_id to get the total transaction amount & number of transactions by counting trans_id for each client & month
    df_loan_agg = df_loan.groupBy("client_id", "month_id").agg(f.count(col("loan_id")).alias("number_of_loans"), f.sum(col("amount")).alias("total_loan_amount"))
    df_trans_agg_path = str(Path.joinpath(rootPath, "data", "client_loan_summary.csv"))
    save_single_csv_file(df_loan_agg, df_trans_agg_path)

    print("End of Spark Process")