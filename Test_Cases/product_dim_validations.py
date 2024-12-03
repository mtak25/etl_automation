import pandas as pd
import json
from datetime import datetime
from Utilities.Source_Target_DB_Conn import MYSQL_DB_Conn, Oracle_DB_Conn
from Utilities.logging import Logs

source_db_conn = MYSQL_DB_Conn()
target_db_conn = Oracle_DB_Conn()

#Calling Log_gen function from Utilities.
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger = Logs.Log_Gen(f'C:\\Users\\LENOVO\\PycharmProjects\\pythonProject\\ETL_Testing_Automation\\Logs\\product_dim_{dt}.log')

with open(r'C:\\Users\\LENOVO\\PycharmProjects\\pythonProject\\ETL_Testing_Automation\\Config\\SQL_Queries_config\\Product_dim.json','r') as Product_data_SQL_fle:
    Product_SQL_Queries = json.load(Product_data_SQL_fle)

# Retrieve dynamic table names
source_table_name = Product_SQL_Queries["table_names"]["source"]
target_table_name = Product_SQL_Queries["table_names"]["target"]

def Source_Target_Count_check(source_db_conn,target_db_conn,Product_SQL_Queries,logger):
    source_result = pd.read_sql(Product_SQL_Queries['count_comparison']['source_query'],source_db_conn)
    target_result = pd.read_sql(Product_SQL_Queries['count_comparison']['target_query'],target_db_conn)

#source Count checking
    if source_result.empty: #Will result true or false
        source_count=0
    else:
        source_count = source_result.iloc[0,0]
    print(source_count)

#Target Count checking
    if target_result.empty: #This will result True or False
        target_count=0
    else:
        target_count = target_result.iloc[0,0]

    print(target_result)

#Comparing Source & Target record counts:
    if source_count == target_count:
        Result_status = "Source & Target counts are matched"
        status = "PASS"
    else:
        Result_status = "Source & Target counts are not matched"
        status = "FAIL"

#Defining a format of output result to write into excel file
    Df_count_result = pd.DataFrame(
        {
        "Database":["Source table", "Target table"],
        "Table_names":[source_table_name,target_table_name],
        "Count":[source_count,target_count],
        "Result":[Result_status,Result_status],
        "Status":[status,status]
    }
    )

    logger.info(f"Source Product table Count:{source_count}")
    logger.info(f"Target Product_dim table Count: {target_count}")
    logger.info(f"Source & Target Count validation status: {status}")
    logger.info("Source & Target Record Count Validation Successfully Completed!")
    return Df_count_result

#Checking Null values in Product_dim table.

def Null_Checks(target_db_conn,Product_SQL_Queries,logger):
    target_result = pd.read_sql(Product_SQL_Queries['null_check']['target_query'],target_db_conn)

    if target_result.empty:
        null_count = 0
    else:
        null_count = target_result.iloc[0,0]

    if null_count == 0:
        Result_status = "No Records found with Null values."
        status = "PASS"
    else:
        Result_status = "Records found with Null values."
        status = "FAIL"
        # Fetching the actual records with null values using the query from JSON
        null_records_query = Product_SQL_Queries['null_check']['null_records_query']
        null_records = pd.read_sql(null_records_query, target_db_conn)
        logger.info(f"Null records found: \n{null_records}")  # Log the null records

    df_nulls = pd.DataFrame(
        {
        "Database": ["Target table"],
        'Table_names':[target_table_name],
        'Count':[null_count],
        'Result':[Result_status],
        'Status':[status]
    }
    )
    logger.info(f"Null records count:{null_count}")
    logger.info (f"Null check validation status:{status}")
    logger.info("Null check Validation Completed Successfully!.")
    return df_nulls

def Duplicate_Records_chk(target_db_conn,Product_SQL_Queries,logger):
    tgt_result = pd.read_sql(Product_SQL_Queries["duplicate_check"]["target_query"],target_db_conn)

    if tgt_result.empty:
        duplicate_count = 0
    else:
        duplicate_count = tgt_result.iloc[0,0]

    if duplicate_count == 0:
        Result_status = "Duplicate rows not found!"
        status = "PASS"
    else:
        Result_status = "Duplicate rows found!"
        status = "FAIL"
        # Fetching the actual duplicate records using the query from JSON
        duplicate_records_query = Product_SQL_Queries["duplicate_check"]["duplicate_records_query"]
        duplicate_records = pd.read_sql(duplicate_records_query, target_db_conn)
        logger.info(f"Duplicate records found: \n{duplicate_records}")  # Log the duplicate records

    df_Duplicate_records = pd.DataFrame(
        {
        "Database": ["Target table"],
        "Table_names": [target_table_name],
        'Count': [duplicate_count],
        'Result': [Result_status],
        'Status': [status]
    }
    )

    logger.info(f"Duplicate Records count:{duplicate_count}")
    logger.info(f"Duplicate check validation status:{status}")
    logger.info(f"Duplicate check validation completed Successfully!")
    return df_Duplicate_records


def Column_mapping_Validation(source_db_conn, target_db_conn, Product_SQL_Queries, logger):
    source_result = pd.read_sql(Product_SQL_Queries["column_mapping"]["source_query"], source_db_conn)
    target_result = pd.read_sql(Product_SQL_Queries["column_mapping"]["target_query"], target_db_conn)

    source_columns = source_result.columns.tolist()# columns.tolist() converts the column names (which are typically stored as a pandas Index object) into a Python LIST
    target_columns = target_result.columns.tolist()

    # Check for column name matches
    if set(source_columns) == set(target_columns):
        # Proceed to check row count matches if column names match
        if source_result.shape[0] == target_result.shape[0]:# in pandas dataframe shape[0] gives no of rows,shape[1] give no of columns
            # If both column names and row counts match, compare the actual data
            comparison_result = (source_result.reset_index(drop=True) == target_result.reset_index(drop=True))#reset_index(drop=True):This method is called on both DataFrames to reset their indices,drop=True:This argument means that the old index will not be added as a column in the new DataFrame. Essentially, it creates a new default integer index (0, 1, 2, ...) for both DataFrames.Why Use It: If the two DataFrames might have different indices (for example, if they were generated from different queries or operations), resetting the index ensures that the comparison is based purely on the data, rather than the index values.
            print(comparison_result)
            if comparison_result.all().all():#The first all() confirms that all values in each column are True, and the second all() confirms that this condition holds for all columns.
                Result_status = "Source & Target tables data matched!"
                status = "PASS"
            else:
                Result_status = "Source & Target tables data not matched!"
                status = "FAIL"

                # Get the indices of mismatched rows
                mismatched_indices = comparison_result[comparison_result.all(axis=1)==False].index #This line is used to identify the indices of rows in the comparison_result DataFrame that contain at least one mismatch (i.e., at least one False value),comparison_result.all(axis=1) == False: This explicitly checks for rows where  all values are False

                # Print source and target records for mismatched rows
                logger.info("Mismatched records found:")
                for index in mismatched_indices:
                    source_row = source_result.iloc[index].to_dict() #The to_dict() method in Pandas is used to convert a DataFrame (or a Series) into a Python dictionary.
                    target_row = target_result.iloc[index].to_dict()
                    logger.info(f"Row {index} - Source: {source_row}, Target: {target_row}")
        else:
            Result_status = "Row count mismatch between source and target tables!"
            status = "FAIL"
            logger.info(Result_status)
    else:
        Result_status = "Source & Target table column mapping data not matched!"
        status = "FAIL"
        logger.info("Column mapping failed due to column names mismatch.")

    # Prepare the result summary DataFrame
    df_column_mapping_result = pd.DataFrame(
        {
            "Database": ["Source table", "Target table"],
            "Table_names": [source_table_name, target_table_name],
            "Result": [Result_status, Result_status],
            "Status": [status, status]
        }
    )

    logger.info(f"Source & Target columns mapping status: {status}")
    logger.info("Source & Target columns mapping completed Successfully!")
    return df_column_mapping_result



#Calling all above functions

counts = Source_Target_Count_check(source_db_conn,target_db_conn,Product_SQL_Queries,logger)
null_chk = Null_Checks(target_db_conn,Product_SQL_Queries,logger)
duplicates = Duplicate_Records_chk(target_db_conn,Product_SQL_Queries,logger)
columns_mapp = Column_mapping_Validation(source_db_conn,target_db_conn,Product_SQL_Queries,logger)

df_Validations_result = {
    "Src_Tgt_count_comp":counts,
    "Null_checks":null_chk,
    "Duplicate_checks":duplicates,
    "Column_mapping":columns_mapp
}
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file_path = f"C:\\Users\\LENOVO\\PycharmProjects\\pythonProject\\ETL_Testing_Automation\\Output_Result\\Product_dim_Result_{dt}.xlsx"
with pd.ExcelWriter(output_file_path) as product_output_file_path:
    for sheet, df in df_Validations_result.items():
        df.to_excel(product_output_file_path,sheet_name=sheet,index = False)