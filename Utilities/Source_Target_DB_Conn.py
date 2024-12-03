from sqlalchemy import create_engine
import cx_Oracle

def MYSQL_DB_Conn():
    mysql_host="localhost"
    mysql_user="root"
    mysql_pwd="admin"
    mysql_source_db="source"
    source_db_conn= create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_pwd}@{mysql_host}/{mysql_source_db}")
    return  source_db_conn


def Oracle_DB_Conn():
    oracle_host= "localhost"
    oracle_port=1521
    oracle_servicename="xe"
    oracle_user="c##target"
    oracle_password="target"
    target_db_conn = (f"oracle+cx_oracle://{oracle_user}:{oracle_password}@{cx_Oracle.makedsn(oracle_host,oracle_port,service_name=oracle_servicename)}")
    return target_db_conn