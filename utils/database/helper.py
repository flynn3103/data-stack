from datetime import datetime, timedelta
from utils.connection.sap_hana_handler import SapHANAHandler
import configparser
import urllib.parse
import time
from functools import wraps

def get_item_by_key(item: dict, key: str, is_date: bool = False):
    if item.get(key):
        if is_date:
            initial_datetime = datetime.fromisoformat(item.get(key))
            new_datetime = initial_datetime + timedelta(hours=7)
            new_datetime_str = new_datetime.strftime("%Y-%m-%d %H:%M:%S")
            return new_datetime_str
        else:
            return item.get(key)
    return None

def proc_incremental_post_load(client, pkg_name):
        connection = client.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        query = f"""
                UPDATE DW.ETL_PKG_TRACKING
                SET STATUS = 1 , LAST_RUN_DT = START_DT, END_DT = CURRENT_TIMESTAMP
                WHERE PKG_NAME = '{pkg_name}'
                AND STATUS <> 1;
                """
        cursor.execute(query)
        connection.close()

def proc_incremental_pre_load(client, pkg_name):
        connection = client.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        query = f"""
                    UPDATE DW.ETL_PKG_TRACKING SET START_DT = CURRENT_TIMESTAMP ,STATUS = - 1
                    WHERE PKG_NAME = '{pkg_name}' ;
                """
        cursor.execute(query)
        connection.close()

def get_last_run_dt(client, pkg_name):
        connection = client.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        query = f"""
                SELECT LAST_RUN_DT FROM DW.ETL_PKG_TRACKING
                WHERE PKG_NAME = '{pkg_name}'
                """
        cursor.execute(query)
        last_run_dt = cursor.fetchall()[0][0]
        connection.close()
        return last_run_dt

def decode_connection_string(connection_string):
    parsed_url = urllib.parse.urlparse(connection_string)
    decoded_password = urllib.parse.quote_plus(parsed_url.password)
    decoded_connection_string = f"{parsed_url.scheme}://{parsed_url.username}:{decoded_password}@{parsed_url.hostname}:{parsed_url.port}/{parsed_url.path}"
    return decoded_connection_string
 
 
def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record start time
        result = func(*args, **kwargs)  # Call the actual function
        end_time = time.time()  # Record end time
        duration = end_time - start_time  # Calculate duration
        print(f"Function '{func.__name__}' executed in {duration:.4f} seconds")
        return result  # Return the result of the function
    return wrapper

def convert_column_names_upper(columns):
    return [col.upper().replace(' ', '_') for col in columns]