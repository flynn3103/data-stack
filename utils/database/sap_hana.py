import logging
from datetime import datetime
from hdbcli import dbapi
import pandas as pd
import concurrent.futures

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding='utf-8'
)

class SapHANAHandler:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def create_connection_by_using_hdbcli(self):
        """Connect to the MSSQL database host"""
        conn = None
        try:
            logging.info("Connecting to the SAP HANA database...")
            conn = dbapi.connect(self.host, self.port, self.username, self.password, schema="DW")
        except:
            raise dbapi.DatabaseError
        logging.info("Connection successful")
        return conn

    def execute_sql(self, query) -> None:
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(query)
        cursor.execute(query)
        connection.commit()
        connection.close()

    def create_table_if_not_exists(self, schema_name: str, table_name: str, dataframe: pd.DataFrame) -> None:
        """Check if table exists in the database; if not, create the table with the schema from the provided DataFrame"""
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()

        # Check if the table exists in the DW schema
        check_table_query = f"""
        SELECT COUNT(*)
        FROM "SYS"."TABLES"
        WHERE "TABLE_NAME" = '{table_name}' AND "SCHEMA_NAME" = '{schema_name}'
        """
        logging.info(f"Checking if table {table_name} exists in schema {schema_name}")
        cursor.execute(check_table_query)
        result = cursor.fetchone()

        if result[0] == 0:  # If the table does not exist
            logging.info(f"Table {table_name.upper()} does not exist, creating the table.")

            # Generate the CREATE TABLE query from the DataFrame schema
            create_table_query = self.construct_create_table_query(schema_name, table_name, dataframe)
            logging.info(f"Creating table {table_name.upper()} with schema: {create_table_query}")
            cursor.execute(create_table_query)
            connection.commit()
        else:
            logging.info(f"Table {table_name} already exists.")

        connection.close()

    def construct_create_table_query(self, schema_name:str, table_name: str, dataframe: pd.DataFrame) -> str:
        """Construct the CREATE TABLE SQL query based on the DataFrame schema"""
        columns = []
        for column_name, dtype in dataframe.dtypes.items():
            # Transform column name to uppercase and map the data type to SAP HANA types
            hana_type = self.map_dtype_to_hana(dtype)
            columns.append(f'"{column_name.upper()}" {hana_type}')

        # Construct the final CREATE TABLE SQL statement
        create_table_query = f"""
        CREATE TABLE "{schema_name}"."{table_name.upper()}" (
            {', '.join(columns)}
        )
        """
        return create_table_query
    
    def map_dtype_to_hana(self, dtype) -> str:
        """Map pandas dtypes to SAP HANA SQL types"""
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'TINYINT'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'VARCHAR(255)'  # Default to VARCHAR for any unknown or string types
    
    def execute_sql_to_df(self, query):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        # logging.info(query)
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
        connection.close()
        df = pd.DataFrame(records)
        return df
    
    def execute_sql_to_dict(self, query):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        # logging.info(query)
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
        connection.close()
        return records

    def load_staging_table(self, table_name, data_list, num_threads=6,batch_size=1000):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(f"Inserting {len(data_list)} records to {table_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        connection.close()
        def insert_chunk(data_chunk):
            """Function to insert a chunk of data using batch insert"""
            chunk_connection = self.create_connection_by_using_hdbcli()
            chunk_cursor = chunk_connection.cursor()
            try:
                for i in range(0, len(data_chunk), batch_size):
                    batch = data_chunk[i:i + batch_size]
                    if batch:
                        placeholders = ", ".join(["?"] * len(batch[0]))
                        columns = ", ".join(batch[0].keys())
                        values_list = [tuple(record.values()) for record in batch]
                        # Construct the INSERT statement
                        insert_sql = f"""
                        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                        """
                        chunk_cursor.executemany(insert_sql, values_list)  # Batch insert
                chunk_connection.commit()
            except Exception as e:
                logging.error(f"Error inserting data chunk: {e}")
            finally:
                chunk_cursor.close()
                chunk_connection.close()
        # Split data_list into roughly equal chunks based on the number of threads
        chunk_size = len(data_list) // num_threads + 1
        data_chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]
        # Use ThreadPoolExecutor to insert data in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(insert_chunk, data_chunks)
        logging.info(f"Successfully inserted all records into {table_name}")
        connection.close()

    def truncate_table(self, schema_name, table_name):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(f"Truncate table: {table_name}")
        cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
        connection.commit()
        connection.close()

    def bulk_loading(self, schema_name, table_name, data_list, num_threads=6, batch_size=1000):
        connection = self.create_connection_by_using_hdbcli()
        logging.info(f"Inserting {len(data_list)} records into {table_name} using {num_threads} threads")
        def insert_chunk(data_chunk):
            """Function to insert a chunk of data using batch insert"""
            chunk_connection = self.create_connection_by_using_hdbcli()
            chunk_cursor = chunk_connection.cursor()
            try:
                for i in range(0, len(data_chunk), batch_size):
                    batch = data_chunk[i:i + batch_size]
                    if batch:
                        placeholders = ", ".join(["?"] * len(batch[0]))
                        columns = ", ".join(batch[0].keys())
                        values_list = [tuple(record.values()) for record in batch]
                        # Construct the INSERT statement
                        insert_sql = f"""
                        INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})
                        """
                        chunk_cursor.executemany(insert_sql, values_list)  # Batch insert
                chunk_connection.commit()
            except Exception as e:
                logging.error(f"Error inserting data chunk: {e}")
            finally:
                chunk_cursor.close()
                chunk_connection.close()
        # Split data_list into roughly equal chunks based on the number of threads
        chunk_size = len(data_list) // num_threads + 1
        data_chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]
        # Use ThreadPoolExecutor to insert data in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(insert_chunk, data_chunks)
        logging.info(f"Successfully inserted all records into {table_name}")
        connection.close()

    def upsert_to_database_model(self, records, target_table, keys):
        # Use LAST_UPDATED_DT instead of created_at & updated_at
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(f"Upsert {len(records)} records to table {target_table}")
        # Step 1: Create Temporary Table
        cursor.execute(
            f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)"
        )
        # Step 2: Insert Data into Temporary Table
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        columns_with_timestamps = columns + ["LAST_UPDATED_DT"]
        columns_str_with_timestamps = ", ".join(columns_with_timestamps)
        for record in records:
            values_with_timestamps = tuple(
                list(record.values()) + [datetime.now(), datetime.now()]
            )
            placeholders_with_timestamps = ", ".join(
                ["?"] * len(columns_with_timestamps)
            )
            insert_query = f"INSERT INTO #TempTable ({columns_str_with_timestamps}) VALUES ({placeholders_with_timestamps})"
            cursor.execute(insert_query, values_with_timestamps)
        # Step 3: Mark Records for Update or Insert
        # cursor.execute(f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)")

        # Next, identify records that exist in the target table and mark them for update
        key_conditions = " AND ".join(
            [f"{target_table}.{key} = #TempTable.{key}" for key in keys]
        )
        mark_update_query = f"""
            UPDATE #TempTable
            SET UpsertAction = 'U'
            WHERE EXISTS (SELECT 1 FROM {target_table} WHERE {key_conditions});
        """
        cursor.execute(mark_update_query)

        # Then, identify records that do not exist in the target table and mark them for insert
        mark_insert_query = """
            UPDATE #TempTable
            SET UpsertAction = 'I'
            WHERE UpsertAction IS NULL;
        """
        cursor.execute(mark_insert_query)
        # Step 4: Perform Update and Insert Operations
        update_columns_str = ", ".join(
            [
                f"{col} = (SELECT {col} FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U')"
                for col in columns
            ]
        )
        update_columns_str += (
            ", LAST_UPDATED_DT = NOW()"  # Update the updated_at timestamp
        )

        update_sql = f"""
            -- Update existing records
            UPDATE {target_table} 
            SET {update_columns_str}
            WHERE EXISTS (SELECT 1 FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U');
        """
        cursor.execute(update_sql)
        insert_sql = f"""
            INSERT INTO {target_table} ({columns_str}, LAST_UPDATED_DT)
            SELECT {columns_str}, NOW() FROM #TempTable WHERE UpsertAction = 'I';
        """
        cursor.execute(insert_sql)
        # Step 5: Cleanup
        cursor.execute("DROP TABLE #TempTable")
        # Commit the transaction
        connection.commit()
        # Close the connection
        logging.info("Upsert successfully")
        connection.close()

    def upsert_records(self, records, target_table, keys):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(f"Upsert {len(records)} records to table {target_table}")
        # Step 1: Create Temporary Table
        cursor.execute(
            f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)"
        )
        # Step 2: Insert Data into Temporary Table
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        columns_with_timestamps = columns + ["created_at", "updated_at"]
        columns_str_with_timestamps = ", ".join(columns_with_timestamps)
        for record in records:
            values_with_timestamps = tuple(
                list(record.values()) + [datetime.now(), datetime.now()]
            )
            placeholders_with_timestamps = ", ".join(
                ["?"] * len(columns_with_timestamps)
            )
            insert_query = f"INSERT INTO #TempTable ({columns_str_with_timestamps}) VALUES ({placeholders_with_timestamps})"
            cursor.execute(insert_query, values_with_timestamps)
        # Step 3: Mark Records for Update or Insert
        # cursor.execute(f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)")

        # Next, identify records that exist in the target table and mark them for update
        key_conditions = " AND ".join(
            [f"{target_table}.{key} = #TempTable.{key}" for key in keys]
        )
        mark_update_query = f"""
            UPDATE #TempTable
            SET UpsertAction = 'U'
            WHERE EXISTS (SELECT 1 FROM {target_table} WHERE {key_conditions});
        """
        cursor.execute(mark_update_query)

        # Then, identify records that do not exist in the target table and mark them for insert
        mark_insert_query = """
            UPDATE #TempTable
            SET UpsertAction = 'I'
            WHERE UpsertAction IS NULL;
        """
        cursor.execute(mark_insert_query)
        # Step 4: Perform Update and Insert Operations
        update_columns_str = ", ".join(
            [
                f"{col} = (SELECT {col} FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U')"
                for col in columns
            ]
        )
        update_columns_str += ", updated_at = NOW()"  # Update the updated_at timestamp

        update_sql = f"""
            -- Update existing records
            UPDATE {target_table} 
            SET {update_columns_str}
            WHERE EXISTS (SELECT 1 FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U');
        """
        cursor.execute(update_sql)
        insert_sql = f"""
            INSERT INTO {target_table} ({columns_str}, created_at, updated_at)
            SELECT {columns_str}, NOW(), NOW() FROM #TempTable WHERE UpsertAction = 'I';
        """
        cursor.execute(insert_sql)
        # Step 5: Cleanup
        cursor.execute("DROP TABLE #TempTable")
        # Commit the transaction
        connection.commit()
        # Close the connection
        logging.info("Upsert successfully")
        connection.close()

    def upsert_records_without_timestamp(self, records, target_table, keys):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        logging.info(f"Upsert {len(records)} records to table {target_table}")
        # Step 1: Create Temporary Table
        cursor.execute(
            f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)"
        )
        # Step 2: Insert Data into Temporary Table
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        columns_with_timestamps = columns
        columns_str_with_timestamps = ", ".join(columns_with_timestamps)
        for record in records:
            values_with_timestamps = tuple(list(record.values()))
            placeholders_with_timestamps = ", ".join(
                ["?"] * len(columns_with_timestamps)
            )
            insert_query = f"INSERT INTO #TempTable ({columns_str_with_timestamps}) VALUES ({placeholders_with_timestamps})"
            try:
                cursor.execute(insert_query, values_with_timestamps)
            except Exception as e:
                logging.error(f"Error inserting record: {insert_query}\n{e}")
        # Step 3: Mark Records for Update or Insert
        # cursor.execute(f"CREATE LOCAL TEMPORARY TABLE #TempTable AS (SELECT *, CAST(NULL AS CHAR(1)) AS UpsertAction FROM {target_table} WHERE 1=0)")

        # Next, identify records that exist in the target table and mark them for update
        key_conditions = " AND ".join(
            [f"{target_table}.{key} = #TempTable.{key}" for key in keys]
        )
        mark_update_query = f"""
            UPDATE #TempTable
            SET UpsertAction = 'U'
            WHERE EXISTS (SELECT 1 FROM {target_table} WHERE {key_conditions});
        """
        try:
            cursor.execute(mark_update_query)
        except Exception as e:
            logging.error(f"Error inserting record: {mark_update_query}\n{e}")
        

        # Then, identify records that do not exist in the target table and mark them for insert
        mark_insert_query = """
            UPDATE #TempTable
            SET UpsertAction = 'I'
            WHERE UpsertAction IS NULL;
        """
        try:
            cursor.execute(mark_insert_query)
        except Exception as e:
            logging.error(f"Error inserting record: {mark_insert_query}\n{e}")
        
        # Step 4: Perform Update and Insert Operations
        update_columns_str = ", ".join(
            [
                f"{col} = (SELECT {col} FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U')"
                for col in columns
            ]
        )
        # update_columns_str += ', updated_at = NOW()'  # Update the updated_at timestamp
        update_sql = f"""
            -- Update existing records
            UPDATE {target_table} 
            SET {update_columns_str}
            WHERE EXISTS (SELECT 1 FROM #TempTable WHERE {key_conditions} AND UpsertAction = 'U');
        """
        try:
            cursor.execute(update_sql)
        except Exception as e:
            logging.error(f"Error inserting record: {update_sql}\n{e}")
        
        insert_sql = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT {columns_str} FROM #TempTable WHERE UpsertAction = 'I';
        """
        try:
            cursor.execute(insert_sql)
        except Exception as e:
            logging.error(f"Error inserting record: {insert_sql}\n{e}")
        # Step 5: Cleanup
        cursor.execute("DROP TABLE #TempTable")
        # Commit the transaction
        connection.commit()
        # Close the connection
        logging.info("Upsert successfully")
        connection.close()

    def upsert_records_by_stg_table(self, upsert_query, target_table, keys):
        try:
            connection = self.create_connection_by_using_hdbcli()
            cursor = connection.cursor()
            records = self.execute_sql_to_dict(upsert_query)
            if not records:
                logging.info("There is no data")
            else:
                logging.info(f"Upsert {len(records)} records to table {target_table}")

                # Step 1: Create Temporary Table
                temp_table_query = (
                    f"CREATE LOCAL TEMPORARY TABLE #TempTable AS ({upsert_query})"
                )
                cursor.execute(temp_table_query)

                # Step 4: Perform Update and Insert Operations
                columns = list(records[0].keys())
                columns.remove("RECORD_STATUS")
                key_conditions = " AND ".join(
                    [f"tbl.{key} = tmp.{key}" for key in keys]
                )
                update_columns_str = ", ".join(
                    [
                        f"{col} = (SELECT {col} FROM #TempTable as tmp WHERE {key_conditions} AND RECORD_STATUS = 'U')"
                        for col in columns
                    ]
                )
                insert_column_str = ", ".join(columns)
                # update_columns_str += ', LAST_UPDATED_DT = NOW()'  # Update the updated_at timestamp

                update_sql = f"""
                    -- Update existing records
                    UPDATE {target_table} as tbl
                    SET {update_columns_str}
                    WHERE EXISTS (SELECT 1 FROM #TempTable as tmp WHERE {key_conditions} AND RECORD_STATUS = 'U');
                """
                cursor.execute(update_sql)
                insert_sql = f"""
                    INSERT INTO {target_table} ({insert_column_str})
                    SELECT {insert_column_str} FROM #TempTable WHERE RECORD_STATUS = 'I';
                """
                cursor.execute(insert_sql)
                # Step 5: Cleanup
                cursor.execute("DROP TABLE #TempTable")
                # Commit the transaction
                connection.commit()
                # Close the connection
                logging.info("Upsert successfully")
                connection.close()
        except Exception as e:
            logging.debug(e)
            raise e
    
    def upsert_records_to_target(self, staging_table, target_table, unique_key, upsert_query="", except_cols=[], num_threads=8):
        connection = self.create_connection_by_using_hdbcli()
        cursor = connection.cursor()
        # Fetch column names and data types from the target table
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE TABLE_NAME = '{target_table[3:]}' 
            ORDER BY POSITION
        """)
        columns_info = cursor.fetchall()
        columns = [row[0] for row in columns_info]

        # Generate condition for comparing columns excluding certain columns
        conditions = []
        for col in columns:
            if col == unique_key or col == 'LAST_UPDATED_DT' or col in except_cols:
                continue
            column_type = next((row[1] for row in columns_info if row[0] == col), '')
            if column_type in ('VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TIMESTAMP', 'DATE'):
                conditions.append(f"IFNULL(DS.{col}, '') <> IFNULL(D.{col}, '')")
            else:
                conditions.append(f"IFNULL(DS.{col}, 0) <> IFNULL(D.{col}, 0)")

        # Construct UPSERT CTE query
        if not upsert_query:
            upsert_query = f"""
                WITH CTE_TMP AS (
                    SELECT 
                        DS.*,
                        CASE 
                            WHEN D.{unique_key} IS NULL THEN 'I'
                            ELSE 'U'
                        END AS RECORD_STATUS
                    FROM {staging_table} DS
                    LEFT JOIN {target_table} D ON DS.{unique_key} = D.{unique_key}
                    WHERE D.{unique_key} IS NULL OR 
                        (DS.{unique_key} = D.{unique_key} AND (
                            { ' OR '.join(conditions) }
                        ))
                )
                SELECT * FROM CTE_TMP
            """

        # Fetch records to upsert from the query
        cursor.execute(upsert_query)
        records_to_upsert = cursor.fetchall()

        # Prepare insert and update SQL queries
        insert_sql = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in columns])})
        """

        update_columns_str = ", ".join([f"{col} = ?" for col in columns if col != unique_key and col not in except_cols])
        update_sql = f"""
            UPDATE {target_table} SET {update_columns_str} 
            WHERE {unique_key} = ?
        """

        def process_upserts(chunk):
            chunk_connection = self.create_connection_by_using_hdbcli()
            chunk_cursor = chunk_connection.cursor()
            try:
                # Split chunk into insert and update records
                insert_records = [record for record in chunk if record[-1] == 'I']
                update_records = [record for record in chunk if record[-1] == 'U']

                # Perform batch inserts
                if insert_records:
                    insert_values = [record[:-1] for record in insert_records]
                    chunk_cursor.executemany(insert_sql, insert_values)

                # Perform batch updates
                if update_records:
                    update_values = [record[1:-1] + (record[0],) for record in update_records]
                    chunk_cursor.executemany(update_sql, update_values)

                chunk_connection.commit()
            except Exception as e:
                logging.error(f"Error in upsert chunk: {e}")
                chunk_connection.rollback()
            finally:
                chunk_cursor.close()
                chunk_connection.close()

        # Split records_to_upsert into chunks for parallel processing
        num_records = len(records_to_upsert)
        chunk_size = num_records // num_threads + 1
        record_chunks = [records_to_upsert[i:i + chunk_size] for i in range(0, num_records, chunk_size)]

        # Use ThreadPoolExecutor to process upserts in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(process_upserts, chunk) for chunk in record_chunks]
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    logging.error(f"Thread raised an exception: {future.exception()}")

        logging.info(f"Successfully upserted {num_records} records from {staging_table} to {target_table}")
        connection.close()