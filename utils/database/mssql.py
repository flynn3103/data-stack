import logging
import pymssql
import pandas as pd


logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] %(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

class MSSQLHandler:
    def __init__(self, host, db_name, username=None, password=None):
        self.host = host
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection_by_using_pymssql(self):
        """ Connect to the MSSQL database host """
        conn = None
        try:
            logging.info('Connecting to the MSSQL database...')
            conn = pymssql.connect(server=self.host, 
                                   database=self.db_name,
                                   user=self.username,
                                   password=self.password)
        except:
            raise pymssql.DatabaseError
        logging.info("Connection successful")
        return conn

    def execute_sql_to_dict(self, query):
        connection = self.create_connection_by_using_pymssql()
        cursor = connection.cursor(as_dict=True)
        logging.info(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        connection.close()
        return rows

    def upsert_to_database(self, table_name: str, unique_keys: list, records):
        connection = self.create_connection_by_using_pymssql()
        cursor = connection.cursor(as_dict=True)
        try:
            # Step 1: Create a temporary table
            cursor.execute(f"SELECT * INTO #TempTable FROM {table_name} WHERE 1 = 0;")

            # Step 2: Bulk insert into the temporary table
            self.bulk_insert("#TempTable", records, cursor)

            # Step 3: Perform a single MERGE operation
            unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_keys])
            update_columns = ', '.join([f"target.{key} = source.{key}" for key in records[0].keys() if key != 'updated_at'])

            merge_query = f'''
                MERGE INTO {table_name} AS target
                USING #TempTable AS source
                ON {unique_key_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {update_columns}, updated_at = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(records[0].keys())}, created_at, updated_at)
                    VALUES (source.{', source.'.join(records[0].keys())}, GETDATE(), GETDATE());
            '''
            cursor.execute(merge_query)
            # Step 4: Drop the temporary table
            cursor.execute("DROP TABLE #TempTable;")
            connection.commit()
            logging.info(f'Upsert successfully')
            connection.close()
            logging.info("Connection closed")
        except Exception as e:
            raise e

        
    def upsert_to_database_model(self, table_name: str, unique_keys: list, records):
        # Use LAST_UPDATED_DT instead of created_at & updated_at
        connection = self.create_connection_by_using_pymssql()
        cursor = connection.cursor(as_dict=True)
        # Step 1: Create a temporary table
        cursor.execute(f"SELECT * INTO #TempTable FROM {table_name} WHERE 1 = 0;")

        # Step 2: Bulk insert into the temporary table
        self.bulk_insert("#TempTable", records, cursor)

        # Step 3: Perform a single MERGE operation
        unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_keys])
        update_columns = ', '.join([f"target.{key} = source.{key}" for key in records[0].keys() if key != 'updated_at'])

        merge_query = f'''
            MERGE INTO {table_name} AS target
            USING #TempTable AS source
            ON {unique_key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_columns}, LAST_UPDATED_DT = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(records[0].keys())}, LAST_UPDATED_DT)
                VALUES (source.{', source.'.join(records[0].keys())}, GETDATE());
        '''
        cursor.execute(merge_query)
        # Step 4: Drop the temporary table
        cursor.execute("DROP TABLE #TempTable;")
        connection.commit()
        logging.info(f'Upsert successfully')
        connection.close()
        logging.info("Connection closed")

    def upsert_to_database_without_last_update(self, table_name: str, unique_keys: list, records):
        # Upsert don't use LAST_UPDATED_DT
        connection = self.create_connection_by_using_pymssql()
        cursor = connection.cursor(as_dict=True)
        # Step 1: Create a temporary table
        cursor.execute(f"SELECT * INTO #TempTable FROM {table_name} WHERE 1 = 0;")
        
        # Step 2: Bulk insert into the temporary table
        self.bulk_insert("#TempTable", records, cursor)
        
        # Step 3: Perform a single MERGE operation
        unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_keys])
        update_columns = ', '.join([f"target.{key} = source.{key}" for key in records[0].keys() if key != 'updated_at'])
        
        merge_query = f'''
            MERGE INTO {table_name} AS target
            USING #TempTable AS source
            ON {unique_key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(records[0].keys())})
                VALUES (source.{', source.'.join(records[0].keys())});
        '''
        cursor.execute(merge_query)
        # Step 4: Drop the temporary table
        cursor.execute("DROP TABLE #TempTable;")
        connection.commit()
        logging.info(f'Upsert successfully')
        connection.close()
        logging.info("Connection closed")
    
    def upsert_to_database_do_nothing(self, table_name: str, unique_keys: list, records):
        connection = self.create_connection_by_using_pymssql()
        cursor = connection.cursor(as_dict=True)
        # Step 1: Create a temporary table
        cursor.execute(f"SELECT * INTO #TempTable FROM {table_name} WHERE 1 = 0;")

        # Step 2: Bulk insert into the temporary table
        self.bulk_insert("#TempTable", records, cursor)

        # Step 3: Perform a single MERGE operation
        unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_keys])
        # update_columns = ', '.join([f"target.{key} = source.{key}" for key in records[0].keys() if key != 'updated_at'])

        merge_query = f'''
            MERGE INTO {table_name} AS target
            USING #TempTable AS source
            ON {unique_key_conditions}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(records[0].keys())}, created_at, updated_at)
                VALUES (source.{', source.'.join(records[0].keys())}, GETDATE(), GETDATE());
        '''
        cursor.execute(merge_query)
        # Step 4: Drop the temporary table
        cursor.execute("DROP TABLE #TempTable;")
        connection.commit()
        logging.info(f'Upsert successfully')
        connection.close()
        logging.info("Connection closed")
    
    def bulk_insert(self, temp_table_name: str, records: list, cursor):
        columns = ', '.join(records[0].keys())
        placeholders = ', '.join(['%s'] * len(records[0]))
        insert_query = f"INSERT INTO {temp_table_name} ({columns}) VALUES ({placeholders})"
        error_records = []  # List to keep track of records that fail
        for record in records:
            try:
                # Convert the record to a tuple of values
                values = tuple(record.values())
                cursor.execute(insert_query, values)
            except Exception as record_error:
                logging.error(f"Error inserting record {record}: {record_error}")
                error_records.append(record)  # Collect the failed record

        if error_records:
            logging.error(f"Failed to insert the following records: {error_records}")