import cx_Oracle
import pandas as pd
import re
from sqlalchemy import create_engine, text
import os
import logging
from datetime import datetime
import shutil

# Set up logging
log_file_path = 'program_log.txt'  # Change this to your desired log file path
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
# Replace these values with your Oracle database credentials
username = 'EMAUDIT'
password = 'EMAUDIT'
host = 'xxxx'
port = 1521
sid = 'emdev123'

username1 = 'EMAUDIT'
password1 = 'EMAUDIT'
host1 = '10.114.218.123'
port1 = 1521
sid1 = 'emdev123'

# List of schema owners
schema_owners = ['PCLP_','KMBP_']
# Replace with your desired folder path
output_folder = 'D:/CSV/2'
output_zip_path = 'D:/CSV/em-audit'
# Function to construct DSN
def construct_dsn(host, port, sid):
    return cx_Oracle.makedsn(host, port, sid)

# Function to export data to CSV and XLSX formats
def export_to_csv_xlsx(data_frame, csv_filename, xlsx_filename):
    # Check if the output folder exists, and create it if not
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Specify the CSV file path
    csv_file_path = os.path.join(output_folder, csv_filename)
    xlsx_file_path = os.path.join(output_folder, xlsx_filename)

    data_frame.to_csv(csv_file_path, index=False)
    data_frame.to_excel(xlsx_file_path, index=False)

try:
        # Construct the Oracle connection string
        conn_str = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{sid}"

        # Create an SQLAlchemy engine
        engine = create_engine(conn_str)

        # Create a connection using the SQLAlchemy engine
        connection = engine.connect()
               
        # Replace with your actual table and WHERE condition
        where_condition = "IS_ACTIVE= 'Y'"
        
        # Specific column you want to retrieve
        columns_to_retrieve = ['SCAN_NAME', 'QUERY', 'ORDER_BY', 'AUDIT_SCAN_ID']
        select_columns = ', '.join(columns_to_retrieve)
        # Replace 'your_table' with the actual table in the schema
        #sql_query = f'SELECT {select_columns} FROM AUDIT_SCAN WHERE {where_condition}'
        sql_query = text('SELECT SCAN_NAME, QUERY, ORDER_BY , AUDIT_SCAN_ID FROM AUDIT_SCAN WHERE IS_ACTIVE = :is_active')
       
        # Create a cursor
        #cursor = connection.cursor()
        
        # Set the parameter value
        #param_value = 'Y' 
        params = {'is_active': 'Y'}       
                
        #print(f'....................{sql_query}')
        # Execute the SELECT query
        #cursor.execute(sql_query, parameters)
        qryresult = connection.execute(sql_query.bindparams(**params))
        # Fetch all rows
        rows = qryresult.fetchall()

           
        results =[dict(zip(columns_to_retrieve, row)) for row in rows]
       
        if results is not None:
                #print("Row Data:")
                for result in results:
                   order_by=result.get('ORDER_BY')
                   query=result.get('QUERY')
                   scan_name=result.get('SCAN_NAME')
                   audit_scan_id=result.get('AUDIT_SCAN_ID')
                   logging.info(f"   ")
                   logging.info(f"#################################################")
                   logging.info(f"Report Name.......{scan_name}")
                   # Convert LOB data to a string
                   #print(f'....................{query}')
                   #lob_string = query.read()
                   # Loop through each schema owner
                   combined_result = pd.DataFrame()
                   ##########
                   for schema_owner in schema_owners:
                    logging.info(f"Schema ...{schema_owner}")
                    new_query=re.sub(re.escape("&&OWNER."), schema_owner, query, flags=re.IGNORECASE)
                    #print(f'@@@@.... {new_query}')                   
                    sql_query1 = new_query
                    if order_by is not None:
                        sql_query1 = f'{new_query} order by {order_by}' 
                    df = pd.read_sql_query(sql_query1, connection)
                    #result-count
                    result_count = df.shape[0]
                    logging.info(f"Count of results: {result_count}")
                    
                    # Example data
                    data = {
                        'AUDIT_SCHEMA': [schema_owner],
                        'AUDIT_SCAN_ID': [audit_scan_id],
                        'EM_VERSION': ['8'],
                        'ITEMS': [result_count],
                        'QUERY_DATE': [datetime.now()]
                    }
                    df1 = pd.DataFrame(data)
                    table_name = 'query_log'
                    df1.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                    #print(f'<<<<<<<<{df}')
                    combined_result = pd.concat([combined_result, df], ignore_index=True)
                   csv_filename = f'{scan_name}.csv'
                   xlsx_filename = f'{scan_name}.xlsx'
                   export_to_csv_xlsx(combined_result, csv_filename, xlsx_filename)
                   logging.info(f".......Data exported successfully for report {scan_name}")
             

    # Create a zip file from the specified folder
        shutil.make_archive(output_zip_path, 'zip', output_folder)
        print(f"Folder '{output_folder}' successfully zipped to '{output_zip_path}'.")

except cx_Oracle.Error as error:
    print(f'Error: {error}')
except Exception as e:
    print(f"Error: {e}")
finally:
    engine.dispose()
    if connection:
        connection.close()
        
      
