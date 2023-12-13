import pyodbc
import pandas as pd

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=DESKTOP-EKJH7K7;'
                        'DATABASE=checkcdc;'
                        'Trusted_Connection=yes;')

cursor = conn.cursor()

csv_file_path = 'C:\\Users\\yuvag\\Downloads\\master.csv'

# SQL Server table name
table_name = 'discrepancy'
# Read CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

def pandas_inbuilt():
    # Insert data into SQL Server table
    df.to_sql(name=table_name, con=conn)

    # Close the connection
    conn.close()

    print(f'Data inserted into {table_name} table.')


def using_insert_statement():
    column_names = "" # list out all column name here. For Example: "columnname1, columnname2 and so on" without quotes
    for index, row in df.iterrows():
        try:
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES (?,?);" # put no. of '?' on how many columns
            cursor.execute(insert_query, tuple(row))
        except Exception as e:
            print(e)
    conn.commit()
    conn.close()

using_insert_statement()