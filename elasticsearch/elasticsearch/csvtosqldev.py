import pyodbc
import pandas as pd

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=DESKTOP-EKJH7K7;'
                        'DATABASE=checkcdc;'
                        'Trusted_Connection=yes;')

#cursor = conn.cursor()

csv_file_path = 'C:\\Users\\yuvag\\Downloads\\master.csv'

# SQL Server table name
table_name = 'discrepancy'

# Read CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Insert data into SQL Server table
df.to_sql(name=table_name, con=conn, schema='dbo', if_exists='replace', index=False)

# Close the connection
conn.close()

print(f'Data inserted into {table_name} table.')
