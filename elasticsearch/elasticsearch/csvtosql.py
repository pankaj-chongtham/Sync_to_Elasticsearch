import csv
import pyodbc

# CSV file path
csv_file_path = 'C:\\Users\\yuvag\\Downloads\\sentences_100.csv'

# Table details
table_name = 'discrepancy'
column_name = 'description'

# Establish a connection to the database
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=localhost\SQLEXPRESS;'
                        'DATABASE=master;'
                        'Trusted_Connection=yes;')

cursor = conn.cursor()

# Read data from CSV and insert into the SQL table
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    for row in csv_reader:
        insert_query = f"UPDATE {table_name} SET {column_name} = (?) "

        # Execute the INSERT statement with the value from the CSV
        cursor.execute(insert_query, row[0])

# Commit the changes and close the connection
conn.commit()
conn.close()