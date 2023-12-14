import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=DESKTOP-EKJH7K7;'
                        'DATABASE=checkcdc;'
                        'Trusted_Connection=yes;')

cursor = conn.cursor()

query = """
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_discrepancy(
    sys.fn_cdc_get_min_lsn('dbo_discrepancy', 'disc_cdc'), 
    sys.fn_cdc_get_max_lsn(), 
    'all'
)
"""

cursor.execute(query)
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()
