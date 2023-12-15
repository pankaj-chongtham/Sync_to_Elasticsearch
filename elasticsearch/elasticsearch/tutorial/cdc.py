import pyodbc
import pandas as pd

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=DESKTOP-EKJH7K7;'
                        'DATABASE=checkcdc;'
                        'Trusted_Connection=yes;')

cursor = conn.cursor()

cmn_df = pd.read_sql("select * from CMN_Elstc_index_tbl", con=conn)
condition_records = cmn_df.to_dict(orient='records')
for record in condition_records:
    Elstc_sync_mode_name = record.get('Elstc_sync_mode')
    print(Elstc_sync_mode_name)

if Elstc_sync_mode_name = 'cdc':
    try:
        __$start_lsn = slsn
        query = """
        USE checkcdc
        DECLARE @from_lsn binary(10), @to_lsn binary(10)
        SELECT @from_lsn = sys.fn_cdc_get_min_lsn('prime')
        SELECT @to_lsn = sys.fn_cdc_get_max_lsn()
        SELECT * FROM cdc.fn_cdc_get_all_changes_prime(@from_lsn, @to_lsn, 'all') where where slsn = 
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            print(row)

conn.close()
