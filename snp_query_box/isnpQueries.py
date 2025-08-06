import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
import warnings
from snp_query_box.ServerConnection import ServerConnection

warnings.filterwarnings("ignore")

"""
This is to keep data pull queries as functions.
By keeping queries here we can avoid long jupyter notebooks and it is easy to maintain and debug it. 
"""

def pull_isnp_hra(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection()
    query=f'''
    Select temp.MEMBER_ID,temp.HRA_Completion_Date,temp.INSERTED_DTS, mepr.Contract_Number from 
    (Select Member_ID,HRA_DATE as HRA_Completion_Date,INSERTED_DTS from [dm].[MSBI_LHP_HRA_DTL]
    where INSERTED_DTS > GETDATE()-10) temp left join DM.MSBI_MEPR mepr on
    temp.MEMBER_ID=mepr.Member_ID
    group by temp.MEMBER_ID,temp.HRA_Completion_Date,temp.INSERTED_DTS, mepr.Contract_Number
    order by temp.MEMBER_ID
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(dfs, ignore_index = True)
    return df