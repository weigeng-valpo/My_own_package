import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.select_lists import *
from snp_query_box.ServerConnection import ServerConnection
from snp_query_box import hotMacQueryList

import warnings
warnings.filterwarnings("ignore")

def pull_hot_mac_call(min_call_date, max_call_date):
    sc = ServerConnection()
    conn, cursor = sc.stars_sop_rpt_connection()
    query =  """
    select
            MFB.[SGK_MBR_FOCUS_ID]
            ,MFB.[SGK_RPT_MBR_FOCUS_ID]
            ,MFB.[FOCUS_DESC] 
            ,MFB.[INDIVIDUAL_ID]
            ,MFB.[MEMBER_ID]
            ,MFB.[MEDICARE_MEMBER_ID]
            ,MFB.[CALL_SCRIPT_DESC]
            ,MFB.[RECOMMENDATION]
            ,MFB.[UPDATE_TIMESTAMP]
            ,MFB.[MFS_CUR_FOCUS_STATUS_REASON_DESC]
            ,MFB.[MFS_CUR_FOCUS_STATUS_DESC]
            ,MFB.[LEGACY]
            ,MFB.REPORTING_YEAR
            ,C.[CALL_TIMESTAMP]
            ,C.[CALL_TO]
            ,CT.LOOKUP_CD_DISPLAY_NM 
            ,C.[CALL_STATUS_DESC]
            ,C.[NOT_REACH_REASON_DESC]
            ,C.[CALL_TYPE_DESC]
            ,C.[INSERT_TEAM_NM]
            from 
            [STARSSOPRPT].[dbo].[STARS_WEB_RPT_MBR_FOCUS_BASE] (nolock) AS MFB
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_CALL_TRACKING_HX_BASE] (nolock) C ON MFB.[SGK_RPT_MBR_FOCUS_ID] = C.[SGK_RPT_MBR_FOCUS_ID]
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_SOP_LOOKUP_DATA_REF] (nolock) CT on C.CALL_TO= CT.LOOKUP_CD_ID and CT.SGK_LOOKUP_COL_ID=168
            WHERE 
              1=1
            AND LOOKUP_CD_DISPLAY_NM = 'Member'
            AND CALL_TIMESTAMP >= ?
            AND CALL_TIMESTAMP <= ?
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[min_call_date, max_call_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[min_call_date, max_call_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_hot_mac_call_bq(min_call_date, max_call_date):
    import subprocess
    from google.cloud import bigquery

    query = hotMacQueryList.pull_hot_mac_call_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "min_call_date",
            "DATE",
            min_call_date
            ),
            bigquery.ScalarQueryParameter(
            "max_call_date",
            "DATE",
            max_call_date
            )
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()
    