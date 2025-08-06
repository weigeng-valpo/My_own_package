import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.select_lists import *
from snp_query_box.ServerConnection import ServerConnection
import warnings
warnings.filterwarnings("ignore")

def pull_mac_dsnp_call():
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
            ,MFB.[CALL_SCRIPT_CD]
            ,MFB.[CALL_SCRIPT_DESC]
            ,MFB.[RECOMMENDATION]
            ,MFB.[UPDATE_TIMESTAMP]
            ,MFB.[INSERT_TIMESTAMP]
            ,MFB.[MFS_CUR_FOCUS_STATUS_REASON_DESC] as  'Call Disposition'
            ,MFB.[MFS_CUR_FOCUS_STATUS_REASON_DESC]
            ,MFB.[MFS_CUR_FOCUS_STATUS_DESC]
            ,MFB.[LEGACY]
            ,MFB.MFS_CUR_ASSIGNED_TO_USER_NM
            ,MFB.MFS_CUR_ASSIGNED_TO_SUPERVISOR_USER_NM
            ,MFB.MFS_CUR_ASSIGNED_DT as ASSIGNED_DT
            ,MFB.REPORTING_YEAR
            ,I.[CMS_CNTRCT_NBR]
            ,I.[PBP_ID]
            ,I.[REGION_DESC]
            ,I.[GROUP_CTL_NM] as 'PlanSponsorPlan'
            ,I.[STATE_POSTAL_DESC] as 'State'
            ,I.FIRST_NM
            ,I.LAST_NM
            ,I.MIDDLE_NM
            ,I.HICN_NBR
            ,I.MEMBER_PHONE_NBR
            ,C.SGK_CALL_TRACKING_HX_ID as outreaches
            ,C.[CALL_TIMESTAMP]
            ,C.[CALL_TIMESTAMP] as 'Call Timestamp Copy'
            ,C.[CALL_TO]
            ,CT.LOOKUP_CD_DISPLAY_NM as [Call to]
            ,C.[CALL_STATUS_DESC]
            ,C.[NOT_REACH_REASON_DESC]
            ,C.[UPDATE_USER_NM] as CALL_ADVOCATE
            ,C.[UPDATE_SUPERVISOR_USER_NM] as CALL_SUPERVISOR
            ,C.[TIME_ZONE_DESC] as [Time Zone Desc]
            ,MFB.FOCUS_PRIORITY_RANK,CASE WHEN I.LANGUAGE_DESC IS NULL Or I.LANGUAGE_DESC ='' THEN NULL ELSE I.LANGUAGE_DESC END AS LANGUAGE_DESC
            ,IB.MEMBER_EFF_DT
            from 
            [STARSSOPRPT].[dbo].[STARS_WEB_RPT_MBR_FOCUS_BASE] (nolock) AS MFB
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_CALL_TRACKING_HX_BASE] (nolock) C ON MFB.[SGK_RPT_MBR_FOCUS_ID] = C.[SGK_RPT_MBR_FOCUS_ID]
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_SOP_LOOKUP_DATA_REF] (nolock) CT on C.CALL_TO= CT.LOOKUP_CD_ID and CT.SGK_LOOKUP_COL_ID=168
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_INDVDL_DETAILS_BASE] AS I ON MFB.[INDIVIDUAL_ID] = I.[INDIVIDUAL_ID]
            LEFT JOIN [STARSSOPRPT].[dbo].[STARS_WEB_RPT_MBR_X_INDVDL_BASE] AS IB  on MFB.[INDIVIDUAL_ID] =IB.[INDIVIDUAL_ID]
              WHERE 
              1=1
            --AND MFB.WORKFLOW_CD_ID = 3 
            AND MFB.REPORTING_YEAR >= YEAR(GETDATE())-3
            AND MFB.[RECOMMENDATION] = 'CALL-MAC-DSNP'
            --AND MFB.[WORKFLOW_CD_ID] = 3 --User wants another view with all other MAC Campaigns
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df


