import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
import warnings
from snp_query_box import DsnpHelperFunction
from snp_query_box.ServerConnection import ServerConnection


warnings.filterwarnings("ignore")

"""
This is to keep data pull queries as functions.
By keeping queries here we can avoid long jupyter notebooks and it is easy to maintain and debug it. 
"""

def pull_provider_demographic(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = '''
    select ProviderID, AddressLine1, AddressLine2, City, County, State, Zip, LabelName
    from [StarsDataHubProd].[Provider].[Demographics] as t1
    where LastUpdated = (
        select max(LastUpdated) from [StarsDataHubProd].[Provider].[Demographics] as t2
        where t1.ProviderID = t2.ProviderID
        )
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query), conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df


def pull_pcp_visit(USERNAME, PASSWORD, start_date, end_date, reporting_year=2025, mbr_list=None):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)
    query = '''
    select distinct m360.SRC_MEMBER_ID as SRC_MEMBER_ID,
    CMS_CNTRCT_NBR,
    PHONE_NBR,
    PROVIDER_ID,
    GROUP,
    GROUP_NAME,
    cast(cl.SRV_START_DT as varchar(10)) as SRV_START_DT, 
    cast(cl.SRV_STOP_DT as varchar(10)) as SRV_STOP_DT
    from starstemp.stars_analytics_mbr_360 m360
    inner join iwh.claim_line cl on m360.member_id=cl.member_id
    where m360.reporting_year='''+ str(reporting_year) +'''
    and m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
    and cl.srv_start_dt between ? and ?
    and cl.business_ln_cd = 'ME'
    and cl.summarized_srv_ind='Y'
    and cl.clm_ln_status_cd='P'
    and (cl.src_prvdr_ty_cd in ('PP', 'OB') 
    or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG'))
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query), conn, params=[start_date, end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[start_date, end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df_all["dsnp_member_id"] = df_all["SRC_MEMBER_ID"].str.strip()
    if mbr_list!=None:
        df_selected = df_all[df_all["dsnp_member_id"].isin(mbr_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()

    df['SRV_START_DT']=pd.to_datetime(df['SRV_START_DT'], errors='coerce')
    df['SRV_STOP_DT']=pd.to_datetime(df['SRV_STOP_DT'], errors='coerce')

    df['PCP_Visit_ID'] = df['SRC_MEMBER_ID']+'_'+df['SRV_START_DT'].astype(str)

    df = df.drop_duplicates()
    return df


def pull_geo_info(medicare_number_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection(driver)
    
    query = '''
    select MEDICARE_NBR, MBR_LAT, MBR_LONG, INSERTED_DTS from dm.MSBI_INDVDL_PROFILE_DTL
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    temp_df = pd.concat(dfs, ignore_index = True)
    df_selected = temp_df.sort_values(by=['MEDICARE_NBR', 'INSERTED_DTS'])
    df_all = df_selected.drop_duplicates(subset=["MEDICARE_NBR"], keep='last')\
        .drop(columns = ["INSERTED_DTS"])  
    
    if medicare_number_list!=None:
        df_selected = df_all[df_all["MEDICARE_NBR"].str.strip().isin(medicare_number_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df