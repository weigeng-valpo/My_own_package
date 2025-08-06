import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.select_lists import *
from snp_query_box.ServerConnection import ServerConnection
from snp_query_box import qnxtQueryList
import warnings

def pull_call_log_nj(call_log_date1, call_log_date2):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    query =  """
                SELECT distinct 'Dynamo' as src_sys_nm
                    , 'calllogs' as src_file_nm
                    , cms.Hic as MedicareID
                    , m.memid as QNXT_MemberID
                    , m.fullname
                    , 'H6399' as Contract
                    , '001' as PBP
                    , 'Member/Self' as cl_call_log_rel_type_desc
                    , NOTE_CREATE_DATE as cl_contact_dts
                    , Outreach_Outcome_descr as cl_attempt_status_key_desc
                    , created_by as cl_inserted_by_user_nm
                    , Outreach_type as cl_call_log_type_desc
                    , 'Dynamo' as Source
                FROM Duals_Prod.CM.Member_Information_Outreaches_COE a
                    join Duals_Prod.CM.Member_Information_NJ m on a.Member_ID = m.carriermemid
                    join (
                            select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) 
                            from Planreport_QNXT_NJ.dbo.membercmshic
                        ) cms  on m.memid = cms.memid and crn = 1
                where UniqueGroup='Non Letter'
                    and cast(NOTE_CREATE_DATE as date) >= ?
                    and cast(NOTE_CREATE_DATE as date) <= ?
                    and Outreach_Outcome_descr in ('Successful - FU','Successful','Face to Face') 
                """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[call_log_date1, call_log_date2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[call_log_date1, call_log_date2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_call_log_nj_all(call_log_date1, call_log_date2):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    cursor = conn.cursor
    query =  """SELECT distinct 'Dynamo' as src_sys_nm
                    , 'calllogs' as src_file_nm
                    , cms.Hic as MedicareID
                    , m.memid as QNXT_MemberID
                    , m.fullname
                    , 'H6399' as Contract
                    , '001' as PBP
                    , 'Member/Self' as cl_call_log_rel_type_desc
                    , NOTE_CREATE_DATE as cl_contact_dts
                    , Outreach_Outcome_descr as cl_attempt_status_key_desc
                    , created_by as cl_inserted_by_user_nm
                    , Outreach_type as cl_call_log_type_desc
                    , 'Dynamo' as Source
                FROM Duals_Prod.CM.Member_Information_Outreaches_COE a
                    join Duals_Prod.CM.Member_Information_NJ m on a.Member_ID = m.carriermemid
                    join (
                            select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) 
                            from Planreport_QNXT_NJ.dbo.membercmshic
                        ) cms  on m.memid = cms.memid and crn = 1
                WHERE UniqueGroup='Non Letter'
                    and cast(NOTE_CREATE_DATE as date) >= ?
                    and cast(NOTE_CREATE_DATE as date) <= ?
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[call_log_date1, call_log_date2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[call_log_date1, call_log_date2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_call_log_va(call_log_date1, call_log_date2):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    cursor = conn.cursor

    query =  """
            SELECT distinct 'Dynamo' as src_sys_nm
                    , 'calllogs' as src_file_nm
                    , cms.Hic as MedicareID
                    , m.memid as QNXT_MemberID
                    , m.fullname
                    , 'H1610' as Contract
                    , m.pbp as PBP
                    , 'Member/Self' as cl_call_log_rel_type_desc
                    , NOTE_CREATE_DATE as cl_contact_dts
                    , Outreach_Outcome_descr as cl_attempt_status_key_desc
                    , created_by as cl_inserted_by_user_nm
                    , Outreach_type as cl_call_log_type_desc
                    , 'Dynamo' as Source
            FROM Duals_Prod.CM.Member_Information_Outreaches_COE a 
                join Duals_Prod.CM.Member_Information_VA m on a.Member_ID = m.carriermemid and cast(a.[note_create_date] as date) between m.adj_rate_eff and m.adj_rate_term
                join (select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) from Planreport_QNXT_VA.dbo.membercmshic) cms  
                    on m.memid = cms.memid and crn = 1
            WHERE UniqueGroup='Non Letter'
                    and cast(NOTE_CREATE_DATE as date) >= ?
                    and cast(NOTE_CREATE_DATE as date) <= ?
                    and Outreach_Outcome_descr in ('Successful - FU','Successful')
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[call_log_date1, call_log_date2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[call_log_date1, call_log_date2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_call_log_va_all(call_log_date1, call_log_date2):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    query =  """
            SELECT distinct 'Dynamo' as src_sys_nm
                    , 'calllogs' as src_file_nm
                    , cms.Hic as MedicareID
                    , m.memid as QNXT_MemberID
                    , m.fullname
                    , 'H1610' as Contract
                    , m.pbp as PBP
                    , 'Member/Self' as cl_call_log_rel_type_desc
                    , NOTE_CREATE_DATE as cl_contact_dts
                    , Outreach_Outcome_descr as cl_attempt_status_key_desc
                    , created_by as cl_inserted_by_user_nm
                    , Outreach_type as cl_call_log_type_desc
                    , 'Dynamo' as Source
            FROM Duals_Prod.CM.Member_Information_Outreaches_COE a 
                join Duals_Prod.CM.Member_Information_VA m on a.Member_ID = m.carriermemid and cast(a.[note_create_date] as date) between m.adj_rate_eff and m.adj_rate_term
                join (select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) from Planreport_QNXT_VA.dbo.membercmshic) cms  
                    on m.memid = cms.memid and crn = 1
            WHERE UniqueGroup='Non Letter'
                    and cast(NOTE_CREATE_DATE as date) >= ?
                    and cast(NOTE_CREATE_DATE as date) <= ?    
                """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[call_log_date1, call_log_date2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[call_log_date1, call_log_date2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

# pull Risk strat QNxt
def pull_risk_strat_va():
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    query =  """SELECT distinct  carriermemid
                    , Report_Stratification 
                    , Stratification
                    , Stratification_Date
                FROM [Duals_Prod].[CM].[Member_Information_VA]
                ORDER BY carriermemid, Stratification_Date desc
    """
    temp_df = pd.read_sql_query(query, conn)
    temp_df.drop_duplicates('carriermemid', keep='first', inplace=True)
    df = temp_df[['carriermemid','Report_Stratification']].rename(columns={'carriermemid':'Member_ID','Report_Stratification':'Member_risk_Stratification'})
    df['Member_risk_Stratification']=df['Member_risk_Stratification'].map({'Low Risk':'Low Risk','Medium Risk':'Medium Risk','High Risk':'High Risk'})
    return df

# pull Risk strat QNxt
def pull_risk_strat_nj():
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    query =  """SELECT distinct carriermemid
                    , Report_Stratification 
                    , Stratification
                    , Stratification_Date
    from [Duals_Prod].[CM].[Member_Information_NJ]
    order by carriermemid, Stratification_Date desc
    """
    temp_df = pd.read_sql_query(query, conn)
    temp_df.drop_duplicates('carriermemid', keep='first', inplace=True)
    df = temp_df[['carriermemid','Report_Stratification']].rename(columns={'carriermemid':'Member_ID','Report_Stratification':'Member_risk_Stratification'})
    df['Member_risk_Stratification']=df['Member_risk_Stratification'].map({'Low Risk':'Low Risk','Medium Risk':'Medium Risk','High Risk':'High Risk'})
    return df

# pull Risk strat QNxt
def pull_risk_strat_ca(addl_cols=[]):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_ca_connection()
    select_list = pull_risk_strat_ca_select_list
    if len(addl_cols) >= 1:
        select_list += addl_cols
    query =  """SELECT distinct """ + ', '.join(select_list) + """
    from CA_EAE_TEMP.REF.Member_Information
    order by carriermemid, Stratification_Date desc
    """
    temp_df = pd.read_sql_query(query, conn)
    temp_df.drop_duplicates('carriermemid', keep='first', inplace=True)
    df = temp_df[['carriermemid','Report_Stratification']].rename(columns={'carriermemid':'Member_ID','Report_Stratification':'Member_risk_Stratification'})
    df['Member_risk_Stratification']=df['Member_risk_Stratification'].map({'Low Risk':'Low Risk','Medium Risk':'Medium Risk','High Risk':'High Risk'})
    return df

def pull_call_log_ca(ca_create_date, addl_cols = []):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_ca_connection()
    select_list = pull_call_log_ca_select_list
    if len(addl_cols) > 1:
        select_list += addl_cols
    query=f'''
    SELECT distinct ''' + ', '.join(select_list) + '''
      FROM CA_EAE_TEMP.REF.Member_Information_Outreaches a
      join CA_EAE_TEMP.ref.Member_Information m on a.Member_ID = m.carriermemid
      join (select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) from planreport_QNXT_CA.dbo.membercmshic) cms  on m.memid = cms.memid and crn = 1
      where UniqueGroup='Non Letter'
      and cast(NOTE_CREATE_DATE as date) >= ?
    '''

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[ca_create_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[ca_create_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_letter_ca(ca_create_date, addl_cols=[]):
    sc = ServerConnection()
    conn, cursor = sc.qnxt_ca_connection()
    select_list = pull_letter_ca_select_list
    if len(addl_cols) > 1:
        select_list += addl_cols
    query=f'''
    SELECT distinct ''' + ', '.join(select_list) + '''
      FROM CA_EAE_TEMP.REF.Member_Information_Outreaches a
      join CA_EAE_TEMP.ref.Member_Information m on a.Member_ID = m.carriermemid
      join (select *, crn = ROW_NUMBER() over(partition by memid order by termdate desc, effdate desc) from planreport_QNXT_CA.dbo.membercmshic) cms  on m.memid = cms.memid and crn = 1
      where UniqueGroup='Letter'
      and cast(NOTE_CREATE_DATE as date) >= ?
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[ca_create_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[ca_create_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df


def pull_prefer_language_qnxt():
    sc = ServerConnection()
    conn, cursor = sc.amdprodcoe01_connection()

    query=f'''
    select MBI as MEDICARE_NUMBER, language
    from Duals_Prod.CM.[SNP_Prioritization_CurrentAndFutureMembership]
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

def pull_pbm_data_fide_fmc():
    from google.cloud import bigquery
    import subprocess

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    query = qnxtQueryList.pull_fide_fmc_pbm_sql

    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_fide_fmc_chronic_condition():
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()

    query="""
    select distinct carriermemid
        , ICM_Conditions_Dx_Of_Interest
        ,  'H6399' as contract_number
    from Duals_Prod.CM.Member_Information_NJ mi (nolock)
    where ICM_Conditions_Dx_Of_Interest like '%Alzheimer’s/Dementia%' or
    ICM_Conditions_Dx_Of_Interest like '%Asthma%' or
    ICM_Conditions_Dx_Of_Interest like '%Cardiovascular Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%Chronic Kidney Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%COPD%' or
    ICM_Conditions_Dx_Of_Interest like '%Coronary Artery Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%CVA/Stroke%' or
    ICM_Conditions_Dx_Of_Interest like '%Depressive Disorder(s)%' or
    ICM_Conditions_Dx_Of_Interest like '%End Stage Renal Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%Heart Failure%' or
    ICM_Conditions_Dx_Of_Interest like '%Serious Mental Illness (SMI)%' or
    ICM_Conditions_Dx_Of_Interest like '%Stroke%'
    union select distinct carriermemid
            , ICM_Conditions_Dx_Of_Interest
            , 'H1610' as contract_number
    from Duals_Prod.CM.Member_Information_VA mi (nolock)
    where ICM_Conditions_Dx_Of_Interest like '%Alzheimer’s/Dementia%' or
    ICM_Conditions_Dx_Of_Interest like '%Asthma%' or
    ICM_Conditions_Dx_Of_Interest like '%Cardiovascular Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%Chronic Kidney Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%COPD%' or
    ICM_Conditions_Dx_Of_Interest like '%Coronary Artery Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%CVA/Stroke%' or
    ICM_Conditions_Dx_Of_Interest like '%Depressive Disorder(s)%' or
    ICM_Conditions_Dx_Of_Interest like '%End Stage Renal Disease%' or
    ICM_Conditions_Dx_Of_Interest like '%Heart Failure%' or
    ICM_Conditions_Dx_Of_Interest like '%Serious Mental Illness (SMI)%' or
    ICM_Conditions_Dx_Of_Interest like '%Stroke%'"""

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_cm_data():
    import pyodbc
    import pandas as pd
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()
    query=f'''
    SELECT carriermemid
        ,[CM_Name]
        ,[CM_ID]
        ,[ManagerName]
        ,[ManagerID]
    FROM [Duals_Prod].[CM].[Member_Information_NJ] m
    union SELECT carriermemid
        ,[CM_Name]
        ,[CM_ID]
        ,[ManagerName]
        ,[ManagerID]
    FROM [Duals_Prod].[CM].[Member_Information_VA] m
    union SELECT carriermemid
        ,[CM_Name]
        ,[CM_ID]
        ,[ManagerName]
        ,[ManagerID]
    FROM [Duals_Prod].[CM].[Member_Information_CA] m
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_fide_cob_ach_diag_exclusions(cob_exclusions: tuple, poly_exclusions: tuple):
    import subprocess
    from google.cloud import bigquery

    query =  qnxtQueryList.pull_fide_cob_ach_diag_exclusions_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ArrayQueryParameter('cob_exclusions', 'STRING', cob_exclusions),
        bigquery.ArrayQueryParameter('poly_exclusions', 'STRING', poly_exclusions)]
    )

    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    return df.drop_duplicates()

def pull_qnxt_member_id_crosswalk():
    import pyodbc
    import pandas as pd
    sc = ServerConnection()
    conn, cursor = sc.amdprodcoe01_connection()
    query = """
    select distinct [carriermemid], [memid]
    from [Duals_Prod].[CM].[Member_Information_VA]
    union 
    select distinct [carriermemid], [memid]
    from [Duals_Prod].[CM].[Member_Information_NJ]
    union 
    select distinct [carriermemid], [memid]
    from [Duals_Prod].[CM].[Member_Information_CA]
    """

    tr = pd.read_sql_query(query, conn, chunksize=10000)
    crosswalk = pd.concat(tr, ignore_index=True)
    return crosswalk

def pull_fide_cob_ach_detail(reporting_year: str, contract_list: str):
    # example usage:
    # qnxtQueries.pull_fide_cob_ach_detail("2025", "('H6399','H1610')")
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection()
    query =  f""" SELECT mcd.SourceMemberID, 
                    LastName,
                    FirstName,
                    DateOfBirth,
                    ResidentialAddressLine1,
                    ResidentialCity,
                    ResidentialState,
                    ResidentialZip,
                    PhoneNumber,
                    CMSContractNumber, 
                    MeasureStatus,
                    ValueSetItem [Medication(s)],
                    DrugLabelName,
                    AdjudicatedGPICode,
                    NDCCode,
                    DaysSupplyCount,
                    RXClaimID,
                    PrescriberIDQualifyingCode,
                    UnitsDispensedQuantity,
                    DispProviderID,
                    PharmacyName,
                    PharmacyPhoneNumber,
                    PharmacyAddressLine1,
                    PharmacyAddressCityName,
                    PharmacyAddressStateCode,
                    PharmacyAddressZip5Code,
                    PrescriberID,
                    PrescriberFullName,
                    PrimaryPhoneNumber,
                    MeasureName
            FROM [StarsDataHubProd].[PartD].[VW_MeasureClaimDetail] mcd 
                inner join [StarsDataHubProd].[Member].[VW_PlanPerReportingYear] ppry
                    on mcd.starsmember_id = ppry.starsmember_id
                        and mcd.reportingyear = ppry.reportingyear
                inner join [StarsDataHubProd].[PartD].[VW_Measure] measure_detail
                    on mcd.YOYMeasureID =  measure_detail.YOYMeasureID
                left join [StarsDataHubProd].[Member].[VW_Demographics] demo
                    on mcd.StarsMember_ID = demo.StarsMember_ID
                left join [StarsDataHubProd].[Member].[VW_Phone] phone
                    on mcd.StarsMember_ID = phone.StarsMember_ID
            where ppry.reportingyear = {reporting_year} 
            and CMSContractNumber in {contract_list}"""
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_fide_mem_summary_poly_ach(reporting_year: str, contract_list: str):
    sc = ServerConnection()
    # sample usage:
    #   qnxtQueries.pull_fide_mem_summary_poly_ach("2025", "('H6399','H1610')")
    conn, cursor = sc.stars_data_hub_prod_connection()
    query =  f"""    SELECT MeasureName as Measure, 
                member_detail.StarsMember_ID,
                member_detail.SourceMemberID,
                LastName,
                FirstName,
                DATEDIFF(hour,DateOfBirth,GETDATE())/8766 Age,
                DateOfBirth,
                ResidentialAddressLine1,
                ResidentialCity,
                ResidentialState,
                ResidentialZip,
                PhoneNumber,
                AetnaDNCIND,
                Gender,
                demo.MemberStatus,
                ppry.CMSContractNumber,
                ppry.PBPID,
                ppry.ProductCode,
                ppry.SNPIndicator,
                --LICS_LEVEL,
                Denominator,
                Numerator,
                MeasureStatus,
                OverlapOccurring,
                UniqueDrugCount
            FROM [StarsDataHubProd].[PartD].[VW_PolyMemberDetail] member_detail
                inner join [StarsDataHubProd].[PartD].[VW_Measure] measure_detail
                    on member_detail.MeasureID =  measure_detail.MeasureID
                left join [StarsDataHubProd].[Member].[VW_Demographics] demo
                    on member_detail.StarsMember_ID = demo.StarsMember_ID
                left join [StarsDataHubProd].[Member].[VW_Phone] phone
                    on member_detail.StarsMember_ID = phone.StarsMember_ID
                left join [StarsDataHubProd].[Member].[VW_PlanPerReportingYear] ppry
                    on ppry.StarsMember_ID = member_detail.StarsMember_ID
                        and member_detail.ReportingYear = ppry.ReportingYear
                        and member_detail.CMSContractNumber = ppry.CMSContractNumber
                        and member_detail.pbpid = ppry.pbpid               
                        and demo.MemberStatus = ppry.MemberStatus
            where member_detail.reportingyear = {reporting_year} 
            and member_detail.CMSContractNumber in {contract_list}
            and CurrentRecord=1"""
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_fide_mem_summary_cob(reporting_year: str, contract_list: str):
    # example usage:
    # qnxtQueries.pull_fide_mem_summary_cob("2025", "('H6399','H1610')")
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection()
    query =  f"""    SELECT MeasureName as Measure, 
                member_detail.StarsMember_ID,
                member_detail.SourceMemberID,
                LastName,
                FirstName,
                DATEDIFF(hour,DateOfBirth,GETDATE())/8766 Age,
                DateOfBirth,
                ResidentialAddressLine1,
                ResidentialCity,
                ResidentialState,
                ResidentialZip,
                PhoneNumber,
                AetnaDNCIND,
                Gender,
                demo.MemberStatus,
                ppry.CMSContractNumber,
                ppry.PBPID,
                ppry.ProductCode,
                ppry.SNPIndicator,
                --LICS_LEVEL,
                Denominator,
                Numerator,
                MeasureStatus,
                OverlapOccurring
            FROM [StarsDataHubProd].[PartD].[VW_COBMemberDetail] member_detail
                inner join [StarsDataHubProd].[PartD].[VW_Measure] measure_detail
                    on member_detail.MeasureID =  measure_detail.MeasureID
                left join [StarsDataHubProd].[Member].[VW_Demographics] demo
                    on member_detail.StarsMember_ID = demo.StarsMember_ID
                left join [StarsDataHubProd].[Member].[VW_Phone] phone
                    on member_detail.StarsMember_ID = phone.StarsMember_ID
                left join [StarsDataHubProd].[Member].[VW_PlanPerReportingYear] ppry
                    on ppry.StarsMember_ID = member_detail.StarsMember_ID
                        and member_detail.ReportingYear = ppry.ReportingYear
                        and member_detail.CMSContractNumber = ppry.CMSContractNumber
                        and demo.MemberStatus = ppry.MemberStatus
                        and member_detail.pbpid = ppry.pbpid               
            where member_detail.reportingyear = {reporting_year} 
            and member_detail.CMSContractNumber in {contract_list}
            and CurrentRecord=1"""
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df


def pull_auth(two_days_ago, nine_days_ago):
    import pyodbc
    import pandas as pd

    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()

    q = f"""
    select distinct mi.carriermemid, [MMRHIC] medicare_number, a.received_dts auth_received_date
    from MDW_IODB.[Auth].[IODB_Authorization] a (nolock)
    join MDW_IODB.Enrollment.IODB_Member_Enrollment_Detail e (nolock) 
        on a.IODB_Member_Enrollment_Detail_Key = e.IODB_Member_Enrollment_Detail_Key and a.IODB_Plan_Key = e.IODB_Plan_Key
    join [Duals_Prod].[CM].[Member_Information_IL] mi on e.member_id = mi.memid
    join [Duals_Prod].[REF].[HIC_MBI_MemID_XW_COE] hic on mi.carriermemid = hic.CarrierMemId
    where e.IODB_Plan_Key = '35'
    and e.IODB_Carrier_Program_Key in ('23', '24')
    and a.auth_type_desc = 'BH Inpatient' 
    and status_desc not in ('Dismissed','Cancelled')
    and received_dts between {nine_days_ago} and {two_days_ago}
    """
    data = pd.read_sql_query(q, conn)
    return data

def pull_exclusions():
    import pyodbc
    import pandas as pd
    
    sc = ServerConnection()
    conn, cursor = sc.qnxt_connection()

    query = """ 
    SELECT ph.[SourceMemberID]
        ,eid.mbi
        ,[PhoneNumber]
        ,[Usage]
        ,[Type]
        ,[PhoneValidInd]
        ,[TCPAConsentInd]
        ,[AetnaDNCIND]
        ,[EnterpriseDNCIND]
        ,[AetnaIVRPermIND]
        ,[EnterpriseIVRPermIND]
        ,[AetnaVoicePermIND]
        ,[EnterpriseVoicePermInd]
        ,[BestDayToCall]
        ,[BestTimeToCall]
    FROM [StarsDataHubProd].[Member].[VW_Phone] phone inner join
    (SELECT distinct [SourceMemberID],[StarsMember_ID]
    FROM [StarsDataHubProd].[Member].[VW_PlanHistory] 
    where cmscontractnumber = 'H2506' and pbpid='001')  ph on phone.starsmember_id = ph.StarsMember_ID
    inner join StarsDataHubProd.member.VW_EnterpriseIDs eid on eid.StarsMember_ID = ph.StarsMember_ID and eid.SourceMemberID = ph.SourceMemberID
    where TCPAConsentInd = 'N' or AetnaDNCInd = 'Y' or EnterpriseDNCInd = 'Y' 
    """
    exclude = pd.read_sql(query, conn)
    return exclude