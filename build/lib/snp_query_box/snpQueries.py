import pandas as pd
import numpy as np
import pyodbc
import ibm_db as db
import ibm_db_dbi
from snp_query_box import snpQueryList

from tqdm import tqdm
from snp_query_box.select_lists import *
from snp_query_box.ServerConnection import ServerConnection
from snp_query_box import DsnpHelperFunction

import warnings
warnings.filterwarnings("ignore")

"""
This is to keep data pull queries as functions.
By keeping queries here we can avoid long jupyter notebooks and it is easy to maintain and debug it. 
"""

def pull_snp_member(enroll_start_date, enroll_end_date, addl_cols = [], driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_sales_connection(driver)
    
    select_list = pull_snp_member_select_list
    if len(addl_cols) >= 1:
        select_list = select_list + addl_cols

    query = """select distinct """ + ', '.join(select_list) + """
        from dbo.MEDICARE_ENROLLMENT 
        where PRODUCT='MA' 
        and ISSUED_STATUS = 'Issued' 
        and Group_Status='Individual'
        and Term_Date >= ?
        and Eff_Date <= ?
        """

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[enroll_start_date, enroll_end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[enroll_start_date, enroll_end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(dfs, ignore_index = True)
    return df

def pull_snp_member_bq(enroll_start_date, enroll_end_date):
    import subprocess
    from google.cloud import bigquery
    query =  snpQueryList.pull_snp_member_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    # Member_VW_PlanPerReportingYear reportingyear str -> int -> str again, check if the query fails
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "enroll_start_date",
            "DATE",
            enroll_start_date
            ),
            bigquery.ScalarQueryParameter(
            "enroll_end_date",
            "DATE",
            enroll_end_date
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

def pull_star_member(reporting_year1, reporting_year2, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """
    select *
    from [StarsDataHubProd].[Member].[VW_PlanPerReportingYear]
    where ReportingYear >= ?
    and ReportingYear <= ?
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year1, reporting_year2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year1, reporting_year2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_hedis(reporting_year1, reporting_year2, medicare_number_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """select
            member_plan.individualID,
            member_plan.StarsMEMBER_ID,
            member_hedis.SourceMemberID,
            member_hedis.ReportingYear,
            member_hedis.YOYMeasureID MeasureID,
            member_hedis.MeasureDescription,
            member_hedis.Domain,
            member_hedis.StarMeasure,
            member_hedis.DENOMINATOR,
            member_hedis.NUMERATOR,
            member_hedis.EpisodeDate,
            member_hedis.ComplianceDate,
            member_hedis.CurrGapInd,
            member_hedis.PriorGapInd,
            member_plan.CurrentRecord,
            member_hedis.CMSContractNumber,
            member_plan.MBI,
            member_plan.PBPID
        from [StarsDataHubProd].Member.[VW_AllMeasureDetail]  member_hedis
        inner join Member.VW_PlanPerReportingYear member_plan on member_hedis.StarsMEMBER_ID = member_plan.StarsMEMBER_ID 
        where member_hedis.ReportingYear = ?
        and member_plan.ReportingYear = ?
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year1, reporting_year2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year1, reporting_year2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if medicare_number_list!=None:
        df_selected = df_all[df_all["HICN_NBR"].str.strip().isin(medicare_number_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df


def pull_hedis_bq(reporting_year):
    import subprocess
    from google.cloud import bigquery
    # Member_VW_PlanPerReportingYear reporting year is string. it might change in the future
    query =  snpQueryList.pull_hedis_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    # Member_VW_PlanPerReportingYear reportingyear str -> int -> str again, check if the query fails
    reporting_year_str = str(reporting_year)
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "reporting_year",
            "INT64",
            reporting_year
            ),
            bigquery.ScalarQueryParameter(
            "reporting_year_str",
            "STRING",
            reporting_year_str
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
    df['EpisodeDate'] = pd.to_datetime(df['EpisodeDate'])	
    df['ComplianceDate'] = pd.to_datetime(df['ComplianceDate'])	
    return df.drop_duplicates()


def pull_ked_gap_status(reporting_year, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """SELECT SourceMemberID
            ,nomer.StarsMember_ID as StarsMemberID
            ,MBI
            ,MeasureID
            ,MeasureComponentID
            ,LogicalOperatorCode
            ,Description
            ,LatestTestDT
            ,SourceData
            ,ClaimID
            ,LatestLabResult
            ,LatestCriteriaDate
            ,EventDate
            ,ReportingYear
            ,InsertDate
            ,LastUpdated
            ,LastUpdateUser
            FROM [StarsDataHubProd].[HEDIS].[VW_NomerWeekly] nomer
            inner join [StarsDataHubProd].[Member].[VW_EnterpriseIDs] x on nomer.StarsMember_ID = x.StarsMember_ID
            where nomer.measureid = 600484
            and nomer.reportingyear = ?
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_star_measure(dashboard_year, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)

    query = """select *
            from [StarsModel].[VW_ForecastCutpoint]
            where CurrentRecord = 1
            and DashboardYear = ?
            and DomainName in ('HEDIS', 'PatientSafety')
            and CutpointLevel = 'Mid' 
            and ProductType = 'MAPD'
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[dashboard_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[dashboard_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_cahps(USERNAME, PASSWORD, run_year, sample=False):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)

    # This piece of code was taken from Ajay - 
    # it creates the standardized off-cycle CAHPS scores at member level including NPS, 
    # So we can use it to generate the dependent variable
    # This function needs refactoring work
    query = """
    select *
    --SRC_MEMBER_ID,QUESTION_ID,SHORT_QUESTION,ANSWER 
    from starstemp.cahps_view 
    where substr(question_id,1,1) = '1'
       and RUNYEAR=?
    """
    query_sample = """
    select *
    --SRC_MEMBER_ID,QUESTION_ID,SHORT_QUESTION,ANSWER 
    from starstemp.cahps_view 
    where substr(question_id,1,1) = '1'
       and RUNYEAR=?
    limit 100
    """

    if sample==True:
        row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query_sample),conn, params=[run_year]).iloc[0,0]
    else:
        row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[run_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        if sample==True:
            for chunk in pd.read_sql(query_sample, conn, params=[run_year], chunksize = 100):
                dfs.append(chunk)
                pbar.update(len(chunk))
        else:
            for chunk in pd.read_sql(query, conn, params=[run_year], chunksize = 1000):
                dfs.append(chunk)
                pbar.update(len(chunk))            
    df = pd.concat(dfs, ignore_index = True)
    return df

def pull_cahps_with_transform(USERNAME, PASSWORD, run_year):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)

    # This piece of code was taken from Ajay - 
    # it creates the standardized off-cycle CAHPS scores at member level including NPS, 
    # So we can use it to generate the dependent variable
    # This function needs refactoring work
    query = """
    select *
    --SRC_MEMBER_ID,QUESTION_ID,SHORT_QUESTION,ANSWER 
    from starstemp.cahps_view 
    where substr(question_id,1,1) = '1'
       and RUNYEAR=?
    """

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[run_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[run_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))            
    survey_data = pd.concat(dfs, ignore_index = True)

    table = pd.pivot_table(
        survey_data, 
        values ='ANSWER', 
        index =['SRC_MEMBER_ID', 'CMS_CONTRACT_NUMBER', 'PLAN_BENEFIT_PACKAGE', 'MEASURE_ID'], 
        columns =['QUESTION_ID'],aggfunc='first')
    pivoted_survey_data = pd.DataFrame(table.to_records())
    Measure_ID_List = ['GNC','GNC','GACQ','GACQ','GACQ','HPCS','HPCS','HPCS','GNP','GNP','GNP','CC','CC','CC','CC','CC','CC','Plan_R','Care_R','RX_R']
    Measure_Name_List = ['CARECOMP','CARECOMP','QUIKCOMP','QUIKCOMP','QUIKCOMP','SERVCOMP','SERVCOMP','SERVCOMP','RXCOMP','RXCOMP','RXCOMP','CORRCOMP','CORRCOMP','CORRCOMP','CORRCOMP','CORRCOMP','CORRCOMP','PLANRATE','CARERATE','RXRATE']
    Measure_Des_List = ['Getting Needed Care','Getting Needed Care','Getting Appointments and Care Quickly','Getting Appointments and Care Quickly','Getting Appointments and Care Quickly','Health Plan Customer Service','Health Plan Customer Service','Health Plan Customer Service','Getting Need Prescriptions','Getting Need Prescriptions','Getting Need Prescriptions','Care Coordination','Care Coordination','Care Coordination','Care Coordination','Care Coordination','Care Coordination','Health Plan Ratings','Health Care Ratings','Drug Plan Ratings']
    Measure_Question_List = ['111','105','100','102','103','113','114','115','118','119','120','106','107','108','109','110','112','116','104','117']
    Measure_QTYP_List = ['NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUA','NSUAH1','NSUAH2','NSUA','YYN','NSUA','TEN','TEN','TEN']                   
    Mapping_Questions = pd.DataFrame(list(zip(Measure_ID_List,Measure_Name_List,Measure_Des_List,Measure_Question_List,Measure_QTYP_List)),columns = ['MEASURE_ID','MEASURE_NAME','MEASURE','QUESTION_ID','Q_TYPE'])        
    valid_TEN_answers = ['0','1','2','3','4','5','6','7','8','9','10','NA']
    valid_NSUA_answers = ['1','2','3','4','NA']
    valid_YYN_answers = ['1','2','3','NA']
    for index, row in Mapping_Questions.iterrows():
        if row['Q_TYPE'] in ['NSUA','NSUAH1','NSUAH2']:
            pivoted_survey_data.loc[~pivoted_survey_data[row['QUESTION_ID']].isin(valid_NSUA_answers),row['QUESTION_ID']] = 'NA'
        elif row['Q_TYPE'] == 'TEN':
            pivoted_survey_data.loc[~pivoted_survey_data[row['QUESTION_ID']].isin(valid_TEN_answers),row['QUESTION_ID']] =  'NA'
        elif row['Q_TYPE'] == 'YYN':
              pivoted_survey_data.loc[~pivoted_survey_data[row['QUESTION_ID']].isin(valid_YYN_answers),row['QUESTION_ID']] = 'NA'
        #nathan added
        else:
            continue

    # Adding Denonminator Columns which will store no. of Denominators to be reduced
    Measure_Id = Mapping_Questions.MEASURE_ID.unique()
    for m in Measure_Id:
        col_name = m + '_Den'
        pivoted_survey_data[col_name] = 0
    ## Incrementing the MeasureName_Den where ever Answer = 'Skipped'
    for index, row in Mapping_Questions.iterrows():
        col_name = row['MEASURE_ID'] + '_Den'
        pivoted_survey_data.loc[(pivoted_survey_data[row['QUESTION_ID']].astype(str) == 'NA')& ~(row['Q_TYPE'] in ['NSUAH1','NSUAH2']),col_name] = pivoted_survey_data[col_name] + 1.0    
        pivoted_survey_data.loc[(pivoted_survey_data[row['QUESTION_ID']].astype(str) == 'NA')& (row['Q_TYPE'] in ['NSUAH1','NSUAH2']),col_name] = pivoted_survey_data[col_name] + 0.5
        pivoted_survey_data=pivoted_survey_data.replace('NA',0)
    Que = Mapping_Questions.QUESTION_ID.unique()
    
    for q in Que:
        pivoted_survey_data[q] = pivoted_survey_data[q].astype('float32')
    for col_name in Measure_Id:
        pivoted_survey_data[col_name] = 0.0
    for index, row in Mapping_Questions.iterrows():
        col_name = row['MEASURE_ID']
        if (row['Q_TYPE'] == 'NSUA'):
            pivoted_survey_data[col_name] = pivoted_survey_data[col_name] + (pivoted_survey_data[row['QUESTION_ID']])/4
        elif (row['Q_TYPE'] in ['NSUAH1','NSUAH2']):
            pivoted_survey_data[col_name] = pivoted_survey_data[col_name] + 0.5*(pivoted_survey_data[row['QUESTION_ID']])/4
        elif row['Q_TYPE'] == 'TEN':
            pivoted_survey_data[col_name] = pivoted_survey_data[col_name] + (pivoted_survey_data[row['QUESTION_ID']])/10
        elif row['Q_TYPE'] == 'YYN':
            pivoted_survey_data[col_name] = pivoted_survey_data[col_name] + (pivoted_survey_data[row['QUESTION_ID']])/3
        #nathan added
        else:
            continue
    Measure_Den = Mapping_Questions.copy()
    Measure_Den['Denominator'] = 1.0
    Measure_Den.loc[Measure_Den.Q_TYPE.isin(['NSUAH1','NSUAH2']), 'Denominator'] = 0.5
    Measure_Den = Measure_Den[['MEASURE_ID','MEASURE_NAME','MEASURE','Denominator']]
    Measure_Den = Measure_Den.groupby(['MEASURE_ID','MEASURE_NAME','MEASURE'],as_index=False).sum()
    for index, row in Measure_Den.iterrows():
        den_name = row['MEASURE_ID'] + '_Den'
        pivoted_survey_data[row['MEASURE_ID']] = (pivoted_survey_data[row['MEASURE_ID']])/(row['Denominator'] - pivoted_survey_data[den_name])
    for index, row in Measure_Den.iterrows():
        den_name = row['MEASURE_ID'] + '_Den'
        den = row['MEASURE_ID'] + '_Den_Final'
        pivoted_survey_data[den] = (row['Denominator'] - pivoted_survey_data[den_name] )
    measure_id_list = Mapping_Questions.MEASURE_ID.unique().tolist()
    list_of_columns = ['SRC_MEMBER_ID'] 
    for names in measure_id_list:
        col = names + '_Den_Final'
        list_of_columns +=  [col]
    list_of_columns = ['SRC_MEMBER_ID', 'CMS_CONTRACT_NUMBER', 'PLAN_BENEFIT_PACKAGE', 'MEASURE_ID'] + measure_id_list
    measures_df = pivoted_survey_data[list_of_columns]
    measures_df.set_index('SRC_MEMBER_ID',inplace=True)
    #Composites = measures_df[['GNC','GACQ','HPCS','GNP','CC']]
    for i in measures_df.columns:
            if i not in ['CMS_CONTRACT_NUMBER', 'PLAN_BENEFIT_PACKAGE']:
                measures_df[i] = measures_df[i].round(decimals=1)

    measures_df.loc[measures_df['RX_R'] >= 0.9 , 'RX_Rating_Class'] = 'Promoter'
    measures_df.loc[(measures_df['RX_R'] > 0.6) & (measures_df['RX_R'] < 0.9 ) , 'RX_Rating_Class'] = 'Passive'
    measures_df.loc[measures_df['RX_R'] <= 0.6 , 'RX_Rating_Class'] = 'Detractor'
    measures_df.loc[measures_df['Care_R'] >= 0.9 , 'CARE_Rating_Class'] = 'Promoter'
    measures_df.loc[(measures_df['Care_R'] > 0.6) & (measures_df['Care_R'] < 0.9 ) , 'CARE_Rating_Class'] = 'Passive'
    measures_df.loc[measures_df['Care_R'] <= 0.6 , 'CARE_Rating_Class'] = 'Detractor'
    measures_df.loc[measures_df['Plan_R'] >= 0.9 , 'PLAN_Rating_Class'] = 'Promoter'
    measures_df.loc[(measures_df['Plan_R'] > 0.6) & (measures_df['Plan_R'] < 0.9 ) , 'PLAN_Rating_Class'] = 'Passive'
    measures_df.loc[measures_df['Plan_R'] <= 0.6 , 'PLAN_Rating_Class'] = 'Detractor'
    measures_df.loc[measures_df['GNC'] >= 0.9 , 'GNC_Class'] = 'Promoter'
    measures_df.loc[(measures_df['GNC'] > 0.6) & (measures_df['GNC'] < 0.9 ) , 'GNC_Class'] = 'Passive'
    measures_df.loc[measures_df['GNC'] <= 0.6 , 'GNC_Class'] = 'Detractor'
    measures_df.loc[measures_df['GACQ'] >= 0.9 , 'GCQ_Class'] = 'Promoter'
    measures_df.loc[(measures_df['GACQ'] > 0.6) & (measures_df['GACQ'] < 0.9 ) , 'GCQ_Class'] = 'Passive'
    measures_df.loc[measures_df['GACQ'] <= 0.6 , 'GCQ_Class'] = 'Detractor'
    measures_df.loc[measures_df['HPCS'] >= 0.9 , 'CS_Class'] = 'Promoter'
    measures_df.loc[(measures_df['HPCS'] > 0.6) & (measures_df['HPCS'] < 0.9 ) , 'CS_Class'] = 'Passive'
    measures_df.loc[measures_df['HPCS'] <= 0.6 , 'CS_Class'] = 'Detractor'
    measures_df.loc[measures_df['CC'] >= 0.9 , 'CC_Class'] = 'Promoter'
    measures_df.loc[(measures_df['CC'] > 0.6) & (measures_df['CC'] < 0.9 ) , 'CC_Class'] = 'Passive'
    measures_df.loc[measures_df['CC'] <= 0.6 , 'CC_Class'] = 'Detractor'
    measures_df.loc[measures_df['GNP'] >= 0.9 , 'GNP_Class'] = 'Promoter'
    measures_df.loc[(measures_df['GNP'] > 0.6) & (measures_df['GNP'] < 0.9 ) , 'GNP_Class'] = 'Passive'
    measures_df.loc[measures_df['GNP'] <= 0.6 , 'GNP_Class'] = 'Detractor'
    measures_df =  measures_df.reset_index()
    return measures_df

def pull_measure_monthly_snapshot(reporting_year1, reporting_year2, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """
    select trend.*, starmeasure.MeasureName
    from HEDIS.vw_measuretrendingmonthly trend
    inner join (select distinct MeasureID, MeasureName
    from HEDIS.vw_measure
    where ReportingYear = ? and starMeasure = 1) starmeasure on starmeasure.MeasureID = trend.MeasureID
    where ReportingYear = ?
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year1, reporting_year2]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year1, reporting_year2], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)

    df_all["dsnp_member_id"] = df_all["SourceMemberID"].str.strip()
    df_all["MeasureID"] = df_all["MeasureID"].astype(str)
    df = df_all.drop_duplicates()
    return df


def pull_measure_monthly_snapshot_bq(reporting_year1, reporting_year2):
    import subprocess
    from google.cloud import bigquery

    query =  snpQueryList.pull_measure_monthly_snapshot_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "reporting_year1",
            "INT64",
            reporting_year1
            ),
            bigquery.ScalarQueryParameter(
            "reporting_year2",
            "INT64",
            reporting_year2
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
    df.drop(columns = ["ingest_time", "filename","process_id", "newreportingdt"], inplace=True)
    return df.drop_duplicates()


def pull_clinical_condition(USERNAME, PASSWORD, reporting_year):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)
    query =  """
        SELECT ds.*
        FROM
        (
        SELECT varchar(SRC_MEMBER_ID) SRC_MEMBER_ID,
        MAX(case when DISEASE_CD = 'SKC' then ct  else 0 end) as Skin_Cancer,
        MAX(case when DISEASE_CD = 'HEP' then ct  else 0 end) as Hepatitis,
        MAX(case when DISEASE_CD = 'CHD' then ct  else 0 end) as Congential_Heart_Disease,
        MAX(case when DISEASE_CD = 'LUC' then ct  else 0 end) as Lung_Cancer,
        MAX(case when DISEASE_CD = 'NEU' then ct  else 0 end) as Neurosis,
        MAX(case when DISEASE_CD = 'AUT' then ct  else 0 end) as Autism,
        MAX(case when DISEASE_CD = 'ANX' then ct  else 0 end) as Anxiety,
        MAX(case when DISEASE_CD = 'STC' then ct  else 0 end) as Stomach_Cancer,
        MAX(case when DISEASE_CD = 'CFS' then ct  else 0 end) as Chronic_Fatigue_Syndrome,
        MAX(case when DISEASE_CD = 'ALC' then ct  else 0 end) as Alcoholism,
        MAX(case when DISEASE_CD = 'PRC' then ct  else 0 end) as Prostate_Cancer,
        MAX(case when DISEASE_CD = 'PVD' then ct  else 0 end) as Peripheral_Artery_Disease,
        MAX(case when DISEASE_CD = 'RHA' then ct  else 0 end) as Rheumatoid_Arthritis,
        MAX(case when DISEASE_CD = 'CHF' then ct  else 0 end) as Heart_Failure,
        MAX(case when DISEASE_CD = 'CHO' then ct  else 0 end) as Cholelithiasis_or_Cholecystitis,
        MAX(case when DISEASE_CD = 'CBD' then ct  else 0 end) as Cerebrovascular_Disease,
        MAX(case when DISEASE_CD = 'HCG' then ct  else 0 end) as Hypercoaguable_Syndrome,
        MAX(case when DISEASE_CD = 'MSX' then ct  else 0 end) as Metabolic_Syndrome,
        MAX(case when DISEASE_CD = 'PNC' then ct  else 0 end) as Pancreatic_Cancer,
        MAX(case when DISEASE_CD = 'PUD' then ct  else 0 end) as Peptic_Ulcer_Disease,
        MAX(case when DISEASE_CD = 'EPL' then ct  else 0 end) as Epilepsy,
        MAX(case when DISEASE_CD = 'OST' then ct  else 0 end) as Osteoarthritis,
        MAX(case when DISEASE_CD = 'EDT' then ct  else 0 end) as Endometriosis,
        MAX(case when DISEASE_CD = 'LVB' then ct  else 0 end) as Low_Vision_and_Blindness,
        MAX(case when DISEASE_CD = 'PER' then ct  else 0 end) as Periodontal_Disease,
        MAX(case when DISEASE_CD = 'CAN' then ct  else 0 end) as Other_Cancer,
        MAX(case when DISEASE_CD = 'CVC' then ct  else 0 end) as Cervical_Cancer,
        MAX(case when DISEASE_CD = 'CAT' then ct  else 0 end) as Cataract,
        MAX(case when DISEASE_CD = 'ADD' then ct  else 0 end) as Attention_Deficit_Disorder,
        MAX(case when DISEASE_CD = 'IDA' then ct  else 0 end) as Iron_Deficiency_Anemia,
        MAX(case when DISEASE_CD = 'BIP' then ct  else 0 end) as Bipolar,
        MAX(case when DISEASE_CD = 'MLM' then ct  else 0 end) as Malignant_Melanoma,
        MAX(case when DISEASE_CD = 'MOH' then ct  else 0 end) as Migraine_and_Other_Headaches,
        MAX(case when DISEASE_CD = 'AST' then ct  else 0 end) as Asthma,
        MAX(case when DISEASE_CD = 'IHD' then ct  else 0 end) as Ischemic_Heart_Disease,
        MAX(case when DISEASE_CD = 'SUM' then ct  else 0 end) as Member_Summary,
        MAX(case when DISEASE_CD = 'COC' then ct  else 0 end) as Colorectal_Cancer,
        MAX(case when DISEASE_CD = 'CRF' then ct  else 0 end) as Chronic_Renal_Failure,
        MAX(case when DISEASE_CD = 'SDO' then ct  else 0 end) as Substances_Related_Disorders,
        MAX(case when DISEASE_CD = 'BRC' then ct  else 0 end) as Breast_Cancer,
        MAX(case when DISEASE_CD = 'NGD' then ct  else 0 end) as Nonspecific_Gastritis_or_Dyspepsia,
        MAX(case when DISEASE_CD = 'EDO' then ct  else 0 end) as Eating_Disorders,
        MAX(case when DISEASE_CD = 'HDL' then ct  else 0 end) as HodgkinsDisease_or_Lymphoma,
        MAX(case when DISEASE_CD = 'DTD' then ct  else 0 end) as Diverticular_Disease,
        MAX(case when DISEASE_CD = 'PAR' then ct  else 0 end) as Parkinsons_Disease,
        MAX(case when DISEASE_CD = 'HEM' then ct  else 0 end) as Hemophilia_or_Congenital_Coagulopathies,
        MAX(case when DISEASE_CD = 'COP' then ct  else 0 end) as Chronic_Obstructive_Pulmonary_Disease,
        MAX(case when DISEASE_CD = 'CRO' then ct  else 0 end) as Inflammatory_Bowel_Disease,
        MAX(case when DISEASE_CD = 'ORC' then ct  else 0 end) as Oral_Cancer,
        MAX(case when DISEASE_CD = 'OVC' then ct  else 0 end) as Ovarian_Cancer,
        MAX(case when DISEASE_CD = 'FIB' then ct  else 0 end) as Fibromyalgia,
        MAX(case when DISEASE_CD = 'LBP' then ct  else 0 end) as Low_Back_Pain,
        MAX(case when DISEASE_CD = 'ESC' then ct  else 0 end) as Esophageal_Cancer,
        MAX(case when DISEASE_CD = 'LEU' then ct  else 0 end) as Leukemia_or_Myeloma,
        MAX(case when DISEASE_CD = 'HNC' then ct  else 0 end) as Head_or_NeckCancer,
        MAX(case when DISEASE_CD = 'DNS' then ct  else 0 end) as Downs_Syndrome,
        MAX(case when DISEASE_CD = 'VNA' then ct  else 0 end) as Ventricular_Arrhythmia,
        MAX(case when DISEASE_CD = 'MNP' then ct  else 0 end) as Menopause,
        MAX(case when DISEASE_CD = 'LYM' then ct  else 0 end) as Lyme_Disease,
        MAX(case when DISEASE_CD = 'LBW' then ct  else 0 end) as Maternal_Hist_of_Low_Birth_Weight_or_Preterm_Birth,
        MAX(case when DISEASE_CD = 'OBE' then ct  else 0 end) as Obesity,
        MAX(case when DISEASE_CD = 'GLC' then ct  else 0 end) as Glaucoma,
        MAX(case when DISEASE_CD = 'CTD' then ct  else 0 end) as Chronic_Thyroid_Disorders,
        MAX(case when DISEASE_CD = 'BNC' then ct  else 0 end) as Brain_Cancer,
        MAX(case when DISEASE_CD = 'BLC' then ct  else 0 end) as Bladder_Cancer,
        MAX(case when DISEASE_CD = 'SCA' then ct  else 0 end) as Sickle_Cell_Anemia,
        MAX(case when DISEASE_CD = 'MSS' then ct  else 0 end) as Multiple_Sclerosis,
        MAX(case when DISEASE_CD = 'ALG' then ct  else 0 end) as Allergy,
        MAX(case when DISEASE_CD = 'SLE' then ct  else 0 end) as Systemic_Lupus_Erythematosus,
        MAX(case when DISEASE_CD = 'PAN' then ct  else 0 end) as Pancreatitis,
        MAX(case when DISEASE_CD = 'PSY' then ct  else 0 end) as Psychoses,
        MAX(case when DISEASE_CD = 'CYS' then ct  else 0 end) as Cystic_Fibrosis,
        MAX(case when DISEASE_CD = 'ENC' then ct  else 0 end) as Endometrial_Cancer,
        MAX(case when DISEASE_CD = 'AFF' then ct  else 0 end) as Atrial_Fibrillation,
        MAX(case when DISEASE_CD = 'BPH' then ct  else 0 end) as Benign_Prostatic_Hypertrophy,
        MAX(case when DISEASE_CD = 'CDO' then ct  else 0 end) as ADHD_and_other_Childhood_Disruptive_Disorders,
        MAX(case when DISEASE_CD = 'KST' then ct  else 0 end) as Kidney_Stones,
        MAX(case when DISEASE_CD = 'OSP' then ct  else 0 end) as Osteoporosis,
        MAX(case when DISEASE_CD = 'HYC' then ct  else 0 end) as Hyperlipidemia,
        MAX(case when DISEASE_CD = 'OMD' then ct  else 0 end) as Otitis_Media,
        MAX(case when DISEASE_CD = 'PMC' then ct  else 0 end) as Psychiatric_Disorders_related_to_Med_Conditions,
        MAX(case when DISEASE_CD = 'DEP' then ct  else 0 end) as Depression,
        MAX(case when DISEASE_CD = 'FIF' then ct  else 0 end) as Female_Infertility,
        MAX(case when DISEASE_CD = 'HYP' then ct  else 0 end) as Hypertension,
        MAX(case when DISEASE_CD = 'PPD' then ct  else 0 end) as Post_Partum_BH_Disorder,
        MAX(case when DISEASE_CD = 'AID' then ct  else 0 end) as HIV_or_AIDS,
        MAX(case when DISEASE_CD = 'DEM' then ct  else 0 end) as Dementia,
        MAX(case when DISEASE_CD = 'DIA' then ct  else 0 end) as Diabetes_Mellitus
        FROM
        (
        SELECT a.*,varchar(s.SRC_MEMBER_ID) SRC_MEMBER_ID,1 as ct
        FROM HPD.INDIVIDUAL_DETAIL a
        INNER JOIN STARSTEMP.stars_indvdl_mbr_detail s 
        on a.INDIVIDUAL_ID = s.MEMBER_ID and s.REPORTING_YEAR = ?
        WHERE a.RECORD_TYPE_CD = '0'
        )
        GROUP BY INDIVIDUAL_ID,SRC_MEMBER_ID
        ) ds
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_clinical_condition_bq():
    import subprocess
    from google.cloud import bigquery

    query =  snpQueryList.pull_call_log_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()



def pull_ev_call(addl_cols = []):
    sc = ServerConnection()
    conn, cursor = sc.stars_sop_rpt_connection()
    select_list = pull_ev_call_select_list
    if len(addl_cols) >= 1:
        select_list = select_list + addl_cols
    query =  """
        select  """ + ', '.join(select_list) + """
        from [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
        left join [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
        on mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
        left join [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
        on mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id and casereason.sgk_lookup_col_id=37 
        left join [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
        on mfb.[individual_id] = i.[individual_id]
        where 1=1 and (mfb.focus_cd_id in (25) and MFB.CALL_SCRIPT_CD = 'DSNP') 
        and mfb.reporting_year in (year(getdate())-3,year(getdate())-2,year(getdate())-1, year(getdate()))
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

def pull_ev_call_current_year(addl_cols = []):
    sc = ServerConnection()
    conn, cursor = sc.stars_sop_rpt_connection()
    select_list = pull_ev_call_select_list
    if len(addl_cols) >= 1:
        select_list = select_list + addl_cols
    query =  """
        select  """ + ', '.join(select_list) + """
        from [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
        left join [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
        on mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
        left join [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
        on mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id and casereason.sgk_lookup_col_id=37 
        left join [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
        on mfb.[individual_id] = i.[individual_id]
        where 1=1 and (mfb.focus_cd_id in (25) and MFB.CALL_SCRIPT_CD = 'DSNP') 
        and mfb.reporting_year in (year(getdate()))
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

def pull_ev_call_sop(addl_cols=[]):
    sc = ServerConnection()
    conn, cursor = sc.stars_sop_rpt_connection()
    select_list = pull_ev_call_sop_select_list
    if len(addl_cols) >=1:
        select_list = select_list + addl_cols
    query =  """
        select  """ + ', '.join(select_list) + """
        from [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
        left join [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
        on mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
        left join [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
        on mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id and casereason.sgk_lookup_col_id=37 
        left join [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
        on mfb.[individual_id] = i.[individual_id]
        --where 1=1 and (mfb.focus_cd_id in (25) and MFB.CALL_SCRIPT_CD = 'DSNP') 
        where 1=1 and (MFB.CALL_SCRIPT_CD = 'DSNP')
        and mfb.reporting_year in (year(getdate())-1, year(getdate()))
        --and i.cms_cntrct_nbr='H1610'
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

def pull_med_cost(USERNAME, PASSWORD, start_date, end_date, reporting_year=2025):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)
    query = """
        select m360.SRC_MEMBER_ID,
        sum(cl.paid_amt) + sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Total_Med_Cost
        from starstemp.stars_analytics_mbr_360 m360
        inner join iwh.claim_line cl on m360.member_id=cl.member_id
        where m360.reporting_year=?
        and m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
        and cl.srv_start_dt between ? and ?
        and cl.business_ln_cd = 'ME'
        and cl.summarized_srv_ind='Y'
        and cl.clm_ln_status_cd='P'
        group by m360.src_member_id
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query), conn, params=[reporting_year, start_date, end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year, start_date, end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_dental_util(USERNAME, PASSWORD, start_date, end_date, reporting_year=2025):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)
    query = """
        select m360.SRC_MEMBER_ID,m360.CMS_CNTRCT_NBR,m360.PBP_ID,
        count (distinct case when (cl.SRV_BENEFIT_CD = 'DEN' and cl.file_id='03') then cl.src_clm_id else null end) as HMO_Dental_Rider,
        count (distinct case when (cl.file_id='27' and  cl.PRODUCT_LN_CD in ('20', '23', '24', '25', '44', '45', '46', '47', '48', '82', 'AD','AI')) then cl.src_clm_id else null end) as ACAS, 
        count (distinct case when (cl.REPORTING_CTG_CD in ('CH', 'CD', 'HI', 'UD') and cl.file_id='03') then cl.src_clm_id else null end) as HMO_Stand_Alone, 
        count (distinct case when cl.file_id='44' then cl.src_clm_id else null end) as HMO_Stand_Alone_rider_enct,
        count (distinct case when substr(cl.PRCDR_CD,1,1) in ('D') then cl.src_clm_id else null end) as Dental_procedure
        from starstemp.stars_analytics_mbr_360 m360
        inner join iwh.claim_line cl on m360.member_id=cl.member_id
        where m360.reporting_year=?
        and m360.GROUP_IND='IND'
        and m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
        and cl.srv_start_dt between ? and ?
        and cl.business_ln_cd = 'ME'
        and cl.summarized_srv_ind='Y'
        and cl.clm_ln_status_cd in ('P', 'R')
        group by m360.src_member_id, m360.CMS_CNTRCT_NBR, m360.PBP_ID
        """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query), conn, params=[reporting_year, start_date, end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year, start_date, end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df

def pull_prefer_language(medicare_number_list=None, addl_cols = [], driver = '{ODBC Driver 17 for SQL Server}'):
    '''QNXT members are there but language is null'''
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection(driver)

    select_list = [    
    'MEDICARE_NBR',
    'PREF_SPOKEN_LANG',
    'PREF_WRITTEN_LANG'
    ]
    
    if len(addl_cols) >= 1:
        select_list = select_list + addl_cols
    query =  """
    SELECT """ + ', '.join(select_list) + """
    from dm.MSBI_INDVDL_PROFILE_DTL
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if medicare_number_list!=None:
        df_selected = df_all[df_all["MEDICARE_NBR"].str.strip().isin(medicare_number_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df

def pull_member_phone_info(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query=f'''
    select 
    a.StarsMember_ID,
    b.SourceMemberID,
    PhoneNumber,
    PhoneValidInd,
    Priority,
    b.CurrentRecord,
    TCPAConsentInd,
    DNCInd,
    EnterpriseDNCInd,
    IVRPermInd,
    VoicePermInd,
    EnterpriseIVRPermInd,
    EnterpriseVoicePermInd,
    BestDayToCall,
    BestTimeToCall
    from [StarsDataHubProd].[Member].[Phone] a left join
    [StarsDataHubProd].[Member].[EnterpriseIDs] b
    on a.StarsMember_ID=b.StarsMember_ID
    where b.CurrentRecord=1
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

def pull_member_do_not_mail(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query=f'''
    select 
    a.StarsMember_ID,
    SourceMemberID,
    MBI,
    DirectMailPermInd
    from [StarsDataHubProd].[Member].[Address] a left join
    [StarsDataHubProd].[Member].[EnterpriseIDs] b
    on a.StarsMember_ID=b.StarsMember_ID
    where b.CurrentRecord=1
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

def pull_call_log_bq(min_call_date, max_call_date):
    import subprocess
    from google.cloud import bigquery

    query =  snpQueryList.pull_call_log_bq_sql
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

def pull_call_log_all_bq(min_call_date, max_call_date):
    import subprocess
    from google.cloud import bigquery

    query =  snpQueryList.pull_call_log_all_bq_sql
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


def pull_call_log_all_dsnp_bq(call_log_date1,call_log_date2):
    import subprocess
    from google.cloud import bigquery
    query = snpQueryList.pull_call_log_all_dsnp_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "call_log_date1",
            "DATE",
            call_log_date1
            ),
        bigquery.ScalarQueryParameter(
            "call_log_date2",
            'DATE',
            call_log_date2
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


def pull_vbc(reporting_year, mbr_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query=f'''
    SELECT distinct
    member_plan.StarsMEMBER_ID,
    member_plan.SourceMemberID,
    member_plan.CMSContractNumber, 
    member_plan.PBPID,
    a.PBGGroup,
    a.PBGGroupName, 
    a.TIN, 
    a.TINName,
    a.VBC, 
    a.ProviderID, 
    a.LabelName, 
    --a.par_ind, 
    --a.primary_ind,
    a.PBGSTATUS,
    a.PbgInd
    from Member.VW_PlanPerReportingYear member_plan
    left join (select distinct prov_att.StarsMEMBER_ID, prov_att.SourceMemberID,
                    prov_att.PBGGroup,
                    prov_att.PBGGroupName, prov_att.TIN, prov_att.TINName,
                    case when prov_att.PbgInd = 'Y' then 'VBC' else ' ' end as VBC, 
                    demo.ProviderID,
                    demo.LabelName,
                    demo.NPI, demo.Phone,
                    prov_att.PBGSTATUS, prov_att.PbgInd
                from Member.vw_ProviderAttribution prov_att
                left join Provider.vw_demographics demo on prov_att.ProviderID=demo.ProviderID
                where prov_att.ReportingYear = ?
                ) a on member_plan.StarsMEMBER_ID = a.StarsMEMBER_ID
    where member_plan.ReportingYear = ?
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year, reporting_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year, reporting_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if mbr_list!=None:
        df_selected = df_all[df_all["SourceMemberID"].str.strip().isin(mbr_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()

    df = df_all.drop_duplicates()
    return df

def pull_vbc_bq(reporting_year, mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_vbc_bq_sql

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "reporting_year",
            "STRING",
            reporting_year
            )
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)    
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df_all = query_job.to_dataframe() 

    if mbr_list!=None:
        df_selected = df_all[df_all["SourceMemberID"].str.strip().isin(mbr_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()

    return df

# pull Risk strat from ICP
def pull_risk_strat_from_icp(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection(driver)
    query ='''
    SELECT *
    FROM [StarsBIDataProd].[adm].[MSBI_Data_Pull_ICP_Risk_Strat_With_LOB]
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(dfs, ignore_index = True)
    df["CARS"] = df["MPR_RISK_STRAT_TYP_DESC"]
    df["system_risk_strat"] = np.where(df["MPR_PGM_STRTFCTN_TYPE_KEY_CD"]=='H', "High",
                                np.where(df["MPR_PGM_STRTFCTN_TYPE_KEY_CD"]=='M', "Medium",
                                np.where(df["MPR_PGM_STRTFCTN_TYPE_KEY_CD"]=='L', "Low", "Not assigned")
                                ))
    df["Member_ID"] = df["IDENTIFIER_VALUE_ID"]
    return df

def pull_risk_strat_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_risk_strat_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df

def pull_vbc_report_snp_members(enroll_start_date, enroll_end_date, addl_cols=[], driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection(driver)
    select_list = pull_vbc_report_snp_members_select_list
    if len(addl_cols) >= 1:
        select_list += addl_cols

    query=f''' 
    select distinct ''' + ', '.join(select_list) + '''
    from DM.MSBI_MEPR 
    where PRODUCT='MA' 
    and ISSUED_STATUS = 'Issued'  
    and Group_Status in ('Individual', 'Group')
    and (Eff_Date <= cast(? as datetime)) and (Term_Date >= cast(? as datetime))
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[enroll_start_date, enroll_end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[enroll_start_date, enroll_end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(dfs, ignore_index = True)
    return df

def pull_annual_wellness_visit_bq():
    import subprocess
    from google.cloud import bigquery
    query=snpQueryList.pull_annual_wellness_visit_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_race_language_bq():
    import subprocess
    from google.cloud import bigquery
    query=snpQueryList.pull_race_language_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_race(medicare_number_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    print("Warning: To use Race and Ethnicity data, you need approvals of StarsDataHubProd_RO, StarsDataHuProd-Unmask networkgroup and complete Medicare STARS Production Datahub (3743143775_WBT) attestation.")
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)

    query = """select Race_ID, mi.StarsMember_ID, MBI
    from Member.Individual mi
    left join Member.Demographic md 
    on mi.StarsMember_ID = md.StarsMember_ID
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if medicare_number_list!=None:
        df_selected = df_all[df_all["MBI"].str.strip().isin(medicare_number_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df

def pull_race_language(medicare_number_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    print("Warning: To use Race and Ethnicity data, you need approvals of StarsDataHubProd_RO, StarsDataHuProd-Unmask networkgroup and complete Medicare STARS Production Datahub (3743143775_WBT) attestation.")

    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    
    query ='''
    select * from member.VW_GetMemberRaceLanguage'''

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if medicare_number_list!=None:
        df_selected = df_all[df_all["MBI"].str.strip().isin(medicare_number_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df

def pull_scores_from_medcompass_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_scores_from_medcompass_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()


def pull_provider_demo_info(driver = '{ODBC Driver 17 for SQL Server}'):
    """pull provider info like address, facility name
      use provider_id to join claim paid_prvdr_id(claim_line)"""
    
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """
    select ProviderID, LabelName as FacilityName, SpecialtyCD, AddressLine1, City, County, State, Zip
    from provider.vw_demographics
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

def pull_cm_info(reporting_year=2025, driver = '{ODBC Driver 17 for SQL Server}'):
    
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """
    select     
    b.CaseManagerFirstName,
    b.CaseManagerLastName,
    b.ProgramStatus, 
    b.ProgramClosureReason, 
    b.EnrollmentDate, 
    b.ClosureDate, 
    b.isOpenCM,
    a.StarsMember_ID,
    a.MBI,
    a.SourceMemberID,
    a.CMSContractNumber,
    a.PBPID,
    a.EffectiveDate,
    a.TermDate
   from Member.VW_PlanPerReportingYear a
   left join Member.clinicalprogram b on a.StarsMEMBER_ID = b.StarsMEMBER_ID
   where reportingyear = ? 
   and ProgramStatus != 'Closed'
    """
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[reporting_year]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[reporting_year], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df = df_all.drop_duplicates()
    return df


def pull_med_adherence(driver = '{ODBC Driver 17 for SQL Server}'):
    
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = """
    select *
    FROM [PatientSafetyProd].[dbo].[VR_MCR_BI_MED_ADHERENCE_IN_PLAY]
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

def pull_med_adherence_detail_bq():
    import subprocess
    from google.cloud import bigquery
    query = snpQueryList.pull_med_adherence_detail_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig()

    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    
    date_columns = ["disp_dt", "initial_fill_dt", "late_to_refill", "next_fill_dt", "report_date"]

    for c in date_columns:
        df[c] = pd.to_datetime(df[c])

    return df.drop_duplicates()

def pull_med_adherence_detail_history_bq():
    import subprocess
    from google.cloud import bigquery
    query =  snpQueryList.pull_med_adherence_detail_history_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig()

    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    
    date_columns = ["disp_dt", "initial_fill_dt", "late_to_refill", "next_fill_dt", "report_date"]

    for c in date_columns:
        df[c] = pd.to_datetime(df[c])

    return df.drop_duplicates()

def pull_icp_case_note_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_icp_case_note_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_care_plan_date_bq():
    import subprocess
    from google.cloud import bigquery
    query = snpQueryList.pull_care_plan_date_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_member_ict_case_note_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_member_ict_case_note_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_member_program_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_member_program_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_prefer_language_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_prefer_language_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_member_phone_info_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_member_phone_info_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()

def pull_disability_bq(reporting_year=2025):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_disability_bq_sql
    
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "reporting_year",
            "INT64",
            reporting_year
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

def pull_mbr_contact_legal_bq():
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_mbr_contact_legal_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    return df.drop_duplicates()


# create GCP pull utilization query 
def pull_utilization_bq(start_date, end_date, mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_utilization_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    #old db  edp-prod-hcbstorage.edp_hcb_core_cnsv.CLAIM_LINE
    if mbr_list is None:
        mbr_list = []

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "DATE",
            start_date
            ),
            bigquery.ScalarQueryParameter(
            "end_date",
            "DATE",
            end_date
            ),
            bigquery.ArrayQueryParameter(
            "mbr_list", 
            'STRING',
            mbr_list)
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    df["dsnp_member_id"] = df["src_member_id"].str.strip()
    
    col_amt = [col for col in df.columns if col.endswith("paid_amt")]
    df[col_amt] = df[col_amt].astype(float).round(2)

    return df.drop_duplicates()

# create GCP pull GPI query 
def pull_rx_claims_line_gpi_bq(start_date, end_date,mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_rx_claims_line_gpi_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    if mbr_list is None:
        mbr_list = []

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "DATE",
            start_date
            ),
            bigquery.ScalarQueryParameter(
            "end_date",
            "DATE",
            end_date
            ),
            bigquery.ArrayQueryParameter(
            "mbr_list", 
            'STRING',
            mbr_list)
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


# create GCP pull GPI query 
def pull_rx_claims_line_gpi_bq(start_date, end_date, mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_rx_claims_line_gpi_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    if mbr_list is None:
        mbr_list = []
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "DATE",
            start_date
            ),
            bigquery.ScalarQueryParameter(
            "end_date",
            "DATE",
            end_date
            ),
            bigquery.ArrayQueryParameter(
            "mbr_list", 
            'STRING',
            mbr_list)
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


def pull_nextgen_calls(start_date,end_date, mbr_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query=f'''
    select SourceMemberID as Member_ID, count(CaseNumber) as NextGen_calls
    from calls.NextGenCalls
    where CONTACTMETHOD in ('Phone Inbound','Chat', 'Click to Call' , 'Secure Message') 
    and CASERECORDTYPE in ('Consumer_Business_Medicare',
    'Consumer_Business_Medicare_Privacy_Restriction',
    'Consumer_Business_Read_Only_Medicare','Read_Only_Medicare','Medicare_Privacy_Restriction','Medicare') 
    and (ParentCaseRecordType is null or ParentCaseRecordType = '')
    and CaseCreatedAt between ? and ?
    group by SourceMemberID
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn, params=[start_date, end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[start_date, end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    if mbr_list!=None:
        df_selected = df_all[df_all["SourceMemberID"].str.strip().isin(mbr_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()

    df = df_all.drop_duplicates()
    return df

def pull_disability_esrd_hospice_out_of_area(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query=f'''
    select distinct a.mbi as MEDICARE_NUMBER,
    case when a.hospice_ind>=1 then 1 else 0 end as hospice_ind,
    case when a.disability_ind>=1 then 1 else 0 end as disability_ind,
    case when a.esrd_ind>=1 then 1 else 0 end as esrd_ind,
    case when a.out_of_area_flag>=1 then 1 else 0 end as out_of_area_flag
    from (
    select distinct MBI,
    sum(case when rtrim(ltrim(B.HospiceIndicator))='1' then 1 else 0 end) as HOSPICE_IND,
    sum(case when rtrim(ltrim(B.DisabilityIndicator))='1' then 1 else 0 end) as DISABILITY_IND,
    sum(case when rtrim(ltrim(B.ESRDIndicator))='1' then 1 else 0 end) as ESRD_IND,
    sum(case when rtrim(ltrim(B.OutOfAreaFlag))='1' then 1 else 0 end) as OUT_OF_AREA_FLAG
    from [StarsDataHubProd].[Member].[VW_TRRMemberDetail] B
    where B.CurrentRecord=1
    group by MBI) a
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

# create GCP get IDs query 
def pull_enterpriseids_bq(mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_enterpriseids_bq_sql

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    if mbr_list is None:
        mbr_list = []
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
            bigquery.ArrayQueryParameter(
            "mbr_list", 
            'STRING',
            mbr_list)
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

def pull_fmc_duedate_bq(due_date_start_date = '2025-01-01'):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_fmc_duedate_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "due_date_start_date",
            "DATE",
            due_date_start_date
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

def pull_member_email_address_bq(mbr_list=None):
    import subprocess
    from google.cloud import bigquery

    query = snpQueryList.pull_member_email_address_bq_sql

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    if mbr_list is None:
        mbr_list = []
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
            bigquery.ArrayQueryParameter(
            "mbr_list", 
            'STRING',
            mbr_list)
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

def update_dsnp_contract_pbp_df_file(driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_sales_connection(driver)
    
    query = """select 
        EFF_Date, Member_ID, Contract_Number, PBP, CMS_State, MA_Region, Plan_Type, SNP
        from dbo.MEDICARE_ENROLLMENT 
        where PRODUCT='MA' 
        and ISSUED_STATUS = 'Issued' 
        and Group_Status='Individual'
        and SNP in ('D', 'I', 'C', 'F')
        """

    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query),conn).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df = pd.concat(dfs, ignore_index = True)
    df["EFF_Date"] = pd.to_datetime(df["EFF_Date"])
    df["Year_Eff"] = df["EFF_Date"].dt.strftime('%Y')

    cnt_agg = df.groupby(["Year_Eff", "Contract_Number", "PBP", "CMS_State", "MA_Region", "Plan_Type", "SNP"]).size().reset_index(name="cnt")

    cnt_agg["max_count_of_year_contract_pbp"] = cnt_agg.groupby(["Year_Eff", "Contract_Number", "PBP"])["cnt"].transform("max") == cnt_agg['cnt']
    dsnp_contract_pbp_df = cnt_agg.sort_values(by = ["Year_Eff", "Contract_Number", "PBP"])
    return dsnp_contract_pbp_df


def pull_ctm_bq(start_date, end_date):
    
    import subprocess
    from google.cloud import bigquery

    query=snpQueryList.pull_ctm_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "STRING",
            start_date
            ),
        bigquery.ScalarQueryParameter(
            "end_date",
            "STRING",
            end_date
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
    #handle dbdate issue
    date_columns = ["RECEIVED_DT", "RESOLUTION_DT"]

    for c in date_columns:
        df[c] = pd.to_datetime(df[c])
    return df.drop_duplicates()

def pull_grievance_bq(start_date, end_date):
    
    import subprocess
    from google.cloud import bigquery

    query=snpQueryList.pull_grievance_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "STRING",
            start_date
            ),
        bigquery.ScalarQueryParameter(
            "end_date",
            "STRING",
            end_date
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
    #handle dbdate issue
    date_columns = ["OCCUR_DTS", "REPORTED_DTS"]

    for c in date_columns:
        df[c] = pd.to_datetime(df[c])

    return df.drop_duplicates()

def pull_ip_visit_from_hedis(start_date, end_date, mbr_list=None, driver = '{ODBC Driver 17 for SQL Server}'):
    sc = ServerConnection()
    conn, cursor = sc.stars_data_hub_prod_connection(driver)
    query = '''
    select SourceMemberID,
    ReportingYear,
    CMSContractNumber,
    AdmitDate,
    DischargeDate,
    AdmitDischargeDescription,
    ClaimPrimaryDiagCode,
    ClaimDiagDescription,
    ProviderID,
    FacilityName,
    ReadmitDate,
    ReadmitDischargeDate,
    ReadmitClaimPrimaryDiagCode,
    ReadmitClaimDiagDescription,
    ReadmitFacility
    FROM [StarsDataHubProd].[HEDIS].[pcrmemberdetail]
    where AdmitDate between ? and ?
    '''
    row_count = pd.read_sql_query('select count(*) from ({}) subquery'.format(query), conn, params=[start_date, end_date]).iloc[0,0]
    dfs =[]
    with tqdm(total=row_count) as pbar:
        for chunk in pd.read_sql(query, conn, params=[start_date, end_date], chunksize = 1000):
            dfs.append(chunk)
            pbar.update(len(chunk))
    df_all = pd.concat(dfs, ignore_index = True)
    df_all["dsnp_member_id"] = df_all["SourceMemberID"].str.strip()
    df_all["ProviderID"] = df_all["ProviderID"].astype(str)
    if mbr_list!=None:
        df_selected = df_all[df_all["dsnp_member_id"].isin(mbr_list)]
        df = df_selected.drop_duplicates()
    else:
        df = df_all.drop_duplicates()
    return df

def pull_ip_visit_from_hedis_bq(start_date, end_date):
    
    import subprocess
    from google.cloud import bigquery

    query=snpQueryList.pull_ip_visit_from_hedis_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "start_date",
            "STRING",
            start_date
            ),
        bigquery.ScalarQueryParameter(
            "end_date",
            "STRING",
            end_date
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
    #handle dbdate issue
    date_columns = ["AdmitDate","DischargeDate","ReadmitDate","ReadmitDischargeDate"]

    for c in date_columns:
        df[c] = pd.to_datetime(df[c])
    return df.drop_duplicates()

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


def pull_er_visit(USERNAME, PASSWORD, start_date, end_date, reporting_year=2025, mbr_list=None):
    sc = ServerConnection()
    conn, cursor = sc.db2_connection(USERNAME, PASSWORD)
    query = '''
    select distinct m360.SRC_MEMBER_ID as SRC_MEMBER_ID,
    CMS_CNTRCT_NBR,
    PHONE_NBR,
    m360.PROVIDER_ID,
    GROUP,
    GROUP_NAME,
    PBG_IND,
    cast(cl.SRV_START_DT as varchar(10)) as SRV_START_DT, 
    cast(cl.SRV_STOP_DT as varchar(10)) as SRV_STOP_DT,
    prcdr_cd,
    pri_icd9_dx_cd as diag_code,
    dm.label_nm as facility_nm
    from starstemp.stars_analytics_mbr_360 m360
    inner join iwh.claim_line cl on m360.member_id = cl.member_id
    left join iwh.provider_dm dm on cl.paid_prvdr_id = dm.provider_id	 
    where m360.reporting_year='''+str(reporting_year)+'''
    and m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
    and cl.srv_start_dt between ? and ?
    and cl.business_ln_cd = 'ME'
    and cl.summarized_srv_ind='Y'
    and cl.clm_ln_status_cd='P'
    and (cl.hcfa_plc_srv_cd = '23'
    and (cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') 
    or (cl.REVENUE_CD in ('450', '451', '452', '456','459'))))
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

    df.columns = [col.lower() for col in df.columns]

    df['srv_start_dt']=pd.to_datetime(df['srv_start_dt'], errors='coerce')
    df['srv_stop_dt']=pd.to_datetime(df['srv_stop_dt'], errors='coerce')
    df["diagnosis_code"] = df["diag_code"].str.replace(".", "").str.strip()
    diag_code_df = DsnpHelperFunction.get_diagnosis_code_lookup()
    er_visit_df = df.merge(diag_code_df[["diagnosis_code", "diagnosis_code_short_description"]], how="left", on="diagnosis_code")
    er_visit_df = er_visit_df.drop_duplicates()
    return er_visit_df

def pull_er_visit_bq(enroll_start_date, enroll_end_date, start_date, end_date):
    
    import subprocess
    from google.cloud import bigquery

    query=snpQueryList.pull_er_visit_bq_sql
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "enroll_start_date",
            "DATE",
            enroll_start_date
            ),
            bigquery.ScalarQueryParameter(
            "enroll_end_date",
            "DATE",
            enroll_end_date
            ),
        bigquery.ScalarQueryParameter(
            "start_date",
            "DATE",
            start_date
            ),
        bigquery.ScalarQueryParameter(
            "end_date",
            "DATE",
            end_date
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
    df["dsnp_member_id"] = df["Member_ID"].str.strip()
    #handle dbdate issue
    df['SRV_START_DT']=pd.to_datetime(df['SRV_START_DT'], errors='coerce')
    df['SRV_STOP_DT']=pd.to_datetime(df['SRV_STOP_DT'], errors='coerce')
    df["diagnosis_code"] = df["diag_code"].str.replace(".", "").str.strip()
    diag_code_df = DsnpHelperFunction.get_diagnosis_code_lookup()
    er_visit_df = df.merge(diag_code_df[["diagnosis_code", "diagnosis_code_short_description"]], how="left", on="diagnosis_code")
    er_visit_df = er_visit_df.drop_duplicates()
    return er_visit_df



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


def pull_healthhub(driver = '{ODBC Driver 17 for SQL Server}'):
    
    sc = ServerConnection()
    conn, cursor = sc.stars_bi_data_prod_connection(driver)

    query = '''
    select STORE_ID, PHARMACY_NM, ADDRESS_LN_1_TXT, CITY_NM, 
    STATE_POSTAL_CD, ZIP_CD, HEALTHHUB_IND, LAT_NBR, LONG_NBR,
    FCLTY_TYP_DESC, STORE_LCTN_TYP_DESC, IN_STORE_PHMCY_IND,
    DRV_THRU_IND
    from dm.MSBI_CVS_LOCATION_DTL
    where HEALTHHUB_IND='Y'
    '''
    raw_df =  pd.read_sql(query, conn)
    df = raw_df.drop_duplicates()
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

def pull_residence_type_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery
    
    query = snpQueryList.pull_residence_type_bq_sql

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    if mbr_id_list is not None:
        return df[df["identifiervalue"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()