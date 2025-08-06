import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.ServerConnection import ServerConnection
from snp_query_box import hraQueryList

import warnings
warnings.filterwarnings("ignore")

""" member count by template version, record counts as 2024-04-22
asad_template_version_nbr	asad_section_nm	distinct_member_cnt
3	General Information	292211
3	Chronic Conditions	292211
3	Services and Support	292211
3	Functional Status	292211
3	BH/Depression Screening	292211
3	Utilization	292211
3	Health Status	292211
3	Health Care Metrics	292211
3	Environment and Safety	292211
3	Nutrition	292211

4	Advanced Directives	271919
4	General Information	271919
4	Pain	271919
4	Chronic Conditions	271919
4	Services and Support	271919
4	Functional Status	271919
4	Medication	271919
4	BH/Depression Screening	271919
4	Utilization	271919
4	Health Status	271919
4	Health Care Metrics	271919
4	Environment and Safety	271919

3	Medication History	86929
3	Lifestyle	86929
3	Employment and Education	86929
3	Additional Coverage	86929
3	Readiness to Change	86929

Ignore version 2, low count
2	General Information	194
2	Medication History	194
2	Services and Support	194
2	Lifestyle	194
2	Functional Status	194
2	BH/Depression Screening	194
2	Utilization	194
2	Employment and Education	194
2	Health Status	194
2	Health Care Metrics	194
2	Environment and Safety	194
2	Nutrition	194
2	Readiness to Change	194
2	Assessment Completion Date	90

"""


section_list_template_4 = [
    'Advanced Directives',
    'General Information',
    'Pain',
    'Chronic Conditions',
    'Services and Support',
    'Functional Status',
    'Medication',
    'BH/Depression Screening',
    'Utilization',
    'Health Status',
    'Health Care Metrics',
    'Environment and Safety']

def pull_hra_qa_by_section_bq(asad_section_nm="General Information", mbr_list=None):
    """
    Pull HRA questions and answers by section from medcompass 
    We pull template version 3 and 4 only
    """
    import subprocess
    from google.cloud import bigquery

    query = hraQueryList.pull_hra_qa_by_section_bq_sql

    job_config = bigquery.QueryJobConfig(
       query_parameters=[
           bigquery.ScalarQueryParameter(
               "asad_section_nm",
               "STRING",
               asad_section_nm
               )
           ]
       )

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)

    df = query_job.to_dataframe()
        
    #there is Data issue in BQ, ASCII, invalid UTF
    #total_rows = list(query_job.result())
    #col_names = [col.name for col in query_job.result().schema]
    #for name in col_names:
    #    print(name)
    #    globals()[name] = []
    #
    #for row in total_rows:
    #    for i in range(len(col_names)):
    #        globals()[col_names[i]].append(row[i])
    #df = pd.DataFrame({col_names[i]: globals()[col_names[i]] for i in range(len(col_names))})

    if mbr_list is not None:
        return df[df[["asm_id_value_desc"].str.strip().isin(mbr_list)]].drop_duplicates()
    else:
        return df.drop_duplicates()