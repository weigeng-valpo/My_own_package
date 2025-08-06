from datetime import date, timedelta
import datetime as datetime
from snp_query_box import DsnpHelperFunction
import numpy as np
import pandas as pd

def get_hra_flag(hra_with_broker_wo_iha, reporting_end_date):
    """use mbr_hra_2024_with_broker_without_iha file for actionable CM engagement"""
    df = hra_with_broker_wo_iha.copy()
    
    df['HRA_flag'] = np.where(\
        ((df['I_eligible'] == 1) & (df['I_completed'] == 1) & (df['R_eligible'] == 0)) | \
        ((df['R_eligible'] == 1) & (df['R_completed'] == 1) & (df['I_eligible'] == 0)) | \
        ((df['I_eligible'] == 1) & (df['I_completed'] == 1) & (df['R_eligible'] == 1) & (df['R_completed'] == 1)),
        'HRA Compliant', 'Undefined')
    
    df['HRA_flag'] = np.where(\
                            ((df['I_eligible'] == 1) & \
                             (df['I_completed'] == 0) & \
                             (df['R_eligible'] == 1) & \
                             (df['R_completed'] == 1))
                            ,'Missed Initial HRA, Reassessment Compliant', df['HRA_flag'])
    
    df['HRA_flag'] = np.where(\
                            (df['catch_up_hra_req'] == 1) & (df['long_gap_flag'] == 0)\
                            ,'HRA Completed After Due Date', df['HRA_flag'])
    
    df['HRA_flag'] = np.where(\
                              (df['catch_up_hra_req'] == 1) & (df['long_gap_flag'] == 1)\
                              ,'HRA Long Gap - Need 1 More HRA', df['HRA_flag'])
    
    df['HRA_flag'] = np.where(\
                              (df['catch_up_hra_req'] == 2) & (df['long_gap_flag'] == 1)\
                              ,'HRA Long Gap - Need 2 HRAs', df['HRA_flag'])
    
    df['HRA_flag'] = np.where(
                              (df['catch_up_hra_req'] == 2) & (df['long_gap_flag'] == 0)\
                              ,'HRA Past Due', df['HRA_flag'])
    
    df['HRA_flag'] = np.where(\
                          (
                            (df['R_duedate_max_alt1'] >= np.datetime64(reporting_end_date)) & \
                            (df['R_eligible'] == 1) & (df['R_completed'] == 0)\
                            ) | \
                          (
                            (df['I_duedate_max'] >= np.datetime64(reporting_end_date)) & \
                            (df['I_eligible'] == 1) & (df['I_completed'] == 0))\
                          ,'HRA Coming Due', df['HRA_flag'])
    
    df['HRA_flag'] = np.where((df['I_eligible'] == 0) & (df['R_eligible'] == 0), 'HRA Not Eligible', df['HRA_flag'])
    df['HRA_flag'] = np.where((df['Term_Date'] <= reporting_end_date) & (df['past_due_flag']==0), 'HRA Compliant', df['HRA_flag'])
    df['HRA_flag'] = np.where((df['Term_Date'] <= reporting_end_date) & (df['past_due_flag']==1), 'HRA Past Due - Member Termed', df['HRA_flag'])
    
    return df

def get_hra_flag_for_high_opp(df):
    """Simplify HRA Status for High opp dashboard
    we expect only 4"""
    from datetime import datetime, timedelta
    today = datetime.today()
    three_month_later = today + timedelta(days=90)
    df["HRA Status"] = np.where(df["HRA Status"].isin(['HRA Long Gap - Need 1 More HRA','HRA Long Gap - Need 2 HRAs']), 'HRA Long Gap',
                                np.where((df["HRA Status"].isin(['HRA Coming Due'])) & (df["HRA Due Date"] <= three_month_later), 'HRA Coming Due',
                                np.where((df["HRA Status"].isin(['HRA Coming Due'])) & (df["HRA Due Date"] > three_month_later), 'HRA Not Due',
                                np.where(df["HRA Status"].isin(['HRA Compliant', 'HRA Not Eligible', 'Missed Initial HRA, Reassessment Compliant']), 'HRA Not Due',                                                          
                                np.where(df["HRA Status"].isin(['HRA Completed After Due Date']), 'HRA Past Due',  df["HRA Status"])
                                )))
                            )
    return df