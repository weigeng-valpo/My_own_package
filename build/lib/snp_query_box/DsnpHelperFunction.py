import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from scipy.sparse.csgraph import connected_components
from collections.abc import MutableMapping
import pkg_resources


"""
This is to keep commonly used functions. I found many similar codes are here and there but a little bit different.
This can be a possible risk. 

Using these functions can help 
1. Consistency between reports
2. QA and debugging
3. Code reusability


How to use:

import sys
sys.path.append('/path/to/the_module') 
import DsnpHelperFunction
"""
def _get_data_path():
    data_path = pkg_resources.resource_filename('snp_query_box', 'data')
    return data_path

def get_dsnp_contract_pbp_state_mapping_file():
    base_path = _get_data_path()
    csv_path = f'{base_path}/dsnp_contract_pbp_state_mapping.csv'
    df =  pd.read_csv(csv_path, dtype=str)
    return df

def get_dispatch_health_service_area_zip_code_file():
    base_path = _get_data_path()
    csv_path = f'{base_path}/dispatch_health_service_area_zip_code.csv'
    df =  pd.read_csv(csv_path, dtype=str)
    print("dispatch_health_service_area_zip is numeric value, please make it string and zero pad before use e.g. df['dispatch_health_service_area_zip'].astype(str).str.zfill(5)")
    return df

def first_date_of_month(any_date, output_type:str = "datetime"):
    """
    Return first date of the month from the given date
    input can be datetime format or string like 2023-03-22
    
    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    res = any_date.replace(day=1)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res

def last_date_of_month(any_date, output_type:str = "datetime"):
    """
    Return late date of the month from the given date
    input can be datetime format or string like 2023-03-22

    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    next_month = any_date.replace(day=28) + dt.timedelta(days=4)
    res = next_month - dt.timedelta(days=next_month.day)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res

def first_date_of_last_month(any_date, output_type:str = "datetime"):
    """
    Return first date of the month from the given date
    input can be datetime format or string like 2023-03-22
    
    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    first_date_of_month = any_date.replace(day=1)
    res = first_date_of_month - relativedelta(months=1)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res

def last_date_of_last_month(any_date, output_type:str = "datetime"):
    """
    Return late date of the month from the given date
    input can be datetime format or string like 2023-03-22

    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    first_date_of_month = any_date.replace(day=1)
    res = first_date_of_month - dt.timedelta(days=1)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res


def first_date_of_year(any_date, output_type:str = "datetime"):
    """
    Return late date of the year from the given date
    input can be datetime format or string like 2023-03-22

    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    res = any_date.replace(month=1, day=1)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res


def last_date_of_year(any_date, output_type:str = "datetime"):
    """
    Return late date of the year from the given date
    input can be datetime format or string like 2023-03-22
    
    :param any_date: datetime or string
    :param output_type: if string return string like "2023-03-22" else default is datetime 
    """
    if isinstance(any_date,str):
        any_date = dt.datetime.strptime(any_date, "%Y-%m-%d").date()
    res = any_date.replace(month=12, day=31)
    return res.strftime("%Y-%m-%d") if output_type=="string" else res


def set_format_date_columns(df, column_list: list, date_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    Return new dataframe with the same date type format columns

    :param df: pandas dataframe
    :param column_list: date column list, user want to change
    :param date_format: date_format, user want to change, default is '%Y-%m-%d %H:%M:%S'
    """
    df = df.copy()

    for l in column_list:
        df[l] = pd.to_datetime(df[l], errors='coerce')
        df[l] = df[l].dt.strftime(date_format)
        df[l] = pd.to_datetime(df[l])
    return df

def set_format_str_columns(df, column_list=None):
    """
    Return new dataframe with the same date type format columns

    :param df: pandas dataframe
    :param column_list: str column list, user want to change if not given, it will find most common string columns and change them to string columns
    """
    list_of_str_col = ["src_member_id", "member_id", "pbp", "medicare_number", "pbp_id", "measure_id"]
    if column_list==None:
        column_list = list_of_str_col
    else:
        column_list = [i.lower() for i in column_list]

    df = df.copy()
    for c in df.columns.to_list():
        if c.lower() in column_list:
            df[c] = df[c].astype(str)
            df[c] = df[c].str.strip()
        else:
            continue
    return df

def clean_column_names(df, condition: str = "title"):
    """
    Return a dataframe with the capitalized column names

    :param df: pandas dataframe
    :param condition: title, lower, upper
        - title - Column_Name
        - lower - column_name
        - upper - COLUMN_NAME

    """
    if condition == "lower":
        df.columns = df.columns.str.lower()
    elif condition == "upper":
        df.columns = df.columns.str.upper()
    else:       
        df.columns = df.columns.str.title()
    return df

def add_suffix_columns(df, col_list: list, str_to_add: str = "suffix"):
    """
    Return a dataframe with the additional columns with suffix

    :param df: pandas dataframe
    :param col_list: list of column names to add suffix
    :param str_to_add: string, user want
    
    """
    new_cols = {f'{c}_{str_to_add}': df[c] for c in col_list}
    new_df = pd.DataFrame(new_cols)
    res_df = pd.concat([df, new_df], axis=1)
    return res_df

def add_prefix_columns(df, col_list: list, str_to_add: str = "prefix"):
    """
    Return a dataframe with the additional columns with prefix

    :param df: pandas dataframe
    :param col_list: list of column names to add prefix
    :param str_to_add: string, user want
    
    """
    new_cols = {f'{str_to_add}_{c}': df[c] for c in col_list}
    new_df = pd.DataFrame(new_cols)
    res_df = pd.concat([df, new_df], axis=1)
    return res_df

def create_meta_df(df):
    """
    Return a meta dataframe with columns information.
    This table will be used for QA

    :param df: pandas dataframe
    """
    meta_df = pd.DataFrame(columns=['column', 'min', 'max', 'null-count'])
    for col in df.columns:
        try:
            col_min = df[col].min()
            col_max = df[col].max()
            col_null_count = df[col].isna().sum()
        except:
            col_min = df[col].astype(str).min()
            col_max = df[col].astype(str).max()
            col_null_count = (df[col].str.strip().str.len()==0).sum() + (df[col].str.strip().str.lower()=='none').sum() + (df[col].isnull()).sum()

        meta_df = pd.concat([meta_df, 
        pd.DataFrame({'column':[col], 'min':[col_min], 'max':[col_max], 'null-count': [col_null_count]})], ignore_index=True)

    meta_df['total_count'] = len(df)
    meta_df['meta_process_date'] = dt.datetime.now()

    return meta_df


def collapse_service_date_visit(raw_df, start_date = 'SRV_START_DT', end_date = 'SRV_STOP_DT'):

    def _collapse_continuous_date(df, d, start_date = start_date, end_date = end_date):
        df.sort_values(by=['SRC_MEMBER_ID', start_date, 'CMS_CNTRCT_NBR'], inplace=True)
        df['prev_end_date'] = df.groupby(['SRC_MEMBER_ID', 'CMS_CNTRCT_NBR'])[end_date].shift(1)
    
        def _collapse_group(row):
            if (row[start_date] - row['prev_end_date']).days <= 1:
                _collapse_group.group_num += 0
            else:
                _collapse_group.group_num += 1
    
            return _collapse_group.group_num
        
        _collapse_group.group_num = 0
    
        df['group'] = df.apply(_collapse_group, axis = 1)
    
        result = df.groupby(['SRC_MEMBER_ID', 'CMS_CNTRCT_NBR', 'group'], as_index= False)\
            .agg(d)
        return result

    def _collapse_intervals(intervals):
        intervals.sort(key=lambda x: x[0])
        collapsed = []
        for start, end in intervals:
            if not collapsed or collapsed[-1][1] < start:
                collapsed.append([start, end])
            else:
                collapsed[-1][1] = max(collapsed[-1][1], end)
        return collapsed
    
    d = dict.fromkeys(("PHONE_NBR", "PROVIDER_ID", "GROUP", "GROUP_NAME"), 'first')
    d[start_date] = 'min'
    d[end_date] = 'max'

    #Let's handle continuous date 
    continuous_date_collapsed_df = _collapse_continuous_date(raw_df, d)

    df = continuous_date_collapsed_df[["SRC_MEMBER_ID", start_date, end_date, "CMS_CNTRCT_NBR", "PHONE_NBR", "PROVIDER_ID", "GROUP", "GROUP_NAME"]]

    df = df.sort_values(by=["SRC_MEMBER_ID", start_date]).reset_index(drop=True)

    collapsed_records = []
    current_id = df["SRC_MEMBER_ID"].iloc[0]
    current_intervals = [(df[start_date].iloc[0], df[end_date].iloc[0])]
    for i in range(1, len(df)):
        if df['SRC_MEMBER_ID'].iloc[i] == current_id:
            current_intervals.append((df[start_date].iloc[i], df[end_date].iloc[i]))
        else:
            #when id change, add to the result
            collapsed_records.extend([(current_id, start, end) for start, end in _collapse_intervals(current_intervals)])
            current_id = df['SRC_MEMBER_ID'].iloc[i]
            current_intervals = [(df[start_date].iloc[i], df[end_date].iloc[i])]
    collapsed_records.extend([(current_id, start, end) for start, end in _collapse_intervals(current_intervals)])
    collapsed_df = pd.DataFrame(collapsed_records, columns = ['SRC_MEMBER_ID', start_date, end_date])

    result_df = collapsed_df.merge(df.drop(columns = "SRV_STOP_DT"), on = ["SRC_MEMBER_ID","SRV_START_DT"], how="left")
    result_df = result_df.drop_duplicates(keep = 'first')    
    return result_df

def collapse_enroll_date(df, d, start_date = 'EFF_Date', end_date = 'Term_Date'):
    df.sort_values(by=['MEDICARE_NUMBER', start_date, 'Contract_Number'], inplace=True)
    df['prev_end_date'] = df.groupby(['MEDICARE_NUMBER', 'Contract_Number'])[end_date].shift(1)

    def _collapse_group(row):
        if (row[start_date] - row['prev_end_date']).days == 1:
            _collapse_group.group_num += 0
        else:
            _collapse_group.group_num += 1

        return _collapse_group.group_num
    
    _collapse_group.group_num = 0

    df['group'] = df.apply(_collapse_group, axis = 1)
    result = df.groupby(['MEDICARE_NUMBER', 'Contract_Number', 'group'], as_index= False)\
        .agg(d)
    return result

def collapse_enroll_date_by_key(df, d, key = ['MEDICARE_NUMBER', 'Contract_Number'], start_date = 'EFF_Date', end_date = 'Term_Date'):
    df.sort_values(by= key + [start_date], inplace=True)
    df['prev_end_date'] = df.groupby(key)[end_date].shift(1)

    def _collapse_group(row):
        if (row[start_date] - row['prev_end_date']).days == 1:
            _collapse_group.group_num += 0
        else:
            _collapse_group.group_num += 1

        return _collapse_group.group_num
    
    _collapse_group.group_num = 0

    df['group'] = df.apply(_collapse_group, axis = 1)
    result = df.groupby(key + ['group'], as_index= False)\
        .agg(d)
    return result


def get_standardized_member_id_column(raw_df):
    '''This function is to avoid different member id column name.
    MEMBER_ID, member_id, Member_ID, Member_Id, SRC_MEMBER_ID, MEMBNO
    and Medicare_ID, MEDICARE_NUMBER, HICN_NBR, MEDICARE_NBR
    Let's unify the column name so we can avoid joining failure.
    '''
    df = raw_df.copy()
    column_list = [col.lower() for col in df.columns]

    member_id_columns = ["src_member_id", "membno", "mbr_identifier_value_id", "member_id"]
    
    medicare_number_column_list = ["medicare_id", "medicare_nbr", "medicare_number", "hicn_nbr"]

    member_id_found = set(member_id_columns).intersection(column_list)

    if "member_id_std" in column_list:
        print("member_id_std already exist") 
    else:
         #priortize src_member_id
        if member_id_columns[0] in column_list:
            for col in df.columns:
                if col.lower() == "src_member_id": 
                    df["member_id_std"] = df[col].astype(str).str.strip()
                    df["member_id_std"] = df["member_id_std"].str.replace('^MEN', '', regex=True)
                    df.drop(columns = col, inplace = True)
                    print(f"member_id_std created from {col}")
                    break
        elif "member_id_std" not in df.columns and member_id_found:
            for col in df.columns:
                if col.lower() in member_id_columns: 
                    df["member_id_std"] = df[col].astype(str).str.strip()
                    df["member_id_std"] = df["member_id_std"].str.replace('^MEN', '', regex=True)
                    df.drop(columns = col, inplace = True)
                    print(f"member_id_std created from {col}, you need to check the datasource of member_id")
                    break
        else:
            print("member id column is not found")
    
    #medicare number
    medicare_found = set(medicare_number_column_list).intersection(column_list)
    if "medicare_id_std" in column_list:
        print("medicare_id_std already exist") 
    else:
        if medicare_found:
            for col in df.columns:
                if col.lower() in medicare_number_column_list: 
                    df["medicare_id_std"] = df[col].astype(str).str.strip()
                    df.drop(columns = col, inplace = True)
                    print(f"medicare_id_std created from {col}")
                    break
        else:
            print("medicare id column is not found")

    return df

def get_snp_contract_list(year=2025, historic=True):
    base_path = _get_data_path()
    file_path = f'{base_path}/dsnp_contract_pbp_df.csv'
    df = pd.read_csv(file_path, dtype = {'PBP':str})
    if historic:
        filtered_df = df[df["Year_Eff"] <= year]
    else:
        filtered_df = df[df["Year_Eff"] == year]

    contract_list = filtered_df["Contract_Number"].unique().tolist()
    return contract_list

def get_snp_plan_list(year=2025, historic=True):
    base_path = _get_data_path()
    file_path = f'{base_path}/dsnp_contract_pbp_df.csv'
    df = pd.read_csv(file_path, dtype = {'PBP':str})
    df["contract_pbp"] = df["Contract_Number"] + "-" + df["PBP"]
    if historic:
        filtered_df = df[df["Year_Eff"] <= year]
    else:
        filtered_df = df[df["Year_Eff"] == year]

    contract_list = filtered_df["contract_pbp"].unique().tolist()
    return contract_list


def get_snp_plan_state_mapping_df(year=2025, historic=False):
    """A plan can have the multiple states, so in that case we pick the max counts state"""
    base_path = _get_data_path()
    file_path = f'{base_path}/dsnp_contract_pbp_df.csv'
    df = pd.read_csv(file_path, dtype = {'PBP':str})
    df["contract_pbp"] = df["Contract_Number"] + "-" + df["PBP"]
    df["max_count_of_year_contract_pbp"] = df["max_count_of_year_contract_pbp"].astype(bool)
    max_state_df = df[df["max_count_of_year_contract_pbp"]==True]

    if historic:
        filtered_df = max_state_df[max_state_df["Year_Eff"] <= year]
    else:
        filtered_df = max_state_df[max_state_df["Year_Eff"] == year]

    plan_state_map_df = filtered_df[["contract_pbp", "CMS_State"]].drop_duplicates()
    return plan_state_map_df

def get_hcpcs_lookup(search_string=None):
    base_path = _get_data_path()
    file_path = f'{base_path}/hcpcs_code_lookup.parquet'
    hcpcs_code_lookup = pd.read_parquet(file_path)
    if search_string is not None:
        search_string = search_string.lower()
        hcpcs_code_lookup = hcpcs_code_lookup.loc[hcpcs_code_lookup['hcpcs_description'].str.lower().str.find(search_string)>=0]

    return hcpcs_code_lookup

def get_revenue_code_lookup(search_string=None):
    base_path = _get_data_path()
    file_path = f'{base_path}/revenue_code_lookup.parquet'
    revenue_code_lookup = pd.read_parquet(file_path)
    if search_string is not None:
        search_string = search_string.lower()
        revenue_code_lookup = revenue_code_lookup.loc[revenue_code_lookup['revenue_code_description'].str.lower().str.find(search_string)>=0]

    return revenue_code_lookup

def get_diagnosis_code_lookup(search_string=None):
    '''
    Diagnosis codes are ICD-10-CM codes from here: https://www.cms.gov/medicare/coding-billing/icd-10-codes/2024-icd-10-cm
    They are updated annually from the 2024 Code Tables, Tabular and Index - UPDATED 02/01/2024 (ZIP) archive
    The file to use is icd10cm_order_2024.txt 
    Historically, the following script will clean them and put them in the format seen here:
     diags = pd.read_fwf(r'.\FY24-CMS-1785-F-Code-Descriptions\icd10cm_order_2024.txt', colspecs='infer', widths=None, infer_nrows=20000,header=None)
     diags.columns = ['order','diagnosis_code','billable','diagnosis_code_short_description','diagnosis_code_long_description']
     diags.drop(columns='order',inplace=True)
     diags['fld_len'] = diags['diagnosis_code'].str.len()
     diag_class = diags.loc[diags['fld_len']==3]
     diags['cat_match'] = diags['diagnosis_code'].str[0:3]
     diags=diags.merge(diag_class[['diagnosis_code','diagnosis_code_long_description']].rename(columns={'diagnosis_code':'category','diagnosis_code_long_description':'category_description'}), how='left',left_on='cat_match', right_on='category').drop(columns=['fld_len','cat_match'])
     diags.to_parquet(r'.\icd_10_diagnosis_codes.parquet')
    '''
    base_path = _get_data_path()
    file_path = f'{base_path}/icd_10_diagnosis_codes.parquet'
    diagnosis_code_lookup = pd.read_parquet(file_path)
    if search_string is not None:
        search_string = search_string.lower()
        diagnosis_code_lookup.loc[diagnosis_code_lookup['diagnosis_code_long_description'].str.lower().str.find(search_string)>=0]
    return diagnosis_code_lookup

def get_dsnp_market_crosswalk():
    base_path = _get_data_path()
    file_path = f'{base_path}/dsnp_market_crosswalk.csv'
    df = pd.read_csv(file_path, dtype = {'PBP':str})
    return df

# this file change frequently, should get db access
#def get_dsnp_cm_manager_map():
#    " Got 2024 mapping file is from Guzman, Eladia"
#    base_path = _get_data_path()
#    file_path = f'{base_path}/dsnp_market_benefit_grid_2024.csv'
#    df = pd.read_csv(file_path)
#    return df
#
def get_hedis_patient_safety_measure(year=2025):
    base_path = _get_data_path()
    file_path = f'{base_path}/hedis_patient_safety_measure_{year}.csv'
    df = pd.read_csv(file_path, dtype = {'MEASURE_ID':str})
    return df

def clean_merge(df1, df2, merge_on=['Member_ID']):
    overlap_element = list(set(df1.columns).intersection(df2.columns))
    col_to_drop = [x for x in overlap_element if x not in merge_on]
    print("Below is the columns that will be dropped from the left df")
    print(col_to_drop)
    df = df1.drop(columns = col_to_drop)
    df = df.merge(df2, how="left", on=merge_on)
    return df

def get_pairwise_count(df):
    """columns should have only 1 or 0, this function will get counts of pair, value equals one"""
    columns = df.columns
    agg_df = pd.DataFrame(index=columns, columns=columns)
    for i in range(len(columns)):
        for j in range(i, len(columns)):
            col1=columns[i]
            col2=columns[j]

            count_ones = ((df[col1]==1) & (df[col2]==1)).sum()
            agg_df.at[col1, col2] = count_ones
            if col1 != col2:
                agg_df.at[col2,col1] = count_ones
    return agg_df

def flatten_dict(dictionary, parent_key='', separator=''):
    # see: https://stackoverflow.com/a/6027615
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten_dict(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)
