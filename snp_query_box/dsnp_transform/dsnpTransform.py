from snp_query_box import DsnpHelperFunction
import numpy as np
import pandas as pd

def get_wide_hedis_ps(pull_hedis):
    ''' Use pull_hedis from snp_query_box
    return wide table with hedis patient safety demoninator and numerator and open gap counts
    '''
    #we want only star open gap
    hedis_star_only = pull_hedis[(pull_hedis["StarMeasure"]==1) & (pull_hedis["CurrentRecord"] == 1)]
    hedis_ps_star_df = hedis_star_only.groupby(["SourceMemberID", "MBI", "ReportingYear", "MeasureID", "MeasureDescription", "Domain"])\
    .agg({"DENOMINATOR":"max", "NUMERATOR":"max"}).reset_index()

    hedis_ps_measure_df = DsnpHelperFunction.get_hedis_patient_safety_measure(year=2025)
    hedis_ps_measure_df = hedis_ps_measure_df[['MEASURE_ID','MEASURE_NM']].drop_duplicates()
    hedis_ps_measure_df.rename(columns = {"MEASURE_ID": "MeasureID"}, inplace = True)
    hedis_ps_star_df["MeasureID"] = hedis_ps_star_df["MeasureID"].astype(str)
    hedis_ps_measure_df["MeasureID"] = hedis_ps_measure_df["MeasureID"].astype(str)
    hedis_ps_with_measure_nm_df = hedis_ps_star_df.merge(hedis_ps_measure_df, how="inner", on="MeasureID")


    #hedis_ps_star_df["MeasureDescription"] = hedis_ps_star_df["Domain"] + "_" + hedis_ps_star_df["MeasureID"].astype(str) \
    #                                                  + "_"+ hedis_ps_star_df["MeasureDescription"].str.replace(" ", "_")
    group_cols = ["SourceMemberID", "MBI", "ReportingYear", "MeasureID", "MEASURE_NM"]
    hedis_agg_df = hedis_ps_with_measure_nm_df.groupby(group_cols).agg({"DENOMINATOR":"sum", "NUMERATOR":"sum"}).reset_index()
    #get wide table
    #found some bad data in 2024 OCT run, different MBI for the same member_id, so dedup first
    hedis_agg_df_denom_dedup = hedis_agg_df[["SourceMemberID", "MEASURE_NM", "DENOMINATOR"]].drop_duplicates()
    denom_df = hedis_agg_df_denom_dedup.pivot(index="SourceMemberID", columns='MEASURE_NM', values='DENOMINATOR')

    hedis_agg_df_num_dedup = hedis_agg_df[["SourceMemberID", "MEASURE_NM", "NUMERATOR"]].drop_duplicates()
    num_df = hedis_agg_df_num_dedup.pivot(index="SourceMemberID", columns='MEASURE_NM', values='NUMERATOR')
    denom_df.columns = [f'DENOMINATOR_{col}' for col in denom_df.columns]
    num_df.columns = [f'NUMERATOR_{col}' for col in num_df.columns]
    num_df = num_df.reset_index()
    denom_df = denom_df.reset_index()
    hedis_ps_wide_df = denom_df.merge(num_df, on="SourceMemberID", how="left")
    #count the open gaps
    hedis_ps_wide_df["total_denominator"] = hedis_ps_wide_df.filter(regex='^DENOMINATOR_').sum(axis=1)
    hedis_ps_wide_df["total_numerator"] = hedis_ps_wide_df.filter(regex='^NUMERATOR_').sum(axis=1)
    hedis_ps_wide_df["total_open_gap"] = hedis_ps_wide_df["total_denominator"] - hedis_ps_wide_df["total_numerator"]
    hedis_condition_cols = [c.replace("NUMERATOR_", "") for c in hedis_ps_wide_df.columns if c.startswith("NUMERATOR_")]
    for c in hedis_condition_cols:
        hedis_ps_wide_df["open_gap_"+c] = hedis_ps_wide_df["DENOMINATOR_" + c].astype(float) - hedis_ps_wide_df["NUMERATOR_" + c].astype(float)
        hedis_ps_wide_df["open_gap_"+c] = np.where(hedis_ps_wide_df["open_gap_"+c]==1, 'Y','N')

    return hedis_ps_wide_df

def get_utilization(pull_utilization, pull_ip_visit_from_hedis, pull_er_visit):
    '''Use pull_utilization, pull_ip_visit_from_hedis and pull_er_visit in snp_query_box'''

    # prepare ip visit, for IP_VISITS not using util_df directly since it needs collapse by dates
    ip_visit_dedup = pull_ip_visit_from_hedis[["dsnp_member_id","AdmitDate"]].drop_duplicates()

    ip_visit_final_df = ip_visit_dedup.groupby(["dsnp_member_id"]).size().reset_index(name="IP_VISITS")

    er_visit_dedup = pull_er_visit[["dsnp_member_id","srv_start_dt"]].drop_duplicates()
    er_visit_final_df = er_visit_dedup.groupby(["dsnp_member_id"]).size().reset_index(name="ER_VISITS")

    util_df = pull_utilization.merge(ip_visit_final_df, how='left', on='dsnp_member_id')
    util_df = util_df.merge(er_visit_final_df, how='left', on='dsnp_member_id')
    util_df["IP_VISITS"] = util_df["IP_VISITS"].fillna(0)
    util_df["ER_VISITS"] = util_df["ER_VISITS"].fillna(0)

    return util_df

def get_ked_gap_status(df):
    df["ked_status_desc"] = np.where(df["Description"]=="eGFR (blood test) only during the MY, no uACR during MY", "yes:eGFR no:uACR",
                                     np.where(df["Description"]=="No eGFR (blood test) and No uACR (urine test) during the MY", "no:eGFR no:uACR",
                                     np.where(df["Description"]=="uACR (urine test) only during the MY, no eGFR during MY", "no:eGFR yes:uACR",
                                     np.where(df["Description"]=="Urine Albumin and Urine Creatinine Test > 4 days apart", "urine test > 4 days apart",
                                              "")
                                     )))                                              
    ked_agg_df = df.groupby(["SourceMemberID", "StarsMemberID", "MBI", "MeasureID", "LogicalOperatorCode", "ReportingYear", "LastUpdated"])["ked_status_desc"]\
        .agg(lambda x: sorted(x)).reset_index()
    
    ked_agg_df.sort_values(by=["MBI", "LastUpdated"], inplace=True)
    ked_agg_df_most_recent = ked_agg_df.drop_duplicates(subset="MBI", keep='last')
    return ked_agg_df_most_recent


def get_med_adherence_status(df):
# import med adherence in-play records from the flat file
# soon this step will be converted to be pulled from a database
    med_adherence_df = DsnpHelperFunction.clean_column_names(df, condition="lower")
    med_adherence_df['hicn_nbr'] = med_adherence_df['hicn_nbr'].astype(str)
    med_adherence_df['member_id'] = med_adherence_df['member_id'].astype(str)
    med_adherence_df['member_id'] = med_adherence_df['member_id'].str[:12]

    #use snp_query_box
    med_adherence_df = DsnpHelperFunction.set_format_date_columns(med_adherence_df, column_list = ['diab_next_fill', 'statin_next_fill','rasa_next_fill'])

    med_adherence_df['next_rx_refill_date'] = med_adherence_df[['diab_next_fill', 'statin_next_fill','rasa_next_fill']].min(axis=1)
    med_adherence_df.rename(columns = {'member_id':'src_member_id'}, inplace=True)
    return med_adherence_df


def get_med_adherence_status_detail(pull_med_adherence_detail):
#use pull_med_adherence_detail_bq, which is weekly updated with detail info
    column_list = ["current_status", "current_days_missed", "current_days_covered", "current_days_in_meas_period",\
                   "current_pdc", "next_fill_dt", "disp_dt", "tot_add_days_can_miss", "tot_days_covered", "tot_days_needed",\
                    "tot_days_remaining", "unts_dispensed_qty","year_end_days_in_meas_period", "year_end_pdc",\
                    "adh_decile", "current_pdc", "short_nm", "pharm_name", "pharm_phone", "days_supply_cnt",
                    "srv_copay_amt"]
    dfs = []
    for c in column_list:
        temp_df = pull_med_adherence_detail.pivot_table(index='src_member_id', columns='measure',\
                                                     values = c, aggfunc='first')\
                                                     .reset_index()
        temp_df.columns = [f'{col}_{c}' if col !='src_member_id' else col for col in temp_df.columns]
        dfs.append(temp_df)

    med_adherence_status_detail_df = dfs[0]
    for df in dfs[1:]:
        med_adherence_status_detail_df = DsnpHelperFunction.clean_merge(med_adherence_status_detail_df, df, merge_on = "src_member_id")

    med_adherence_status_detail_df = DsnpHelperFunction.clean_column_names(med_adherence_status_detail_df, condition="upper")

    return med_adherence_status_detail_df

def prepare_icp_review_date(pull_care_plan_date_bq):
    careplan_df = pull_care_plan_date_bq.copy()
    careplan_df['cp_next_review_dts'] = pd.to_datetime(careplan_df['cp_next_review_dts'], errors='coerce')
    
    careplan_df['cp_effective_dts'] = pd.to_datetime(careplan_df['cp_effective_dts'], errors='coerce')
    careplan_df['cp_last_reviewer_dts'] = pd.to_datetime(careplan_df['cp_last_reviewer_dts'], errors='coerce')
    careplan_df['cp_current_reviewer_dts'] = pd.to_datetime(careplan_df['cp_current_reviewer_dts'], errors='coerce')

    #we want to use all digits since update time can be very close each other
    careplan_df['cp_updated_on_dts'] = pd.to_datetime(careplan_df['cp_updated_on_dts'], errors='coerce')

    # apply the filter to get only DSNP members as well as remove closed non ICP records

    careplan_df = careplan_df[careplan_df['cp_title_txt'].isin(['Individualized Care Plan', 'CSNP Individualized Care Plan'])]
    careplan_df = careplan_df[careplan_df['cp_program_type_desc'].isin(['DSNP','Unassigned','', 'CSNP'])]

    #TODO check this filter
    #get most recent careplan status by member id and careplan id
    most_recent_careplan_id_record = careplan_df.sort_values(["mbr_src_member_id", "cp_care_plan_id", "cp_updated_on_dts", "cp_care_plan_stts_type_desc"], ascending=[True, True, False, True])
    most_recent_careplan_id_record = most_recent_careplan_id_record.drop_duplicates(['mbr_src_member_id', 'cp_care_plan_id'], keep='first') #one record per member_id and careplan_id

    most_recent_careplan_status = most_recent_careplan_id_record[["mbr_src_member_id", "cp_care_plan_id", "cp_care_plan_stts_type_desc"]]
    most_recent_careplan_status.rename(columns = {"cp_care_plan_stts_type_desc":"current_status"}, inplace=True)

    #remove closed one by the most recent careplan id status
    closed_careplan_id = most_recent_careplan_id_record[most_recent_careplan_id_record['cp_care_plan_stts_type_desc']=='Closed']["cp_care_plan_id"].drop_duplicates().tolist()


    careplan_not_closed = careplan_df[~careplan_df["cp_care_plan_id"].isin(closed_careplan_id)]

    careplan_with_current_status = careplan_not_closed.merge(most_recent_careplan_status, how="left", on=["mbr_src_member_id", "cp_care_plan_id"])

    #if there are more than one care plan, priortive active
    priority_order = {
        'Active': 3, 
        'Inactive': 2,
        'other' : 1
    }
    careplan_with_current_status["priority"] = careplan_with_current_status['current_status'].map(lambda x: priority_order.get(x, priority_order['other']))
    priority_careplan_id = careplan_with_current_status.loc[careplan_with_current_status.groupby(["mbr_src_member_id"])["priority"].idxmax()]
    final_careplan_id = priority_careplan_id[["mbr_src_member_id", "cp_care_plan_id"]].reset_index()

    #get the right careplan id per member 
    careplan_df_final = careplan_df.merge(final_careplan_id, how="inner", on = ["mbr_src_member_id", "cp_care_plan_id"])

    #now get the most recent record of the member
    careplan_df_final.sort_values(['mbr_src_member_id', 'cp_updated_on_dts'], ascending=[True, False], inplace=True)
    icp_date_df = careplan_df_final.drop_duplicates(['mbr_src_member_id'], keep='first')
    return icp_date_df