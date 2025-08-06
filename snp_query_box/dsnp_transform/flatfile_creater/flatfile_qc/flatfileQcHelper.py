from dsnp_qc_box import QcFunction 
import pandas as pd
import numpy as np
import sys

#Do some mapping preparation

def run_qc_cm(df): 
    cm_criteria = {"final_utr_one_year_back":">= 10",
                    "total_call_count_one_year_back":">= 100",
                    "total_non_successful_one_year_back":">= 100",
                    "non_successful_rate_one_year_back":"<= 1.0",
                    "final_dtr_one_year_back":">= 100",	
                    "cm_engaged_one_year_back":">= 100",	
                    "final_utr_ytd":">= 10",
                    "total_call_count_ytd":">= 100",	
                    "total_non_successful_ytd":">= 100",
                    "non_successful_rate_ytd":"<= 1.0",
                    "final_dtr_ytd":">= 100",	
                    "cm_engaged_ytd":">= 100"}
    cm_agg_dict = {"final_utr_one_year_back":"sum",
                    "total_call_count_one_year_back":"sum",
                    "total_non_successful_one_year_back":"sum",
                    "non_successful_rate_one_year_back":"max",
                    "final_dtr_one_year_back":"sum",	
                    "cm_engaged_one_year_back":"sum",	
                    "final_utr_ytd":"sum",
                    "total_call_count_ytd":"sum",	
                    "total_non_successful_ytd":"sum",
                    "non_successful_rate_ytd":"max",
                    "final_dtr_ytd":"sum",	
                    "cm_engaged_ytd":"sum"}
    print(cm_agg_dict)
    #sum of each open gap member should be bigger than 10

    df_temp = df[cm_criteria.keys()]
    print("QcFunction.check_expectation")
    qc_dict, aggregated_df = QcFunction\
        .check_expectation(df_temp, cm_agg_dict, cm_criteria)
    return qc_dict, aggregated_df


def run_qc_hedis(df): 
    open_gap_columns = [c for c in df.columns if c.startswith("open_gap")]
    opengap_sum_min = [">= 100"] * len(open_gap_columns)
    opengap_agg_method = ["sum"] * len(open_gap_columns)

    hedis_criteria = dict(zip(open_gap_columns, opengap_sum_min))
    hedis_agg_dict =dict(zip(open_gap_columns, opengap_agg_method))
    print(hedis_criteria)
    #sum of each open gap member should be bigger than 10

    df_temp = df[open_gap_columns]
    df_temp = df_temp.applymap(lambda x: 1 if x =='Y' else 0)
    print("QcFunction.check_expectation")
    qc_dict, aggregated_df = QcFunction\
        .check_expectation(df_temp, hedis_agg_dict, hedis_criteria)
    return qc_dict, aggregated_df

def run_qc_pgb_group(df): 

    pgb_criteria = {"Member_ID": ">= 4000000",
                    "VBC": ">= 2000000",
                    "CMSContractNumber": ">=40",
                    "Oak_Street_Ind": ">= 30000"}
    pgb_agg_dict = {"Member_ID": "count",
                    "VBC": "sum",
                    "CMSContractNumber": "nunique",
                    "Oak_Street_Ind": "sum"}
    print(pgb_agg_dict)
    #sum of each open gap member should be bigger than 10

    df_temp = df[pgb_criteria.keys()]
    df_temp.loc[:, "VBC"] = df_temp["VBC"].apply(lambda x: 1 if x =='VBC' else 0)
    df_temp.loc[:, "Oak_Street_Ind"] = df_temp["Oak_Street_Ind"].apply(lambda x: 1 if x ==True else 0)

    print("QcFunction.check_expectation")
    qc_dict, aggregated_df = QcFunction\
        .check_expectation(df_temp, pgb_agg_dict, pgb_criteria)
    return qc_dict, aggregated_df

def qc_alarm(qc_dict, report_name, dist_list):
    qc_result = QcFunction.qc_result_from_qc_dict(qc_dict)
    if qc_result == "Fail":
        QcFunction.send_email_alarm(report_name = report_name, dist_list = dist_list)
        sys.exit("Execution stopped due to QC failure")


utilization_df_ytd_dict = {"dsnp_member_id": "count",
                    "IP_VISITS": "sum",
                    "BEHAVIORAL_HEALTH_VISITS": "sum"}

utilization_df_ytd_criteria = {"dsnp_member_id": "ge",
                    "IP_VISITS": "btw 15%",
                    "BEHAVIORAL_HEALTH_VISITS": "btw 15%"}

utilization_df_12month_dict = {"dsnp_member_id": "count",
                    "IP_VISITS": "sum",
                    "BEHAVIORAL_HEALTH_VISITS": "sum"}

utilization_df_12month_criteria = {"dsnp_member_id": "ge",
                    "IP_VISITS": "btw 15%",
                    "BEHAVIORAL_HEALTH_VISITS": "btw 15%"}

race_df_dict = {"MBI": "count",
                    "Race_Code": "nunique"}
race_df_criteria = {"MBI": "ge",
                    "Race_Code": "equal"}

sop_campaign_unique_df_dict = {"src_member_id": "count",
                    "Engagement_Category_SOP": "nunique"}
sop_campaign_unique_df_criteria = {"src_member_id": "ge",
                    "Engagement_Category_SOP": "equal"}

qnxt_call_df_dict = {"MedicareID": "count",
                    "cl_call_log_type_desc": "nunique",
                    "Source": "nunique",
                    "Contract": "nunique"}
qnxt_call_df_criteria  = {"MedicareID": "ge",
                    "cl_call_log_type_desc": "equal",
                    "Source": "equal",
                    "Contract": "equal"}

call_log_df_dict = {"Member_ID": "nunique",
                    "cl_call_log_type_desc": "nunique",
                    "cl_call_log_method_type_desc": "nunique",
                    "cl_call_log_drctn_type_desc": "nunique",
                    "cl_attempt_status_key_desc": "nunique"}
call_log_df_criteria  = {"Member_ID": "ge",
                    "cl_call_log_type_desc": "equal",
                    "cl_call_log_method_type_desc": "equal",
                    "cl_call_log_drctn_type_desc": "equal",
                    "cl_attempt_status_key_desc": "equal"}

dsnp_base_member_df_dict = {"Member_ID": "count",
                    "Contract_Number": "nunique",
                    "Dual_Status": "nunique",
                    "dsnp_state": "nunique",
                    "Movement_Type_Out": "nunique"}
dsnp_base_member_df_criteria  = {"Member_ID": "btw 15%",
                    "Contract_Number": "equal",
                    "Dual_Status": "equal",
                    "dsnp_state": "equal",
                    "Movement_Type_Out": "btw 15%"}

disability_df_dict = {"Member_ID": "count",
                    "disability_ind": "nunique"}
disability_df_criteria  = {"Member_ID": "ge",
                    "disability_ind": "equal"}

condensed_dsnp_mbr_2024_dict = {"Member_ID": "count",
                    "Contract_Number": "nunique",
                    "SNP": "nunique",
                    "Plan_Type": "nunique",
                    "Submission_Type": "nunique"}
condensed_dsnp_mbr_2024_criteria  = {"Member_ID": "ge",
                    "Contract_Number": "equal",
                    "SNP": "equal",
                    "Plan_Type": "equal",
                    "Submission_Type": "equal"}

hedis_ps_wide_dict = {"SourceMemberID": "count",
                    "DENOMINATOR_Breast Cancer Screening": "sum",
                    "open_gap_Medication Adherence for Cholesterol (Statins)": "nunique",
                    "NUMERATOR_Diabetes Care - Eye Exam": "sum",
                    "open_gap_Statin Use in Persons with Diabetes (SUPD)": "nunique"}

hedis_ps_wide_criteria  = {"SourceMemberID": "ge",
                    "DENOMINATOR_Breast Cancer Screening": "btw 15%",
                    "open_gap_Medication Adherence for Cholesterol (Statins)": "equal",
                    "NUMERATOR_Diabetes Care - Eye Exam": "ge",
                    "open_gap_Statin Use in Persons with Diabetes (SUPD)": "equal"}

iha_df_dict = {"Member_ID": "count",
                    "Contract_Number": "nunique"}
iha_df_criteria  = {"Member_ID": "btw 10%",
                    "Contract_Number": "btw 20%"}

active_membership_sfs_dict = {"MEMBNO": "count",
                    "Sex": "nunique",
                    "Language": "nunique"}
active_membership_sfs_criteria  = {"MEMBNO": "btw 20%",
                    "Sex": "equal",
                    "Language": "btw 20%"}

clinical_conditions_df_dict = {"SRC_MEMBER_ID": "count",
                    "SKIN_CANCER": "sum",
                    "Critical": "mean",
                    "Total": "mean"}
clinical_conditions_df_criteria  = {"SRC_MEMBER_ID": "ge",
                    "SKIN_CANCER": "btw 50%",
                    "Critical": "btw 20%",
                    "Total": "btw 20%"}

ev_call_df_dict = {"src_member_id": "count",
                    "Engagement_Category_MAC": "nunique"}
ev_call_df_criteria  = {"src_member_id": "btw 15%",
                    "Engagement_Category_MAC": "equal"}

ked_gap_status_df_dict = {"SourceMemberID": "count",
                    "MeasureID": "nunique"}
ked_gap_status_df_criteria  = {"SourceMemberID": "btw 20%",
                    "MeasureID": "equal"}

mbr_contact_legal_short_valid_df_dict = {"mbr_identifier_value_id": "count",
                    "mcl_legal_type_desc": "nunique"}
mbr_contact_legal_short_valid_df_criteria  = {"mbr_identifier_value_id": "btw 20%",
                    "mcl_legal_type_desc": "btw 20%"}

med_adherence_df_with_detail_dict = {"SRC_MEMBER_ID": "count",
                    "STATIN_ADH_DECILE": "max",
                    "RAS_DAYS_SUPPLY_CNT": "max",
                    "DIAB_STATUS": "nunique"}
med_adherence_df_with_detail_criteria  = {"SRC_MEMBER_ID": "btw 10%",
                    "STATIN_ADH_DECILE": "equal",
                    "RAS_DAYS_SUPPLY_CNT": "equal",
                    "DIAB_STATUS": "equal"}

member_cm_engagement_df_dict = {"Member_ID": "count",
                    "most_recent_cl_call_log_method_type_desc": "nunique",
                    "most_recent_cl_call_log_type_desc": "nunique",
                    "cm_engaged_ytd": "mean",
                    "final_utr_ytd": "mean"}
member_cm_engagement_df_criteria  = {"Member_ID": "ge",
                    "most_recent_cl_call_log_method_type_desc": "btw 20%",
                    "most_recent_cl_call_log_type_desc": "btw 20%",
                    "cm_engaged_ytd": "btw 20%",
                    "final_utr_ytd": "btw 20%"}

monthly_active_member_utr_dtr_df_dict = {"Member_ID": "count",
                    "utr": "mean",
                    "dtr": "mean"}
monthly_active_member_utr_dtr_df_criteria  = {"Member_ID": "ge",
                    "utr": "btw 20%",
                    "dtr": "btw 20%"}

pbg_group_clean_df_dict = {"Member_ID": "count",
                    "VBC": "nunique",
                    "CMSContractNumber": "nunique",
                    "Oak_Street_Ind": "nunique"}
pbg_group_clean_df_criteria  = {"Member_ID": "ge",
                    "VBC": "equal",
                    "CMSContractNumber": "equal",
                    "Oak_Street_Ind": "nunique"}

prefer_language_df_dict = {"MEDICARE_NUMBER": "count",
                    "language": "nunique"}
prefer_language_df_criteria  = {"MEDICARE_NUMBER": "ge",
                    "language": "btw 20%"}

pull_ev_call_dict = {"src_member_id": "count",
                    "cms_cntrct_nbr": "nunique",
                    "focus status": "nunique"}
pull_ev_call_criteria  = {"src_member_id": "btw 20%",
                    "cms_cntrct_nbr": "equal",
                    "focus status": "btw 20%"}

pull_ev_call_sop_dict = {"src_member_id": "count",
                    "cms_cntrct_nbr": "nunique",
                    "focus status": "nunique"}
pull_ev_call_sop_criteria  = {"src_member_id": "btw 20%",
                    "cms_cntrct_nbr": "equal",
                    "focus status": "btw 20%"}

pull_member_do_not_mail_dict = {"MBI": "count",
                    "DirectMailPermInd": "nunique",
                    "ok_to_mail_ind": "nunique"}
pull_member_do_not_mail_criteria  = {"MBI": "btw 20%",
                    "DirectMailPermInd": "equal",
                    "ok_to_mail_ind": "equal"}

pull_member_phone_info_dict = {"SourceMemberID": "count",
                    "PhoneValidInd": "nunique",
                    "VoicePermInd": "nunique",
                    "DNCInd": "nunique"}
pull_member_phone_info_criteria  = {"SourceMemberID": "btw 10%",
                    "PhoneValidInd": "equal",
                    "VoicePermInd": "equal",
                    "DNCInd": "equal"}

pull_prefer_language_dict = {"MEDICARE_NBR": "count",
                    "PREF_SPOKEN_LANG": "nunique",
                    "PREF_WRITTEN_LANG": "nunique"}
pull_prefer_language_criteria  = {"MEDICARE_NBR": "ge",
                    "PREF_SPOKEN_LANG": "btw 20%",
                    "PREF_WRITTEN_LANG": "btw 20%"}

pull_prefer_language_qnxt_dict = {"MEDICARE_NUMBER": "count",
                    "language": "nunique"}
pull_prefer_language_qnxt_criteria  = {"MEDICARE_NUMBER": "ge",
                    "language": "btw 20%"}

pull_race_dict = {"MBI": "count",
                    "Race_ID": "nunique"}
pull_race_criteria  = {"MBI": "ge",
                    "Race_ID": "equal"}

pull_resident_type_dict = {"asm_id_value_desc": "count",
                    "asad_question_txt": "nunique",
                    "asad_assmnt_ans_data_txt": "nunique"}
pull_resident_type_criteria  = {"asm_id_value_desc": "btw 10%",
                    "asad_question_txt": "equal",
                    "asad_assmnt_ans_data_txt": "btw 20%"}

pull_risk_strat_from_icp_dict = {"Member_ID": "count",
                    "CARS": "nunique",
                    "system_risk_strat": "nunique"}
pull_risk_strat_from_icp_criteria  = {"Member_ID": "ge",
                    "CARS": "equal",
                    "system_risk_strat": "equal"}

pull_risk_strat_nj_dict = {"Member_ID": "count",
                    "Member_risk_Stratification": "nunique"}
pull_risk_strat_nj_criteria  = {"Member_ID": "ge",
                    "Member_risk_Stratification": "equal"}

pull_risk_strat_va_dict = {"Member_ID": "count",
                    "Member_risk_Stratification": "nunique"}
pull_risk_strat_va_criteria  = {"Member_ID": "ge",
                    "Member_risk_Stratification": "equal"}