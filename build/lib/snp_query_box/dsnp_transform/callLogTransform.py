from snp_query_box import DsnpHelperFunction
import numpy as np
import pandas as pd

utr_values = [
'Unsuccessful Disconnected',
'Unsuccessful Invalid Phone Number',
'Unsuccessful Disconnected Send Letter']

dtr_values = [
'Unsuccessful Left Message',
'Unsuccessful No Answer',
'Successful Refused',
'Unsuccessful Visit']

def clean_call_log(call_log_df_raw, exclude_cec_user=True):
    cl_call_log_type_desc_list = ["Action Plan Review","Admission Avoidance","Annual Face to Face Visit","Assessment",
                                "BH","BH Linkage","BH Medicare Care Connection","BRCA","Care Coordination","Care Path Coordination",
                                "Care Planning","Case Management","Cellular Therapy Outreach - Post","Cellular Therapy Outreach - Pre",
                                "Cellular Therapy Renewal","Concurrent Review","COVID-19","Customer Service","Denial Verbal Notification - Member",
                                "Denial Verbal Notification - Provider","DOS Denial - Member Notification","Engagement","Enrollment",
                                "ER Predictive Outreach","Expedited Commercial","Expedited OD","Facility Outreach","Follow Up",
                                "Group Coaching Follow Up","Hard to Reach - Alt Contact Research","Hard to Reach - Engagement Collab",
                                "HRA Outreach","Individual Coaching","Info Provided/Received","Inpatient Confinement Contact","Inpatient Member Engagement",
                                "Inpatient Outreach","Intensive Admission Avoidance","Interdisciplinary Care Team","Live Claim Review","Medicaid Recertification",
                                "Misdirected Call","NICU Post Discharge Baby","NME","OMFS","Other","Pharmacy","Post - Emergency Room Outreach (FMC)",
                                "Post Discharge Outreach","Post Discharge Peds","Pre Admission Outreach","Precertification","Predetermination",
                                "Provider Outreach","Provider Outreach for Medication Review","Referral","Resources Provided","Scheduling",
                                "SNF Inpatient Outreach","Social Worker","Supplemental Benefits","Transfer","Transplant Outreach - Post",
                                "Transplant Outreach - Pre","Transplant Renewal","Vendor Automated Outreach"]

    cl_call_log_rel_type_desc_list = ["Appointment of Representative", "EXP Friend", "EXP Guardian","Family/Daughter","Family/Grandchild",
                                "Family/Other","Family/Parent","Family/Partner","Family/Sibling","Family/Son","Family/Spouse",
                                "Friend","Guardian","Health Care Proxy","Member/Self","Other","Power of Attorney","Third Party Admin"]


    cl_call_log_method_type_desc_list = ["In Person - Community Location","In Person - Member Residence",
                                    "In Person - Other Residence","Phone - Inbound","Phone - Outbound","Video Conferencing - Outbound"]

    call_log_cols = ["mcl_identifier_value_id", "cl_call_log_method_type_desc", "cl_call_log_drctn_type_desc", \
                   "cl_call_log_rel_type_desc", "cl_call_log_type_desc", "cl_contact_dts", "cl_object_typ_desc", \
                    "cl_category_typ_desc", "cl_attempts_typ_key_desc", "cl_attempt_status_key_desc", "cl_comments_txt"]
    if exclude_cec_user:
        call_log_df_raw = call_log_df_raw[call_log_df_raw["su_user_nm"]!="CECUser"]

    call_log_df = call_log_df_raw[(call_log_df_raw["cl_call_log_type_desc"].isin(cl_call_log_type_desc_list)) &
                                 (call_log_df_raw["cl_call_log_rel_type_desc"].isin(cl_call_log_rel_type_desc_list)) &
                                 (call_log_df_raw["cl_call_log_method_type_desc"].isin(cl_call_log_method_type_desc_list))][call_log_cols]

    #current record utr or dtr
    call_log_df["cur_utr"] =np.where(call_log_df["cl_attempt_status_key_desc"].isin(utr_values), 1, 0)
    # UTR also counts as DTR for aggregation
    call_log_df["cur_dtr"] =np.where(call_log_df["cl_attempt_status_key_desc"].isin(dtr_values + utr_values), 1, 0)

    # remove non necessary rows and columns
    call_log_df = call_log_df[call_log_df["cl_attempt_status_key_desc"] != "No Call Attempt Made"]
    call_log_df = call_log_df[call_log_df["mcl_identifier_value_id"] != ""]\
                    .sort_values(by=["mcl_identifier_value_id", "cl_contact_dts", "cl_attempts_typ_key_desc"])
    call_log_df.rename(columns={"mcl_identifier_value_id": "Member_ID"}, inplace=True)
    call_log_df = DsnpHelperFunction.set_format_date_columns(call_log_df, ["cl_contact_dts"])

    call_log_df["cl_attempt_status_key_desc"] = call_log_df["cl_attempt_status_key_desc"].str.strip()

    #This match to medcompass
    call_log_df["cl_contact_dts_old"] = call_log_df["cl_contact_dts"]
    call_log_df["cl_contact_dts"] = call_log_df["cl_contact_dts_old"].dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')
    call_log_df["cl_contact_dts"] = pd.to_datetime(call_log_df["cl_contact_dts"])
    return call_log_df

def call_log_timestamp_clean(call_log_df, threshold_min = 60):
    """ call log data has duplicate records, even their timestamps are different but the same 
    """
    call_log_df.drop_duplicates(inplace=True)
    call_log_df["contact_date"] = call_log_df["cl_contact_dts"].dt.date
    call_same_date_df = call_log_df.groupby(['Member_ID', 'contact_date','cl_attempts_typ_key_desc']).size().reset_index(name='cnt')
    member_to_clean_df = call_same_date_df[call_same_date_df['cnt']>=2]
    call_log_df_with_dup_cnt = call_log_df.merge(member_to_clean_df, how="left", on=["Member_ID", "cl_attempts_typ_key_desc", "contact_date"])
    call_log_df_need_clean = call_log_df_with_dup_cnt[call_log_df_with_dup_cnt["cnt"].notnull()]
    call_log_df_no_need_clean = call_log_df_with_dup_cnt[call_log_df_with_dup_cnt["cnt"].isnull()]
    call_log_df_need_clean = call_log_df_need_clean.sort_values(by=['Member_ID', 'cl_attempts_typ_key_desc','cl_contact_dts']).reset_index(drop=True)

    #if the same member and attempts and their time difference is under 10min, update cl_contact_dts to the earlier one
    #Only records that need dedup work will go under the function and concat back to save computing time
    def _take_earlier_timestamps(df):
        member_df = df.sort_values(by='cl_contact_dts')
        valid_timestamp = member_df['cl_contact_dts'].iloc[0]
        for i in range(1, len(member_df)):
            if (member_df['cl_contact_dts'].iloc[i] - valid_timestamp).total_seconds()/60 <= threshold_min:
                member_df['cl_contact_dts'].iloc[i] = valid_timestamp
            else:
                valid_timestamp = member_df['cl_contact_dts'].iloc[i]
        
        return member_df

    call_log_df_timestamp_clean = call_log_df_need_clean.groupby(['Member_ID', 'cl_attempts_typ_key_desc'], group_keys=False).apply(_take_earlier_timestamps)
    call_log_concat_df = pd.concat([call_log_df_timestamp_clean, call_log_df_no_need_clean], ignore_index=False)

    call_log_timestamp_clean = call_log_concat_df.groupby(["Member_ID", "cl_call_log_drctn_type_desc", "cl_call_log_method_type_desc", "cl_call_log_rel_type_desc", "cl_object_typ_desc", \
                                                   "cl_category_typ_desc","cl_attempts_typ_key_desc", "cl_attempt_status_key_desc", "cl_contact_dts", "contact_date", "cur_utr", "cur_dtr"])\
                    .agg({"cl_call_log_type_desc": lambda x: sorted([v for v in x if v not in ('', None)]),
                          "cl_comments_txt": lambda x: sorted(list([v for v in x if v not in ('', None)]))})\
                    .reset_index()
    
    call_log_timestamp_clean['cl_call_log_type_desc'] =  call_log_timestamp_clean['cl_call_log_type_desc'].astype(str)
    call_log_timestamp_clean['cl_comments_txt'] =  call_log_timestamp_clean['cl_comments_txt'].astype(str)
    return call_log_timestamp_clean

def get_most_recent_call_with_utr_flag(call_log_df, start_date, end_date):
    """Input is cleaned call_log_df, which is the output clean_call_log function"""
    # get most recent data and successful call
    call_log_df.sort_values(by=["Member_ID", "cl_contact_dts"], inplace=True)
    call_log_df_most_recent_call = call_log_df.drop_duplicates(subset="Member_ID", keep='last')
    call_log_df_most_recent_call.columns = ["most_recent_" + col for col in call_log_df_most_recent_call.columns]

    #UTR is defined by most recent call
    call_log_df_most_recent_call["final_utr"] = np.where(call_log_df_most_recent_call["most_recent_cl_attempt_status_key_desc"].isin(utr_values), 1, 0)


    call_log_df_most_recent_call.rename(columns={"most_recent_Member_ID":"Member_ID"}, inplace=True)

    call_log_successful_df = call_log_df[call_log_df["cl_attempt_status_key_desc"]=="Successful"]\
        .sort_values(by=["Member_ID", "cl_call_log_rel_type_desc", "cl_call_log_type_desc", "cl_contact_dts"])

    call_log_successful_df["most_recent_successful_by_category"] = ~call_log_successful_df.duplicated(["Member_ID", "cl_call_log_rel_type_desc", "cl_call_log_type_desc"], keep='last')

    #keep only 1 year back successful call
    most_recent_successful_df = call_log_successful_df[(call_log_successful_df["most_recent_successful_by_category"]==True) &\
                                                        (call_log_successful_df["cl_contact_dts"] >= start_date) &\
                                                        (call_log_successful_df["cl_contact_dts"] <= end_date)]

    def create_dict(row):
        return {'cl_call_log_rel_type_desc': row['cl_call_log_rel_type_desc'],
                'cl_call_log_method_type_desc': row['cl_call_log_method_type_desc'],
                'cl_call_log_type_desc': row['cl_call_log_type_desc'],
                'cl_contact_dts': row['cl_contact_dts']}

    most_recent_successful_df["recent_successful_calls"] = most_recent_successful_df.apply(create_dict, axis = 1)
    most_recent_successful_agg_df = most_recent_successful_df.groupby('Member_ID')['recent_successful_calls'].agg(list).reset_index()

    most_recent_call_log_df = call_log_df_most_recent_call.merge(most_recent_successful_agg_df, how="left", on="Member_ID")
    return most_recent_call_log_df

def get_cm_engagement_rate(call_log_df, start_date, end_date):
    dtr_threshold = 0.7 #more than 7 non successful calls out of 10 calls, we count it as Difficult to Reach member
    call_log_in_period = call_log_df[(call_log_df["cl_contact_dts"] >= start_date) & (call_log_df["cl_contact_dts"] <= end_date)]
    member_call_agg = call_log_in_period.groupby("Member_ID").agg({"cl_attempts_typ_key_desc": "size", "cur_dtr":"sum"}).reset_index()
    member_call_agg.rename(columns = {"cl_attempts_typ_key_desc":"total_call_count", "cur_dtr":"total_non_successful"}, inplace = True)
    member_call_agg["non_successful_rate"] = member_call_agg["total_non_successful"] / member_call_agg["total_call_count"]
    member_call_agg["final_dtr"] = np.where(member_call_agg["non_successful_rate"] > dtr_threshold, 1, 0)
    member_call_agg["cm_engaged"] = np.where((member_call_agg["total_call_count"]>0) & (member_call_agg["total_non_successful"] < member_call_agg["total_call_count"]), 1, 0)
    member_call_agg["cm_engaged"] = member_call_agg["cm_engaged"].fillna(0) #when no data, counts no value return
    return member_call_agg



def _monthly_utr_moving_month(call_log_df, look_back_month=24):
    from datetime import timedelta
    import datetime as dt
    from dateutil.relativedelta import relativedelta
    today = dt.datetime.today()
    today_str = today.strftime('%Y-%m-%d')
    last_date_of_last_month = DsnpHelperFunction.last_date_of_last_month(today_str)
    #moving month
    prev_month = last_date_of_last_month
    monthly_moving_dtr_utr_dfs = []
    end_month_list = []

    #let's get 24 months agg
    for i in range(0, look_back_month):
        end_month_date = prev_month
        start_month_date = end_month_date - relativedelta(years=1) + relativedelta(days=1)
        print(start_month_date)
        print(end_month_date)
        end_month_list.append(end_month_date.strftime('%Y-%m-%d'))
        # Get Total attempt counts last 12 month and unsuccessful count
        subset_12m = call_log_df[(call_log_df["cl_contact_dts"].dt.date >= start_month_date) & (call_log_df["cl_contact_dts"].dt.date <= end_month_date)]

        # for UTR, we look back all the way
        look_back = call_log_df[(call_log_df["cl_contact_dts"].dt.date <= end_month_date)]
        look_back_most_recent = look_back.drop_duplicates(subset="Member_ID", keep='last')
        look_back_most_recent["utr"] = np.where(look_back_most_recent["cl_attempt_status_key_desc"].isin(utr_values), 1, 0)

        subset_12m_agg = subset_12m.groupby("Member_ID").agg({"cl_contact_dts": "size", "cur_dtr":"sum"}).reset_index()
        subset_12m_agg.rename(columns = {"cl_contact_dts":"total_call_count", "cur_dtr":"total_non_successful"}, inplace = True)

        subset_12m_agg["non_successful_rate"] = subset_12m_agg["total_non_successful"] / subset_12m_agg["total_call_count"]
        subset_12m_agg["dtr"] = np.where(subset_12m_agg["non_successful_rate"] > 0.7, 1, 0)

        utr_dtr_df = subset_12m_agg.merge(look_back_most_recent[["Member_ID","utr"]], how="outer", on="Member_ID")

        #make it mutually, exclusive
        utr_dtr_df["dtr"] = np.where((utr_dtr_df["utr"]==1) & (utr_dtr_df["dtr"]==1), 0, utr_dtr_df["dtr"])

        utr_dtr_df["range_start_date"] = start_month_date.strftime('%Y-%m-%d')
        utr_dtr_df["range_end_date"] = end_month_date.strftime('%Y-%m-%d')
        monthly_moving_dtr_utr_dfs.append(utr_dtr_df)
        i += 1
        prev_month = end_month_date.replace(day=1) - timedelta(days=1)

    monthly_moving_dtr_utr_df = pd.concat(monthly_moving_dtr_utr_dfs)
    return monthly_moving_dtr_utr_df, end_month_list


def get_chronic_utr(call_log_df, condensed_df):
    """use call_log_df 3 year look back"""

    active_member_dfs = []
    monthly_moving_dtr_utr_df, end_month_list = _monthly_utr_moving_month(call_log_df, look_back_month=24)

    for i in end_month_list:
        active_month = i[:-2] + '01'
        active_member_df = condensed_df[(condensed_df["EFF_Date"] <= active_month) & (condensed_df["Term_Date"] >= active_month)][["Member_ID"]]
        active_member_df["range_end_date"] = i
        active_member_df["active_month"] = active_month
        active_member_dfs.append(active_member_df.drop_duplicates())

    print("moving month UTR to see how long member is on UTR status")
    monthly_active_member_df = pd.concat(active_member_dfs)

    monthly_active_utr_dtr_df = monthly_active_member_df.merge(monthly_moving_dtr_utr_df, how="left", left_on = ["Member_ID", "range_end_date"], right_on = ["Member_ID", "range_end_date"])\
        .drop(columns = ["range_start_date", "range_end_date"])
    #when there is no call during the active period, fill with 0
    monthly_active_utr_dtr_df["total_call_count"] = monthly_active_utr_dtr_df["total_call_count"].fillna(0)
    monthly_active_utr_dtr_df["total_non_successful"] = monthly_active_utr_dtr_df["total_non_successful"].fillna(0)
    monthly_active_utr_dtr_df["dtr"] = monthly_active_utr_dtr_df["dtr"].fillna(0)
    monthly_active_utr_dtr_df["utr"] = monthly_active_utr_dtr_df["utr"].fillna(0)
    
    def cumsum_utr_month(utrs):
        cumsum = 0
        result = []
        for value in utrs:
            cumsum += value
            if value == 0:
                cumsum = 0
            result.append(cumsum)
        return result
        #get count of continuous UTR months
    monthly_active_utr_dtr_df = monthly_active_utr_dtr_df.groupby('Member_ID', group_keys=False).apply(lambda x: x.sort_values("active_month")).reset_index(drop=True)
    monthly_active_utr_dtr_df["utr_on_going_month"] = monthly_active_utr_dtr_df.groupby('Member_ID')['utr'].transform(cumsum_utr_month)
    print("If members are on UTR more than 12month, count as Chronic UTR")
    monthly_active_utr_dtr_df['chronic_utr'] = np.where(monthly_active_utr_dtr_df['utr_on_going_month']>=12, 1, 0)

    return monthly_active_utr_dtr_df

def clean_hot_mac_call_log(pull_hot_mac_call):
    valid_call_status_list = ['NOT REACHED', 'REACHED']
    valid_hot_mac_call_df = pull_hot_mac_call[pull_hot_mac_call["CALL_STATUS_DESC"].isin(valid_call_status_list)]
    valid_hot_mac_call_df_sorted = valid_hot_mac_call_df.sort_values(by = ['MEDICARE_MEMBER_ID', 'CALL_TIMESTAMP'], ascending=[True, False])
    valid_hot_mac_call_df_sorted["Member_ID"] = valid_hot_mac_call_df_sorted["MEDICARE_MEMBER_ID"].str.replace('--', '', regex=False)

    col_to_select = ["Member_ID", "CALL_STATUS_DESC", "CALL_TIMESTAMP", "FOCUS_DESC", "RECOMMENDATION", "CALL_SCRIPT_DESC", "MFS_CUR_FOCUS_STATUS_REASON_DESC", "NOT_REACH_REASON_DESC", "CALL_TYPE_DESC", "INSERT_TEAM_NM"]
    hot_mac_call_df = valid_hot_mac_call_df_sorted[col_to_select]
    return hot_mac_call_df