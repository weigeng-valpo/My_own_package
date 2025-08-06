import pandas as pd
from datetime import datetime

import numpy as np
from snp_query_box import DsnpHelperFunction, dsnpStyle
from snp_query_box.dsnp_transform import callLogTransform

import seaborn as sns
import matplotlib.pyplot as plt

def mj_prepare_hra_data(mbr_hra_df):
    """
    mbr_hra_df(pandas.DataFrame): use "mbr_hra_{CURRENT_YEAR}_with_broker_without_iha" file
    """
    hra_select_df = mbr_hra_df[["Member_ID", 'I_eligible', 'I_completed',
       'I_completed_date', 'I_completion_channel', 'R_eligible', 'R_completed',
       'R_completed_date', "Anchor_date", "most_recent_hra_completion_prior_to_reporting_year", "most_recent_hra_completion_date", "next_hra_due_date"]]
    hra_select_df.loc[:,'most_recent_hra_completion_prior_to_reporting_year'] = pd.to_datetime(hra_select_df['most_recent_hra_completion_prior_to_reporting_year'])
    hra_select_df.loc[:, 'most_recent_hra_completion_date'] = pd.to_datetime(hra_select_df['most_recent_hra_completion_date'])
    hra_select_df.loc[:, 'anchor_date'] = pd.to_datetime(hra_select_df['Anchor_date'])

    hra_select_df.loc[:, "most_recent_hra_date"] = hra_select_df[["most_recent_hra_completion_prior_to_reporting_year", "most_recent_hra_completion_date"]].max(axis=1)
    return hra_select_df

def mj_prepare_pull_call(pull_call_log_all_dsnp, condensed_dsnp_mbr):
    """
    pull_call_log_all_dsnp(pandas.DataFrame): use "pull_call_log_all_dsnp" file from snpQueries in snp_query_box

    condensed_dsnp_mbr(pandas.DataFrame): use the most recent "condensed_dsnp_mbr" file
    """

    col_to_select = ["cl_call_log_id", "mcl_identifier_value_id", "cl_call_log_method_type_desc", "cl_call_log_rel_type_desc", "cl_call_log_type_desc",
                     "cl_program_type_desc", "cl_contact_dts", "cl_effective_dts", "cl_attempts_typ_key_desc", "cl_attempt_status_key_desc", "cl_comments_txt", "su_user_nm"]
    call_with_member_info = condensed_dsnp_mbr[["Member_ID", "EFF_Date", "Term_Date", "Contract_Number", "PBP", "SNP", "Plan_Type","MA_Market_PDP_Product"]].merge(pull_call_log_all_dsnp[col_to_select], 
                                                                        how="left", left_on="Member_ID", right_on="mcl_identifier_value_id")\
        .sort_values(by=["mcl_identifier_value_id","cl_contact_dts"])

    call_with_member_info["cl_contact_dts"] = call_with_member_info["cl_contact_dts"].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')
    call_with_member_info["cl_contact_dts"] = pd.to_datetime(call_with_member_info["cl_contact_dts"])
    call_with_member_info["contact_date"] = call_with_member_info["cl_contact_dts"].dt.date
    return call_with_member_info

def mj_prepare_input(call_with_member_info, hra_select_df, member_id_list):
    call_member_info = call_with_member_info[call_with_member_info["Member_ID"].isin(member_id_list)]
    mj_input_df = call_member_info.merge(hra_select_df, on="Member_ID", how="left")

    return mj_input_df

def _member_call_journey_at_least_one_call(member_data, limit_left, limit_right, ax, member_contract_id, member_id_to_idx, member_agg):
        
        import matplotlib.pyplot as plt

        journey_color = 'red'
        prev_event_date = member_data['start_date'].iloc[0]
        successful_date = None        
        member_idx = member_id_to_idx[member_contract_id]

        for _, row in member_data.iterrows():
            start_date = row['start_date']
            end_date = row['end_date']
            event_date = row['event_date']
            event_name = row['event_name']
            su_user_nm = row['su_user_nm']

            if start_date > pd.to_datetime(event_date): # event is before start, journey color reset
                journey_color='red'
            if end_date < pd.to_datetime(event_date): # event is after end_date reset the color
                journey_color='red'
            
            event_date = pd.to_datetime(event_date)
            event_type = row['cl_call_log_type_desc']
            next_hra_due_date = row['next_hra_due_date']
            most_recent_hra_date = row['most_recent_hra_date']
            anchor_date = row['anchor_date']
            if event_name =='successful':
                successful_date = event_date

            if  (pd.to_datetime(event_date) <= end_date) & (pd.to_datetime(event_date) >= start_date):
                ax.plot([prev_event_date, event_date], [member_idx, member_idx], color=journey_color, lw=3)

            #color update by call result
            if successful_date is None:
                journey_color='red'
            elif (event_date >= successful_date) & (event_date >= start_date):
                journey_color='skyblue'
            else:
                journey_color='red'
            
            #now plot the event dates
            if (event_date>=pd.to_datetime(start_date)) & (event_date<=pd.to_datetime(end_date)) & (event_date>=pd.to_datetime(limit_left)) & (event_date<=pd.to_datetime(limit_right)):

                if event_name == 'successful':
                    ax.plot([event_date, event_date], [member_idx - 0.1, member_idx + 0.1], color = 'blue', lw=1, label='Successful')
                    ax.text(event_date, member_idx - 0.1, '*', fontsize=10, ha='left', va='bottom', color='blue')
                    ax.text(event_date, member_idx - 0.2, event_type, fontsize=8, ha='left', va='bottom', color='black', rotation=35)
                    if su_user_nm == 'CECUser':
                        ax.text(event_date, member_idx + 0.1, 'cec', fontsize=10, ha='left', va='bottom', color='blue', rotation=90)

                if event_name == 'refused':
                    ax.plot([event_date, event_date], [member_idx - 0.1, member_idx + 0.1], color = 'red', lw=1, label='Refused')
                    ax.text(event_date, member_idx + 0.3, event_name, fontsize=8, ha='left', va='bottom', color='black', rotation=90)
                    if su_user_nm == 'CECUser':
                        ax.text(event_date, member_idx + 0.1, 'cec', fontsize=10, ha='left', va='bottom', color='blue', rotation=90)

                if event_name == 'disconnected':
                    journey_color='red'
                    successful_date = None
                    ax.plot([event_date, event_date], [member_idx - 0.2, member_idx + 0.2], color = 'red', lw=1, label='disconnected')
                    ax.text(event_date, member_idx + 0.3, event_name, fontsize=8, ha='left', va='bottom', color='black', rotation=90)
                    if su_user_nm == 'CECUser':
                        ax.text(event_date, member_idx + 0.1, 'cec', fontsize=10, ha='left', va='bottom', color='blue', rotation=90)
                if event_name == 'unsuccessful':
                    ax.plot([event_date, event_date], [member_idx - 0.1, member_idx + 0.1], color = 'orange', lw=2, label='unsuccessful', alpha=0.4)
                    if su_user_nm == 'CECUser':
                        ax.text(event_date, member_idx + 0.1, 'cec', fontsize=10, ha='left', va='bottom', color='blue', rotation=90)
            #HRA date plot
            if (anchor_date>=pd.to_datetime(limit_left)) & (anchor_date<=pd.to_datetime(limit_right)):
                ax.text(anchor_date, member_idx - 0.2, '^', fontsize=10, ha='left', va='bottom', color='blue')
                ax.text(anchor_date+pd.Timedelta(days=1.5), member_idx - 0.3, f"Anchor Date {anchor_date.strftime('%Y-%m-%d')}", fontsize=9, ha='left', va='bottom', color='black')

            if pd.notna(most_recent_hra_date):
                if (most_recent_hra_date>=pd.to_datetime(limit_left)) & (most_recent_hra_date<=pd.to_datetime(limit_right)):
                    ax.plot([most_recent_hra_date, most_recent_hra_date], [member_idx - 0.3, member_idx + 0.3], color = 'green', lw=3, label='HRA completed')
                    ax.text(most_recent_hra_date+pd.Timedelta(days=1.5), member_idx + 0.3, f"HRA completed {most_recent_hra_date.strftime('%Y-%m-%d')}", fontsize=9, ha='left', va='bottom', color='black')
    
            if pd.notna(next_hra_due_date):
                if (next_hra_due_date>=pd.to_datetime(limit_left)) & (next_hra_due_date<=pd.to_datetime(limit_right)):
                    ax.plot([next_hra_due_date, next_hra_due_date], [member_idx - 0.3, member_idx + 0.3], color = 'green', lw=3, label='HRA due date', linestyle=':')
                    ax.text(next_hra_due_date+pd.Timedelta(days=1.5), member_idx + 0.3, f"HRA due date {next_hra_due_date.strftime('%Y-%m-%d')}", fontsize=9, ha='left', va='bottom', color='black')

            #EFF date Term date plot
            if (start_date>=pd.to_datetime(limit_left)) & (start_date<=pd.to_datetime(limit_right)):
                ax.plot([start_date-pd.Timedelta(days=1.5), start_date-pd.Timedelta(days=1.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Eff Date')
                ax.plot([start_date+pd.Timedelta(days=1.5), start_date+pd.Timedelta(days=1.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Eff Date')
                ax.text(start_date-pd.Timedelta(days=5), member_idx + 0.2, start_date.strftime("%Y-%m-%d"), fontsize=8, ha='left', va='bottom', color='black')
            if (end_date>=pd.to_datetime(limit_left)) & (end_date<=pd.to_datetime(limit_right)):
                ax.plot([end_date-pd.Timedelta(days=3.5), end_date-pd.Timedelta(days=3.5)], [member_idx + 0.1, member_idx - 0.1], color = 'black', lw=1, label='Term Date')
                ax.plot([end_date-pd.Timedelta(days=.5), end_date-pd.Timedelta(days=.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Term Date')
                ax.text(end_date-pd.Timedelta(days=5), member_idx + 0.2, end_date.strftime("%Y-%m-%d"), fontsize=8, ha='left', va='bottom', color='black')

            if event_date >= prev_event_date: #update only new event is newer
                prev_event_date = event_date

            if (pd.to_datetime(prev_event_date) <= end_date) & (pd.to_datetime(prev_event_date) >= start_date):
                ax.plot([prev_event_date, end_date], [member_idx, member_idx], color=journey_color, lw=3)
                #print("ploted with " + journey_color + " between " + str(prev_event_date) + " and " + str(end_date))
        if (pd.isnull(member_agg["total_count"].iloc[0])):
            ax.plot([member_data['start_date'].iloc[0], member_data['end_date'].iloc[0]], [member_idx, member_idx], color="red", lw=3)

def member_call_journey(df, limit_left = "2021-10-01", limit_right='2025-12-31'):

    """
    plot Member Call journey,
    parameters:
    df(pandas.DataFrame): A DataFrame containing the following columns:

    - 'cl_call_log_id' (str): unique identifier
    - 'Member_ID' (str)
    - 'Contract_Number' (str)
    - 'PBP' (str)
    - 'EFF_Date' (datetime or str)
    - 'Term_Date' (datetime or str)
    - 'contact_date' (datetime)
    - 'cl_attempt_status_key_desc' (str)
    - 'next_hra_due_date' (datetime)
    - 'most_recent_hra_date' (datetime)
    - 'anchor_date' (datetime)
   
    limit_left(str) and limit_right(str)
    """
    import matplotlib.pyplot as plt
    
    df["member_contract"] = df["Member_ID"] + '_' + df["Contract_Number"] + '_' + df["PBP"] + '_' + df["EFF_Date"].astype(str) + '_' + df["Term_Date"].astype(str)
    
    fig, ax = plt.subplots(figsize=(20,df['member_contract'].nunique()*2))

    df.loc[:,"start_date"] = df["EFF_Date"].apply(lambda x: max(x, datetime(2013, 1, 1)))
    df.loc[:, "end_date"] = df["Term_Date"].apply(lambda x: min(x, datetime(2025, 12, 31)))
    df.loc[:, "event_date"] = df["contact_date"]
    df.loc[:, "event_name"] = np.where(df['cl_attempt_status_key_desc']=='Successful', 'successful',
                        np.where(df['cl_attempt_status_key_desc']=='Successful Refused', 'refused',
                        np.where(df['cl_attempt_status_key_desc'].isin(['Unsuccessful Disconnected', 'Unsuccessful Invalid Phone Number', 'Unsuccessful Disconnected Send Letter']), 'disconnected', 
                                  'unsuccessful')))
    df = df.sort_values(by = ['Member_ID', 'start_date', 'contact_date'])
    df_distinct = df[["event_date", "start_date", "end_date", "event_name", "member_contract", "cl_call_log_id"]].drop_duplicates()

    cnt_agg = df_distinct[(df_distinct["event_date"]>=df_distinct['start_date']) & (df_distinct["event_date"]<=df_distinct['end_date'])]\
        .groupby("member_contract").agg(
        total_count = ('cl_call_log_id', 'size'),
        call_results = ('event_name', lambda x: x.value_counts().to_dict()),
    )
    #make it same length
    cnt_agg = df[["member_contract"]].merge(cnt_agg, how="left", on="member_contract")
    cnt_agg["call_results"] = cnt_agg["call_results"].astype(str)
    member_id_to_idx = {member_id: idx for idx, member_id in enumerate(df['member_contract'].drop_duplicates())}


    for member_contract_id in list(member_id_to_idx.keys()):
        member_data = df[df['member_contract']==member_contract_id]
        member_agg = cnt_agg[cnt_agg['member_contract']==member_contract_id]
        member_idx = member_id_to_idx[member_contract_id]

        if len(member_agg)>0: #at least one call in period
            _member_call_journey_at_least_one_call(member_data=member_data, limit_left = limit_left, limit_right=limit_right, ax=ax, member_contract_id=member_contract_id, member_id_to_idx=member_id_to_idx, member_agg=member_agg)

        else:
            if (pd.to_datetime(member_data['start_date'].iloc[0])>=pd.to_datetime(limit_left)) & (pd.to_datetime(member_data['start_date'].iloc[0])<=pd.to_datetime(limit_right)):
                ax.plot([pd.to_datetime(member_data['start_date'].iloc[0])-pd.Timedelta(days=1.5), pd.to_datetime(member_data['start_date'].iloc[0])-pd.Timedelta(days=1.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Eff Date')
                ax.plot([pd.to_datetime(member_data['start_date'].iloc[0])+pd.Timedelta(days=1.5), pd.to_datetime(member_data['start_date'].iloc[0])+pd.Timedelta(days=1.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Eff Date')
                ax.text(pd.to_datetime(member_data['start_date'].iloc[0])-pd.Timedelta(days=5), member_idx + 0.2, pd.to_datetime(member_data['start_date'].iloc[0]).strftime("%Y-%m-%d"), fontsize=8, ha='left', va='bottom', color='black')
            if (pd.to_datetime(member_data['end_date'].iloc[0])>=pd.to_datetime(limit_left)) & (pd.to_datetime(member_data['end_date'].iloc[0])<=pd.to_datetime(limit_right)):
                ax.plot([pd.to_datetime(member_data['end_date'].iloc[0])-pd.Timedelta(days=3.5), pd.to_datetime(member_data['end_date'].iloc[0])-pd.Timedelta(days=3.5)], [member_idx + 0.1, member_idx - 0.1], color = 'black', lw=1, label='Term Date')
                ax.plot([pd.to_datetime(member_data['end_date'].iloc[0])-pd.Timedelta(days=.5), pd.to_datetime(member_data['end_date'].iloc[0])-pd.Timedelta(days=.5)], [member_idx - 0.1, member_idx + 0.1], color = 'black', lw=1, label='Term Date')
                ax.text(pd.to_datetime(member_data['end_date'].iloc[0])-pd.Timedelta(days=5), member_idx + 0.2, pd.to_datetime(member_data['end_date'].iloc[0]).strftime("%Y-%m-%d"), fontsize=8, ha='left', va='bottom', color='black')
            ax.plot([member_data['start_date'].iloc[0], member_data['end_date'].iloc[0]], [member_idx, member_idx], color="red", lw=3)


    today = datetime.today()
    if (today>=pd.to_datetime(limit_left)) & (today<=pd.to_datetime(limit_right)):
        ax.axvline(today, color='red', linestyle='--')
        ax.text(today, -0.7, 'Today Line', fontsize=15, color='black', fontname="Impact")
    ax.set_yticks(range(len(df['member_contract'].drop_duplicates())))

    ax.set_yticklabels([f'Member_ID: {i} \nTotal_count: {j} \n*Call_result: {k}' for i, j, k in zip(cnt_agg["member_contract"].drop_duplicates(), cnt_agg[["member_contract","total_count"]].drop_duplicates()["total_count"]
                                                                                                    , cnt_agg[["member_contract","call_results"]].drop_duplicates()["call_results"])])
    ax.set_xlim([ datetime.strptime(limit_left, '%Y-%m-%d').date(), datetime.strptime(limit_right, '%Y-%m-%d').date()])
    ax.invert_yaxis()
    member_counts = len(df["Member_ID"].drop_duplicates())
    ax.set_title(f"Member Journey for {member_counts} members,  Time range: {limit_left} ~ {limit_right}", fontsize=20, fontname="Impact", pad=100)

    plt.xticks(rotation=45)
    #plt.tight_layout()
    plt.show()

