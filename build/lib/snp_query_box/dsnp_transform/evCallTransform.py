import pandas as pd
import numpy as np
def ev_call_transform(ev_dsnp_campaign_df):
    ev_dsnp_campaign_df = ev_dsnp_campaign_df.sort_values(['src_member_id','call date'], ascending=[True,False])
    evcalls_unique_df = ev_dsnp_campaign_df.drop_duplicates(['src_member_id'], keep='first')
    evcalls_unique_df['call date'] = pd.to_datetime(evcalls_unique_df['call date'])
    index_remove_call_dispostion = evcalls_unique_df[evcalls_unique_df['call disposition'].isin(['Active CTM/Grievance/Appeal','DoNotCall',\
                                                'Record does not meet campaign eligibility requirements','System Auto-Close','Unable to Contact - No Phone Number on File',\
                                                'Auto-Close - HU Overlap'])].index
    evcalls_unique_df = evcalls_unique_df.drop(index_remove_call_dispostion)

    index_remove_call_dispostion2=evcalls_unique_df[evcalls_unique_df['call disposition'].isin(['Auto-Close - Year End Activity','ESRD',\
                                                    'Hospice','Member Deceased','Member Terminated','ACCP','Autoclose – BH Wave4 cases that overlap with Eagle',
                                                    'Autoclose – MAC Scrub requests','Long Term Care','VA Benefits'])].index
    evcalls_unique_df = evcalls_unique_df.drop(index_remove_call_dispostion2)

    evcalls_unique_df['Call_Category_MAC'] = np.nan
    evcalls_unique_df.loc[evcalls_unique_df['call disposition']=='Auto Close- Control group','Call_Category_MAC'] = 'Control Group'

    evcalls_unique_df.loc[~(evcalls_unique_df['Call_Category_MAC'].isin(['Reached','Not Reached','Control Group'])) & \
                                (evcalls_unique_df['call_status_desc'] == 'REACHED') & \
                                (evcalls_unique_df['not_reach_reason_desc'].isin(['Member not interested / Member Opt out',\
                                'Activity Completed','Follow-up'])),'Call_Category_MAC']='Reached'
    evcalls_unique_df.loc[~(evcalls_unique_df['Call_Category_MAC'].isin(['Reached','Not Reached','Control Group'])) & \
                                (evcalls_unique_df['call_status_desc'] == 'NOT REACHED') & 
                                (evcalls_unique_df['not_reach_reason_desc'].isin(['No Answer','Non-Working Number',\
                                'Phony Intercept','Left Message with Person','Left Voicemail'])),'Call_Category_MAC']='Not Reached'
    evcalls_unique_df.loc[evcalls_unique_df['call disposition'].isin(['Incomplete - Member Ended',\
                                'Member Reach - Call Completed','Declined – Member Declined Call']),'Call_Category_MAC']='Reached'        
    evcalls_unique_df.loc[evcalls_unique_df['Call_Category_MAC'].isnull() & \
                                evcalls_unique_df['call disposition'].isin(['Active CTM/Grievance/Appeal','DoNotCall',\
                                'Record does not meet campaign eligibility requirements','System Auto-Close','Unable to Contact - No Phone Number on File']),\
                                'Call_Category_MAC'] = 'Need to be Removed'
    evcalls_unique_df.loc[evcalls_unique_df['Call_Category_MAC'].isnull() & \
                                evcalls_unique_df['call disposition'].isin(['Completed on inbound call','Left Message - 1st Attempt',\
                                'Left Message - 2nd Attempt','Member already took health plan action','No Answer - 1st Attempt', 'No Answer - 2nd Attempt',\
                                'No response to follow up Outreach','Unable to Contact - Bad Phone Number']),'Call_Category_MAC'] = 'Not Reached'
    evcalls_unique_df.loc[evcalls_unique_df['Call_Category_MAC'].isnull() & \
                                evcalls_unique_df['call disposition'].isin(['Declined – Member Declined Call','Incomplete - Member Ended',\
                                'Member Reach - Call Completed','Member refused to authenticate','Member unable to verify HIPAA']),'Call_Category_MAC'] = 'Reached'

    index_name = evcalls_unique_df[evcalls_unique_df['Call_Category_MAC'] == 'Need to be Removed'].index

    evcalls_unique_df.drop(index_name,inplace=True)
    evcalls_unique_df['Left Msg'] = np.nan
    evcalls_unique_df.loc[evcalls_unique_df['call disposition']=='Auto Close- Control group','Left Msg']='Control Group'
    evcalls_unique_df.loc[((evcalls_unique_df['call_status_desc'] == 'NOT REACHED') & \
                                (evcalls_unique_df['not_reach_reason_desc'].isin(['Left Message with Person','Left Voicemail']))) |
                                evcalls_unique_df['call disposition'].isin(['Left Message - 1st Attempt','Left Message - 2nd Attempt']),\
                                'Left Msg']='Left Message/Voicemail'
    evcalls_unique_df.loc[(~evcalls_unique_df['Left Msg'].isin(['Left Message/Voicemail','Control Group'])), ['Left Msg']]='No Msg Left'
    evcalls_unique_df['Engagement_Category_MAC'] = np.nan
    evcalls_unique_df.loc[evcalls_unique_df['call disposition']=='Auto Close- Control group','Engagement_Category_MAC']='Control Group'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 (evcalls_unique_df['call_status_desc'] == 'REACHED') & \
                                 (evcalls_unique_df['not_reach_reason_desc'].isin(["Activity Completed" ])), \
                                 'Engagement_Category_MAC']='Engaged'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 (evcalls_unique_df['call_status_desc'] == 'NOT CALLED') & \
                                 (evcalls_unique_df['not_reach_reason_desc'] == 'Research'),\
                                 'Engagement_Category_MAC']='Engaged'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 (evcalls_unique_df['call disposition'].isin(['Member already took health plan action',\
                                 'Completed on inbound call'])),'Engagement_Category_MAC']='Engaged'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 (evcalls_unique_df['call_status_desc'] == 'REACHED') & \
                                 (evcalls_unique_df['not_reach_reason_desc'].isin(['Follow-up','Member Reached - Not Engaged in Campaign',\
                                 'Member not interested / Member Opt out'])),'Engagement_Category_MAC']='Not Engaged'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 (evcalls_unique_df['call_status_desc'] == 'NOT CALLED') & \
                                 (evcalls_unique_df['not_reach_reason_desc'].isin(['No Phone Number'])), \
                                 'Engagement_Category_MAC']='Not Engaged'
    evcalls_unique_df.loc[~(evcalls_unique_df['Engagement_Category_MAC'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                 ~(evcalls_unique_df['call disposition'].isin(['Completed on inbound call',\
                                 'Member already took health plan action'])),'Engagement_Category_MAC']='Not Engaged'
    evcalls_unique_df['Five_Group_Ind'] = 'Other Call Dispositions'
    evcalls_unique_df.loc[evcalls_unique_df['call disposition']=='Auto Close- Control group','Five_Group_Ind'] ='Control Group'
    evcalls_unique_df.loc[(evcalls_unique_df['Call_Category_MAC']=='Reached') & \
                                (evcalls_unique_df['Engagement_Category_MAC'] == 'Not Engaged'), \
                                'Five_Group_Ind'] ='Reached but not Engaged'
    evcalls_unique_df.loc[(evcalls_unique_df['Engagement_Category_MAC'] == 'Engaged'), \
                                'Five_Group_Ind'] ='Engaged'
    evcalls_unique_df.loc[(evcalls_unique_df['Call_Category_MAC'] == 'Not Reached') & \
                                (evcalls_unique_df['Left Msg'] == 'No Msg Left'), \
                                'Five_Group_Ind'] ='Not Reached and No Msg Left'
    evcalls_unique_df.loc[(evcalls_unique_df['Call_Category_MAC'] == 'Not Reached') & \
                                (evcalls_unique_df['Left Msg'] == 'Left Message/Voicemail'), \
                                'Five_Group_Ind'] ='Not Reached but with Msg Left'
    index_drop=evcalls_unique_df[evcalls_unique_df['Five_Group_Ind'] == 'Other Call Dispositions'].index
    evcalls_unique_df.drop(index_drop,inplace=True)

    evcalls_unique_df['src_member_id'] = evcalls_unique_df['src_member_id'].str.replace('-','')
    ev_call_df = evcalls_unique_df[['src_member_id','Engagement_Category_MAC']]
    
    return ev_call_df