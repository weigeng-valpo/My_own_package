import pandas as pd
import numpy as np

def sop_campaign_transform(sop_campaign_df):
    sop_campaign_not_null_df = sop_campaign_df[~sop_campaign_df['call date'].isnull()]

    # Roll up the call records to the member and campaign level by selecting the latest call
    sop_campaign_not_null_df.sort_values(['src_member_id','campaign','sub campaign','call date'], ascending=(False), inplace=True)
    sop_campaign_unique_df = sop_campaign_not_null_df.drop_duplicates(['src_member_id','campaign','sub campaign'], keep='first')

    sop_campaign_unique_df['call date']=pd.to_datetime(sop_campaign_unique_df['call date'])
    print('Check the uniqueness of the EV Call dataset: ',sop_campaign_unique_df.shape)

    # Drop any records that falls into the removal bucket below
    index_remove_call_dispostion = sop_campaign_unique_df[sop_campaign_unique_df['call disposition'].isin(['Active CTM/Grievance/Appeal','DoNotCall',\
                            'Record does not meet campaign eligibility requirements','System Auto-Close','Unable to Contact - No Phone Number on File','Auto-Close - HU Overlap'])].index
    sop_campaign_unique_df.drop(index_remove_call_dispostion, inplace=True)

    index_remove_call_dispostion2 = sop_campaign_unique_df[sop_campaign_unique_df['call disposition'].isin(['Auto-Close - Year End Activity','ESRD',\
                                    'Hospice','Member Deceased','Member Terminated','ACCP','Autoclose – BH Wave4 cases that overlap with Eagle',
                                        'Autoclose – MAC Scrub requests','Long Term Care','VA Benefits'])].index
    sop_campaign_unique_df.drop(index_remove_call_dispostion2, inplace=True)

    sop_campaign_unique_df['Call_Category_SOP'] = np.nan
    sop_campaign_unique_df.loc[sop_campaign_unique_df['call disposition']=='Auto Close- Control group','Call_Category_SOP'] = 'Control Group'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Call_Category_SOP'].isin(['Reached','Not Reached','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'REACHED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(['Member not interested / Member Opt out',\
                                'Activity Completed','Follow-up'])),'Call_Category_SOP'] = 'Reached'
    
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Call_Category_SOP'].isin(['Reached','Not Reached','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'NOT REACHED') & 
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(['No Answer','Non-Working Number',\
                                'Phony Intercept','Left Message with Person','Left Voicemail'])),'Call_Category_SOP'] = 'Not Reached'
    
    sop_campaign_unique_df.loc[sop_campaign_unique_df['call disposition'].isin(['Incomplete - Member Ended',\
                                'Member Reach - Call Completed','Declined – Member Declined Call']),'Call_Category_SOP'] = 'Reached'        
    
    print('After categorization steps above, the new call_category has been created, and the missing in that column is: ',(sop_campaign_unique_df.Call_Category_SOP==np.nan).isnull().sum())
    sign_test_df_test = sop_campaign_unique_df.loc[:,['src_member_id','Call_Category_SOP']]
    print('The shape of big hug dataset for significant test is :', sign_test_df_test.shape)

    sop_campaign_unique_df.loc[sop_campaign_unique_df['Call_Category_SOP'].isnull() & \
    sop_campaign_unique_df['call disposition'].isin(['Active CTM/Grievance/Appeal','DoNotCall',\
    'Record does not meet campaign eligibility requirements','System Auto-Close','Unable to Contact - No Phone Number on File']),\
    'Call_Category_SOP'] = 'Need to be Removed'

    sop_campaign_unique_df.loc[sop_campaign_unique_df['Call_Category_SOP'].isnull() & \
    sop_campaign_unique_df['call disposition'].isin(['Completed on inbound call','Left Message - 1st Attempt',\
    'Left Message - 2nd Attempt','Member already took health plan action','No Answer - 1st Attempt', 'No Answer - 2nd Attempt',\
    'No response to follow up Outreach','Unable to Contact - Bad Phone Number']),'Call_Category_SOP'] = 'Not Reached'

    sop_campaign_unique_df.loc[sop_campaign_unique_df['Call_Category_SOP'].isnull() & \
    sop_campaign_unique_df['call disposition'].isin(['Declined – Member Declined Call','Incomplete - Member Ended',\
    'Member Reach - Call Completed','Member refused to authenticate','Member unable to verify HIPAA']),'Call_Category_SOP'] = 'Reached'
    index_name = sop_campaign_unique_df[sop_campaign_unique_df['Call_Category_SOP'] == 'Need to be Removed'].index
    sop_campaign_unique_df.drop(index_name, inplace=True)

    print('After categorization steps above, the new call_category has been created, and the missing in that column is: ',(sop_campaign_unique_df.Call_Category_SOP==np.nan).isnull().sum())
    sign_test_df=sop_campaign_unique_df.loc[:,['src_member_id','Call_Category_SOP']]

    print('The shape of big hug dataset for significant test is :', sign_test_df.shape)
    sign_test_df.Call_Category_SOP.value_counts(dropna=False)
    sop_campaign_unique_df['Left Msg'] = np.nan
    sop_campaign_unique_df.loc[sop_campaign_unique_df['call disposition']=='Auto Close- Control group','Left Msg']='Control Group'
    sop_campaign_unique_df.loc[((sop_campaign_unique_df['call_status_desc'] == 'NOT REACHED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(['Left Message with Person','Left Voicemail']))) |
                                sop_campaign_unique_df['call disposition'].isin(['Left Message - 1st Attempt','Left Message - 2nd Attempt']),\
                                'Left Msg'] = 'Left Message/Voicemail'
    
    sop_campaign_unique_df.loc[(~sop_campaign_unique_df['Left Msg'].isin(['Left Message/Voicemail','Control Group'])), ['Left Msg']]='No Msg Left'
    print('After categorization steps above, the new left_msg has been created, and the missing in that column is: ',\
        (sop_campaign_unique_df['Left Msg'].isnull()).isnull().sum())
    sign_test_df=sop_campaign_unique_df.loc[:,['src_member_id','Left Msg']]
    print('The shape of big hug dataset for significant test is :', sign_test_df.shape)
    sop_campaign_unique_df['Left Msg'].value_counts(dropna=False)  
    sop_campaign_unique_df['Engagement_Category_SOP'] = np.nan
    sop_campaign_unique_df.loc[sop_campaign_unique_df['call disposition']=='Auto Close- Control group','Engagement_Category_SOP']='Control Group'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'REACHED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(["Activity Completed" ])), \
                                'Engagement_Category_SOP']='Engaged'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'NOT CALLED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'] == 'Research'),\
                                'Engagement_Category_SOP']='Engaged'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                (sop_campaign_unique_df['call disposition'].isin(['Member already took health plan action',\
                                'Completed on inbound call'])),'Engagement_Category_SOP']='Engaged'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'REACHED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(['Follow-up','Member Reached - Not Engaged in Campaign',\
                                'Member not interested / Member Opt out'])),'Engagement_Category_SOP']='Not Engaged'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                (sop_campaign_unique_df['call_status_desc'] == 'NOT CALLED') & \
                                (sop_campaign_unique_df['not_reach_reason_desc'].isin(['No Phone Number'])), \
                                'Engagement_Category_SOP']='Not Engaged'
    sop_campaign_unique_df.loc[~(sop_campaign_unique_df['Engagement_Category_SOP'].isin(['Engaged','Not Engaged','Control Group'])) & \
                                ~(sop_campaign_unique_df['call disposition'].isin(['Completed on inbound call',\
                                'Member already took health plan action'])),'Engagement_Category_SOP']='Not Engaged'
    print('After categorization steps above, the new call_category has been created, and the missing in that column is: ',\
        (sop_campaign_unique_df.Engagement_Category_SOP==np.nan).isnull().sum())
    
    sign_test_df=sop_campaign_unique_df.loc[:,['src_member_id','Call_Category_SOP',\
                                        'Left Msg','Engagement_Category_SOP']]
    print('The shape of big hug dataset for significant test is :', sop_campaign_unique_df.shape)
    sop_campaign_unique_df.Engagement_Category_SOP.value_counts(dropna=False)
    print(sop_campaign_unique_df[sop_campaign_unique_df.Engagement_Category_SOP=='Engaged'].Call_Category_SOP.value_counts(dropna=False),'\n')
    print(sop_campaign_unique_df[sop_campaign_unique_df.Engagement_Category_SOP=='Not Engaged'].Call_Category_SOP.value_counts(dropna=False),'\n')
    pd.crosstab(sop_campaign_unique_df['Call_Category_SOP'],sop_campaign_unique_df['Engagement_Category_SOP'], dropna=False)
    sop_campaign_unique_df['Five_Group_Ind'] = 'Other Call Dispositions'
    sop_campaign_unique_df.loc[sop_campaign_unique_df['call disposition']=='Auto Close- Control group','Five_Group_Ind'] ='Control Group'
    sop_campaign_unique_df.loc[(sop_campaign_unique_df['Call_Category_SOP']=='Reached') & \
                                (sop_campaign_unique_df['Engagement_Category_SOP'] == 'Not Engaged'), \
                                'Five_Group_Ind'] ='Reached but not Engaged'
    sop_campaign_unique_df.loc[(sop_campaign_unique_df['Engagement_Category_SOP'] == 'Engaged'), \
                                'Five_Group_Ind'] ='Engaged'
    sop_campaign_unique_df.loc[(sop_campaign_unique_df['Call_Category_SOP'] == 'Not Reached') & \
                                (sop_campaign_unique_df['Left Msg'] == 'No Msg Left'), \
                                'Five_Group_Ind'] ='Not Reached and No Msg Left'
    sop_campaign_unique_df.loc[(sop_campaign_unique_df['Call_Category_SOP'] == 'Not Reached') & \
                                (sop_campaign_unique_df['Left Msg'] == 'Left Message/Voicemail'), \
                                'Five_Group_Ind'] ='Not Reached but with Msg Left'
    index_drop = sop_campaign_unique_df[sop_campaign_unique_df['Five_Group_Ind'] == 'Other Call Dispositions'].index
    sop_campaign_unique_df.drop(index_drop,inplace=True)
    sop_campaign_unique_df['Five_Group_Ind'].value_counts(dropna=False)
    sop_campaign_unique_df['src_member_id'] = sop_campaign_unique_df['src_member_id'].str.replace('-','')
    sop_campaign_unique_df=sop_campaign_unique_df[['src_member_id','Engagement_Category_SOP','call date']]

    return sop_campaign_unique_df