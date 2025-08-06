

pull_snp_member_select_list = ["MEDICARE_NUMBER", "Member_ID", "SNP", "cast(Eff_Date as varchar(10)) as Eff_Date",
                    "cast (Term_Date as varchar(10)) as Term_Date","Contract_Number","PBP","CMS_State",
                    "CMS_County","Age", "Gender", "FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL", "Address1",
                    "Address2", "City", "State", "left(Zip,5) as Zip", "MEM_Phone", "Writing_Agent_Name",
                    "Recruiter_Name", "TOH_Name", "Sales_Channel", "PCP_Name", "PCP_Tax_ID", "Enroll_Status",
                    "DRC_Term_Status","Movement_Type_Out"]

pull_snp_member_btw_date_select_list = ['MEDICARE_NUMBER', 'Member_ID', 'SNP', 'cast(Eff_Date as varchar(10)) as Eff_Date', 'cast (Term_Date as varchar(10)) as Term_Date','Contract_Number','PBP','CMS_State','CMS_County','Age','Gender','FIRST_NAME','LAST_NAME','MIDDLE_INITIAL',
        'Address1', 'Address2','City','State','left(Zip,5) as Zip','MEM_Phone','Writing_Agent_Name','Recruiter_Name','TOH_Name', 'Sales_Channel','PCP_Name','PCP_Tax_ID','Enroll_Status','DRC_Term_Status','Movement_Type_Out']

pull_hedis_select_list = ['bou.measure_id',
            'bou.individual_id',
            'bou.member_id',
            'bou.SRC_MEMBER_ID',
            'bou.reporting_year',
            'bou.DENOMINATOR',
            'bou.NUMERATOR',
            'bou.EPISODE_DT',
            'bou.COMPLIANCE_DT',
            'bou.CURR_GAP_IND',
            'bou.PRIOR_GAP_IND',
            'bou.CURRENT_RECORD_IND',
            'mbr.CMS_CNTRCT_NBR',
            'mbr.HICN_NBR',
            'mbr.PBP_ID']

pull_star_measure_select_list = ['dashboard_year','measure_id', 'measure_nm', 'domain','trend_ind', 
        'forecast_weight','star_5_forecast','star_4_forecast','star_3_forecast','star_2_forecast']

pull_weightage_measure_select_list = pull_star_measure_select_list

pull_ev_call_select_list = ["mfb.[focus_desc] as 'campaign'",
    "mfb.[call_script_desc] as 'sub campaign'",
     "concat('` ',mfb.[member_id]) as 'member_id'",
     "mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc'",
    "mfb.[mfs_cur_focus_status_desc] as 'focus status'",
    "c.[call_timestamp] as 'call date'",
    "c.[call_status_desc]", 
    "c.[not_reach_reason_desc]", 
    "casereason.lookup_cd_display_nm as 'call disposition'",
    "concat('',mfb.[medicare_member_id]) as 'medicare_member_id'",
    "concat('',i.[individual_id]) as 'individual_id'",
    "i.first_nm", 
    "i.last_nm", 
    "i.birth_dt", 
    "i.gender_desc", 
    "i.address_line_1_txt", 
    "i.address_line_2_txt",
    "i.city_nm",
    "i.state_postal_desc", 
    "i.zip_cd",
    "i.region_desc", 
    "i.member_phone_nbr", 
    "i.cms_cntrct_nbr",
    "i.pbp_id",
    "i.lis_desc"]

pull_ev_call_sop_select_list = [
    "mfb.[focus_desc] as 'campaign'",
    "mfb.[call_script_desc] as 'sub campaign'",
    "concat('` ',mfb.[member_id]) as 'member_id'",
    "mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc'",
    "mfb.[mfs_cur_focus_status_desc] as 'focus status'",
    "c.[call_timestamp] as 'call date'",
    "c.[call_status_desc]", 
    "c.[not_reach_reason_desc]", 
    "casereason.lookup_cd_display_nm as 'call disposition'",
    "concat('',mfb.[medicare_member_id]) as 'medicare_member_id'",
    "concat('',i.[individual_id]) as 'individual_id'",
    "i.first_nm", 
    "i.last_nm", 
    "i.birth_dt", 
    "i.gender_desc", 
    "i.address_line_1_txt", 
    "i.address_line_2_txt", 
    "i.city_nm",
    "i.state_postal_desc", 
    "i.zip_cd",
    "i.region_desc", 
    "i.member_phone_nbr", 
    "i.cms_cntrct_nbr",
    "i.pbp_id",
    "i.lis_desc"
    ]

pull_disability_esrd_select_list = [
    'MEMBER_ID',
    'SRC_MEMBER_ID',
    'INDIVIDUAL_ID',
    'case when HOSPICE_IND>=1 then 1 else 0 end as HOSPICE_IND',
    'case when DISABILITY_IND>=1 then 1 else 0 end as DISABILITY_IND',
    'case when ESRD_IND>=1 then 1 else 0 end as ESRD_IND',
    'case when OUT_OF_AREA_FLAG>=1 then 1 else 0 end as OUT_OF_AREA_FLAG'
]

pull_prefer_language_select_list = [    
    'MEDICARE_NBR',
    'PREF_SPOKEN_LANG',
    'PREF_WRITTEN_LANG'
]

pull_call_log_nj_select_list = [
    "'Dynamo' as src_sys_nm", 
    "'calllogs' as src_file_nm",
    "cms.Hic as MedicareID", 
    "m.memid as QNXT_MemberID", 
    "m.fullname",
    "'H6399' as Contract", 
    "'001' as PBP", 
    "'Member/Self' as cl_call_log_rel_type_desc",
    "NOTE_CREATE_DATE as cl_contact_dts",
    "Outreach_Outcome_descr as ca_attempt_status_key_desc", 
    "created_by as cl_inserted_by_user_nm",
    "Outreach_type as cl_call_log_type_desc", 
    "'Dynamo' as Source",
    "m.carriermemid as Member_ID" 
]

pull_call_log_nj_all_select_list = pull_call_log_nj_select_list

pull_call_log_va_select_list = [
    "'Dynamo' as src_sys_nm", 
    "'calllogs' as src_file_nm", 
    "cms.Hic as MedicareID", 
    "m.memid as QNXT_MemberID", 
    "m.fullname",
    "'H1610' as Contract", 
    "'001' as PBP", 
    "'Member/Self' as cl_call_log_rel_type_desc", 
    "NOTE_CREATE_DATE as cl_contact_dts",
    "Outreach_Outcome_descr as ca_attempt_status_key_desc", 
    "created_by as cl_inserted_by_user_nm",
    "Outreach_type as cl_call_log_type_desc", 
    "'Dynamo' as Source,m.carriermemid as Member_ID"
]

pull_call_log_va_all_select_list = pull_call_log_va_select_list

pull_call_log_ca_select_list = [
    "'Dynamo' as src_sys_nm", 
    "'calllogs' as src_file_nm", 
    "cms.Hic as MedicareID", 
    "m.memid as QNXT_MemberID", 
    "m.fullname",
    "'H4982' as Contract", 
    "'016' as PBP", 
    "'Member/Self' as cl_call_log_rel_type_desc", 
    "NOTE_CREATE_DATE as cl_contact_dts",
    "Outreach_Outcome_descr as cl_attempt_status_key_desc", 
    "created_by as cl_inserted_by_user_nm",
    "Outreach_type as cl_call_log_type_desc", 
    "'Dynamo' as Source"
]

pull_letter_ca_select_list = [
    "'Dynamo' as src_sys_nm", 
    "'documents' as src_file_nm", 
    "cms.Hic as MedicareID", 
    "m.memid as QNXT_MemberID", 
    "m.fullname",
    "'H4982' as Contract", 
    "'016' as PBP", 
    "'Member/Self' as cl_call_log_rel_type_desc", 
    "NOTE_CREATE_DATE as dp_dcmnt_dts",
    "Outreach_Outcome_descr as dp_dcmnt_category_typ_desc", 
    "NOTE_NAME as dp_title_txt", 
    "created_by as cl_inserted_by_user_nm",
    "Outreach_type as cl_call_log_type_desc", 
    "'Dynamo' as Source", "1 as Type"
    ]

pull_vbc_select_list = [ 
    'varchar(m360.SRC_MEMBER_ID) as SRC_MEMBER_ID',
    'a.Group_ID',
    'a.Group_Name', 
    'a.TIN', 
    'a.TIN_Name',
    'a.VBC', 
    'a.Provider_ID', 
    'a.Provider_Name', 
    'a.par_ind', 
    'a.primary_ind' 
    ]

pull_risk_strat_va_select_list = [
    'carriermemid',
    'Report_Stratification', 
    'Stratification', 
    'Stratification_Date'
]

pull_risk_strat_nj_select_list = pull_risk_strat_va_select_list

pull_risk_strat_ca_select_list = pull_risk_strat_nj_select_list