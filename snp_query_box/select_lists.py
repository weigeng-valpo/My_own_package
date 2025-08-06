

pull_snp_member_select_list = ["MEDICARE_NUMBER", "Member_ID", "SNP", "cast(Eff_Date as varchar(10)) as Eff_Date",
                    "cast (Term_Date as varchar(10)) as Term_Date","Contract_Number","PBP","CMS_State",
                    "CMS_County","Age", "Gender", "FIRST_NAME", "LAST_NAME", "MIDDLE_INITIAL", "Address1",
                    "Address2", "City", "State", "left(Zip,5) as Zip", "MEM_Phone", "Writing_Agent_Name",
                    "Recruiter_Name", "TOH_Name", "Sales_Channel", "PCP_Name", "PCP_Tax_ID", "Enroll_Status",
                    "DRC_Term_Status","DRC_Term_Reason", "Movement_Type_Out", "Dual_Status"]

pull_snp_member_btw_date_select_list = ['MEDICARE_NUMBER', 'Member_ID', 'SNP', 'cast(Eff_Date as varchar(10)) as Eff_Date', 'cast (Term_Date as varchar(10)) as Term_Date','Contract_Number','PBP','CMS_State','CMS_County','Age','Gender','FIRST_NAME','LAST_NAME','MIDDLE_INITIAL',
        'Address1', 'Address2','City','State','left(Zip,5) as Zip','MEM_Phone','Writing_Agent_Name','Recruiter_Name','TOH_Name', 'Sales_Channel','PCP_Name','PCP_Tax_ID','Enroll_Status','DRC_Term_Status',"DRC_Term_Reason", 'Movement_Type_Out', "Dual_Status"]

pull_ev_call_select_list = ["mfb.[focus_desc] as 'campaign'",
    "mfb.[call_script_desc] as 'sub campaign'",
    "mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc'",
    "mfb.[mfs_cur_focus_status_desc] as 'focus status'",
    "c.[call_timestamp] as 'call date'",
    "c.[call_status_desc]", 
    "c.[not_reach_reason_desc]", 
    "casereason.lookup_cd_display_nm as 'call disposition'",
    "concat('',mfb.[medicare_member_id]) as 'src_member_id'",
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
    "mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc'",
    "mfb.[mfs_cur_focus_status_desc] as 'focus status'",
    "c.[call_timestamp] as 'call date'",
    "c.[call_status_desc]", 
    "c.[not_reach_reason_desc]", 
    "casereason.lookup_cd_display_nm as 'call disposition'",
    "concat('',mfb.[medicare_member_id]) as 'src_member_id'",
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


pull_call_log_all_select_list = [
    'src_sys_nm', 
    'src_file_nm', 
    'cl_call_log_id',
    'mcl_member_id', 
    '(case when mli.mbr_src_member_id is null then call.mcl_identifier_value_id else mli.mbr_src_member_id end) as mcl_identifier_value_id',
    'cl_call_log_drctn_type_desc',
    'cl_call_log_drctn_type_key_cd',
    'cl_call_log_method_type_desc', 
    'cl_call_log_method_type_key_cd', 
    'cl_call_log_rel_type_desc',
    'cl_call_log_rel_type_key_cd',
    'cl_call_log_source_type_desc',
    'cl_call_log_source_type_key_cd',
    'cl_call_log_type_desc',
    'cl_call_log_type_key_cd',
    'cl_contact_dts',
    'cl_effective_dts',
    'cl_inserted_by_id',
    'mcl_inserted_by_id',
    'cl_member_program_id', 
    'cl_program_type_desc',
    'cl_program_type_key_cd',
    'cl_object_typ_desc', 
    'cl_object_typ_key_cd', 
    'cl_attempt_status_key_desc', 
    'cl_attempt_status_typ_key_cd',
    'cl_category_typ_desc',
    'cl_category_typ_key_cd'
]

pull_call_log_select_list = pull_call_log_all_select_list

pull_call_log_all_dsnp_select_list = pull_call_log_all_select_list +\
      ['cl_comments_txt',
       'cl_contact_phone_number_2_nbr',
       'cl_contact_phone_number_1_nbr',
       'cl_contact_email_txt',
       'cl_attempts_typ_key_desc']


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

pull_risk_strat_ca_select_list = [
    'carriermemid',
    'Report_Stratification', 
    'Stratification', 
    'Stratification_Date'
]

pull_vbc_report_snp_members_select_list = [
    'Member_ID', 
    'MEDICARE_NUMBER', 
    'FIRST_NAME',
    'MIDDLE_INITIAL',
    'LAST_NAME',
    'DOB', 
    'Age', 
    'Gender',
    'Address1', 
    'Address2',
    'left(Zip,5) as Zip',
    'cast(Eff_Date as varchar(10)) as Eff_Date', 
    'cast (Term_Date as varchar(10)) as Term_Date', 
    'Mbr_Mths', 
    'Contract_Number',
    'PBP',
    'Product', 
    'CMS_County', 
    'CMS_State', 
    'NEW_REGION', 
    'MA_Territory', 
    'FIPS', 
    'Plan_Type', 
    'dual_Status', 
    'PCP_Name', 
    'PCP_Tax_ID', 
    'MEM_Phone as Member_Phone',
    'TRR_Parent_Name', 
    'TOH_Name', 
    'Recruiter_Name', 
    'Writing_Agent_Name', 
    'Sales_Channel', 
    'New_Market', 
    'SNP', 
    'LIS_Flag',
    'Enroll_Status',
    'Issued_Status',
    'Group_Status', 
    'MA_Market_PDP_Product as Market'
    ]

pull_annual_wellness_visit_select_list = [
    'substr(ecl.member_id,1,12) as Member_ID', 
    'ecl.srv_start_dt', 
    'pri_icd9_dx_cd,ecl.PRCDR_CD', 
    'ecl.srv_spclty_ctg_cd', 
    'ecl.subctg_short_nm'
]