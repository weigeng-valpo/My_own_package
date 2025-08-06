import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.ServerConnection import ServerConnection
import warnings
warnings.filterwarnings("ignore")

"""
This is to keep data pull queries as functions.
By keeping queries here we can avoid long jupyter notebooks and it is easy to maintain and debug it. 
"""

def pull_icp_audit_trail_curated_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery
        
    query="""select member_memberid
                    , identifiervalue
                    , careplan_careplanid
                    , careplan_ammappinggroupid
                    , careplan_title
                    , careplan_careplandescription
                    , careplan_ownersecurityuserid
                    , careplan_careplanstatustypekey
                    , careplan_careplanstatustypekeydescription
                    , careplan_careplansourcetypekey
                    , careplan_careplansourcetypekeydescription
                    , careplan_othersource
                    , careplan_startdate
                    , careplan_estimateenddate
                    , careplan_nextreviewdate
                    , careplan_closeddate
                    , careplan_careplanreviewed
                    , careplan_lastreviewersecurityuserid
                    , careplan_lastreviewerdate
                    , careplan_lastupdated
                    , careplan_currentreviewersecurityuserid
                    , careplan_currentreviewerdate
                    , careplan_reviewernote
                    , careplan_memberprogramid
                    , careplan_programtypekeydescription
                    , careplan_effectivedate
                    , careplan_expirationdate
                    , careplan_insertedon
                    , careplan_updatedon
                    , careplan_voidedon
                    , careplan_activeflag
                    , count(*) as rr 
            from 
                    (select (case when mli.mbr_src_member_id is null then cp.cp_identifier_value_id else mli.mbr_src_member_id end) as identifiervalue
                        , mbr_member_id member_memberid
                        , cp_care_plan_id careplan_careplanid
                        , cp_am_mapping_group_id careplan_ammappinggroupid
                        , cp_title_txt careplan_title
                        , cp_care_plan_desc careplan_careplandescription
                        , cp_owner_security_user_id careplan_ownersecurityuserid
                        , cp_care_plan_stts_type_key_cd careplan_careplanstatustypekey
                        , cp_care_plan_stts_type_desc careplan_careplanstatustypekeydescription
                        , cp_care_plan_src_type_key_cd careplan_careplansourcetypekey
                        , cp_care_plan_src_type_desc careplan_careplansourcetypekeydescription
                        , cp_other_source_txt careplan_othersource
                        , cp_start_dts careplan_startdate
                        , cp_estimate_end_dts careplan_estimateenddate
                        , cp_next_review_dts careplan_nextreviewdate
                        , cp_closed_dts careplan_closeddate
                        , cp_care_plan_reviewed_ind careplan_careplanreviewed
                        , cp_last_rvwr_scrty_user_id careplan_lastreviewersecurityuserid
                        , cp_last_reviewer_dts careplan_lastreviewerdate
                        , cp_last_updated_dts careplan_lastupdated
                        , cp_current_rvwr_scrty_user_id careplan_currentreviewersecurityuserid
                        , cp_current_reviewer_dts careplan_currentreviewerdate
                        , cp_reviewer_note_txt careplan_reviewernote
                        , cp_member_program_id careplan_memberprogramid 
                        , cp_program_type_desc careplan_programtypekeydescription
                        , cp_effective_dts careplan_effectivedate
                        , cp_expiration_dts careplan_expirationdate
                        , cp_inserted_on_dts careplan_insertedon
                        , cp_updated_on_dts careplan_updatedon
                        , cp_voided_on_dts careplan_voidedon
                        , cp_active_flag_ind careplan_activeflag
                    from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.CARE_PLAN` cp 
                    inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli on cp.cp_member_id=mli.mbr_member_id
                    where mli.mi_line_of_business_typ_key_cd = 'Medicare'
                ) t
            group by all"""
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["identifiervalue"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_icp_case_note_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery

    query="""
    select mli.mbr_src_member_id
        , mbr.mbr_member_id as member_id
        , mbr.mbr_conversion_id as conversion_id
        , mbr.mbr_identifier_value_id as identifier_value_id
        , nt_note_id
        , nt_object_id
        , nt_note_type_desc
        , nt_object_type_desc
        , nt_updated_on_dts
        , nt_member_program_id
        , count(*) as rr
    from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_MEMBER` mbr 
        inner join 
        (select mbr_member_id, mbr_src_member_id from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE`) mli on mbr.mbr_member_id=mli.mbr_member_id
        inner join 
        (select mpr_member_id, mpr_member_program_id from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM`  where  mpr_program_type_key_cd='DSNP') mp on mbr.mbr_member_id=mp.mpr_member_id
        left join 
            (select nt_note_id
            , nt_object_id
            , nt_note_type_desc
            , nt_object_type_desc
            , nt_updated_on_dts
            , nt_member_program_id
            , bus_exp_dt
            from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ALL_NOTES`
            where (nt_note_type_desc in ('Care Planning',
                                        'Initial - Care Plan',
                                        'Annual - Care Plan',
                                        'Medications',
                                        'Social Work',
                                        'Care Coordination',
                                        'Supplemental Benefits',
                                        'Interdisciplinary Care Team (ICT)',
                                        'Change in Status - Care Plan',
                                        'Post Discharge',
                                        'Pre Admission') and 
                nt_object_type_desc in ('Care Plan Intervention',
                                        'Care Plan',
                                        'Care Plan Problem',
                                        'Member',
                                        'Care Plan Goal'))
            ) note on mp.mpr_member_program_id = note.nt_member_program_id
        group by all
    """
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    if mbr_id_list is not None:
        return df[df["identifier_value_id"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_icp_risk_strat_with_lob_bq(mbr_id_list):
    import subprocess
    from google.cloud import bigquery

    query = """
        select distinct
            (case when mli.mbr_src_member_id is null then mbr.mbr_identifier_value_id else mli.mbr_src_member_id end) as identifier_value_id1
            , mbr.mbr_identifier_value_id as identifier_value_id
            , a.mpr_member_id,a.mpr_member_program_id
            , a.mpr_risk_strat_typ_cd,a.mpr_risk_strat_typ_desc    
            , a.mpr_updated_by_user_nm
            , a.mpr_updated_by_first_nm
            , a.mpr_updated_by_last_nm
            , a.mpr_pgm_strtfctn_type_key_cd
            ,a.mpr_updated_on_dts as mpr_risk_strat_date
        from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_PROGRAM` a
        inner join 
            (select mbr_identifier_value_id, mbr_member_id
            from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_MEMBER`) mbr
        on mbr.mbr_member_id=a.mpr_member_id
        inner join 
            (select mbr_src_member_id, mbr_member_id
            from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE`
            where mi_line_of_business_typ_key_cd = 'Medicare') mli
        on mbr.mbr_member_id=mli.mbr_member_id 
        where a.mpr_program_type_key_cd in ('DSNP','CSNP')
        and (a.mpr_risk_strat_typ_cd in ('L', 'M', 'H') 
        or a.mpr_pgm_strtfctn_type_key_cd in ('L', 'M', 'H'))
    """
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe() 
    if mbr_id_list is not None:
        return df[df["identifier_value_id1"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_hra_call_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery

    query = """
        select call.src_sys_nm
        , call.src_file_nm
        , cl_call_log_id
        , call.mcl_member_id
        , (case when mli.mbr_src_member_id is null then call.mcl_identifier_value_id else mli.mbr_src_member_id end) as mcl_identifier_value_id
        , cl_call_log_drctn_type_desc
        , cl_call_log_drctn_type_key_cd
        , call.cl_attempts_typ_key_desc
        , cl_call_log_method_type_desc
        , cl_call_log_method_type_key_cd
        , cl_call_log_rel_type_desc
        , cl_call_log_rel_type_key_cd
        , cl_call_log_source_type_desc
        , cl_call_log_source_type_key_cd
        , cl_call_log_type_desc
        , cl_call_log_type_key_cd
        , cl_contact_dts
        , cl_effective_dts
        , cl_inserted_by_id
        , mcl_inserted_by_id
        , cl_member_program_id
        , cl_program_type_desc
        ,cl_program_type_key_cd
        , cl_attempt_status_key_desc
        , cl_attempt_status_typ_key_cd
        , cl_category_typ_desc
        , cl_category_typ_key_cd
        from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR` call
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli on call.mcl_member_id = mli.mbr_member_id
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CURATED_SECURITY_AUDIT` sa on call.cl_inserted_by_id=sa.suc_security_user_context_id 
        where cl_program_type_key_cd='DSNP'
        and cl_call_log_method_type_desc in ('Phone - Outbound', 'In Person - Member Residence', 'Phone - Inbound', 'In Person - Community Location', 'In Person - Other Residence','In Person - Provider Location', 'Video Conferencing - Inbound','Video Conferencing - Outbound')
        and (
            (cl_call_log_type_desc in ('HRA Outreach', 'Care Planning', 'Case Management', 'Care Coordination','Annual Face to Face Visit')
                and cl_call_log_rel_type_desc in ('Member/Self','Family/Spouse','Family/Daughter', 'Family/Parent', 'Guardian', 'Family/Son', 'Friend', 'Family/Sibling', 'Family/Other', 'Power of Attorney', 'Family/Grandchild', 'Health Care Proxy', 'Third Party Admin', 'Appointment of Representative', 'Family/Partner', 'EXP Friend', 'EXP Guardian'))
            or 
            (cl_call_log_type_desc = 'HRA Outreach' and cl_call_log_rel_type_desc = 'Other' and sa.su_user_nm = 'CECUser'))
    """
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["mcl_identifier_value_id"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_ict_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery

    query = """ 
        select asm_id_value_desc
        , asm_assmnt_typ_key_cd
        , asm_assmnt_typ_desc
        , asm_assmnt_status_typ_key_cd
        , asm_assmnt_status_typ_desc
        , asm_template_nm
        , asm_eff_dts
        , asad_assmnt_typ_key_cd
        , asad_assmnt_typ_desc
        , asad_assmnt_status_typ_key_cd
        , asad_assmnt_status_typ_desc
        , asad_template_nm
        , asad_template_desc
        , asad_template_version_nbr
        , asad_obj_typ_key_cd
        , asad_obj_typ_desc
        , asad_page_nm
        , asad_page_desc
        , asad_tab_nm
        , asad_tab_desc
        , asad_section_desc
        , asad_question_txt
        , asad_assmnt_ans_typ_key_cd
        , asad_assmnt_ans_typ_desc
        , asad_assmnt_ans_data_txt
        , asad_assmnt_ans_nbr_txt
        , asad_assmnt_question_nbr_txt
        , asad_value_txt
        , asad_eff_dts
        , asad_exp_dts
        , asad_inserted_on_dts
        , asad_updated_on_dts
        , asad_voided_by_id
        , asad_voided_on_dts
        , asad_voided_reason_id
        , asad_voided_reason_desc
        , asad_active_flag_ind
        , asad_voided_on_dts
        , asm_voided_on_dts
        , bus_eff_dt
        , bus_exp_dt
        from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ASSESSMENT
        where asm_assmnt_status_typ_key_cd = 'Completed'
            and asm_template_nm = 'ICT_IDT Notes'
    """
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["asm_id_value_desc"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_ict_case_note_from_assessment_bq(mbr_id_list, asad_updated_on_dts):
    import subprocess
    from google.cloud import bigquery

    query = """
        select asm_id_value_desc
        , asm_assmnt_typ_key_cd
        , asm_assmnt_typ_desc
        , asm_assmnt_status_typ_key_cd
        , asm_assmnt_status_typ_desc
        , asm_template_nm
        , asm_eff_dts
        , asad_assmnt_typ_key_cd
        , asad_assmnt_typ_desc
        , asad_assmnt_status_typ_key_cd
        , asad_assmnt_status_typ_desc
        , asad_template_nm
        , asad_template_desc
        , asad_template_version_nbr
        , asad_obj_typ_key_cd
        , asad_obj_typ_desc
        , asad_page_nm
        , asad_page_desc
        , asad_tab_nm
        , asad_tab_desc
        , asad_section_desc
        , asad_question_txt
        , asad_assmnt_ans_typ_key_cd
        , asad_assmnt_ans_typ_desc
        , asad_assmnt_ans_data_txt
        , asad_assmnt_ans_nbr_txt
        , asad_assmnt_question_nbr_txt
        , asad_value_txt
        , asad_eff_dts
        , asad_exp_dts
        , asad_inserted_on_dts
        , asad_updated_on_dts
        , asad_voided_by_id
        , asad_voided_on_dts
        , asad_voided_reason_id
        , asad_voided_reason_desc
        , asad_active_flag_ind
        , asad_voided_on_dts
        , asm_voided_on_dts
        , bus_eff_dt
        , bus_exp_dt
        from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ASSESSMENT
        where asm_assmnt_status_typ_key_cd = 'Completed'
        and asm_template_nm = 'ICT_IDT Notes'
        and asad_voided_on_dts is null
        and asm_voided_on_dts is null
        and cast(asad_updated_on_dts AS DATE) <= @updated_max
    """

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "updated_max",
            "DATE",
            asad_updated_on_dts
            )
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["asm_id_value_desc"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_hospitalization_pre_auth_records_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery

    query = """
        select distinct mbr_identifier_value_id
        , bus_eff_dt
        , bus_exp_dt
        , rec_strt_dt
        , rec_end_dt
        , sa_service_auth_nbr
        , sa_received_dts
        , sa_status_dts
        , sa_admit_dts
        , sa_expected_discharge_dts
        , sa_discharge_dts
        , sa_next_review_dts
        , sa_sat_typ_key_cd
        , sa_sat_typ_desc
        , sa_status_typ_key_cd
        , sa_status_typ_desc
        , sa_admission_status_typ_key_cd
        , sa_admission_status_typ_desc
        , sa_discharge_typ_key_cd
        , sa_discharge_typ_desc
        , sa_category_typ_key_cd
        , sa_category_typ_desc
        , sa_active_flag_ind
        , sa_effective_dts
        , sa_expiration_dts
        , sa_voided_on_dts
        , sa_updated_on_dts
        from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_SRVC_AUTH`
        where sa_sat_typ_desc in ('BH Inpatient','Transition of Care','Medical Inpatient','NME Inpatient')
        and sa_admission_status_typ_desc in ('Emergency','Elective','Other','Transfer','Maternity','Urgent')
    """

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["mbr_identifier_value_id"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()

def pull_post_discharge_call_bq(cl_contact_dts):
    import subprocess
    from google.cloud import bigquery
    query = '''
        select src_sys_nm
        , src_file_nm
        , cl_call_log_id
        , mcl_member_id
        , mcl_identifier_value_id
        , cl_call_log_drctn_type_desc
        , cl_call_log_drctn_type_key_cd
        , cl_call_log_method_type_desc
        , cl_call_log_method_type_key_cd
        , cl_call_log_rel_type_desc
        , cl_call_log_rel_type_key_cd
        , cl_call_log_source_type_desc
        , cl_call_log_source_type_key_cd
        , cl_call_log_type_desc
        , cl_call_log_type_key_cd
        , cl_contact_dts
        , cl_effective_dts
        , cl_inserted_by_id
        , mcl_inserted_by_id
        , cl_member_program_id
        , cl_program_type_desc
        , cl_program_type_key_cd
        , cl_object_typ_desc
        , cl_object_typ_key_cd
        , cl_attempt_status_key_desc
        , cl_attempt_status_typ_key_cd
        , cl_category_typ_desc
        , cl_category_typ_key_cd
        from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR
        where cl_program_type_key_cd='DSNP'
        and cast(cl_contact_dts as date) <= @max_call_date
        and cl_call_log_method_type_desc in ('In Person - Community Location', 'In Person - Member Residence','In Person - Post Acute Care Facility', 'In Person - Acute Care Facility','In Person - Other Residence','In Person - Provider Location','Phone - Inbound','Phone - Outbound','Video Conferencing - Inbound','Video Conferencing - Outbound','Text - Inbound')
        and cl_call_log_type_desc in ('Post Discharge Outreach')
        and cl_call_log_rel_type_desc in ('Appointment of Representative','Family/Daughter','Family/Grandchild','Family/Other','Family/Parent','Family/Partner','Family/Sibling','Family/Son','Family/Spouse','Friend','Guardian','Health Care Proxy','Member/Self','Power of Attorney','Third Party Admin', 'Appointment of Representative','Family/Partner','EXP Friend','EXP Guardian')
    '''
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "max_call_date",
            "DATE",
            cl_contact_dts
            )
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe() 
    return df

def pull_hospitalization_approved_days_bq(min_admit_date, max_admit_date):
    import subprocess
    from google.cloud import bigquery
    query = '''
    select auth.mbr_identifier_value_id
    , auth.sa_service_auth_nbr
    , auth.sa_admit_dts
    , max(auth.sa_discharge_dts) as discharge_date
    , max(line.sl_approved_qty) as approved_qty
    , max(line.sl_requested_qty) as requested_qty
    from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_SRVC_AUTH` auth
    left join 
        (select line.sa_service_auth_id, line.sl_approved_qty, line.sl_requested_qty, count(*) as r 
        from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_SRVC_AUTH_LINE` line
        group by all) line 
    on auth.sa_service_auth_id=line.sa_service_auth_id
    where auth.sa_sat_typ_desc in ('BH Inpatient','Transition of Care','Medical Inpatient','NME Inpatient')
    and auth.sa_admission_status_typ_desc in ('Emergency','Elective','Other','Transfer','Maternity','Urgent')
    and cast(auth.sa_admit_dts as date) between @min_admit_date and @max_admit_date
    group by all
    '''

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "min_admit_date",
            "DATE",
            min_admit_date
            ),
            bigquery.ScalarQueryParameter(
            "max_admit_date",
            "DATE",
            max_admit_date
            )
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query, job_config=job_config)        
    df = query_job.to_dataframe()
    return df.drop_duplicates()

def pull_fax_and_portal_instruction_bq():
    import subprocess
    from google.cloud import bigquery

    query = '''
    select de.dp_member_id,
        (case when mli.mbr_src_member_id is null then de.dpe_id_value_txt else mli.mbr_src_member_id end) as Member_ID,
        dpe_mbr_conversion_id as Medicare_Number,
        de.dp_title_txt, de.dp_dcmnt_dts, dpv.pvd_dcmnt_status_typ_key_cd,
        dpv.pvd_dcmnt_status_typ_key_desc,dpv.pvd_effective_dts,dpv.pvd_sent_dts,dpv.dp_voided_on_dts
    from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_DCMNT_EVENT` de
        left join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_DCMNT_PRINT_VNDR` dpv on de.dp_member_id=dpv.dp_member_id and de.dp_dcmnt_no=dpv.dp_dcmnt_no
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli on de.dp_member_id = mli.mbr_member_id
    where de.dp_title_txt like '%SNP ICT Fax Notification (Provider)%' 
        or de.dp_title_txt like '%SNP Portal Instructions (Provider).docx%' 
        or de.dp_title_txt like '%SNP Portal Instructions (Provider)%'
    '''

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    return df

# pull raw hra data - this function should be  in the query box. I could not find it. So, I copy the function here.
def pull_hra_data_bq():
    import subprocess
    from google.cloud import bigquery
    query = f'''
    with asm as (
        select a.asm_object_id
            , a.asm_id_value_desc
            , a.asm_template_nm
            , a.asm_update_on_dts as hra_update_date
            , a.asm_inserted_by_id as inserted_by
            , a.asad_assmnt_ans_data_txt as HRA_Completion_Date
            , a.asm_assmnt_id
        from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ASSESSMENT a
        where a.asm_template_nm in ('Healthy Home Visit','DSNP Health Risk Assessment', 'DSNP Health Risk Assessment (Converted- Refer to PDF)','Health Risk Assessment') 
            and a.asm_assmnt_status_typ_key_cd in ('Completed')
            and a.asad_section_nm = 'General Information'
            and (a.asad_question_txt = 'Assessment Date:' or a.asad_question_txt = 'Assessment Completion Date:')
            and a.asm_voided_on_dts is NULL 
    )
    select distinct (case when substr(mli.mbr_src_member_id,1,2)='10' then mli.mbr_src_member_id
            when substr(mbr.mbr_identifier_value_id,1,2)='10' then mbr.mbr_identifier_value_id
            when substr(mp.mpr_identifier_value_id,1,2)='10' then mp.mpr_identifier_value_id
            when substr(asm.asm_id_value_desc,1,2)='10' then asm.asm_id_value_desc
            else mli.mbr_src_member_id end) as source_member_id    
        , asm.hra_update_date
        , asm.inserted_by
        , asm.hra_completion_date
        , asm.asm_assmnt_id
        , asm_template_nm
        , mli.mi_policy_ident_txt
        , mli.mi_effective_dts as Coverage_Eff_Date
        , mli.mi_termination_dts as Coverage_Term_Date
        , mp.mpr_enrollment_dts as Program_Enroll_Date
        , mp.mpr_closure_dts as Program_Closure_Date
        , mp.mpr_program_status_type_desc as Program_Status
        , mp.mpr_elig_determination_dts as Eligible_Determination_Date
        , mp.mpr_last_visit_dts as HRA_Due_Date
    from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_MEMBER mbr 
    inner join edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE mli on mbr.mbr_member_id=mli.mbr_member_id
    inner join edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_PROGRAM mp on mbr.mbr_member_id=mp.mpr_member_id
    right join asm on mbr.mbr_member_id=asm.asm_object_id
    where mp.mpr_program_type_key_cd like '%SNP'
        and mli.mi_plan_benefit_package_nm like '%SNP%'
    '''
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    return df.drop_duplicates()

# pull doc event utr data for table 2IA - this need to be added to the query_box
def pull_doc_event_utr_bq():
    import subprocess
    from google.cloud import bigquery

    query = '''
    select de.dp_member_id,
        (case when mli.mbr_src_member_id is null then de.dpe_id_value_txt else mli.mbr_src_member_id end) as Member_ID,
        dpe_mbr_conversion_id as Medicare_Number,
        de.dp_title_txt, de.dp_dcmnt_dts, dpv.pvd_dcmnt_status_typ_key_cd,
        dpv.pvd_dcmnt_status_typ_key_desc,dpv.pvd_effective_dts,dpv.pvd_sent_dts,dpv.dp_voided_on_dts
    from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_DCMNT_EVENT` de
        left join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_DCMNT_PRINT_VNDR` dpv on de.dp_member_id=dpv.dp_member_id and de.dp_dcmnt_no=dpv.dp_dcmnt_no
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli on de.dp_member_id = mli.mbr_member_id
    where (de.dp_title_txt like '%UTR%') or (de.dp_title_txt like '%Unable To Reach%') or (de.dp_title_txt like '%Opt Out%') or (de.dp_title_txt like '%HRA%Package%')
    '''

    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    return df

# pull hra non dsnp call - this needs to be added to the query box
def pull_hra_non_dsnp_call_bq(mbr_id_list=None):
    import subprocess
    from google.cloud import bigquery

    query = """
        select call.src_sys_nm
        , call.src_file_nm
        , cl_call_log_id
        , call.mcl_member_id
        , (case when mli.mbr_src_member_id is null then call.mcl_identifier_value_id else mli.mbr_src_member_id end) as mcl_identifier_value_id
        , cl_call_log_drctn_type_desc
        , cl_call_log_drctn_type_key_cd
        , call.cl_attempts_typ_key_desc
        , cl_call_log_method_type_desc
        , cl_call_log_method_type_key_cd
        , cl_call_log_rel_type_desc
        , cl_call_log_rel_type_key_cd
        , cl_call_log_source_type_desc
        , cl_call_log_source_type_key_cd
        , cl_call_log_type_desc
        , cl_call_log_type_key_cd
        , cl_contact_dts
        , cl_effective_dts
        , cl_inserted_by_id
        , mcl_inserted_by_id
        , cl_member_program_id
        , cl_program_type_desc
        ,cl_program_type_key_cd
        , cl_attempt_status_key_desc
        , cl_attempt_status_typ_key_cd
        , cl_category_typ_desc
        , cl_category_typ_key_cd
        from `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR` call
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli on call.mcl_member_id = mli.mbr_member_id
        inner join `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CURATED_SECURITY_AUDIT` sa on call.cl_inserted_by_id=sa.suc_security_user_context_id 
        where cl_program_type_key_cd!='DSNP'
        and cl_call_log_method_type_desc in ('Phone - Outbound', 'In Person - Member Residence', 'Phone - Inbound', 'In Person - Community Location', 'In Person - Other Residence','In Person - Provider Location', 'Video Conferencing - Inbound','Video Conferencing - Outbound')
        and (
            (cl_call_log_type_desc in ('HRA Outreach', 'Care Planning', 'Case Management', 'Care Coordination','Annual Face to Face Visit')
                and cl_call_log_rel_type_desc in ('Member/Self','Family/Spouse','Family/Daughter', 'Family/Parent', 'Guardian', 'Family/Son', 'Friend', 'Family/Sibling', 'Family/Other', 'Power of Attorney', 'Family/Grandchild', 'Health Care Proxy', 'Third Party Admin', 'Appointment of Representative', 'Family/Partner', 'EXP Friend', 'EXP Guardian'))
            or 
            (cl_call_log_type_desc = 'HRA Outreach' and cl_call_log_rel_type_desc = 'Other' and sa.su_user_nm = 'CECUser'))
    """
    client = bigquery.Client(project='edp-prod-mdcrbi-starsbi')
    try:
        query_job = client.query(query)
    except:
        print("GCP, Need authentication!")
        subprocess.call('gcloud auth application-default login',shell=True)
        query_job = client.query(query)        
    df = query_job.to_dataframe()
    if mbr_id_list is not None:
        return df[df["mcl_identifier_value_id"].str.strip().isin(mbr_id_list)].drop_duplicates()
    else:
        return df.drop_duplicates()