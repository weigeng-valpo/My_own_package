#This is only for GCP BQ, bq_sql should be used
#The others are for reference at this moment they should be migrate to BQ

pull_snp_member_sql = """
    SELECT DISTINCT 
    MEDICARE_NUMBER,
    Member_ID,
    SNP,
    cast(Eff_Date as varchar(10)) as Eff_Date,
    cast (Term_Date as varchar(10)) as Term_Date,
    Contract_Number,
    PBP,
    CMS_State,
    CMS_County,
    Age,
    Gender,
    FIRST_NAME,
    LAST_NAME,
    MIDDLE_INITIAL,
    Address1,
    Address2,
    City,
    State,
    left(Zip,5) as Zip,
    MEM_Phone,
    Writing_Agent_Name,
    Recruiter_Name,
    TOH_Name,
    Sales_Channel,
    PCP_Name,
    PCP_Tax_ID,
    Enroll_Status,
    DRC_Term_Status,
    DRC_Term_Reason,
    Movement_Type_Out,
    Dual_Status
    FROM dbo.MEDICARE_ENROLLMENT 
    WHERE PRODUCT='MA' 
    AND ISSUED_STATUS = 'Issued' 
    AND Group_Status='Individual'
    AND Term_Date >= @enroll_start_date
    AND Eff_Date <= @enroll_end_date
    """

pull_snp_member_bq_sql = """
    SELECT DISTINCT 
    MEDICARE_NUMBER,
    Member_ID,
    SNP,
    Eff_Date,
    Term_Date,
    Contract_Number,
    PBP,
    CMS_State,
    CMS_County,
    Age,
    Gender,
    FIRST_NAME,
    LAST_NAME,
    MIDDLE_INITIAL,
    Address1,
    Address2,
    City,
    State,
    left(Zip,5) as Zip,
    MEM_Phone,
    Writing_Agent_Name,
    Recruiter_Name,
    TOH_Name,
    Sales_Channel,
    PCP_Name,
    PCP_Tax_ID,
    Enroll_Status,
    DRC_Term_Status,
    DRC_Term_Reason,
    Movement_Type_Out,
    Dual_Status
    FROM `edp-prod-hcbstorage.edp_hcb_msts_srmepr_srcv.MEPR_SQL_MEDICARE_ENROLLMENT_VW`
    WHERE PRODUCT='MA' 
    AND ISSUED_STATUS = 'Issued' 
    AND Group_Status='Individual'
    AND Term_Date >= @enroll_start_date
    AND Eff_Date <= @enroll_end_date
    """

pull_star_member_sql = """
    SELECT *
    FROM [StarsDataHubProd].[Member].[VW_PlanPerReportingYear]
    WHERE ReportingYear >= @reporting_year1
    AND ReportingYear <= @reporting_year2
    """

pull_hedis_sql = """
    SELECT
    member_plan.individualID,
    member_plan.StarsMEMBER_ID,
    member_hedis.SourceMemberID,
    member_hedis.ReportingYear,
    member_hedis.YOYMeasureID MeasureID,
    member_hedis.MeasureDescription,
    member_hedis.Domain,
    member_hedis.StarMeasure,
    member_hedis.DENOMINATOR,
    member_hedis.NUMERATOR,
    member_hedis.EpisodeDate,
    member_hedis.ComplianceDate,
    member_hedis.CurrGapInd,
    member_hedis.PriorGapInd,
    member_plan.CurrentRecord,
    member_hedis.CMSContractNumber,
    member_plan.MBI,
    member_plan.PBPID
    FROM [StarsDataHubProd].Member.[VW_AllMeasureDetail]  member_hedis
    INNER JOIN Member.VW_PlanPerReportingYear member_plan ON member_hedis.StarsMEMBER_ID = member_plan.StarsMEMBER_ID 
    WHERE member_hedis.ReportingYear = @reporting_year1
    AND member_plan.ReportingYear = @reporting_year2
    """

pull_hedis_bq_sql =  """
    SELECT
    member_plan.individualID,
    member_plan.StarsMEMBER_ID,
    member_hedis.SourceMemberID,
    member_hedis.ReportingYear,
    member_hedis.YOYMeasureID MeasureID,
    member_hedis.MeasureDescription,
    member_hedis.Domain,
    member_hedis.StarMeasure,
    member_hedis.DENOMINATOR,
    member_hedis.NUMERATOR,
    member_hedis.EpisodeDate,
    member_hedis.ComplianceDate,
    member_hedis.CurrGapInd,
    member_hedis.PriorGapInd,
    member_plan.CurrentRecord,
    member_hedis.CMSContractNumber,
    member_plan.MBI,
    member_plan.PBPID
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_AllMeasureDetail`  member_hedis
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_PlanPerReportingYear` member_plan ON member_hedis.StarsMEMBER_ID = member_plan.StarsMEMBER_ID 
    WHERE member_hedis.ReportingYear = @reporting_year
    AND member_plan.reportingyear = @reporting_year_str
    """

pull_ked_gap_status_sql = """SELECT 
    SourceMemberID,
    nomer.StarsMember_ID as StarsMemberID,
    MBI,
    MeasureID,
    MeasureComponentID,
    LogicalOperatorCode,
    Description,
    LatestTestDT,
    SourceData,
    ClaimID,
    LatestLabResult,
    LatestCriteriaDate,
    EventDate,
    ReportingYear,
    InsertDate,
    LastUpdated,
    LastUpdateUser
    FROM [StarsDataHubProd].[HEDIS].[VW_NomerWeekly] nomer
    INNER JOIN [StarsDataHubProd].[Member].[VW_EnterpriseIDs] x ON nomer.StarsMember_ID = x.StarsMember_ID
    WHERE nomer.measureid = 600484
    AND nomer.reportingyear = @reporting_year
    """

pull_star_measure_sql = """SELECT *
    FROM [StarsModel].[VW_ForecastCutpoint]
    WHERE CurrentRecord = 1
    AND DashboardYear = @dashboard_year
    AND DomainName in ('HEDIS', 'PatientSafety')
    AND CutpointLevel = 'Mid' 
    AND ProductType = 'MAPD'
    """

pull_cahps_sql = """
    SELECT *
    --SRC_MEMBER_ID,QUESTION_ID,SHORT_QUESTION,ANSWER 
    FROM starstemp.cahps_view 
    WHERE substr(question_id,1,1) = '1'
    AND RUNYEAR=@run_year
    """

pull_measure_monthly_snapshot_sql = """
    SELECT trend.*, starmeasure.MeasureName
    FROM HEDIS.vw_measuretrendingmonthly trend
    INNER JOIN (SELECT DISTINCT MeasureID, MeasureName
    FROM HEDIS.vw_measure
    WHERE ReportingYear = @reporting_year1 AND starMeasure = 1) starmeasure ON starmeasure.MeasureID = trend.MeasureID
    WHERE ReportingYear = @reporting_year2
    """

pull_measure_monthly_snapshot_bq_sql =  """
    SELECT
    trend.*, sm.measurename
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.HEDIS_VW_MeasureTrendingMonthly` trend
    INNER JOIN (SELECT DISTINCT measureid, measurename
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.HEDIS_VW_Measure`
    WHERE reportingyear =  @reporting_year1 AND starmeasure = true) sm 
    ON sm.measureid = trend.measureid
    WHERE reportingyear =  @reporting_year2
    """

pull_clinical_condition_sql =  """
    SELECT ds.*
    FROM
    (
    SELECT varchar(SRC_MEMBER_ID) SRC_MEMBER_ID,
    MAX(case when DISEASE_CD = 'SKC' then ct  else 0 end) as Skin_Cancer,
    MAX(case when DISEASE_CD = 'HEP' then ct  else 0 end) as Hepatitis,
    MAX(case when DISEASE_CD = 'CHD' then ct  else 0 end) as Congential_Heart_Disease,
    MAX(case when DISEASE_CD = 'LUC' then ct  else 0 end) as Lung_Cancer,
    MAX(case when DISEASE_CD = 'NEU' then ct  else 0 end) as Neurosis,
    MAX(case when DISEASE_CD = 'AUT' then ct  else 0 end) as Autism,
    MAX(case when DISEASE_CD = 'ANX' then ct  else 0 end) as Anxiety,
    MAX(case when DISEASE_CD = 'STC' then ct  else 0 end) as Stomach_Cancer,
    MAX(case when DISEASE_CD = 'CFS' then ct  else 0 end) as Chronic_Fatigue_Syndrome,
    MAX(case when DISEASE_CD = 'ALC' then ct  else 0 end) as Alcoholism,
    MAX(case when DISEASE_CD = 'PRC' then ct  else 0 end) as Prostate_Cancer,
    MAX(case when DISEASE_CD = 'PVD' then ct  else 0 end) as Peripheral_Artery_Disease,
    MAX(case when DISEASE_CD = 'RHA' then ct  else 0 end) as Rheumatoid_Arthritis,
    MAX(case when DISEASE_CD = 'CHF' then ct  else 0 end) as Heart_Failure,
    MAX(case when DISEASE_CD = 'CHO' then ct  else 0 end) as Cholelithiasis_or_Cholecystitis,
    MAX(case when DISEASE_CD = 'CBD' then ct  else 0 end) as Cerebrovascular_Disease,
    MAX(case when DISEASE_CD = 'HCG' then ct  else 0 end) as Hypercoaguable_Syndrome,
    MAX(case when DISEASE_CD = 'MSX' then ct  else 0 end) as Metabolic_Syndrome,
    MAX(case when DISEASE_CD = 'PNC' then ct  else 0 end) as Pancreatic_Cancer,
    MAX(case when DISEASE_CD = 'PUD' then ct  else 0 end) as Peptic_Ulcer_Disease,
    MAX(case when DISEASE_CD = 'EPL' then ct  else 0 end) as Epilepsy,
    MAX(case when DISEASE_CD = 'OST' then ct  else 0 end) as Osteoarthritis,
    MAX(case when DISEASE_CD = 'EDT' then ct  else 0 end) as Endometriosis,
    MAX(case when DISEASE_CD = 'LVB' then ct  else 0 end) as Low_Vision_and_Blindness,
    MAX(case when DISEASE_CD = 'PER' then ct  else 0 end) as Periodontal_Disease,
    MAX(case when DISEASE_CD = 'CAN' then ct  else 0 end) as Other_Cancer,
    MAX(case when DISEASE_CD = 'CVC' then ct  else 0 end) as Cervical_Cancer,
    MAX(case when DISEASE_CD = 'CAT' then ct  else 0 end) as Cataract,
    MAX(case when DISEASE_CD = 'ADD' then ct  else 0 end) as Attention_Deficit_Disorder,
    MAX(case when DISEASE_CD = 'IDA' then ct  else 0 end) as Iron_Deficiency_Anemia,
    MAX(case when DISEASE_CD = 'BIP' then ct  else 0 end) as Bipolar,
    MAX(case when DISEASE_CD = 'MLM' then ct  else 0 end) as Malignant_Melanoma,
    MAX(case when DISEASE_CD = 'MOH' then ct  else 0 end) as Migraine_and_Other_Headaches,
    MAX(case when DISEASE_CD = 'AST' then ct  else 0 end) as Asthma,
    MAX(case when DISEASE_CD = 'IHD' then ct  else 0 end) as Ischemic_Heart_Disease,
    MAX(case when DISEASE_CD = 'SUM' then ct  else 0 end) as Member_Summary,
    MAX(case when DISEASE_CD = 'COC' then ct  else 0 end) as Colorectal_Cancer,
    MAX(case when DISEASE_CD = 'CRF' then ct  else 0 end) as Chronic_Renal_Failure,
    MAX(case when DISEASE_CD = 'SDO' then ct  else 0 end) as Substances_Related_Disorders,
    MAX(case when DISEASE_CD = 'BRC' then ct  else 0 end) as Breast_Cancer,
    MAX(case when DISEASE_CD = 'NGD' then ct  else 0 end) as Nonspecific_Gastritis_or_Dyspepsia,
    MAX(case when DISEASE_CD = 'EDO' then ct  else 0 end) as Eating_Disorders,
    MAX(case when DISEASE_CD = 'HDL' then ct  else 0 end) as HodgkinsDisease_or_Lymphoma,
    MAX(case when DISEASE_CD = 'DTD' then ct  else 0 end) as Diverticular_Disease,
    MAX(case when DISEASE_CD = 'PAR' then ct  else 0 end) as Parkinsons_Disease,
    MAX(case when DISEASE_CD = 'HEM' then ct  else 0 end) as Hemophilia_or_Congenital_Coagulopathies,
    MAX(case when DISEASE_CD = 'COP' then ct  else 0 end) as Chronic_Obstructive_Pulmonary_Disease,
    MAX(case when DISEASE_CD = 'CRO' then ct  else 0 end) as Inflammatory_Bowel_Disease,
    MAX(case when DISEASE_CD = 'ORC' then ct  else 0 end) as Oral_Cancer,
    MAX(case when DISEASE_CD = 'OVC' then ct  else 0 end) as Ovarian_Cancer,
    MAX(case when DISEASE_CD = 'FIB' then ct  else 0 end) as Fibromyalgia,
    MAX(case when DISEASE_CD = 'LBP' then ct  else 0 end) as Low_Back_Pain,
    MAX(case when DISEASE_CD = 'ESC' then ct  else 0 end) as Esophageal_Cancer,
    MAX(case when DISEASE_CD = 'LEU' then ct  else 0 end) as Leukemia_or_Myeloma,
    MAX(case when DISEASE_CD = 'HNC' then ct  else 0 end) as Head_or_NeckCancer,
    MAX(case when DISEASE_CD = 'DNS' then ct  else 0 end) as Downs_Syndrome,
    MAX(case when DISEASE_CD = 'VNA' then ct  else 0 end) as Ventricular_Arrhythmia,
    MAX(case when DISEASE_CD = 'MNP' then ct  else 0 end) as Menopause,
    MAX(case when DISEASE_CD = 'LYM' then ct  else 0 end) as Lyme_Disease,
    MAX(case when DISEASE_CD = 'LBW' then ct  else 0 end) as Maternal_Hist_of_Low_Birth_Weight_or_Preterm_Birth,
    MAX(case when DISEASE_CD = 'OBE' then ct  else 0 end) as Obesity,
    MAX(case when DISEASE_CD = 'GLC' then ct  else 0 end) as Glaucoma,
    MAX(case when DISEASE_CD = 'CTD' then ct  else 0 end) as Chronic_Thyroid_Disorders,
    MAX(case when DISEASE_CD = 'BNC' then ct  else 0 end) as Brain_Cancer,
    MAX(case when DISEASE_CD = 'BLC' then ct  else 0 end) as Bladder_Cancer,
    MAX(case when DISEASE_CD = 'SCA' then ct  else 0 end) as Sickle_Cell_Anemia,
    MAX(case when DISEASE_CD = 'MSS' then ct  else 0 end) as Multiple_Sclerosis,
    MAX(case when DISEASE_CD = 'ALG' then ct  else 0 end) as Allergy,
    MAX(case when DISEASE_CD = 'SLE' then ct  else 0 end) as Systemic_Lupus_Erythematosus,
    MAX(case when DISEASE_CD = 'PAN' then ct  else 0 end) as Pancreatitis,
    MAX(case when DISEASE_CD = 'PSY' then ct  else 0 end) as Psychoses,
    MAX(case when DISEASE_CD = 'CYS' then ct  else 0 end) as Cystic_Fibrosis,
    MAX(case when DISEASE_CD = 'ENC' then ct  else 0 end) as Endometrial_Cancer,
    MAX(case when DISEASE_CD = 'AFF' then ct  else 0 end) as Atrial_Fibrillation,
    MAX(case when DISEASE_CD = 'BPH' then ct  else 0 end) as Benign_Prostatic_Hypertrophy,
    MAX(case when DISEASE_CD = 'CDO' then ct  else 0 end) as ADHD_and_other_Childhood_Disruptive_Disorders,
    MAX(case when DISEASE_CD = 'KST' then ct  else 0 end) as Kidney_Stones,
    MAX(case when DISEASE_CD = 'OSP' then ct  else 0 end) as Osteoporosis,
    MAX(case when DISEASE_CD = 'HYC' then ct  else 0 end) as Hyperlipidemia,
    MAX(case when DISEASE_CD = 'OMD' then ct  else 0 end) as Otitis_Media,
    MAX(case when DISEASE_CD = 'PMC' then ct  else 0 end) as Psychiatric_Disorders_related_to_Med_Conditions,
    MAX(case when DISEASE_CD = 'DEP' then ct  else 0 end) as Depression,
    MAX(case when DISEASE_CD = 'FIF' then ct  else 0 end) as Female_Infertility,
    MAX(case when DISEASE_CD = 'HYP' then ct  else 0 end) as Hypertension,
    MAX(case when DISEASE_CD = 'PPD' then ct  else 0 end) as Post_Partum_BH_Disorder,
    MAX(case when DISEASE_CD = 'AID' then ct  else 0 end) as HIV_or_AIDS,
    MAX(case when DISEASE_CD = 'DEM' then ct  else 0 end) as Dementia,
    MAX(case when DISEASE_CD = 'DIA' then ct  else 0 end) as Diabetes_Mellitus
    FROM
    (
    SELECT a.*,varchar(s.SRC_MEMBER_ID) SRC_MEMBER_ID,1 as ct
    FROM HPD.INDIVIDUAL_DETAIL a
    INNER JOIN STARSTEMP.stars_indvdl_mbr_detail s 
    ON a.INDIVIDUAL_ID = s.MEMBER_ID AND s.REPORTING_YEAR = @reporting_year
    WHERE a.RECORD_TYPE_CD = '0'
    )
    GROUP BY INDIVIDUAL_ID,SRC_MEMBER_ID
    ) ds
    """

pull_ev_call_sql =  """
    SELECT 
    mfb.[focus_desc] as 'campaign',
    mfb.[call_script_desc] as 'sub campaign',
    mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc',
    mfb.[mfs_cur_focus_status_desc] as 'focus status',
    c.[call_timestamp] as 'call date',
    c.[call_status_desc], 
    c.[not_reach_reason_desc], 
    casereason.lookup_cd_display_nm as 'call disposition',
    concat('',mfb.[medicare_member_id]) as 'src_member_id',
    concat('',i.[individual_id]) as 'individual_id',
    i.first_nm, 
    i.last_nm, 
    i.birth_dt, 
    i.gender_desc, 
    i.address_line_1_txt, 
    i.address_line_2_txt,
    i.city_nm,
    i.state_postal_desc, 
    i.zip_cd,
    i.region_desc, 
    i.member_phone_nbr, 
    i.cms_cntrct_nbr,
    i.pbp_id,
    i.lis_desc
    FROM [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
    ON mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
    ON mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id AND casereason.sgk_lookup_col_id=37 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
    ON mfb.[individual_id] = i.[individual_id]
    WHERE 1=1 AND (mfb.focus_cd_id in (25) AND MFB.CALL_SCRIPT_CD = 'DSNP') 
    AND mfb.reporting_year in (year(getdate())-3,year(getdate())-2,year(getdate())-1, year(getdate()))
    """


pull_ev_call_current_year_sql =  """
    SELECT 
    mfb.[focus_desc] as 'campaign',
    mfb.[call_script_desc] as 'sub campaign',
    mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc',
    mfb.[mfs_cur_focus_status_desc] as 'focus status',
    c.[call_timestamp] as 'call date',
    c.[call_status_desc], 
    c.[not_reach_reason_desc], 
    casereason.lookup_cd_display_nm as 'call disposition',
    concat('',mfb.[medicare_member_id]) as 'src_member_id',
    concat('',i.[individual_id]) as 'individual_id',
    i.first_nm, 
    i.last_nm, 
    i.birth_dt, 
    i.gender_desc, 
    i.address_line_1_txt, 
    i.address_line_2_txt,
    i.city_nm,
    i.state_postal_desc, 
    i.zip_cd,
    i.region_desc, 
    i.member_phone_nbr, 
    i.cms_cntrct_nbr,
    i.pbp_id,
    i.lis_desc
    FROM [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
    ON mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
    ON mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id AND casereason.sgk_lookup_col_id=37 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
    ON mfb.[individual_id] = i.[individual_id]
    WHERE 1=1 AND (mfb.focus_cd_id in (25) AND MFB.CALL_SCRIPT_CD = 'DSNP') 
    AND mfb.reporting_year in (year(getdate()))
    """

pull_ev_call_sop_sql =  """
    SELECT 
    mfb.[focus_desc] as 'campaign',
    mfb.[call_script_desc] as 'sub campaign',
    mfb.[mfs_cur_focus_status_reason_desc] as 'focus_reason_desc',
    mfb.[mfs_cur_focus_status_desc] as 'focus status',
    c.[call_timestamp] as 'call date',
    c.[call_status_desc], 
    c.[not_reach_reason_desc], 
    casereason.lookup_cd_display_nm as 'call disposition',
    concat('',mfb.[medicare_member_id]) as 'src_member_id',
    concat('',i.[individual_id]) as 'individual_id',
    i.first_nm, 
    i.last_nm, 
    i.birth_dt, 
    i.gender_desc, 
    i.address_line_1_txt, 
    i.address_line_2_txt,
    i.city_nm,
    i.state_postal_desc, 
    i.zip_cd,
    i.region_desc, 
    i.member_phone_nbr, 
    i.cms_cntrct_nbr,
    i.pbp_id,
    i.lis_desc
    FROM [starssoprpt].[dbo].[stars_web_rpt_mbr_focus_base] (nolock) as mfb 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_call_tracking_hx_base] (nolock) c  
    ON mfb.[sgk_rpt_mbr_focus_id] = c.[sgk_rpt_mbr_focus_id] 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_sop_lookup_data_ref] (nolock) casereason 
    ON mfb.mfs_cur_focus_status_reason_cd_id= casereason.lookup_cd_id AND casereason.sgk_lookup_col_id=37 
    LEFT JOIN [starssoprpt].[dbo].[stars_web_rpt_indvdl_details_base] as i 
    ON mfb.[individual_id] = i.[individual_id]
    --WHERE 1=1 AND (mfb.focus_cd_id in (25) AND MFB.CALL_SCRIPT_CD = 'DSNP') 
    WHERE 1=1 AND (MFB.CALL_SCRIPT_CD = 'DSNP')
    AND mfb.reporting_year in (year(getdate())-1, year(getdate()))
    --AND i.cms_cntrct_nbr='H1610'
    """


pull_med_cost_sql = """
    SELECT m360.SRC_MEMBER_ID,
    sum(cl.paid_amt) + sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Total_Med_Cost
    FROM starstemp.stars_analytics_mbr_360 m360
    INNER JOIN iwh.claim_line cl ON m360.member_id=cl.member_id
    WHERE m360.reporting_year=@reporting_year
    AND m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
    AND cl.srv_start_dt between @start_date AND @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    GROUP BY m360.src_member_id
    """

pull_dental_util_sql = """
    SELECT m360.SRC_MEMBER_ID,m360.CMS_CNTRCT_NBR,m360.PBP_ID,
    count (distinct case when (cl.SRV_BENEFIT_CD = 'DEN' AND cl.file_id='03') then cl.src_clm_id else null end) as HMO_Dental_Rider,
    count (distinct case when (cl.file_id='27' AND  cl.PRODUCT_LN_CD in ('20', '23', '24', '25', '44', '45', '46', '47', '48', '82', 'AD','AI')) then cl.src_clm_id else null end) as ACAS, 
    count (distinct case when (cl.REPORTING_CTG_CD in ('CH', 'CD', 'HI', 'UD') AND cl.file_id='03') then cl.src_clm_id else null end) as HMO_Stand_Alone, 
    count (distinct case when cl.file_id='44' then cl.src_clm_id else null end) as HMO_Stand_Alone_rider_enct,
    count (distinct case when substr(cl.PRCDR_CD,1,1) in ('D') then cl.src_clm_id else null end) as Dental_procedure
    FROM starstemp.stars_analytics_mbr_360 m360
    INNER JOIN iwh.claim_line cl ON m360.member_id=cl.member_id
    WHERE m360.reporting_year=@reporting_year
    AND m360.GROUP_IND='IND'
    AND m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
    AND cl.srv_start_dt between @start_date AND @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd in ('P', 'R')
    GROUP BY m360.src_member_id, m360.CMS_CNTRCT_NBR, m360.PBP_ID
    """

pull_prefer_language_sql =  """
    SELECT 
    MEDICARE_NBR,
    PREF_SPOKEN_LANG,
    PREF_WRITTEN_LANG
    FROM dm.MSBI_INDVDL_PROFILE_DTL
    """

pull_member_phone_info_sql="""
    SELECT 
    a.StarsMember_ID,
    b.SourceMemberID,
    PhoneNumber,
    PhoneValidInd,
    Priority,
    b.CurrentRecord,
    TCPAConsentInd,
    DNCInd,
    EnterpriseDNCInd,
    IVRPermInd,
    VoicePermInd,
    EnterpriseIVRPermInd,
    EnterpriseVoicePermInd,
    BestDayToCall,
    BestTimeToCall
    FROM [StarsDataHubProd].[Member].[Phone] a LEFT JOIN
    [StarsDataHubProd].[Member].[EnterpriseIDs] b
    ON a.StarsMember_ID=b.StarsMember_ID
    WHERE b.CurrentRecord=1
    """

pull_member_do_not_mail_sql ="""
    SELECT 
    a.StarsMember_ID,
    SourceMemberID,
    MBI,
    DirectMailPermInd
    FROM [StarsDataHubProd].[Member].[Address] a LEFT JOIN
    [StarsDataHubProd].[Member].[EnterpriseIDs] b
    ON a.StarsMember_ID=b.StarsMember_ID
    WHERE b.CurrentRecord=1
    """

pull_clinical_condition_bq_sql = """
SELECT sourcememberid,
    MAX(case when DISEASE_CD = 'SKC' then 1  else 0 end) as Skin_Cancer,
    MAX(case when DISEASE_CD = 'HEP' then 1  else 0 end) as Hepatitis,
    MAX(case when DISEASE_CD = 'CHD' then 1  else 0 end) as Congential_Heart_Disease,
    MAX(case when DISEASE_CD = 'LUC' then 1  else 0 end) as Lung_Cancer,
    MAX(case when DISEASE_CD = 'NEU' then 1  else 0 end) as Neurosis,
    MAX(case when DISEASE_CD = 'AUT' then 1  else 0 end) as Autism,
    MAX(case when DISEASE_CD = 'ANX' then 1  else 0 end) as Anxiety,
    MAX(case when DISEASE_CD = 'STC' then 1  else 0 end) as Stomach_Cancer,
    MAX(case when DISEASE_CD = 'CFS' then 1  else 0 end) as Chronic_Fatigue_Syndrome,
    MAX(case when DISEASE_CD = 'ALC' then 1  else 0 end) as Alcoholism,
    MAX(case when DISEASE_CD = 'PRC' then 1  else 0 end) as Prostate_Cancer,
    MAX(case when DISEASE_CD = 'PVD' then 1  else 0 end) as Peripheral_Artery_Disease,
    MAX(case when DISEASE_CD = 'RHA' then 1  else 0 end) as Rheumatoid_Arthritis,
    MAX(case when DISEASE_CD = 'CHF' then 1  else 0 end) as Heart_Failure,
    MAX(case when DISEASE_CD = 'CHO' then 1  else 0 end) as Cholelithiasis_or_Cholecystitis,
    MAX(case when DISEASE_CD = 'CBD' then 1  else 0 end) as Cerebrovascular_Disease,
    MAX(case when DISEASE_CD = 'HCG' then 1  else 0 end) as Hypercoaguable_Syndrome,
    MAX(case when DISEASE_CD = 'MSX' then 1  else 0 end) as Metabolic_Syndrome,
    MAX(case when DISEASE_CD = 'PNC' then 1  else 0 end) as Pancreatic_Cancer,
    MAX(case when DISEASE_CD = 'PUD' then 1  else 0 end) as Peptic_Ulcer_Disease,
    MAX(case when DISEASE_CD = 'EPL' then 1  else 0 end) as Epilepsy,
    MAX(case when DISEASE_CD = 'OST' then 1  else 0 end) as Osteoarthritis,
    MAX(case when DISEASE_CD = 'EDT' then 1  else 0 end) as Endometriosis,
    MAX(case when DISEASE_CD = 'LVB' then 1  else 0 end) as Low_Vision_and_Blindness,
    MAX(case when DISEASE_CD = 'PER' then 1  else 0 end) as Periodontal_Disease,
    MAX(case when DISEASE_CD = 'CAN' then 1  else 0 end) as Other_Cancer,
    MAX(case when DISEASE_CD = 'CVC' then 1  else 0 end) as Cervical_Cancer,
    MAX(case when DISEASE_CD = 'CAT' then 1  else 0 end) as Cataract,
    MAX(case when DISEASE_CD = 'ADD' then 1  else 0 end) as Attention_Deficit_Disorder,
    MAX(case when DISEASE_CD = 'IDA' then 1  else 0 end) as Iron_Deficiency_Anemia,
    MAX(case when DISEASE_CD = 'BIP' then 1  else 0 end) as Bipolar,
    MAX(case when DISEASE_CD = 'MLM' then 1  else 0 end) as Malignant_Melanoma,
    MAX(case when DISEASE_CD = 'MOH' then 1  else 0 end) as Migraine_and_Other_Headaches,
    MAX(case when DISEASE_CD = 'AST' then 1  else 0 end) as Asthma,
    MAX(case when DISEASE_CD = 'IHD' then 1  else 0 end) as Ischemic_Heart_Disease,
    MAX(case when DISEASE_CD = 'SUM' then 1  else 0 end) as Member_Summary,
    MAX(case when DISEASE_CD = 'COC' then 1  else 0 end) as Colorectal_Cancer,
    MAX(case when DISEASE_CD = 'CRF' then 1  else 0 end) as Chronic_Renal_Failure,
    MAX(case when DISEASE_CD = 'SDO' then 1  else 0 end) as Substances_Related_Disorders,
    MAX(case when DISEASE_CD = 'BRC' then 1  else 0 end) as Breast_Cancer,
    MAX(case when DISEASE_CD = 'NGD' then 1  else 0 end) as Nonspecific_Gastritis_or_Dyspepsia,
    MAX(case when DISEASE_CD = 'EDO' then 1  else 0 end) as Eating_Disorders,
    MAX(case when DISEASE_CD = 'HDL' then 1  else 0 end) as HodgkinsDisease_or_Lymphoma,
    MAX(case when DISEASE_CD = 'DTD' then 1  else 0 end) as Diverticular_Disease,
    MAX(case when DISEASE_CD = 'PAR' then 1  else 0 end) as Parkinsons_Disease,
    MAX(case when DISEASE_CD = 'HEM' then 1  else 0 end) as Hemophilia_or_Congenital_Coagulopathies,
    MAX(case when DISEASE_CD = 'COP' then 1  else 0 end) as Chronic_Obstructive_Pulmonary_Disease,
    MAX(case when DISEASE_CD = 'CRO' then 1  else 0 end) as Inflammatory_Bowel_Disease,
    MAX(case when DISEASE_CD = 'ORC' then 1  else 0 end) as Oral_Cancer,
    MAX(case when DISEASE_CD = 'OVC' then 1  else 0 end) as Ovarian_Cancer,
    MAX(case when DISEASE_CD = 'FIB' then 1  else 0 end) as Fibromyalgia,
    MAX(case when DISEASE_CD = 'LBP' then 1  else 0 end) as Low_Back_Pain,
    MAX(case when DISEASE_CD = 'ESC' then 1  else 0 end) as Esophageal_Cancer,
    MAX(case when DISEASE_CD = 'LEU' then 1  else 0 end) as Leukemia_or_Myeloma,
    MAX(case when DISEASE_CD = 'HNC' then 1  else 0 end) as Head_or_NeckCancer,
    MAX(case when DISEASE_CD = 'DNS' then 1  else 0 end) as Downs_Syndrome,
    MAX(case when DISEASE_CD = 'VNA' then 1  else 0 end) as Ventricular_Arrhythmia,
    MAX(case when DISEASE_CD = 'MNP' then 1  else 0 end) as Menopause,
    MAX(case when DISEASE_CD = 'LYM' then 1  else 0 end) as Lyme_Disease,
    MAX(case when DISEASE_CD = 'LBW' then 1  else 0 end) as Maternal_Hist_of_Low_Birth_Weight_or_Preterm_Birth,
    MAX(case when DISEASE_CD = 'OBE' then 1  else 0 end) as Obesity,
    MAX(case when DISEASE_CD = 'GLC' then 1  else 0 end) as Glaucoma,
    MAX(case when DISEASE_CD = '1D' then 1  else 0 end) as Chronic_Thyroid_Disorders,
    MAX(case when DISEASE_CD = 'BNC' then 1  else 0 end) as Brain_Cancer,
    MAX(case when DISEASE_CD = 'BLC' then 1  else 0 end) as Bladder_Cancer,
    MAX(case when DISEASE_CD = 'SCA' then 1  else 0 end) as Sickle_Cell_Anemia,
    MAX(case when DISEASE_CD = 'MSS' then 1  else 0 end) as Multiple_Sclerosis,
    MAX(case when DISEASE_CD = 'ALG' then 1  else 0 end) as Allergy,
    MAX(case when DISEASE_CD = 'SLE' then 1  else 0 end) as Systemic_Lupus_Erythematosus,
    MAX(case when DISEASE_CD = 'PAN' then 1  else 0 end) as Pancreatitis,
    MAX(case when DISEASE_CD = 'PSY' then 1  else 0 end) as Psychoses,
    MAX(case when DISEASE_CD = 'CYS' then 1  else 0 end) as Cystic_Fibrosis,
    MAX(case when DISEASE_CD = 'ENC' then 1  else 0 end) as Endometrial_Cancer,
    MAX(case when DISEASE_CD = 'AFF' then 1  else 0 end) as Atrial_Fibrillation,
    MAX(case when DISEASE_CD = 'BPH' then 1  else 0 end) as Benign_Prostatic_Hypertrophy,
    MAX(case when DISEASE_CD = 'CDO' then 1  else 0 end) as ADHD_and_other_Childhood_Disruptive_Disorders,
    MAX(case when DISEASE_CD = 'KST' then 1  else 0 end) as Kidney_Stones,
    MAX(case when DISEASE_CD = 'OSP' then 1  else 0 end) as Osteoporosis,
    MAX(case when DISEASE_CD = 'HYC' then 1  else 0 end) as Hyperlipidemia,
    MAX(case when DISEASE_CD = 'OMD' then 1  else 0 end) as Otitis_Media,
    MAX(case when DISEASE_CD = 'PMC' then 1  else 0 end) as Psychiatric_Disorders_related_to_Med_Conditions,
    MAX(case when DISEASE_CD = 'DEP' then 1  else 0 end) as Depression,
    MAX(case when DISEASE_CD = 'FIF' then 1  else 0 end) as Female_Infertility,
    MAX(case when DISEASE_CD = 'HYP' then 1  else 0 end) as Hypertension,
    MAX(case when DISEASE_CD = 'PPD' then 1  else 0 end) as Post_Partum_BH_Disorder,
    MAX(case when DISEASE_CD = 'AID' then 1  else 0 end) as HIV_or_AIDS,
    MAX(case when DISEASE_CD = 'DEM' then 1  else 0 end) as Dementia,
    MAX(case when DISEASE_CD = 'DIA' then 1  else 0 end) as Diabetes_Mellitus
    FROM `edp-prod-hcbstorage.edp_hcb_core_cnsv.INDIVIDUAL_DETAIL` a
    INNER JOIN edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_EnterpriseIDs s 
    on a.INDIVIDUAL_ID = s.iwhmemberid 
    WHERE a.RECORD_TYPE_CD = '0'
    group by sourcememberid
"""

pull_call_log_bq_sql =  """
    SELECT 
    call.src_sys_nm,
    call.src_file_nm,
    call.cl_call_log_id,
    call.mcl_member_id,
    (case when mli.mbr_src_member_id is null then call.mcl_identifier_value_id else mli.mbr_src_member_id end) as mcl_identifier_value_id,
    call.cl_call_log_drctn_type_desc,
    call.cl_call_log_drctn_type_key_cd,
    call.cl_call_log_method_type_desc,
    call.cl_call_log_method_type_key_cd,
    call.cl_call_log_rel_type_desc,
    call.cl_call_log_rel_type_key_cd,
    call.cl_call_log_source_type_desc,
    call.cl_call_log_source_type_key_cd,
    call.cl_call_log_type_desc,
    call.cl_call_log_type_key_cd,
    call.cl_contact_dts,
    call.cl_effective_dts,
    call.cl_inserted_by_id,
    call.mcl_inserted_by_id,
    call.cl_member_program_id,
    call.cl_program_type_desc,
    call.cl_program_type_key_cd,
    call.cl_object_typ_desc,
    call.cl_object_typ_key_cd,
    call.cl_attempt_status_key_desc,
    call.cl_attempt_status_typ_key_cd,
    call.cl_category_typ_desc,
    call.cl_category_typ_key_cd,
    call.cl_comments_txt,
    call.cl_contact_phone_number_2_nbr,
    call.cl_contact_phone_number_1_nbr,
    call.cl_contact_email_txt,
    call.cl_attempts_typ_key_desc
    FROM `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR` call
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli ON call.mcl_member_id = mli.mbr_member_id
    WHERE call.cl_program_type_key_cd in ('DSNP', 'CSNP')
    AND cast(call.bus_exp_dt as date) = '9999-12-31'
    AND cast(call.cl_contact_dts as date) >= @min_call_date
    AND cast(call.cl_contact_dts as date) <= @max_call_date

    AND cl_call_log_method_type_desc in ('In Person - Community Location', 'In Person - Member Residence',
        'In Person - Post Acute Care Facility', 'In Person - Acute Care Facility',
        'In Person - Other Residence','In Person - Provider Location','Phone - Inbound','Phone - Outbound','Video Conferencing - Inbound',
        'Video Conferencing - Outbound')

    AND (
        (cl_call_log_type_desc in ('Assessment','Care Coordination','Care Planning','COVID-19','Engagement',
        'HRA Outreach','Inpatient Confinement Contact','Inpatient Outreach','Interdisciplinary Care Team',
        'Post Discharge Outreach','Pre Admission Outreach','Referral','SNF Inpatient Outreach','BH','Case Management',
        'Supplemental Benefits','Medicaid Recertification','Social Worker','Annual Face to Face Visit',
        'Hard to Reach - Alt Contact Research', 'Hard to Reach - Engagement Collab', 'Post - Emergency Room Outreach (FMC)')

    AND cl_call_log_rel_type_desc in ('Appointment of Representative','Family/Daughter','Family/Grandchild',
        'Family/Other','Family/Parent','Family/Partner','Family/Sibling','Family/Son','Family/Spouse','Friend',
        'Guardian','Health Care Proxy','Member/Self','Power of Attorney','Third Party Admin','EXP Friend','EXP Guardian'))

        or 
        (cl_call_log_type_desc = 'HRA Outreach' 
    AND cl_call_log_rel_type_desc = 'Other')
    )

    AND cl_attempt_status_key_desc ='Successful' 
    """

pull_call_log_all_bq_sql =  """
    SELECT 
    call.src_sys_nm,
    call.src_file_nm,
    call.cl_call_log_id,
    call.mcl_member_id,
    (case when mli.mbr_src_member_id is null 
        then call.mcl_identifier_value_id 
        else mli.mbr_src_member_id end
    ) as mcl_identifier_value_id,
    call.cl_call_log_drctn_type_desc,
    call.cl_call_log_drctn_type_key_cd,
    call.cl_call_log_method_type_desc,
    call.cl_call_log_method_type_key_cd,
    call.cl_call_log_rel_type_desc,
    call.cl_call_log_rel_type_key_cd,
    call.cl_call_log_source_type_desc,
    call.cl_call_log_source_type_key_cd,
    call.cl_call_log_type_desc,
    call.cl_call_log_type_key_cd,
    call.cl_contact_dts,
    call.cl_effective_dts,
    call.cl_inserted_by_id,
    call.mcl_inserted_by_id,
    call.cl_member_program_id,
    call.cl_program_type_desc,
    call.cl_program_type_key_cd,
    call.cl_object_typ_desc,
    call.cl_object_typ_key_cd,
    call.cl_attempt_status_key_desc,
    call.cl_attempt_status_typ_key_cd,
    call.cl_category_typ_desc,
    call.cl_category_typ_key_cd,
    call.cl_comments_txt,
    call.cl_contact_phone_number_2_nbr,
    call.cl_contact_phone_number_1_nbr,
    call.cl_contact_email_txt,
    call.cl_attempts_typ_key_desc,
    sa.su_user_nm
    FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR call
    INNER JOIN edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE mli ON call.mcl_member_id = mli.mbr_member_id
    LEFT JOIN edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CURATED_SECURITY_AUDIT sa ON call.cl_inserted_by_id=sa.suc_security_user_context_id
    WHERE cl_program_type_key_cd in ('DSNP', 'CSNP')
    AND cast(call.cl_contact_dts as date) >= @min_call_date
    AND cast(call.cl_contact_dts as date) <= @max_call_date

    AND cl_call_log_method_type_desc in ('In Person - Community Location', 'In Person - Member Residence',
        'In Person - Post Acute Care Facility', 'In Person - Acute Care Facility',
        'In Person - Other Residence','In Person - Provider Location','Phone - Inbound','Phone - Outbound','Video Conferencing - Inbound',
        'Video Conferencing - Outbound')

    AND (
        (cl_call_log_type_desc in ('Assessment','Care Coordination','Care Planning','COVID-19','Engagement',
        'HRA Outreach','Inpatient Confinement Contact','Inpatient Outreach','Interdisciplinary Care Team',
        'Post Discharge Outreach','Pre Admission Outreach','Referral','SNF Inpatient Outreach','BH','Case Management',
        'Supplemental Benefits','Medicaid Recertification','Social Worker','Annual Face to Face Visit')

    AND cl_call_log_rel_type_desc in ('Appointment of Representative','Family/Daughter','Family/Grandchild',
        'Family/Other','Family/Parent','Family/Partner','Family/Sibling','Family/Son','Family/Spouse','Friend',
        'Guardian','Health Care Proxy','Member/Self','Power of Attorney','Third Party Admin','EXP Friend','EXP Guardian'))

        or 
        (cl_call_log_type_desc = 'HRA Outreach' 
    AND cl_call_log_rel_type_desc = 'Other')
    )

    """

pull_call_log_all_dsnp_bq_sql = """SELECT 
        call.src_sys_nm,
        call.src_file_nm,
        call.cl_call_log_id,
        call.mcl_member_id,
        (case when mli.mbr_src_member_id is null 
            then call.mcl_identifier_value_id 
            else mli.mbr_src_member_id end
        ) as mcl_identifier_value_id,
        call.cl_call_log_drctn_type_desc,
        call.cl_call_log_drctn_type_key_cd,
        call.cl_call_log_method_type_desc,
        call.cl_call_log_method_type_key_cd,
        call.cl_call_log_rel_type_desc,
        call.cl_call_log_rel_type_key_cd,
        call.cl_call_log_source_type_desc,
        call.cl_call_log_source_type_key_cd,
        call.cl_call_log_type_desc,
        call.cl_call_log_type_key_cd,
        call.cl_contact_dts,
        call.cl_effective_dts,
        call.cl_inserted_by_id,
        call.mcl_inserted_by_id,
        call.cl_member_program_id,
        call.cl_program_type_desc,
        call.cl_program_type_key_cd,
        call.cl_object_typ_desc,
        call.cl_object_typ_key_cd,
        call.cl_attempt_status_key_desc,
        call.cl_attempt_status_typ_key_cd,
        call.cl_category_typ_desc,
        call.cl_category_typ_key_cd,
        call.cl_comments_txt,
        call.cl_contact_phone_number_2_nbr,
        call.cl_contact_phone_number_1_nbr,
        call.cl_contact_email_txt,
        call.cl_attempts_typ_key_desc,
        sa.su_user_nm
        FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CALL_LOG_MBR call
        INNER JOIN edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE mli ON call.mcl_member_id = mli.mbr_member_id
        LEFT JOIN edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CURATED_SECURITY_AUDIT sa ON call.cl_inserted_by_id=sa.suc_security_user_context_id
        WHERE cl_program_type_key_cd in ('DSNP', 'CSNP')
        AND cast(cl_contact_dts as date) >= @call_log_date1
        AND cast(cl_contact_dts as date) <= @call_log_date2
    """

pull_vbc_sql= """
    SELECT DISTINCT
    member_plan.StarsMEMBER_ID,
    member_plan.SourceMemberID,
    member_plan.CMSContractNumber, 
    member_plan.PBPID,
    a.PBGGroup,
    a.PBGGroupName, 
    a.TIN, 
    a.TINName,
    a.VBC, 
    a.ProviderID, 
    a.LabelName, 
    --a.par_ind, 
    --a.primary_ind,
    a.PBGSTATUS,
    a.PbgInd
    FROM Member.VW_PlanPerReportingYear member_plan
    LEFT JOIN (SELECT DISTINCT prov_att.StarsMEMBER_ID, prov_att.SourceMemberID,
                    prov_att.PBGGroup,
                    prov_att.PBGGroupName, prov_att.TIN, prov_att.TINName,
                    case when prov_att.PbgInd = 'Y' then 'VBC' else ' ' end as VBC, 
                    demo.ProviderID,
                    demo.LabelName,
                    demo.NPI, demo.Phone,
                    prov_att.PBGSTATUS, prov_att.PbgInd
                FROM Member.vw_ProviderAttribution prov_att
                LEFT JOIN Provider.vw_demographics demo ON prov_att.ProviderID=demo.ProviderID
                WHERE prov_att.ReportingYear = @reporting_year
                ) a ON member_plan.StarsMEMBER_ID = a.StarsMEMBER_ID
    WHERE member_plan.ReportingYear = @reporting_year
    """

pull_risk_strat_from_icp_sql ="""
    SELECT *
    FROM [StarsBIDataProd].[adm].[MSBI_Data_Pull_ICP_Risk_Strat_With_LOB]
    """

pull_risk_strat_bq_sql = """
    SELECT DISTINCT
    (case when mli.mbr_src_member_id is null then mbr.mbr_identifier_value_id else mli.mbr_src_member_id end) as mbr_src_member_id,
    mp.mpr_member_id,
    mp.mpr_member_program_id,
    mp.mpr_risk_strat_typ_cd as cars_cd,
    mp.mpr_risk_strat_typ_desc as CARS,
    mp.mpr_case_mngmnt_lvl_typ_key_cd as case_mngmnt_level,
    mp.mpr_cs_mngmnt_lvl_typ_ky_desc as case_mngmnt_level_desc,
    mp.mpr_updated_by_user_nm,
    mp.mpr_updated_by_first_nm,
    mp.mpr_updated_by_last_nm,
    mp.mpr_pgm_strtfctn_type_key_cd,
    mp.mpr_pgm_strtfctn_type_desc as system_risk_strat,
    mp.mpr_updated_on_dts as mpr_risk_strat_date,
    FROM `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM` mp
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_MEMBER` mbr ON mbr.mbr_member_id=mp.mpr_member_id
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli ON mbr.mbr_member_id=mli.mbr_member_id
    WHERE mp.mpr_program_type_key_cd in ('DSNP', 'CSNP') AND
    (mp.mpr_risk_strat_typ_cd in ('L', 'M', 'H') or
    mp.mpr_pgm_strtfctn_type_key_cd in ('L', 'M', 'H') or
    mp.mpr_case_mngmnt_lvl_typ_key_cd!='')
    """

pull_vbc_report_snp_members_sql = """ 
    SELECT DISTINCT
    Member_ID, 
    MEDICARE_NUMBER, 
    FIRST_NAME,
    MIDDLE_INITIAL,
    LAST_NAME,
    DOB, 
    Age, 
    Gender,
    Address1, 
    Address2,
    left(Zip,5) as Zip,
    cast(Eff_Date as varchar(10)) as Eff_Date, 
    cast (Term_Date as varchar(10)) as Term_Date, 
    Mbr_Mths, 
    Contract_Number,
    PBP,
    Product, 
    CMS_County, 
    CMS_State, 
    NEW_REGION, 
    MA_Territory, 
    FIPS, 
    Plan_Type, 
    dual_Status, 
    PCP_Name, 
    PCP_Tax_ID, 
    MEM_Phone as Member_Phone,
    TRR_Parent_Name, 
    TOH_Name, 
    Recruiter_Name, 
    Writing_Agent_Name, 
    Sales_Channel, 
    New_Market, 
    SNP, 
    LIS_Flag,
    Enroll_Status,
    Issued_Status,
    Group_Status, 
    MA_Market_PDP_Product as Market
    FROM DM.MSBI_MEPR 
    WHERE PRODUCT='MA' 
    AND ISSUED_STATUS = 'Issued'  
    AND Group_Status in ('Individual', 'Group')
    AND (Eff_Date <= cast(? as datetime)) AND (Term_Date >= cast(? as datetime))
    """

pull_annual_wellness_visit_bq_sql="""
    SELECT substr(cast(ecl.member_id as string), 1, 12) as Member_ID
        , ecl.srv_start_dt
        , pri_icd9_dx_cd
        , ecl.PRCDR_CD
        , ecl.srv_spclty_ctg_cd
        , ecl.subctg_short_nm
    FROM edp-prod-hcbstorage.edp_hcb_core_cnsv.D_EMIS_CLAIM_LINE ecl
    WHERE trim(ecl.summarized_srv_ind)='Y'
        AND trim(ecl.duplicate_ind)='N'
        AND (trim(ecl.pri_icd9_dx_cd) LIKE 'Z00.%' OR trim(ecl.pri_icd9_dx_cd) = 'V70.0')
        AND TRIM(ecl.srv_spclty_ctg_cd) IN ('FP', 'I')
        AND (TRIM(ecl.prcdr_cd) IN ('99381','99382','99383','99384','99385','99386','99387','99391','99392','99393','99394','99395','99396','99397')
            or Trim(ecl.PRCDR_CD) in ('99202','99203','99204','99205')
            or Trim(ecl.PRCDR_CD) in ('99212','99213' ,'99214','99215')
            or Trim(ecl.PRCDR_CD) IN ('G0438','G0439','G0468'))
    """

pull_race_language_bq_sql = """
    SELECT
    eid.SourceMemberID,
    rl.Race_Description,
    rl.WrittenLanguageDescription,
    rl.SpokenLanguageDescription,
    rl.ingest_time
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_GetMemberRaceLanguage` rl LEFT JOIN
    `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_EnterpriseIDs` eid
    ON rl.StarsMember_ID=eid.StarsMember_ID
    """

pull_race_sql = """SELECT Race_ID, mi.StarsMember_ID, MBI
    FROM Member.Individual mi
    LEFT JOIN Member.Demographic md 
    ON mi.StarsMember_ID = md.StarsMember_ID
    """

pull_race_language_sql ="""
    SELECT * FROM member.VW_GetMemberRaceLanguage"""

pull_scores_from_medcompass_bq_sql = """
    SELECT
    DISTINCT
    (case when mli.mbr_src_member_id is null then rs.identifiervalue else mli.mbr_src_member_id end) as mbr_src_member_id,
      memberriskscore_memberid,
      memberriskscore_scorename,
      memberriskscore_scorevalue,
      edp_load_timestamp
    FROM
    (
      SELECT
      identifiervalue,
      memberriskscore_memberid,
      memberriskscore_scorename,
      memberriskscore_scorevalue,
      edp_load_timestamp,
      ROW_NUMBER() OVER (PARTITION BY identifiervalue, memberriskscore_memberid, memberriskscore_scorename ORDER BY edp_load_timestamp DESC) as rn
      FROM `edp-prod-hcbstorage.edp_hcb_core_srcv.MEMBER_RISKSCORE` rs
      WHERE memberriskscore_scorename in ('ML_ACCP_Pred_Score', 'RAP (RRR) Score', 'DSNP_ER_3_90', 'DSNP_ER_3_90_DX', 'DSNP_ER_4_90', 'Condition Management Opportunity Score')
    ) rs
      INNER JOIN  `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli ON rs.memberriskscore_memberid=mli.mbr_member_id
      INNER JOIN (SELECT mpr_member_id, mpr_member_program_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM WHERE mpr_program_type_key_cd in ('DSNP', 'CSNP')) mp ON rs.memberriskscore_memberid = mp.mpr_member_id
    WHERE rn = 1
    """

pull_provider_demo_info_sql = """
    SELECT
    ProviderID,
    LabelName as FacilityName,
    SpecialtyCD,
    AddressLine1,
    City,
    County,
    State,
    Zip
    FROM provider.vw_demographics
    """

pull_cm_info_sql = """
    SELECT     
    b.CaseManagerFirstName,
    b.CaseManagerLastName,
    b.ProgramStatus, 
    b.ProgramClosureReason, 
    b.EnrollmentDate, 
    b.ClosureDate, 
    b.isOpenCM,
    a.StarsMember_ID,
    a.MBI,
    a.SourceMemberID,
    a.CMSContractNumber,
    a.PBPID,
    a.EffectiveDate,
    a.TermDate
    FROM Member.VW_PlanPerReportingYear a
    LEFT JOIN Member.clinicalprogram b ON a.StarsMEMBER_ID = b.StarsMEMBER_ID
    WHERE reportingyear = @reporting_year
    AND ProgramStatus != 'Closed'
    """

pull_med_adherence_sql = """
    SELECT *
    FROM [PatientSafetyProd].[dbo].[VR_MCR_BI_MED_ADHERENCE_IN_PLAY]
    """

pull_med_adherence_detail_bq_sql =  """
    SELECT * FROM (
    SELECT 
        src_member_id,
        measure,
        new_to_therapy,
        numerator_prior_year,
        current_status,
        current_days_missed,
        current_days_covered,
        current_days_in_meas_period,
        current_pdc,
        pdc_rolling_6,
        pdc_rolling_12,
        days_late_last_fill,
        days_supply_cnt,
        disp_dt,
        initial_fill_dt,
        late_to_refill,
        next_fill_dt,
        prescriber_name,
        prescriber_phone,
        pharm_name,
        pharm_phone,
        report_date,
        report_year,
        adh_decile,
        short_nm,
        srv_copay_amt,
        tot_add_days_can_miss,
        tot_days_covered,
        tot_days_needed,
        tot_days_remaining,
        unts_dispensed_qty,
        year_end_days_in_meas_period,
        year_end_pdc,
        ROW_NUMBER() OVER (partition by src_member_id, measure ORDER BY report_date desc) as rn
        FROM `edp-prod-hcbstorage.edp_hcb_mdcrbi_ptsfty_cnsv.v_ptsfty_meas_dtl_cy_rpt`
    ) as t
    WHERE t.rn = 1
    """

pull_med_adherence_detail_history_bq_sql =  """
    SELECT 
    src_member_id,
    measure,
    new_to_therapy,
    numerator_prior_year,
    current_status,
    current_days_missed,
    current_days_covered,
    current_days_in_meas_period,
    current_pdc,
    pdc_rolling_6,
    pdc_rolling_12,
    days_late_last_fill,
    days_supply_cnt,
    disp_dt,
    initial_fill_dt,
    late_to_refill,
    next_fill_dt,
    pharm_name,
    pharm_phone,
    report_date,
    report_year,
    adh_decile,
    short_nm,
    srv_copay_amt,
    tot_add_days_can_miss,
    tot_days_covered,
    tot_days_needed,
    tot_days_remaining,
    unts_dispensed_qty,
    year_end_days_in_meas_period,
    year_end_pdc
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrbi_ptsfty_cnsv.v_ptsfty_meas_dtl_cy_rpt`
    WHERE report_year IN (EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(YEAR FROM CURRENT_DATE())-1)
    """

pull_icp_case_note_bq_sql = """
    SELECT mli.mbr_src_member_id,
    mbr.mbr_member_id as member_id,
    mbr.mbr_conversion_id as conversion_id,
    mbr.mbr_identifier_value_id as identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_note_dts,
    count(*) as rr
    FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_MEMBER mbr 
    INNER JOIN (SELECT mbr_member_id, mbr_src_member_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE) mli ON mbr.mbr_member_id=mli.mbr_member_id
    INNER JOIN (SELECT mpr_member_id, mpr_member_program_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM WHERE mpr_program_type_key_cd in ('DSNP', 'CSNP')) mp ON mbr.mbr_member_id=mp.mpr_member_id
    LEFT JOIN 
        (SELECT 
        nt_note_id,
        nt_object_id,
        nt_note_type_desc,
        nt_object_type_desc,
        nt_updated_on_dts,
        nt_member_program_id,
        nt_effective_dts,
        nt_inserted_on_dts,
        nt_note_dts
        FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ALL_NOTES
        WHERE (nt_note_type_desc IN ('Care Planning',
                                     'Initial - Care Plan',
                                     'Annual - Care Plan',
                                     'Medications',
                                     'Social Work',
                                     'Care Coordination',
                                     'Supplemental Benefits',
                                     'Interdisciplinary Care Team (ICT)',
                                     'Change in Status - Care Plan',
                                     'Post Discharge',
                                     'Pre Admission') AND 
              nt_object_type_desc IN ('Care Plan Intervention',
                                      'Care Plan',
                                      'Care Plan Problem',
                                      'Member',
                                      'Care Plan Goal') 
            )
        ) note ON mp.mpr_member_program_id = note.nt_member_program_id
    GROUP BY mli.mbr_src_member_id,
    mbr.mbr_member_id, 
    mbr.mbr_conversion_id,
    mbr.mbr_identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_note_dts
    """

pull_care_plan_date_bq_sql = """
    SELECT 
    cp_identifier_value_id,
    (case when mli.mbr_src_member_id is null then cp.cp_identifier_value_id else mli.mbr_src_member_id end) as mbr_src_member_id,
    cp_member_id,
    cp_care_plan_id,
    cp_owner_security_user_id,
    cp_care_plan_stts_type_desc,
    cp_start_dts,
    cp_estimate_end_dts,
    cp_effective_dts,
    cp_last_reviewer_dts,
    cp_current_reviewer_dts,
    cp_title_txt,
    cp_next_review_dts,
    cp_closed_dts,
    cp_reviewer_note_txt,
    cp_updated_on_dts,
    cp_program_type_desc,
    cp_active_flag_ind
    FROM `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_CARE_PLAN` cp 
    INNER JOIN  `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli ON cp.cp_member_id=mli.mbr_member_id
    WHERE mli.mi_line_of_business_typ_key_cd = 'Medicare'
    """

pull_member_ict_case_note_detail_bq_sql = """
    SELECT * FROM (
    SELECT mli.mbr_src_member_id,
    mbr.mbr_member_id as member_id,
    mbr.mbr_identifier_value_id as identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_note_txt,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_note_dts,
    note.nt_inserted_by_id,
    count(*) as rr
    FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_MEMBER mbr 
    INNER JOIN (SELECT mbr_member_id, mbr_src_member_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE) mli ON mbr.mbr_member_id=mli.mbr_member_id
    INNER JOIN (SELECT mpr_member_id, mpr_member_program_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM WHERE mpr_program_type_key_cd in ('DSNP', 'CSNP')) mp ON mbr.mbr_member_id=mp.mpr_member_id
    LEFT JOIN 
        (SELECT 
        nt_note_id,
        nt_object_id,
        nt_note_type_desc,
        nt_object_type_desc,
        `edp-prod-mdcrbi-starsbi.voltage_edp_prod_mdcrbi_starsbi.decryptNotesEntUsEast4`(nt_note_txt) nt_note_txt,
        nt_updated_on_dts,
        nt_member_program_id,
        nt_effective_dts,
        nt_inserted_on_dts,
        nt_inserted_by_id, 
        nt_note_dts
        FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ALL_NOTES
        WHERE (nt_note_type_desc IN ('Interdisciplinary Care Team (ICT)')
            ) or (nt_note_type_desc = 'Health Risk Assessment')
        ) note ON mp.mpr_member_program_id = note.nt_member_program_id
    GROUP BY mli.mbr_src_member_id,
    mbr.mbr_member_id, 
    mbr.mbr_identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_note_txt,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_inserted_by_id,
    note.nt_note_dts
    ) note_agg
    WHERE note_agg.nt_note_type_desc IS NOT NULL
    """

pull_member_ict_case_note_bq_sql = """
    SELECT * FROM (
    SELECT mli.mbr_src_member_id,
    mbr.mbr_member_id as member_id,
    mbr.mbr_identifier_value_id as identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_note_dts,
    note.nt_inserted_by_id,
    count(*) as rr
    FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_MEMBER mbr 
    INNER JOIN (SELECT mbr_member_id, mbr_src_member_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE) mli ON mbr.mbr_member_id=mli.mbr_member_id
    INNER JOIN (SELECT mpr_member_id, mpr_member_program_id FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM WHERE mpr_program_type_key_cd in ('DSNP', 'CSNP')) mp ON mbr.mbr_member_id=mp.mpr_member_id
    LEFT JOIN 
        (SELECT 
        nt_note_id,
        nt_object_id,
        nt_note_type_desc,
        nt_object_type_desc,
        nt_updated_on_dts,
        nt_member_program_id,
        nt_effective_dts,
        nt_inserted_on_dts,
        nt_inserted_by_id, 
        nt_note_dts
        FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ALL_NOTES
        WHERE (nt_note_type_desc IN ('Interdisciplinary Care Team (ICT)')
            ) or (nt_note_type_desc = 'Health Risk Assessment')
        ) note ON mp.mpr_member_program_id = note.nt_member_program_id
    GROUP BY mli.mbr_src_member_id,
    mbr.mbr_member_id, 
    mbr.mbr_identifier_value_id, 
    note.nt_note_id,
    note.nt_object_id,
    note.nt_note_type_desc,
    note.nt_object_type_desc,
    note.nt_updated_on_dts,
    note.nt_member_program_id,
    note.nt_effective_dts,
    note.nt_inserted_on_dts,
    note.nt_inserted_by_id,
    note.nt_note_dts
    ) note_agg
    WHERE note_agg.nt_note_type_desc IS NOT NULL
    """

pull_member_program_bq_sql = """
    SELECT 
    mpr_identifier_value_id,
    mpr_enrollment_dts,
    mpr_closure_dts,
    mpr_program_type_desc,
    mpr_program_status_type_desc,
    mpr_pgm_closure_rsn_type_desc,
    mpr_pgm_strtfctn_type_desc,
    mpr_risk_strat_typ_desc,
    mpr_cs_mngmnt_lvl_typ_ky_desc,
    mpr_updated_on_dts FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_PROGRAM
    WHERE mpr_program_type_desc in ('DSNP', 'CSNP')
    """

pull_prefer_language_bq_sql = """
    SELECT MBI as MEDICARE_NBR,
    SpokenLanguageDescription as PREF_SPOKEN_LANG,
    WrittenLanguageDescription as PREF_WRITTEN_LANG
    FROM edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_GetMemberRaceLanguage
    """

pull_member_phone_info_bq_sql = """
    SELECT
       a.StarsMember_ID,
        b.SourceMemberID,
        a.PhoneNumber,
        a.PhoneValidInd,
        a.TCPAConsentInd,
        a.EnterpriseDNCInd,
        a.EnterpriseIVRPermInd,
        a.EnterpriseVoicePermInd,
        a.BestDayToCall,
        a.BestTimeToCall
    FROM edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_Phone a LEFT JOIN
    edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_EnterpriseIDs b
    ON a.StarsMember_ID=b.StarsMember_ID
    """

pull_disability_bq_sql = """
    Select SRC_MEMBER_ID as Member_ID,
    Disability_Ind,
    ESRD_Ind FROM (
    SELECT DISTINCT Medicare_nbr, SRC_MEMBER_ID,ORIG_ENTITLMENT_RSN_CD,
    case when (ORIG_ENTITLMENT_RSN_CD ='1' or ORIG_ENTITLMENT_RSN_CD='3') then 'Yes' else 'No' end as Disability_Ind,
    case when (ORIG_ENTITLMENT_RSN_CD ='2' or ORIG_ENTITLMENT_RSN_CD='3') then 'Yes' else 'No' end as ESRD_Ind
    FROM edp-prod-hcbstorage.edp_hcb_medbi_rptg.MSBI_MBR_COVG_DTL_GCP CVG 
    WHERE ORIG_ENTITLMENT_RSN_CD  in ('1','2','3')
    ) A
    GROUP BY SRC_MEMBER_ID,Disability_Ind,ESRD_Ind 
    order by SRC_MEMBER_ID
    """

pull_mbr_contact_legal_bq_sql = """
    SELECT mbr_identifier_value_id,
    mcl_legal_type_desc,
    mcl_start_dts,
    mcl_end_dts,
    mcl_conversion_id 
    FROM edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.MBR_CONTACT_LEGAL"""

pull_utilization_wo_eid_bq_sql = """
    with all_claim as (
    SELECT case when left(cast(cl.member_id as string), 2) = '10' then left(cast(cl.member_id as string),12) else cast(cl.member_id as string) end as src_member_id, 
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.src_clm_id else null end) as emerg_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.src_clm_id else null end) as pcp_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.src_clm_id else null end) as spclst_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.src_clm_id else null end) as IP_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as emerg_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as primary_care_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as spclst_visits,
    count (distinct case when cl.hcfa_plc_srv_cd in ('21', '51') AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as ip_visits_non_collapsed,
    count (distinct case when (cl.PRCDR_CD in ('G0402','G0403','G0404','G0405','G0438','G0439') or
    (PRCDR_CD  IN ('99381','99382','99383','99384','99385','99386','99387','99388','99389','99390',
        '99391','99392','99393','99394','99395','99396','99397','99401','99402','99403','99404',
        '99201','99202','99203','99204','99205','99211','99212','99213','99214','99215')
    AND pri_icd9_dx_cd IN ('V03.0','V03.1','V03.2','V03.3','V03.4','V03.5','V03.6','V03.7','V03.8',
        'V03.9','V04.0','V04.1','V04.2','V04.3','V04.4','V04.5','V04.6',
        'V04.7','V04.8','V04.9','V04.10','V04.11','V04.12','V04.13','V04.14','V04.15','V04.16','V04.17','V04.18',
        'V04.19','V04.20','V04.21','V04.22','V04.23','V04.24','V04.25','V04.26','V04.27','V04.28','V04.29','V04.30',
        'V04.31','V04.32','V04.33','V04.34','V04.35','V04.36','V04.37','V04.38','V04.39','V04.40','V04.41','V04.42',
        'V04.43','V04.44','V04.45','V04.46','V04.47','V04.48','V04.49','V04.50','V04.51','V04.52','V04.53','V04.54',
        'V04.55','V04.56','V04.57','V04.58','V04.59','V04.60','V04.61','V04.62','V04.63','V04.64','V04.65','V04.66',
        'V04.67','V04.68','V04.69','V04.70','V04.71','V04.72','V04.73','V04.74','V04.75','V04.76','V04.77','V04.78',
        'V04.79','V04.80','V04.81','V04.82','V04.83','V04.84','V04.85','V04.86','V04.87','V04.88','V04.89',
        'V05.0','V05.1','V05.2','V05.3','V05.4','V05.5','V05.6','V05.7','V05.8','V05.9',
        'V06.0','V06.1','V06.2','V06.3','V06.4','V06.5','V06.6','V06.7','V06.8','V06.9','V20.0','V20.1','V20.2',
        'V70.0','V70.3','V70.5','Z23','Z76.1','Z76.2')))
    then cast(cl.member_id as string) ||
    cast(cl.SRV_START_DT as string) else null end) as preventive_care_visits
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.paid_amt else 0 end) as emerg_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.paid_amt else 0 end) as pcp_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.paid_amt else 0 end) as spclst_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.paid_amt else 0 end) as IP_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as emerg_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as pcp_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as spclst_mbr_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as IP_mbr_paid_amt
    ,count(distinct case when  cl.paid_prvdr_par_cd='Y' then cl.src_clm_id else null end) as Par_Med_claim_count
    ,count(distinct case when  cl.paid_prvdr_par_cd='N' then cl.src_clm_id else null end) as NonPar_Med_claim_count
    ,count(distinct case when cl.paid_prvdr_par_cd='Y' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Par_Med_visits
    ,count(distinct case when cl.paid_prvdr_par_cd='N' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as NonPar_Med_visits
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.paid_amt else 0 end) as Par_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.paid_amt else 0 end) as NonPar_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as Par_Med_mbr_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as NonPar_Med_mbr_paid_amt
    ,sum(cl.paid_amt) as Aetna_Med_paid_amt
    ,sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Mbr_Med_paid_amt
    ,sum(cl.paid_amt) + sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Total_Med_Cost
    ,sum(cl.deductible_amt) as Mbr_Med_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.deductible_amt else 0 end) as Par_Med_mbr_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.deductible_amt else 0 end) as NonPar_Med_mbr_deductible_paid_amt
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cl.src_clm_id else null end) as Ambulance_claim_count
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Ambulance_event_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cl.src_clm_id else null end) as Behavioral_Health_claim_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Behavioral_Health_visits /*I observed a situation that member had 2 visits ON 2 different dates, but both visits were filed under the same source claim id.*/
    FROM `edp-prod-hcbstorage.edp_hcb_core_cnsv.D_CLAIM_LINE` cl 
    join (SELECT DISTINCT Member_ID
    FROM `edp-prod-hcbstorage.edp_hcb_msts_srmepr_srcv.MEPR_SQL_MEDICARE_ENROLLMENT_VW`
    WHERE PRODUCT='MA' 
    AND ISSUED_STATUS = 'Issued' 
    AND Group_Status='Individual'
    AND SNP in ('Y','D','F','C','I')) snp_member on left(cast(cl.member_id as string),12) = snp_member.Member_ID
    WHERE cast(cl.srv_start_dt AS DATE) >= @start_date
    AND cast(cl.srv_start_dt AS DATE) <= @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    GROUP BY src_member_id)
    SELECT *
    FROM all_claim"""

pull_utilization_me_business_wo_eid_bq_sql = """
    with all_claim as (
    SELECT case when left(cast(member_id as string), 2) = '10' then left(cast(member_id as string),12) else cast(member_id as string) end as src_member_id, 
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.src_clm_id else null end) as emerg_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.src_clm_id else null end) as pcp_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.src_clm_id else null end) as spclst_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.src_clm_id else null end) as IP_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as emerg_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as primary_care_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as spclst_visits,
    count (distinct case when cl.hcfa_plc_srv_cd in ('21', '51') AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as ip_visits_non_collapsed,
    count (distinct case when (cl.PRCDR_CD in ('G0402','G0403','G0404','G0405','G0438','G0439') or
    (PRCDR_CD  IN ('99381','99382','99383','99384','99385','99386','99387','99388','99389','99390',
        '99391','99392','99393','99394','99395','99396','99397','99401','99402','99403','99404',
        '99201','99202','99203','99204','99205','99211','99212','99213','99214','99215')
    AND pri_icd9_dx_cd IN ('V03.0','V03.1','V03.2','V03.3','V03.4','V03.5','V03.6','V03.7','V03.8',
        'V03.9','V04.0','V04.1','V04.2','V04.3','V04.4','V04.5','V04.6',
        'V04.7','V04.8','V04.9','V04.10','V04.11','V04.12','V04.13','V04.14','V04.15','V04.16','V04.17','V04.18',
        'V04.19','V04.20','V04.21','V04.22','V04.23','V04.24','V04.25','V04.26','V04.27','V04.28','V04.29','V04.30',
        'V04.31','V04.32','V04.33','V04.34','V04.35','V04.36','V04.37','V04.38','V04.39','V04.40','V04.41','V04.42',
        'V04.43','V04.44','V04.45','V04.46','V04.47','V04.48','V04.49','V04.50','V04.51','V04.52','V04.53','V04.54',
        'V04.55','V04.56','V04.57','V04.58','V04.59','V04.60','V04.61','V04.62','V04.63','V04.64','V04.65','V04.66',
        'V04.67','V04.68','V04.69','V04.70','V04.71','V04.72','V04.73','V04.74','V04.75','V04.76','V04.77','V04.78',
        'V04.79','V04.80','V04.81','V04.82','V04.83','V04.84','V04.85','V04.86','V04.87','V04.88','V04.89',
        'V05.0','V05.1','V05.2','V05.3','V05.4','V05.5','V05.6','V05.7','V05.8','V05.9',
        'V06.0','V06.1','V06.2','V06.3','V06.4','V06.5','V06.6','V06.7','V06.8','V06.9','V20.0','V20.1','V20.2',
        'V70.0','V70.3','V70.5','Z23','Z76.1','Z76.2')))
    then cast(cl.member_id as string) ||
    cast(cl.SRV_START_DT as string) else null end) as preventive_care_visits
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.paid_amt else 0 end) as emerg_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.paid_amt else 0 end) as pcp_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.paid_amt else 0 end) as spclst_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.paid_amt else 0 end) as IP_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as emerg_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as pcp_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as spclst_mbr_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as IP_mbr_paid_amt
    ,count(distinct case when  cl.paid_prvdr_par_cd='Y' then cl.src_clm_id else null end) as Par_Med_claim_count
    ,count(distinct case when  cl.paid_prvdr_par_cd='N' then cl.src_clm_id else null end) as NonPar_Med_claim_count
    ,count(distinct case when cl.paid_prvdr_par_cd='Y' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Par_Med_visits
    ,count(distinct case when cl.paid_prvdr_par_cd='N' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as NonPar_Med_visits
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.paid_amt else 0 end) as Par_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.paid_amt else 0 end) as NonPar_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as Par_Med_mbr_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as NonPar_Med_mbr_paid_amt
    ,sum(cl.paid_amt) as Aetna_Med_paid_amt
    ,sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Mbr_Med_paid_amt
    ,sum(cl.paid_amt) + sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Total_Med_Cost
    ,sum(cl.deductible_amt) as Mbr_Med_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.deductible_amt else 0 end) as Par_Med_mbr_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.deductible_amt else 0 end) as NonPar_Med_mbr_deductible_paid_amt
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cl.src_clm_id else null end) as Ambulance_claim_count
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Ambulance_event_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cl.src_clm_id else null end) as Behavioral_Health_claim_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Behavioral_Health_visits /*I observed a situation that member had 2 visits ON 2 different dates, but both visits were filed under the same source claim id.*/
    FROM `edp-prod-hcbstorage.edp_hcb_core_cnsv.D_CLAIM_LINE` cl
    WHERE cast(cl.srv_start_dt AS DATE) >= @start_date
    AND cast(cl.srv_start_dt AS DATE) <= @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    GROUP BY src_member_id)
    SELECT *
    FROM all_claim"""

pull_utilization_bq_sql = """
    with all_claim as (
    SELECT case when left(cast(member_id as string), 2) = '10' then left(cast(member_id as string),12) else cast(member_id as string) end as src_member_id, 
    ppy.reportingyear as reportingyear,
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.src_clm_id else null end) as emerg_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.src_clm_id else null end) as pcp_transactions,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.src_clm_id else null end) as spclst_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.src_clm_id else null end) as IP_transactions,
    count (distinct case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as emerg_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as primary_care_visits,
    count (distinct case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as spclst_visits,
    count (distinct case when cl.hcfa_plc_srv_cd in ('21', '51') AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as ip_visits_non_collapsed,
    count (distinct case when (cl.PRCDR_CD in ('G0402','G0403','G0404','G0405','G0438','G0439') or
    (PRCDR_CD  IN ('99381','99382','99383','99384','99385','99386','99387','99388','99389','99390',
        '99391','99392','99393','99394','99395','99396','99397','99401','99402','99403','99404',
        '99201','99202','99203','99204','99205','99211','99212','99213','99214','99215')
    AND pri_icd9_dx_cd IN ('V03.0','V03.1','V03.2','V03.3','V03.4','V03.5','V03.6','V03.7','V03.8',
        'V03.9','V04.0','V04.1','V04.2','V04.3','V04.4','V04.5','V04.6',
        'V04.7','V04.8','V04.9','V04.10','V04.11','V04.12','V04.13','V04.14','V04.15','V04.16','V04.17','V04.18',
        'V04.19','V04.20','V04.21','V04.22','V04.23','V04.24','V04.25','V04.26','V04.27','V04.28','V04.29','V04.30',
        'V04.31','V04.32','V04.33','V04.34','V04.35','V04.36','V04.37','V04.38','V04.39','V04.40','V04.41','V04.42',
        'V04.43','V04.44','V04.45','V04.46','V04.47','V04.48','V04.49','V04.50','V04.51','V04.52','V04.53','V04.54',
        'V04.55','V04.56','V04.57','V04.58','V04.59','V04.60','V04.61','V04.62','V04.63','V04.64','V04.65','V04.66',
        'V04.67','V04.68','V04.69','V04.70','V04.71','V04.72','V04.73','V04.74','V04.75','V04.76','V04.77','V04.78',
        'V04.79','V04.80','V04.81','V04.82','V04.83','V04.84','V04.85','V04.86','V04.87','V04.88','V04.89',
        'V05.0','V05.1','V05.2','V05.3','V05.4','V05.5','V05.6','V05.7','V05.8','V05.9',
        'V06.0','V06.1','V06.2','V06.3','V06.4','V06.5','V06.6','V06.7','V06.8','V06.9','V20.0','V20.1','V20.2',
        'V70.0','V70.3','V70.5','Z23','Z76.1','Z76.2')))
    then cast(cl.member_id as string) ||
    cast(cl.SRV_START_DT as string) else null end) as preventive_care_visits
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.paid_amt else 0 end) as emerg_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.paid_amt else 0 end) as pcp_benefit_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.paid_amt else 0 end) as spclst_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.paid_amt else 0 end) as IP_benefit_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '23' AND cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as emerg_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('PP', 'OB') or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as pcp_mbr_paid_amt
    ,sum (case when (cl.src_prvdr_ty_cd in ('SP', 'SG') or cl.srv_spclty_ctg_cd in ('A','C','D','E','G','H','II','MG','N','NE','NN','OP','PD','PY','RH', 'CS','DC','EN','NS','O','OR','OS','PS', 'S','U','VS', 'RO', 'VVDT', 'VVMH', 'VVPD', 'VVRH', 'VRAD', 'WBHP', 'WDI', 'WRAD', 'WSD', 'WBHF', 'WSA', 'WOTF')) then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as spclst_mbr_paid_amt
    ,sum (case when cl.hcfa_plc_srv_cd = '21' AND cl.prcdr_cd in ('99221', '99222', '99223', '99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239') then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as IP_mbr_paid_amt
    ,count(distinct case when  cl.paid_prvdr_par_cd='Y' then cl.src_clm_id else null end) as Par_Med_claim_count
    ,count(distinct case when  cl.paid_prvdr_par_cd='N' then cl.src_clm_id else null end) as NonPar_Med_claim_count
    ,count(distinct case when cl.paid_prvdr_par_cd='Y' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Par_Med_visits
    ,count(distinct case when cl.paid_prvdr_par_cd='N' then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as NonPar_Med_visits
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.paid_amt else 0 end) as Par_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.paid_amt else 0 end) as NonPar_Med_benefit_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as Par_Med_mbr_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt else 0 end) as NonPar_Med_mbr_paid_amt
    ,sum(cl.paid_amt) as Aetna_Med_paid_amt
    ,sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Mbr_Med_paid_amt
    ,sum(cl.paid_amt) + sum(cl.srv_copay_amt + cl.coinsurance_amt + cl.deductible_amt) as Total_Med_Cost
    ,sum(cl.deductible_amt) as Mbr_Med_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='Y' then cl.deductible_amt else 0 end) as Par_Med_mbr_deductible_paid_amt
    ,sum(case when cl.paid_prvdr_par_cd='N' then cl.deductible_amt else 0 end) as NonPar_Med_mbr_deductible_paid_amt
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cl.src_clm_id else null end) as Ambulance_claim_count
    ,count(distinct case when cl.hcfa_plc_srv_cd in ('41', '42') or cl.prcdr_cd in ('A0425', 'A0426', 'A0427', 'A0428', 'A0429', 'A0432', 'A0433', 'A0434') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Ambulance_event_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cl.src_clm_id else null end) as Behavioral_Health_claim_count
    ,count(distinct case when cl.SRV_LINE_TYPE_CD='BH' or cl.src_prvdr_ty_cd in ('ABA', 'BAA',  'BHG', 'BHR', 'CP', 'DAC', 'MH', 'MR', 'NPB', 'PAB', 'PE', 'PN', 'PSH', 'SA') or cl.srv_spclty_ctg_cd in ('WBHP', 'WBHF', 'WSA', 'PY', 'VVMH') then cast(cl.member_id as string) || cast(cl.SRV_START_DT as string) else null end) as Behavioral_Health_visits /*I observed a situation that member had 2 visits ON 2 different dates, but both visits were filed under the same source claim id.*/
    FROM `edp-prod-hcbstorage.edp_hcb_core_cnsv.D_CLAIM_LINE` cl join `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_EnterpriseIDs_ED` eid
    ON case when left(cast(member_id as string), 2) = '10' then left(cast(member_id as string),12) else cast(member_id as string) end = eid.sourcememberid join edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_PlanPerReportingYear ppy
    ON eid.starsmember_id = ppy.starsmember_id
    WHERE cast(cl.srv_start_dt AS DATE) >= @start_date
    AND cast(cl.srv_start_dt AS DATE) <= @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    AND (ARRAY_LENGTH(IFNULL(@mbr_list, [])) = 0 OR case when left(cast(member_id as string), 2) = '10' then left(cast(member_id as string),12) else cast(member_id as string) end in UNNEST(@mbr_list))
    GROUP BY src_member_id, reportingyear)
    SELECT *
    FROM all_claim"""

pull_rx_claims_line_gpi_bq_sql = """
    SELECT
    case when left(cast(CL.MEMBER_ID as string), 2) = '10' 
    then left(cast(CL.MEMBER_ID as string),12) 
    else cast(CL.MEMBER_ID as string) end as Member_ID,
    SUM(CL.GROSS_MBR_RESP_AMT) AS RX_MBR_PAID_AMOUNT,
    COUNT(CL.CLAIM_REF_NBR) AS RX_CLAIM_COUNT,
    COUNT(DISTINCT SUBSTR(CL.ADJUDICATED_GPI_CD,1,10)) AS DISTINCT_GPI  FROM
    `edp-prod-hcbstorage.edp_hcb_core_cnsv.RX_CLAIM_DTL` cl
    WHERE CL.DISP_DT BETWEEN DATE(@start_date) AND DATE(@end_date)
    AND CL.BUSINESS_LN_CD = 'ME'
    AND CL.CLM_LN_STATUS_CD='P'
    AND (ARRAY_LENGTH(IFNULL(@mbr_list, [])) = 0 OR case when left(cast(CL.MEMBER_ID as string), 2) = '10' then left(cast(CL.MEMBER_ID as string),12) else cast(CL.MEMBER_ID as string) end in UNNEST(@mbr_list))
    GROUP BY CL.MEMBER_ID"""

pull_rx_claims_line_gpi_bq_sql = """
    SELECT
    case when left(cast(CL.MEMBER_ID as string), 2) = '10' 
    then left(cast(CL.MEMBER_ID as string),12) 
    else cast(CL.MEMBER_ID as string) end as Member_ID,
    SUM(CL.GROSS_MBR_RESP_AMT) AS RX_MBR_PAID_AMOUNT,
    COUNT(CL.CLAIM_REF_NBR) AS RX_CLAIM_COUNT,
    COUNT(DISTINCT SUBSTR(CL.ADJUDICATED_GPI_CD,1,10)) AS DISTINCT_GPI  FROM
    `edp-prod-hcbstorage.edp_hcb_core_cnsv.RX_CLAIM_DTL` cl
    WHERE CL.DISP_DT BETWEEN DATE(@start_date) AND DATE(@end_date)
    AND CL.BUSINESS_LN_CD = 'ME'
    AND CL.CLM_LN_STATUS_CD='P'
    AND (ARRAY_LENGTH(IFNULL(@mbr_list, [])) = 0 OR case when left(cast(CL.MEMBER_ID as string), 2) = '10' then left(cast(CL.MEMBER_ID as string),12) else cast(CL.MEMBER_ID as string) end in UNNEST(@mbr_list))
    GROUP BY CL.MEMBER_ID"""

pull_nextgen_calls_sql = """
    SELECT SourceMemberID as Member_ID, count(CaseNumber) as NextGen_calls
    FROM calls.NextGenCalls
    WHERE CONTACTMETHOD in ('Phone Inbound','Chat', 'Click to Call' , 'Secure Message') 
    AND CASERECORDTYPE in ('Consumer_Business_Medicare',
    'Consumer_Business_Medicare_Privacy_Restriction',
    'Consumer_Business_Read_Only_Medicare','Read_Only_Medicare','Medicare_Privacy_Restriction','Medicare') 
    AND (ParentCaseRecordType is null or ParentCaseRecordType = '')
    AND CaseCreatedAt between @start_date AND @end_date
    GROUP BY SourceMemberID
    """

pull_disability_esrd_hospice_out_of_area_sql = """
    SELECT DISTINCT a.mbi as MEDICARE_NUMBER,
    case when a.hospice_ind>=1 then 1 else 0 end as hospice_ind,
    case when a.disability_ind>=1 then 1 else 0 end as disability_ind,
    case when a.esrd_ind>=1 then 1 else 0 end as esrd_ind,
    case when a.out_of_area_flag>=1 then 1 else 0 end as out_of_area_flag
    FROM (
    SELECT DISTINCT MBI,
    sum(case when rtrim(ltrim(B.HospiceIndicator))='1' then 1 else 0 end) as HOSPICE_IND,
    sum(case when rtrim(ltrim(B.DisabilityIndicator))='1' then 1 else 0 end) as DISABILITY_IND,
    sum(case when rtrim(ltrim(B.ESRDIndicator))='1' then 1 else 0 end) as ESRD_IND,
    sum(case when rtrim(ltrim(B.OutOfAreaFlag))='1' then 1 else 0 end) as OUT_OF_AREA_FLAG
    FROM [StarsDataHubProd].[Member].[VW_TRRMemberDetail] B
    WHERE B.CurrentRecord=1
    GROUP BY MBI) a
    """

pull_enterpriseids_bq_sql = """
    SELECT DISTINCT eid.mbi_de, eid.sourcememberid
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_EnterpriseIDs_ED` eid
    WHERE (ARRAY_LENGTH(IFNULL(@mbr_list, [])) = 0 OR eid.sourcememberid in UNNEST(@mbr_list))
    """

pull_fmc_duedate_bq_sql = """
    SELECT src_member_id, 
    admit_date, 
    discharge_date, 
    due_date, 
    cms_contract_number, 
    facility_name, 
    dsnp_flag, 
    patient_diagnosis
    FROM `edp-prod-hcbstorage.edp_hcb_medbi_rptg.v2_de_stars_fmc_stars_formatted_final`
    WHERE due_date >=@due_date_start_date
    """

pull_member_email_address_bq_sql = """
    SELECT DISTINCT mli.mbr_src_member_id,
    `edp-prod-mdcrbi-starsbi.voltage_edp_prod_mdcrbi_starsbi.decryptEmailEntUsEast4`(mea_email_txt) as member_email
    FROM `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_EMAIL_ADDRESS` email
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_MBR_LOB_INSURANCE` mli ON email.mbr_member_id = mli.mbr_member_id
    WHERE (ARRAY_LENGTH(IFNULL(@mbr_list, [])) = 0 OR mli.mbr_src_member_id in UNNEST(@mbr_list))
    """

update_dsnp_contract_pbp_df_file_sql = """SELECT 
        EFF_Date,
        Member_ID,
        Contract_Number, PBP, CMS_State, MA_Region, Plan_Type, SNP
        FROM dbo.MEDICARE_ENROLLMENT 
        WHERE PRODUCT='MA' 
        AND ISSUED_STATUS = 'Issued' 
        AND Group_Status='Individual'
        AND SNP in ('D', 'I', 'C', 'F')
        """

pull_ctm_bq_sql = """SELECT 
    DISTINCT COMPLAINT_ID,
    RECEIVED_DT,
    COMPLAINT_CATEGORY,
    COMPLAINT_SUBCATEGORY,
    COMPLAINT_SUMMARY,
    RESOLUTION_DT,
    Contract_Number,
    PBP,
    LIS_Flag,
    MEDICARE_NUMBER,
    REPORTING_YEAR
    FROM `edp-prod-hcbstorage.edp_hcb_medbi_rptg.MSBI_CTMS_DTL` 
    WHERE CURRENT_RECORD_IND = 'Y'
    AND EXCLUSION_IND='N'
    AND COALESCE(STATUS_REASON, '') NOT LIKE 'CMS Removed%'
    AND CMS_CNTRCT_NBR NOT IN ('S5768','S5810','S5601', 'H2506', 'H7172', 'H8026')
    AND (RECEIVED_DT >= @start_date AND 
    RECEIVED_DT <= @end_date )
    """

pull_grievance_bq_sql = """SELECT 
    DISTINCT GRV_NBR,
    Contract_Number,
    PBP,
    LIS_Flag,
    MEDICARE_NUMBER,
    SOURCE,
    CTG,
    SUB_CTG,
    PRIORITY,
    OCCUR_DTS,
    REPORTED_DTS,
    REPORTING_YEAR,
    STATUS_REASON,
    Age,
    Gender
    FROM `edp-prod-hcbstorage.edp_hcb_medbi_rptg.MSBI_GRV_DTL` 
    WHERE
    CMS_CNTRCT_NBR not like 'S%'
    AND STATUS NOT IN ('Avert', 'Dismissed', 'Withdrawn')
    AND REQ_TYP IN ('Part C Grievance', 'Part D Grievance')
    AND (REPORTED_DTS >= @start_date AND 
    REPORTED_DTS <= @end_date )
    """

pull_ip_visit_from_hedis_bq_sql = """
    SELECT SourceMemberID,
    ReportingYear,
    CMSContractNumber,
    AdmitDate,
    DischargeDate,
    AdmitDischargeDescription,
    ClaimPrimaryDiagCode,
    ClaimDiagDescription,
    ProviderID,
    FacilityName,
    ReadmitDate,
    ReadmitDischargeDate,
    ReadmitClaimPrimaryDiagCode,
    ReadmitClaimDiagDescription,
    ReadmitFacility
    FROM `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.HEDIS_VW_PCRMemberDetailWeekly`
    WHERE AdmitDate >= @start_date AND AdmitDate <= @end_date
    """

pull_provider_demographic_sql = """
    SELECT ProviderID, AddressLine1, AddressLine2, City, County, State, Zip, LabelName
    FROM [StarsDataHubProd].[Provider].[Demographics] as t1
    WHERE LastUpdated = (
        SELECT max(LastUpdated) FROM [StarsDataHubProd].[Provider].[Demographics] as t2
        WHERE t1.ProviderID = t2.ProviderID
        )
    """

pull_er_visit_bq_sql = """
    SELECT DISTINCT
    mbr.Member_ID,
    mbr.Contract_Number,
    cl.SRV_START_DT, 
    cl.SRV_STOP_DT,
    prcdr_cd,
    pri_icd9_dx_cd as diag_code,
    dm.label_nm as facility_nm
    FROM `edp-prod-hcbstorage.edp_hcb_msts_srmepr_srcv.MEPR_SQL_MEDICARE_ENROLLMENT_VW` mbr
    INNER JOIN `edp-prod-hcbstorage.edp_hcb_core_cnsv.D_CLAIM_LINE` cl on mbr.Member_ID = SUBSTR(CAST(cl.member_id AS STRING), 1, 12)
    LEFT JOIN `edp-prod-hcbstorage.edp_hcb_core_cnsv.PROVIDER_DM` dm on cl.paid_prvdr_id = dm.provider_id	 
    WHERE mbr.Term_Date >= @enroll_start_date
    AND mbr.Eff_Date <= @enroll_end_date
    AND cl.srv_start_dt >= @start_date
    AND cl.srv_start_dt <= @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    AND (cl.hcfa_plc_srv_cd = '23'
    AND (cl.prcdr_cd in ('99281', '99282', '99283', '99284', '99285', '99288', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384') 
        OR (cl.REVENUE_CD in ('450', '451', '452', '456','459'))))
    """

pull_pcp_visit_sql = """
    SELECT DISTINCT m360.SRC_MEMBER_ID as SRC_MEMBER_ID,
    CMS_CNTRCT_NBR,
    PHONE_NBR,
    PROVIDER_ID,
    GROUP,
    GROUP_NAME,
    cast(cl.SRV_START_DT as varchar(10)) as SRV_START_DT, 
    cast(cl.SRV_STOP_DT as varchar(10)) as SRV_STOP_DT
    FROM starstemp.stars_analytics_mbr_360 m360
    INNER JOIN iwh.claim_line cl on m360.member_id=cl.member_id
    WHERE m360.reporting_year= @reporting_year
    AND m360.mdcr_offer_typ_cd in ('MAPD', 'MA')
    AND cl.srv_start_dt between @start_date AND @end_date
    AND cl.business_ln_cd = 'ME'
    AND cl.summarized_srv_ind='Y'
    AND cl.clm_ln_status_cd='P'
    AND (cl.src_prvdr_ty_cd in ('PP', 'OB') 
    or cl.srv_spclty_ctg_cd in ('FP','I','P', 'OG'))
    """

pull_healthhub_sql = """
    SELECT STORE_ID, PHARMACY_NM, ADDRESS_LN_1_TXT, CITY_NM, 
    STATE_POSTAL_CD, ZIP_CD, HEALTHHUB_IND, LAT_NBR, LONG_NBR,
    FCLTY_TYP_DESC, STORE_LCTN_TYP_DESC, IN_STORE_PHMCY_IND,
    DRV_THRU_IND
    FROM dm.MSBI_CVS_LOCATION_DTL
    WHERE HEALTHHUB_IND='Y'
    """

pull_geo_info_sql = """
    SELECT MEDICARE_NBR, MBR_LAT, MBR_LONG, INSERTED_DTS FROM dm.MSBI_INDVDL_PROFILE_DTL
    """

pull_residence_type_bq_sql = """
        SELECT asm_id_value_desc,
            asad_assmnt_status_typ_key_cd,
            asad_question_txt,
            asad_updated_on_dts,
            asad_assmnt_ans_data_txt,
            asad_section_nm
        FROM `edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ASSESSMENT`  a
        WHERE a.asm_template_nm in ('DSNP Health Risk Assessment', 'DSNP Health Risk Assessment (Converted- Refer to PDF)','Health Risk Assessment')
            AND asm_assmnt_status_typ_key_cd='Completed'
            AND a.asm_voided_on_dts is NULL 
            AND asad_question_txt='Current living arrangement?'
    """

pull_vbc_bq_sql = """
    SELECT distinct
    member_plan.StarsMEMBER_ID,
    member_plan.SourceMemberID,
    member_plan.CMSContractNumber, 
    member_plan.PBPID,
    a.PBGGroup,
    a.PBGGroupName, 
    a.TIN, 
    a.TINName,
    a.VBC, 
    a.ProviderID, 
    a.LabelName, 
    --a.par_ind, 
    --a.primary_ind,
    a.PBGSTATUS,
    a.PbgInd
    from `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_PlanPerReportingYear` member_plan
    left join (select distinct prov_att.StarsMEMBER_ID, prov_att.SourceMemberID,
                    prov_att.PBGGroup,
                    prov_att.PBGGroupName, prov_att.TIN, prov_att.TINName,
                    case when prov_att.PbgInd = 'Y' then 'VBC' else ' ' end as VBC, 
                    demo.ProviderID,
                    demo.LabelName,
                    demo.NPI, demo.Phone,
                    prov_att.PBGSTATUS, prov_att.PbgInd
                from `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Member_VW_ProviderAttribution` prov_att
                left join `edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Provider_VW_Demographics` demo on prov_att.ProviderID=demo.ProviderID
                where prov_att.ReportingYear = @reporting_year
                ) a on member_plan.StarsMEMBER_ID = a.StarsMEMBER_ID
    where member_plan.ReportingYear = @reporting_year
    """