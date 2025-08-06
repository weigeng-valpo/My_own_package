pull_fide_cob_ach_diag_exclusions_sql = """SELECT member_id, cms_contract_no,
    case when diagnosis_cd in UNNEST(@cob_exclusions) then 1 else 0 end as cob_exclusion,
    case when diagnosis_cd in UNNEST(@poly_exclusions) then 1 else 0 end as poly_exclusion
    FROM `edp-prod-hcbstorage.edp_hcb_mdcd_core_srcv.MDW_MEDICARE_MEMBER` mdw_mem
    inner join `edp-prod-hcbstorage.edp_hcb_mdcd_core_srcv.IODB_CLAIM_HEADER` clm_head on mdw_mem.iodb_member_key = clm_head.iodb_member_key
    inner join `edp-prod-hcbstorage.edp_hcb_mdcd_core_srcv.IODB_CLAIM_DIAGNOSIS` clm_diag on
    clm_head.iodb_claim_header_key = clm_diag.iodb_claim_header_key
    where cms_contract_no in ('H1610','H6399') and 
        (diagnosis_cd in  UNNEST(@cob_exclusions) or 
         diagnosis_cd in  UNNEST(@poly_exclusions))"""


#TODO: This could be cleaned up significantly. Leaving as-is to preserve daily FIDE process 
# but PBM is not limited to FIDE and should really sit in snpQueries long-term.
pull_fide_fmc_pbm_sql = """
    select distinct
        `edp-prod-mdcrbi-starsbi.voltage_edp_prod_mdcrbi_starsbi.decryptUs7Ascii2EntUsEast4`(mbr.mbr_acct_id) as carriermemid
        , cms_cntrct_id
        , phmcy.phmcy_pty_gid
        , phmcy.PHMCY_NM
        , prs.PRSCBR_ID
        , prs.PRSCBR_FULL_NM
        , prs.PRMRY_SPCLT_DESC
        , prs.NPI_ID
        , clm.fill_dt
        , clm.rx_wrtn_dt
        , drg.drug_abbr_nm
        , wk.phcyname
        , max(case when prs.prmry_spclt_desc like '%EMERGENCY%' 
                        or prs.scndry_spclt_desc  like '%EMERGENCY%' 
                        or prs.trtry_spclt_desc  like '%EMERGENCY%' 
            then 1 
            else 0 
        end )  as emergency_physician
        , 1 AS discharge_drug
    from 
        edp-prod-pbmstorage.edp_pbm_core_cnsv.V_PHMCY_CLM_PAID as clm 
        inner join edp-prod-pbmstorage.edp_pbm_core_cnsv.V_MBR_ACCT_DENORM as mbr
        on clm.mbr_acct_gid = mbr.mbr_acct_gid
                    and clm.lvl3_acct_gid = mbr.lvl3_acct_gid
    
    inner join edp-prod-pbmstorage.edp_pbm_core_cnsv.V_DRUG_DENORM as drg 
        on drg.drug_prod_gid = clm.drug_prod_gid
    inner join edp-prod-pbmstorage.edp_pbm_core_cnsv.V_PHMCY_DENORM as phmcy 
        on phmcy.phmcy_pty_gid = clm.phmcy_pty_gid
        left join edp-prod-hcbstorage.edp_hcb_mdcrstarsdi_apps_v.Provider_VW_RetailNetwork wk
        on cast(wk.NPI as STRING) = phmcy.NPI_ID
    inner join edp-prod-pbmstorage.edp_pbm_core_cnsv.V_PRSCBR_DENORM as prs 
        on prs.prscbr_pty_gid = clm.prscbr_pty_gid
    where clm.cms_cntrct_id in ('H1610','H6399')
        and  clm.fill_dt between CURRENT_DATE() -INTERVAL '7' DAY and CURRENT_DATE()
        and drg.drug_abbr_nm in ('PREDNISONE','ONDANSETRON','HYDROCO/APAP','CEPHALEXIN','AMOX/K CLAV','AZITHROMYCIN','TRAMADOL HCL','DOXYCYCL HYC',
            'CEFDINIR','OXYCODONE','CIPROFLOXACN','OXYCOD/APAP','METHYLPRED','LEVOFLOXACIN','SMZ/TMP DS','ONE TOUCH',
            'METRONIDAZOL','DEXAMETHASON','CEFUROXIME','FLUTICASONE','ALPRAZOLAM','LORAZEPAM','CYCLOBENZAPR','TIZANIDINE',
            'CLINDAMYCIN','NITROFUR MON','AMOXICILLIN','CLONAZEPAM','BUSPIRONE','PAXLOVID','DOXYCYC MONO','APAP/CODEINE',
            'FLUCONAZOLE','NYSTATIN','CEFPODOXIME','MUPIROCIN','OSELTAMIVIR','BACLOFEN','TRIAMCINOLON','HYDROXYZ HCL',
            'ONE TCH 33G','DICYCLOMINE','VALACYCLOVIR','METOCLOPRAM','BD PEN NEEDL','MIDODRINE','ZOLPIDEM','MECLIZIN RX',
            'LIDOCAINE','VANCOMYCIN','COLCHICINE','KETOROLAC','PROMETHAZINE','DIAZEPAM','LOPERAMIDE','NALOXONE HCL',
            'ERYTHROMYCIN','HYDROCORTISO','MORPHINE SUL','DICLOFENAC','ACYCLOVIR','HYDROXYZ PAM','HYDROMORPHON')
        and ( upper(wk.phcyname) like '%HOSP%' or prs.prmry_spclt_desc like '%EMERGENCY%' or prs.scndry_spclt_desc  like '%EMERGENCY%'  or prs.trtry_spclt_desc  like '%EMERGENCY%' )
    group by `edp-prod-mdcrbi-starsbi.voltage_edp_prod_mdcrbi_starsbi.decryptUs7Ascii2EntUsEast4`(mbr.mbr_acct_id)
        , cms_cntrct_id
        , phmcy.phmcy_pty_gid
        , phmcy.PHMCY_NM
        , prs.PRSCBR_ID
        , prs.PRSCBR_FULL_NM
        , prs.PRMRY_SPCLT_DESC
        , prs.NPI_ID
        , clm.fill_dt
        , clm.rx_wrtn_dt
        , drg.drug_abbr_nm
        , wk.phcyname
    """