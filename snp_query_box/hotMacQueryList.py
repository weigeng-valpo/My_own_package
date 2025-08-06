#This is only for GCP BQ, bq_sql should be used
#The others are for reference at this moment they should be migrate to BQ

pull_hot_mac_call_bq_sql = """
    SELECT
    MFB.SGK_MBR_FOCUS_ID
    ,MFB.SGK_RPT_MBR_FOCUS_ID
    ,MFB.FOCUS_DESC 
    ,MFB.INDIVIDUAL_ID
    ,MFB.MEMBER_ID
    ,MFB.MEDICARE_MEMBER_ID
    ,MFB.CALL_SCRIPT_DESC
    ,MFB.RECOMMENDATION
    ,MFB.UPDATE_TIMESTAMP
    ,MFB.MFS_CUR_FOCUS_STATUS_REASON_DESC
    ,MFB.MFS_CUR_FOCUS_STATUS_DESC
    ,MFB.LEGACY
    ,MFB.REPORTING_YEAR
    ,CTHB.CALL_TIMESTAMP
    ,CTHB.CALL_TO
    ,CT.LOOKUP_CD_DISPLAY_NM 
    ,CTHB.CALL_STATUS_DESC
    ,CTHB.NOT_REACH_REASON_DESC
    ,CTHB.CALL_TYPE_DESC
    ,CTHB.INSERT_TEAM_NM
    FROM 
    `anbc-hcb-prod.ca_mdcr_share_hcb_prod.STARS_WEB_RPT_MBR_FOCUS_BASE` AS MFB
    LEFT JOIN `anbc-hcb-prod.ca_mdcr_share_hcb_prod.STARS_WEB_RPT_CALL_TRACKING_HX_BASE` CTHB ON MFB.SGK_RPT_MBR_FOCUS_ID = CTHB.SGK_RPT_MBR_FOCUS_ID
    LEFT JOIN `anbc-hcb-prod.ca_mdcr_share_hcb_prod.STARS_WEB_RPT_SOP_LOOKUP_DATA_REF` CT on CTHB.CALL_TO= CT.LOOKUP_CD_ID and CT.SGK_LOOKUP_COL_ID=168
    WHERE LOOKUP_CD_DISPLAY_NM = 'Member'
    AND cast(CTHB.CALL_TIMESTAMP as date) >= @min_call_date
    AND cast(CTHB.CALL_TIMESTAMP as date) <= @max_call_date
    """
