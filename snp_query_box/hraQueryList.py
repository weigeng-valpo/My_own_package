#This is only for GCP BQ, bq_sql should be used
#The others are for reference at this moment they should be migrate to BQ

pull_hra_qa_by_section_bq_sql = """
    SELECT asm_id_value_desc,
    asad_template_nm,
    asad_template_version_nbr,
    asad_page_nm,
    asad_assmnt_tab_id,
    asad_tab_nm,
    asad_assmnt_section_id,
    asad_section_nm,
    asad_assmnt_status_typ_key_cd,
    asad_assmnt_question_id,
    asad_question_txt,
    asad_updated_on_dts,
    asad_assmnt_ans_id,
    asad_assmnt_ans_typ_desc,
    ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(asad_assmnt_ans_data_txt, r'[a-zA-z0-9\s.,+|()/]+'), '') as asad_assmnt_ans_data_txt
    from edp-prod-hcbstorage.edp_hcb_clinical_curt_cnsv.VW_LR_ASSESSMENT a
    where a.asm_template_nm in ('DSNP Health Risk Assessment', 'DSNP Health Risk Assessment (Converted- Refer to PDF)')
    and asm_assmnt_status_typ_key_cd='Completed'
    and asad_template_version_nbr in (3, 4)
    and a.asm_voided_on_dts is NULL
    and a.asad_section_nm = @asad_section_nm
    """
