def splitCaseNoteSupplementalAssessment(pull_member_ict_case_note_bq):
    pull_member_ict_supplemental_assessment_bq = pull_member_ict_case_note_bq.loc[pull_member_ict_case_note_bq['nt_note_type_desc'] == 'Health Risk Assessment']
    pull_member_ict_case_note_bq = pull_member_ict_case_note_bq.loc[pull_member_ict_case_note_bq['nt_note_type_desc'] != 'Health Risk Assessment'].drop(columns='nt_inserted_by_id').drop_duplicates()
    pull_member_ict_case_note_bq = pull_member_ict_case_note_bq.groupby(['mbr_src_member_id', 'member_id', 'identifier_value_id', 'nt_note_id',
       'nt_object_id', 'nt_note_type_desc', 'nt_object_type_desc',
       'nt_updated_on_dts', 'nt_member_program_id',
       'nt_effective_dts', 'nt_inserted_on_dts', 'nt_note_dts']).agg({'rr':'sum'}).reset_index()
    return pull_member_ict_case_note_bq, pull_member_ict_supplemental_assessment_bq