import pandas as pd
import numpy as np

def member_contact_legal_transform(pull_member_contact_legal, first_date_of_last_month, last_day_reporting_year):
        mbr_contact_legal_short = pull_member_contact_legal[['mbr_identifier_value_id','mcl_legal_type_desc','mcl_start_dts','mcl_end_dts','mcl_conversion_id']].drop_duplicates()
        mbr_contact_legal_short = mbr_contact_legal_short[mbr_contact_legal_short["mcl_start_dts"].notnull()]
        mbr_contact_legal_short = mbr_contact_legal_short[~((mbr_contact_legal_short['mbr_identifier_value_id']=='')|(mbr_contact_legal_short['mbr_identifier_value_id'].isna()))]
        mbr_contact_legal_short = mbr_contact_legal_short.sort_values(by=['mbr_identifier_value_id','mcl_start_dts','mcl_end_dts'])
        mbr_contact_legal_short['mcl_end_dts'] = np.where(mbr_contact_legal_short['mcl_end_dts'].isnull(), last_day_reporting_year, mbr_contact_legal_short['mcl_end_dts'])

        mbr_contact_legal_short['mcl_start_dts_old'] = pd.to_datetime(mbr_contact_legal_short['mcl_start_dts'],errors='coerce')
        mbr_contact_legal_short['mcl_end_dts_old'] = pd.to_datetime(mbr_contact_legal_short['mcl_end_dts'],errors='coerce')
        mbr_contact_legal_short['mcl_start_dts'] = mbr_contact_legal_short['mcl_start_dts_old'].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')
        mbr_contact_legal_short['mcl_end_dts'] = mbr_contact_legal_short['mcl_end_dts_old'].dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')

        mbr_contact_legal_short['mcl_start_dts'] = pd.to_datetime(mbr_contact_legal_short['mcl_start_dts'])
        mbr_contact_legal_short['mcl_end_dts'] = pd.to_datetime(mbr_contact_legal_short['mcl_end_dts'])
        mbr_contact_legal_short_valid = mbr_contact_legal_short.loc[(mbr_contact_legal_short['mcl_start_dts']<=pd.to_datetime(first_date_of_last_month)) & 
            (mbr_contact_legal_short['mcl_end_dts'] >= pd.to_datetime(first_date_of_last_month))]
        mbr_contact_legal_short_valid.drop(columns = ['mcl_start_dts_old', 'mcl_end_dts_old'], inplace = True)
        return mbr_contact_legal_short_valid 
