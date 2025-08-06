from dsnp_qc_box import QcFunction 
import pandas as pd
import numpy as np
import datetime as dt
import os
from snp_query_box import DsnpHelperFunction
from dateutil.relativedelta import relativedelta
import flatfileQcHelper

today = dt.datetime.today()
today_str = today.strftime('%Y-%m-%d')
last_date_of_last_month_str = DsnpHelperFunction.last_date_of_last_month(today_str, output_type="string")
last_date_of_last_month = DsnpHelperFunction.last_date_of_last_month(today_str)

last_month = last_date_of_last_month_str[:7]
one_year_rolling_back_date = pd.Timestamp(last_date_of_last_month + dt.timedelta(days=1) - relativedelta(years = 1))
print("one_year_rolling_back_date")
print(one_year_rolling_back_date)

first_date_of_year = DsnpHelperFunction.first_date_of_year(today_str, output_type="string")
print("first_date_of_year")
print(first_date_of_year)
first_date_of_last_month_str = DsnpHelperFunction.first_date_of_last_month(today_str, output_type="string")
print("first_date_of_last_month")
print(first_date_of_last_month_str)

reporting_end_date = last_date_of_last_month_str
print("reporting_end_date")
print(reporting_end_date)

last_reporting_end_date = DsnpHelperFunction.last_date_of_last_month(reporting_end_date, output_type="string")
last_reporting_end_date

storage_path = r'//mbip/medicarepBI/Projects/COE/DSNP/dsnp_data_storage/monthly_flat_files'
flat_file_path = f'{storage_path}/{reporting_end_date}'
prev_flat_file_path = f'{storage_path}/{last_reporting_end_date}'

qc_file_list = [
 'active_membership_sfs.pkl',
 'call_log_df.parquet',
 'clinical_conditions_df.parquet',
 'condensed_dsnp_mbr_2024.pkl',
 'disability_df.parquet',
 'dsnp_base_member_df.parquet',
 'ev_call_df.parquet',
 'hedis_ps_wide.parquet',
 'iha_df.parquet',
 'ked_gap_status_df.parquet',
 'mbr_contact_legal_short_valid_df.parquet',
 'med_adherence_df_with_detail.parquet',
 'member_cm_engagement_df.parquet',
 'monthly_active_member_utr_dtr_df.parquet',
 'pbg_group_clean_df.parquet',
 'prefer_language_df.parquet',
 'pull_ev_call.parquet',
 'pull_ev_call_sop.parquet',
 'pull_member_do_not_mail.parquet',
 'pull_member_phone_info.parquet',
 'pull_prefer_language.parquet',
 'pull_prefer_language_qnxt.parquet',
 'pull_race.parquet',
 'pull_resident_type.parquet',
 'pull_risk_strat_from_icp.pkl',
 'pull_risk_strat_nj.pkl',
 'pull_risk_strat_va.pkl',
 'qnxt_call_df.parquet',
 'not_real_df.pkl',
 'race_df.parquet',
 'sop_campaign_unique_df.parquet',
 'utilization_df_12month.parquet',
 'utilization_df_ytd.parquet']

def run_flatfile_qc():
    import logging
    import io
    import sys
    import win32com.client as win32
    import time
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    start_time = time.time()

    output_capture = io.StringIO()
    sys.stdout = output_capture

    for f in qc_file_list:
        file_name, file_type = f.split(".")
        print(f"{file_name}, QC start")
        if file_type == "parquet":
            try:
                try:
                    new_file = pd.read_parquet(f'{flat_file_path}/{f}')
                    prev_file = pd.read_parquet(f'{prev_flat_file_path}/{f}')
                except: #in case previously pickle but changed to parquet
                    new_file = pd.read_parquet(f'{flat_file_path}/{f}')
                    prev_file = pd.read_pickle(f'{prev_flat_file_path}/{file_name}.pkl')
                    print("Previously it was pickle file, but now parquet")
            except:
                print(f'{f} not found')
                logging.error(f"File not found: {file_name}")
                print("-------------------------------------------------------------------")
                continue
        if file_type == "pkl":
            try:
                prev_file = pd.read_pickle(f'{prev_flat_file_path}/{f}')
                new_file = pd.read_pickle(f'{flat_file_path}/{f}')
            except:
                print(f'{f} not found')
                logging.error(f"File not found: {file_name}")
                print("-------------------------------------------------------------------")
                continue
        logging.info(f"Successfully read file: {file_name}")
        print("Two dfs columns are the same?")
        column_check = QcFunction.compare_dfs(prev_file, new_file, check=["column"])
        print(column_check)
        if column_check["column"]==False:
            print("Columns removed:", set(prev_file.columns) - set(new_file.columns))
            print("Columns added:", set(new_file.columns) - set(prev_file.columns))

        agg_dict = getattr(flatfileQcHelper, file_name + "_dict")
        agg_criteria = getattr(flatfileQcHelper, file_name + "_criteria")
        try:
            qc_dict, prev_agg, new_agg = QcFunction.compare_previous_report(prev_file, new_file, agg_dict, agg_criteria)
            qc_result = QcFunction.qc_result_from_qc_dict(qc_dict)
            logging.info(f"{qc_dict}<br>previous report agg, {prev_agg}<br>new report agg, {new_agg}")
            if qc_result == "Fail":
                QcFunction.send_email_alarm(report_name = f"Flatfile QC failed, {file_name}", report_body = f"{qc_dict}<br>previous report agg, {prev_agg}<br>new report agg, {new_agg}", dist_list = 'LimS@aetna.com;')
        except:
            print(f"Can't compare the two files, check {file_name} manually")
            print("-------------------------------------------------------------------")
            continue

    print("Process time -- %s seconds" % (time.time() - start_time))
    sys.stdout=sys.__stdout__
    captured_output = output_capture.getvalue()
    captured_output_new_line = captured_output.replace('\n', '<br>')

    # Construct outlook Application instance
    olApp = win32.Dispatch('Outlook.Application')
    olNS=olApp.GetNameSpace('MAPI')
    #QC
    CC_List = 'lims@aetna.com'
    Distribution_List = 'lims@aetna.com;'

    # Construct the email object
    mailItem = olApp.CreateItem(0)
    mailItem.Subject ='Monthly flatfile QC report '+ dt.datetime.today().strftime("%B %Y")
    mailItem.BodyFormat = 1 ##'olFormatHTML'
    mailItem.HTMLBody = f"{captured_output_new_line}"

    mailItem.To = Distribution_List
    mailItem.CC = CC_List
    mailItem.Display()
    mailItem.Save()
    mailItem.Send()

if __name__ == "__main__":
    print("Start QC run")
    run_flatfile_qc()
    print("QC report is delivered")