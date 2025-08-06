from snp_query_box import DsnpHelperFunction
import numpy as np

oak_street_group_id_list = ['078308460','443944890','493493480','616208150','000300571','000541978',
                            '000827923','165733340','285638300','331444000','342270630','387583810',
                            '493493480','553193050','555854920','569078430','734908860','782749110',
                            '875357760','949383740']

oak_street_tin_list = ['862881266','923587529','871280001','881873193','853897058','922797872','364852089',
                       '462799683','474338846','462799683','862187389','851187027','364838998','862282500',
                       '850923324','862296173','364885791','862240615','320550273','851171884','364838998',
                       '301196182','853862139','364838998','843880989','922830291','462799683','462799683',
                       '553193050','875357760', '923587733', '931317211'] # '923587733', '931317211' added after Oak str manual check


def _get_group_id_name(df):
    '''This function is to create a new group name, which is concate of names
    because group id and group name is not always 1:1 match'''
    # recreate the group name to concatenate the multiple group names 
    group_name_all = df[['GROUP_ID','GROUP_NAME']]\
        .drop_duplicates()\
        .sort_values(['GROUP_ID'])\
        .reset_index(drop = True)
    group_name_df = group_name_all\
        .pivot_table(index = 'GROUP_ID', values = 'GROUP_NAME', \
                     columns = group_name_all.groupby('GROUP_ID').cumcount(),\
                     aggfunc = 'first')\
        .reset_index()
    group_name_df.set_index('GROUP_ID', inplace = True)
    group_name_df['GROUP_NAME'] = group_name_df.apply(lambda row: '|'.join(row.dropna().astype(str)), axis=1)
    group_name_df.reset_index(drop=False, inplace=True)
    group_name_df = group_name_df[['GROUP_ID','GROUP_NAME']]
    print(group_name_df.shape)
    group_name_df.head()
    return group_name_df

def _get_pbg_status_agg(df):
    '''This function is to create a new pbg status by group id
    because group id and pbg status is not always 1:1 match'''
    # recreate the PBG_STATUS to concatenate the multiple PBG_STATUS
    pbg_status_all = df[['GROUP_ID','PBG_STATUS']]\
        .drop_duplicates()\
        .sort_values(['PBG_STATUS'], ascending=False)\
        .reset_index(drop=True)
    pbg_dedup = pbg_status_all[~((pbg_status_all["PBG_STATUS"] == '') | (pbg_status_all["PBG_STATUS"].isnull()))]\
        .drop_duplicates()
    pbg_status_agg = pbg_dedup\
    .groupby("GROUP_ID")["PBG_STATUS"]\
    .apply(lambda x: '|'.join(x))\
    .reset_index()
    return pbg_status_agg


def get_pbg_group_clean(pull_vbc):
    '''Use pull_vbc in snp_query_box'''
    group_df = pull_vbc[~pull_vbc['PBGGroup'].isnull()]
    group_df['GROUP_NAME'] = group_df['PBGGroupName'].str.replace("[,./\']","", regex = True)
    group_df['GROUP_ID'] = group_df['PBGGroup'].astype('int').astype('str')
    
    invalid_list = ['Invalid Tin-Do Not Use','Invalid TinPin']
    group_valid = group_df[~group_df["GROUP_NAME"].isin(invalid_list)]
    group_valid = group_valid[group_valid['GROUP_NAME'].notnull()]
    group_valid['VBC'] = group_valid['VBC'].str.strip().fillna('non-vbc')
    group_valid.replace({'VBC':''}, {'VBC':'non-vbc'}, inplace = True)
    group_valid["PBG_STATUS"] = group_valid["PBGSTATUS"].str.strip()

    group_name_agg = _get_group_id_name(group_valid)
    print(group_name_agg.head(2))
    pbg_status_agg = _get_pbg_status_agg(group_valid)
    print(pbg_status_agg.head(2))

    # merge back, so we get aggregated pbg status, and group name
    group_selected_df = group_valid[['SourceMemberID', 'GROUP_ID', 'VBC', 'PbgInd', 'CMSContractNumber']]\
        .drop_duplicates()

    group_name_unique_df = group_selected_df.merge(group_name_agg, how = 'left', on = 'GROUP_ID')
    group_name_pbg_unique_df = group_name_unique_df.merge(pbg_status_agg, how = 'left', on = 'GROUP_ID')

    print(group_name_pbg_unique_df.PBG_STATUS.value_counts(dropna = False))
    
    # merge back the pivoted group name
    group_tin_df = group_valid[['SourceMemberID', 'CMSContractNumber', 'TIN', 'TINName', 'LabelName']]\
        .drop_duplicates()
    pbg_group_clean_df = group_name_pbg_unique_df.merge(group_tin_df, how = 'left', on = ['SourceMemberID', 'CMSContractNumber'])
    pbg_group_clean_df.rename(columns = {"SourceMemberID":"Member_ID"}, inplace=True)

    #One PBG status not null but non-vbc, clean this
    pbg_group_clean_df["PBG_STATUS"] = np.where(pbg_group_clean_df["VBC"] =="non-vbc", "non-vbc", pbg_group_clean_df["PBG_STATUS"])

    pbg_group_clean_df["Oak_Street_Ind"] = np.where((pbg_group_clean_df["GROUP_NAME"].str.lower().str.contains("oak street", na=False)) |
                                                (pbg_group_clean_df["TINName"].str.lower().str.contains("oak street", na=False)), True, False)
    print(pbg_group_clean_df.head(2))
    return pbg_group_clean_df