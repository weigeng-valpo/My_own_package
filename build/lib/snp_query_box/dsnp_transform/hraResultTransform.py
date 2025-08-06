import pandas as pd

def get_most_recent_hra(df):
    df = df[df["asad_assmnt_status_typ_key_cd"]=="Completed"]
    df["asad_updated_on_dts"] = pd.to_datetime(df["asad_updated_on_dts"])
    #get the most recent version
    version_max_df = df.groupby("asm_id_value_desc")["asad_template_version_nbr"].max().reset_index()
    df_sorted = df.sort_values(["asm_id_value_desc", "asad_template_version_nbr", "asad_updated_on_dts"])
    most_recent_version_df = df_sorted.merge(version_max_df, how="inner", on=["asm_id_value_desc", "asad_template_version_nbr"])
    
    #if there is multiple records select the most recent records
    most_recent_not_null_df = most_recent_version_df[(most_recent_version_df["asad_assmnt_ans_data_txt"].notnull()) & (most_recent_version_df["asad_assmnt_ans_data_txt"] != '')]
    most_recent_hra_df = most_recent_not_null_df.drop_duplicates(["asm_id_value_desc", "asad_question_txt"], keep='last')
    most_recent_hra_df["asad_question_txt_cleaned"] = most_recent_hra_df["asad_question_txt"].str.replace(r'<[^>]*>', '', regex = True)
    return most_recent_hra_df

def pivot_hra_qa(df, version = 4):
    df_version = df[df["asad_template_version_nbr"] == version]
    df_update_dt = df_version.groupby("asm_id_value_desc").agg({"asad_updated_on_dts":"max"})
    wide_df = df_version.pivot_table(index = "asm_id_value_desc", columns='asad_question_txt_cleaned', values ='asad_assmnt_ans_data_txt', aggfunc='first')
    wide_df = wide_df.reset_index().rename_axis(None, axis=1)
    wide_df = wide_df.merge(df_update_dt, how="left", on = "asm_id_value_desc")
    return wide_df

version_three_to_four_dict = {
    "column": "new_column"
}

def version_three_to_four(ver_three_df):
    ver_three_df_renamed = ver_three_df.rename(columns = version_three_to_four_dict)
    return ver_three_df_renamed


def clean_general_info(df):
    '''clean template 4, general information, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    'Age:': "age",
    "Assessment Date:": "assessment_date",
    'Assessment Setting:': "assessment_type",
    'Assessment completed by:': "completed_by",
    'Describe Other living situation:': "living_stuation",
    'Describe your hearing challenges:': "hearing_challenge",
    'Describe your language barriers:' : "language_challenge",
    'Describe your vision challenges:' : "vision_challenge",
    'Do you have any language challenges, or hearing or vision challenges?': "any_challenge",
    'Do you have someone living with you?': "live_with",
    'Ethnicity:If ethnicity is not prepopulated manually, enter in text box.': 'ethnicity',
    'Gender:': 'gender',
    'What healthcare and support services are you currently receiving?': "current_receiving_service",
    'If other, describe support services:': 'other_current_receiving_service',
    'If Other, describe assessment setting': 'assessment_type',
    'Primary Spoken Language:': 'primary_spoken_language',
    'If Other, describe primary spoken language:': 'other_primary_spoken_language',
    'If someone has assisted you (or your authorized representative) with completing this assessment; describe relationship:': 'hra_complete_helper',
    'If Other External Party or Other, describe relationship to member:': 'hra_complete_helper_relationship',
    'In general, what would you say your health is?': "general_health",
    'My Authorized Representatives name is:': 'authorized_representative_nm'
    }
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["current_receiving_service"] = cleaned_df["current_receiving_service"].str.split('|')
    cleaned_df["hra_section"] = "general_info"
    return cleaned_df


def clean_service_support(df):
    '''clean template 4, service and support section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    'These days, a lot of people are having trouble getting the things they need, like groceries, medicines, or housing. In the past year, have you or any family members you live with been unable to get any of the following when it was really needed?': "any_concern",
    "Are you worried about losing your housing?": "losing_house_concern",
    'If Housing is checked What is your housing situation today?': "housing_situation",
    'If Transportation is checked, has lack of transportation kept you from medical appointments, meetings, work, or from getting things needed for daily living?  Check all that apply:': "transportation_concern",
    'If Food is checked, do you always have enough food for your family?': "enough_food", 
    "If other, describe the trouble you have getting the things you need:": "other_concern",
    "Would you like help with this?": "need_help"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["any_concern"] = cleaned_df["any_concern"].str.split('|')
    cleaned_df["any_concern"] = cleaned_df["any_concern"].fillna('')
    cleaned_df["hra_section"] = "service_and_support"

    return cleaned_df

def clean_health_status(df):
    '''clean template 4, health_status section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    'Do you have any additional health concerns?': "health_concern",
    "If yes, describe additional health concerns": "health_concern_desc"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "health_status"
    return cleaned_df

def clean_pain(df):
    '''clean template 4, pain section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    'Do you have any pain?': "any_pain",
    'If Yes is selected, Pain on scale of 1 to 10?': "pain_level",
    'If Yes is selected, where is your pain?': "pain_location",
    'If Other, describe the location of your pain': "other_pain_location",
    'If Yes is selected, Are you receiving treatment for this pain by a Doctor (Pain Management, Orthopedic, PCP or other)?':"pain_treatment",
    "If Other is selected: Describe pain treatment:": "other_pain_treatment"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "pain"
    return cleaned_df

def clean_advanced_directives(df):
    '''clean template 4, advanced_directives section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    'Are you interested in receiving information about advanced directives?': "want_info_about_advanced_directives",
    "Do you have documents with instructions about the medical care you wish to receive if you are unable to make decisions for yourself.  These may be known as a living will, advanced directives or healthcare/medical power of attorney.": "documents_about_medical_care"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "advanced_directives"
    return cleaned_df

def clean_bh_depression(df):
    '''clean template 4, bh_depression section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Many folks these days tell us that theyre feeling down, lonely, or sad. Is this something you have been feeling? Over the last two weeks, how often have you experienced any of the following:": "question_desc",
    "Depression Score Explanation:": "depression_score_explanation",
    "Feeling down, depressed, hopeless, or lonely:":"depresed_hopeless_or_lonely",
    "Little interest or pleasure in doing things:":"little_interest_or_pleasure"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    cleaned_df = cleaned_df[["dsnp_member_id", "depresed_hopeless_or_lonely", "little_interest_or_pleasure"]]
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "bh_depression"
    return cleaned_df

def clean_environment_safety(df):
    '''clean template 4, environment_safety section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Are you afraid of anyone or is anyone hurting you?": "afraid_of_anyone_hurting_you",
    "Are you afraid of falling?": "afraid_of_falling",
    "Have you fallen in the last month?":"have_fallen_last_month",
    "Is anyone using your money without your OK?":"money_used_without_your_ok"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "environment_safety"
    return cleaned_df

def clean_functional_status(df):
    '''clean template 4, functional_status section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Do you use any assistive or adaptive devices? (Examples: cane, walker, wheelchair, artificial limb)": "any_assistive_adaptive_devices",
    "If yes, describe assistive or adaptive devices:": "what_assistive_adaptive_device",
    "For all of the areas for which you need assistance, are you getting the help you need?":"getting_help_you_need",
    "If no, you are not getting the help you need, describe what additional help is needed:":"help_you_need_but_not_getting",
    "Have you had any changes in thinking, remembering, or making decisions?Check all that apply:":"any_difficulty_thinking_remembering",
    "If other, describe changes in thinking, remembering, or making decisions:": "other_difficulty_thinking_remembering",
    "Here is a list of activities you might do in a typical day.  Please select the daily activities that you need help with. (check all that apply):": "daily_activities"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "functional_status"
    return cleaned_df

def clean_health_care_metrics(df):
    '''clean template 4, health_care_metrics section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "BMI (Body Mass Index)": "bmi",
    "Have you received a flu shot within the last 12 months?": "flu_shot_last_12months",
    "How many times in the past 12 months have you used tobacco products (like cigarettes, cigars, snuff, chew, electronic cigarettes)?":"tobacco_last_12months",
    "How much do you weigh? (pounds)":"weight_pounds"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df["height"] = cleaned_df["Feet"] + "feet " + cleaned_df["Inches"] + "inches"
    cleaned_df.drop(columns = ["Feet", "Inches", "How tall are you?"], inplace=True )
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "health_care_metrics"
    return cleaned_df

def clean_medication_help(df):
    '''clean template 4, medication_help section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Do you need help taking your medicines?": "need_help_taking_medicines"}
    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "medication_help"
    return cleaned_df

def clean_utilization(df):
    '''clean template 4, utilization section, use it after clean_hra_qa'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Have you been hospitalized in the last 12 months?": "ip_last_12months",
    "In the past year, have you gone to the ER?": "er_last_12months"}

    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    cleaned_df["hra_section"] = "utilization"
    return cleaned_df

def clean_chronic_condition(df):
    '''clean template 4, chronic_condition section, use it after clean_hra_qa
    current logic is minining only diagnosed condition lists, not how_long, treated'''

    question_map = {'asm_id_value_desc':"dsnp_member_id",
    "Have you ever been diagnosed with any of the following conditions? (check all that apply)": "diagnosed_conditions",
    "If other describe condition 1:": 'other_diagnosed_1',
    "If other describe condition 2:": 'other_diagnosed_2',
    "If other describe condition 3:": 'other_diagnosed_3',
    "If other describe condition 4:": 'other_diagnosed_4',
    "If other describe condition 5:": 'other_diagnosed_5',
    "If there are other conditions, please describe all other conditions": 'other_diagnosed_6'}

    df.columns = df.columns.str.strip().str.replace("'", "").str.replace('"', "")
    cleaned_df = df.rename(columns = question_map)
    print(cleaned_df.columns)
    cleaned_df = cleaned_df.apply(lambda x: x.str.lower().str.strip() if x.dtype =='object' and x.name !='dsnp_member_id' else x)
    concatenated_col = ['other_diagnosed_1','other_diagnosed_2','other_diagnosed_3','other_diagnosed_4','other_diagnosed_5','other_diagnosed_6']
    cleaned_df["other_diagnosed"] = cleaned_df[concatenated_col].apply(lambda x: '|'.join(x.dropna().astype(str)), axis=1)
    cleaned_df["hra_section"] = "chronic_condition"
    cleaned_df = cleaned_df[["dsnp_member_id", "diagnosed_conditions", "other_diagnosed", "asad_updated_on_dts", "hra_section"]]
    return cleaned_df