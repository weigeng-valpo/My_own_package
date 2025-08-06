import pandas as pd
import numpy as np


def get_clinical_condition_df_bq(pull_clinical_condition_bq):
    '''Record what columns are in each disease group'''
    ABDOMEN_RELATED_DISORDERS = [
        'Cholelithiasis_or_Cholecystitis',
        'Peptic_Ulcer_Disease',
        'Nonspecific_Gastritis_or_Dyspepsia',
        'Eating_Disorders',
        'Diverticular_Disease',
        'Inflammatory_Bowel_Disease',
        'Pancreatitis',]
    BLOOD_RELATED_DISORDERS = [
        'Hypercoaguable_Syndrome',
        'Iron_Deficiency_Anemia',
        'Hemophilia_or_Congenital_Coagulopathies',
        'Sickle_Cell_Anemia',]
    CANCERS = [
        'Lung_Cancer',
        'Stomach_Cancer',
        'Prostate_Cancer',
        'Pancreatic_Cancer',
        'Other_Cancer',
        'Cervical_Cancer',
        'Malignant_Melanoma',
        'Colorectal_Cancer',
        'Breast_Cancer',
        'HodgkinsDisease_or_Lymphoma',
        'Oral_Cancer',
        'Ovarian_Cancer',
        'Esophageal_Cancer',
        'Leukemia_or_Myeloma',
        'Head_or_NeckCancer',
        'Brain_Cancer',
        'Bladder_Cancer',
        'Endometrial_Cancer',
        'Skin_Cancer',]
    HEART_RELATED_DISORDERS = [
        'Atrial_Fibrillation',
        'Congential_Heart_Disease',
        'Heart_Failure',
        'Ischemic_Heart_Disease',
        'Ventricular_Arrhythmia']
    LUNG_RELATED_DISORDERS = [
        'Asthma',
        'Chronic_Obstructive_Pulmonary_Disease',
        'Cystic_Fibrosis',]
    MENTAL_DISORDERS = [
        'Autism',
        'Anxiety',
        'Attention_Deficit_Disorder',
        'Bipolar',
        'Depression',
        'Neurosis',
        'Psychoses',
        'ADHD_and_other_Childhood_Disruptive_Disorders',]
    NEURAL_DISORDERS = [
        'Epilepsy',
        'Cerebrovascular_Disease',
        'Migraine_and_Other_Headaches',
        'Parkinsons_Disease',
        'Downs_Syndrome',
        'Multiple_Sclerosis',]
    ORTHOPEDIC = [
        'Rheumatoid_Arthritis',
        'Osteoarthritis',
        'Fibromyalgia',
        'Low_Back_Pain',
        'Osteoporosis',]
    RENAL = [
        'Chronic_Renal_Failure',
        'Kidney_Stones',]
    REPRODUCTIVE = [
        'Endometriosis',
        'Menopause',
        'Maternal_Hist_of_Low_Birth_Weight_or_Preterm_Birth',
        'Female_Infertility',
        'Post_Partum_BH_Disorder',]
    SUBSTANCE_RELATED_DISORDERS = [
        'Substances_Related_Disorders',
        'Psychiatric_Disorders_related_to_Med_Conditions',]
    VISION_RELATED_DISORDERS = [
        'Low_Vision_and_Blindness',
        'Cataract',
        'Glaucoma',]
    CRITICAL_CONDITIONS = [
        'Bladder_Cancer',
        'Brain_Cancer',
        'Breast_Cancer',
        'Cervical_Cancer',
        'Chronic_Obstructive_Pulmonary_Disease',
        'Chronic_Renal_Failure',
        'Colorectal_Cancer',
        'Endometrial_Cancer',
        'Esophageal_Cancer',
        'Head_or_NeckCancer',
        'Heart_Failure',
        'HIV_or_AIDS',
        'HodgkinsDisease_or_Lymphoma',
        'Leukemia_or_Myeloma',
        'Lung_Cancer',
        'Malignant_Melanoma',
        'Oral_Cancer',
        'Other_Cancer',
        'Ovarian_Cancer',
        'Pancreatic_Cancer',
        'Peripheral_Artery_Disease',
        'Prostate_Cancer',
        'Skin_Cancer', ]
    ALL_CONDITIONS = [
        'Bladder_Cancer',
        'Brain_Cancer',
        'Breast_Cancer',
        'Cervical_Cancer',
        'Chronic_Obstructive_Pulmonary_Disease',
        'Chronic_Renal_Failure',
        'Colorectal_Cancer',
        'Endometrial_Cancer',
        'Esophageal_Cancer',
        'Head_or_NeckCancer',
        'Heart_Failure',
        'HIV_or_AIDS',
        'HodgkinsDisease_or_Lymphoma',
        'Leukemia_or_Myeloma',
        'Lung_Cancer',
        'Malignant_Melanoma',
        'Oral_Cancer',
        'Other_Cancer',
        'Ovarian_Cancer',
        'Pancreatic_Cancer',
        'Peripheral_Artery_Disease',
        'Prostate_Cancer',
        'Skin_Cancer',
        'ADHD_and_other_Childhood_Disruptive_Disorders',
        'Alcoholism',
        'Anxiety',
        'Asthma',
        'Atrial_Fibrillation',
        'Autism',
        'Bipolar',
        'Cerebrovascular_Disease',
        'Congential_Heart_Disease',
        'Dementia',
        'Depression',
        'Diabetes_Mellitus',
        'Downs_Syndrome',
        'Epilepsy',
        'Fibromyalgia',
        'Hepatitis',
        'Hyperlipidemia',
        'Hypertension',
        'Ischemic_Heart_Disease',
        'Low_Back_Pain',
        'Neurosis',
        'Osteoarthritis',
        'Osteoporosis',
        'Parkinsons_Disease',
        'Psychoses',
        'Rheumatoid_Arthritis',
        'Substances_Related_Disorders',
        'Ventricular_Arrhythmia',]

    clinical_conditions_df = pull_clinical_condition_bq.copy()
    clinical_conditions_df['Abdomen_Related_Disorder'] = clinical_conditions_df[ABDOMEN_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Allergy'] = clinical_conditions_df['Allergy'].values
    clinical_conditions_df['Autoimmune_Disease'] = clinical_conditions_df['Systemic_Lupus_Erythematosus'].values
    clinical_conditions_df['Blood_Related_Disorder'] = clinical_conditions_df[BLOOD_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Cancer'] =  clinical_conditions_df[CANCERS].any(axis=1).astype('int8')
    clinical_conditions_df['Dementia'] = clinical_conditions_df['Dementia'].values
    clinical_conditions_df['Diabetes'] = clinical_conditions_df['Diabetes_Mellitus'].values
    clinical_conditions_df['DENTAL'] = clinical_conditions_df['Periodontal_Disease'].values
    clinical_conditions_df['Ear'] = clinical_conditions_df['Otitis_Media'].values
    clinical_conditions_df['Fatigue'] = clinical_conditions_df['Chronic_Fatigue_Syndrome'].values
    clinical_conditions_df['Heart_Related_Disorder'] = clinical_conditions_df[HEART_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Hepatitus'] = clinical_conditions_df['Hepatitis'].values
    clinical_conditions_df['Hiv'] = clinical_conditions_df['HIV_or_AIDS'].values
    clinical_conditions_df['Hyperlipidemia'] = clinical_conditions_df['Hyperlipidemia'].values
    clinical_conditions_df['Hypertension'] = clinical_conditions_df['Hypertension'].values
    clinical_conditions_df['Lungs_Related_Disorder'] = clinical_conditions_df[LUNG_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Lyme_Disease'] = clinical_conditions_df['Lyme_Disease'].values
    clinical_conditions_df['Mental_Disorder'] = clinical_conditions_df[MENTAL_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Neural'] = clinical_conditions_df[NEURAL_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Obesity'] = clinical_conditions_df['Obesity'].values
    clinical_conditions_df['Orthopedic'] = clinical_conditions_df[ORTHOPEDIC].any(axis=1).astype('int8')
    clinical_conditions_df['Peripheral_Artery_Disease'] = clinical_conditions_df['Peripheral_Artery_Disease'].values
    clinical_conditions_df['Prostate'] = clinical_conditions_df['Benign_Prostatic_Hypertrophy'].values
    clinical_conditions_df['Renal'] = clinical_conditions_df[RENAL].any(axis=1).astype('int8')
    clinical_conditions_df['Reproductive'] = clinical_conditions_df[REPRODUCTIVE].any(axis=1).astype('int8')
    clinical_conditions_df['Substances_Related_Disorders'] = clinical_conditions_df[SUBSTANCE_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Thyroid_disorder'] = clinical_conditions_df['Chronic_Thyroid_Disorders'].values
    clinical_conditions_df['Vision_Related_Disorder'] = clinical_conditions_df[VISION_RELATED_DISORDERS].any(axis=1).astype('int8')

    clinical_conditions_df['Critical'] = clinical_conditions_df[CRITICAL_CONDITIONS].sum(axis=1).astype('uint8')
    clinical_conditions_df['Total'] = clinical_conditions_df[ALL_CONDITIONS].sum(axis=1).astype('uint8')

    clinical_conditions_df['High_Need_Group'] = ((clinical_conditions_df['Critical'] > 1) |
                                            (clinical_conditions_df['Total'] > 5)
                                            ).astype('int8')
    return clinical_conditions_df

def get_clinical_condition_df(pull_clinical_condition):
    '''Record what columns are in each disease group'''
    ABDOMEN_RELATED_DISORDERS = [
        'CHOLELITHIASIS_OR_CHOLECYSTITIS',
        'PEPTIC_ULCER_DISEASE',
        'NONSPECIFIC_GASTRITIS_OR_DYSPEPSIA',
        'EATING_DISORDERS',
        'DIVERTICULAR_DISEASE',
        'INFLAMMATORY_BOWEL_DISEASE',
        'PANCREATITIS',]
    BLOOD_RELATED_DISORDERS = [
        'HYPERCOAGUABLE_SYNDROME',
        'IRON_DEFICIENCY_ANEMIA',
        'HEMOPHILIA_OR_CONGENITAL_COAGULOPATHIES',
        'SICKLE_CELL_ANEMIA',]
    CANCERS = [
        'LUNG_CANCER',
        'STOMACH_CANCER',
        'PROSTATE_CANCER',
        'PANCREATIC_CANCER',
        'OTHER_CANCER',
        'CERVICAL_CANCER',
        'MALIGNANT_MELANOMA',
        'COLORECTAL_CANCER',
        'BREAST_CANCER',
        'HODGKINSDISEASE_OR_LYMPHOMA',
        'ORAL_CANCER',
        'OVARIAN_CANCER',
        'ESOPHAGEAL_CANCER',
        'LEUKEMIA_OR_MYELOMA',
        'HEAD_OR_NECKCANCER',
        'BRAIN_CANCER',
        'BLADDER_CANCER',
        'ENDOMETRIAL_CANCER',]
    HEART_RELATED_DISORDERS = [
        'ATRIAL_FIBRILLATION',
        'CONGENTIAL_HEART_DISEASE',
        'HEART_FAILURE',
        'ISCHEMIC_HEART_DISEASE',
        'VENTRICULAR_ARRHYTHMIA']
    LUNG_RELATED_DISORDERS = [
        'ASTHMA',
        'CHRONIC_OBSTRUCTIVE_PULMONARY_DISEASE',
        'CYSTIC_FIBROSIS',]
    MENTAL_DISORDERS = [
        'AUTISM',
        'ANXIETY',
        'ATTENTION_DEFICIT_DISORDER',
        'BIPOLAR',
        'DEPRESSION',
        'NEUROSIS',
        'PSYCHOSES',
        'ADHD_AND_OTHER_CHILDHOOD_DISRUPTIVE_DISORDERS',]
    NEURAL_DISORDERS = [
        'EPILEPSY',
        'CEREBROVASCULAR_DISEASE',
        'MIGRAINE_AND_OTHER_HEADACHES',
        'PARKINSONS_DISEASE',
        'DOWNS_SYNDROME',
        'MULTIPLE_SCLEROSIS',]
    ORTHOPEDIC = [
        'RHEUMATOID_ARTHRITIS',
        'OSTEOARTHRITIS',
        'FIBROMYALGIA',
        'LOW_BACK_PAIN',
        'OSTEOPOROSIS',]
    RENAL = [
        'CHRONIC_RENAL_FAILURE',
        'KIDNEY_STONES',]
    REPRODUCTIVE = [
        'ENDOMETRIOSIS',
        'MENOPAUSE',
        'MATERNAL_HIST_OF_LOW_BIRTH_WEIGHT_OR_PRETERM_BIRTH',
        'FEMALE_INFERTILITY',
        'POST_PARTUM_BH_DISORDER',]
    SUBSTANCE_RELATED_DISORDERS = [
        'SUBSTANCES_RELATED_DISORDERS',
        'PSYCHIATRIC_DISORDERS_RELATED_TO_MED_CONDITIONS',]
    VISION_RELATED_DISORDERS = [
        'LOW_VISION_AND_BLINDNESS',
        'CATARACT',
        'GLAUCOMA',]
    CRITICAL_CONDITIONS = [
        'BLADDER_CANCER',
        'BRAIN_CANCER',
        'BREAST_CANCER',
        'CERVICAL_CANCER',
        'CHRONIC_OBSTRUCTIVE_PULMONARY_DISEASE',
        'CHRONIC_RENAL_FAILURE',
        'COLORECTAL_CANCER',
        'ENDOMETRIAL_CANCER',
        'ESOPHAGEAL_CANCER',
        'HEAD_OR_NECKCANCER',
        'HEART_FAILURE',
        'HIV_OR_AIDS',
        'HODGKINSDISEASE_OR_LYMPHOMA',
        'LEUKEMIA_OR_MYELOMA',
        'LUNG_CANCER',
        'MALIGNANT_MELANOMA',
        'ORAL_CANCER',
        'OTHER_CANCER',
        'OVARIAN_CANCER',
        'PANCREATIC_CANCER',
        'PERIPHERAL_ARTERY_DISEASE',
        'PROSTATE_CANCER',
        'SKIN_CANCER', ]
    ALL_CONDITIONS = [
        'BLADDER_CANCER',
        'BRAIN_CANCER',
        'BREAST_CANCER',
        'CERVICAL_CANCER',
        'CHRONIC_OBSTRUCTIVE_PULMONARY_DISEASE',
        'CHRONIC_RENAL_FAILURE',
        'COLORECTAL_CANCER',
        'ENDOMETRIAL_CANCER',
        'ESOPHAGEAL_CANCER',
        'HEAD_OR_NECKCANCER',
        'HEART_FAILURE',
        'HIV_OR_AIDS',
        'HODGKINSDISEASE_OR_LYMPHOMA',
        'LEUKEMIA_OR_MYELOMA',
        'LUNG_CANCER',
        'MALIGNANT_MELANOMA',
        'ORAL_CANCER',
        'OTHER_CANCER',
        'OVARIAN_CANCER',
        'PANCREATIC_CANCER',
        'PERIPHERAL_ARTERY_DISEASE',
        'PROSTATE_CANCER',
        'SKIN_CANCER',
        'ADHD_AND_OTHER_CHILDHOOD_DISRUPTIVE_DISORDERS',
        'ALCOHOLISM',
        'ANXIETY',
        'ASTHMA',
        'ATRIAL_FIBRILLATION',
        'AUTISM',
        'BIPOLAR',
        'CEREBROVASCULAR_DISEASE',
        'CONGENTIAL_HEART_DISEASE',
        'DEMENTIA',
        'DEPRESSION',
        'DIABETES_MELLITUS',
        'DOWNS_SYNDROME',
        'EPILEPSY',
        'FIBROMYALGIA',
        'HEPATITIS',
        'HYPERLIPIDEMIA',
        'HYPERTENSION',
        'ISCHEMIC_HEART_DISEASE',
        'LOW_BACK_PAIN',
        'NEUROSIS',
        'OSTEOARTHRITIS',
        'OSTEOPOROSIS',
        'PARKINSONS_DISEASE',
        'PSYCHOSES',
        'RHEUMATOID_ARTHRITIS',
        'SUBSTANCES_RELATED_DISORDERS',
        'VENTRICULAR_ARRHYTHMIA',]

    clinical_conditions_df = pull_clinical_condition.copy()
    clinical_conditions_df['Abdomen_Related_Disorder'] = clinical_conditions_df[ABDOMEN_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Allergy'] = clinical_conditions_df.ALLERGY.values
    clinical_conditions_df['Autoimmune_Disease'] = clinical_conditions_df['SYSTEMIC_LUPUS_ERYTHEMATOSUS'].values
    clinical_conditions_df['Blood_Related_Disorder'] = clinical_conditions_df[BLOOD_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Cancer'] =  clinical_conditions_df[CANCERS].any(axis=1).astype('int8')
    clinical_conditions_df['Dementia'] = clinical_conditions_df['DEMENTIA'].values
    clinical_conditions_df['Diabetes'] = clinical_conditions_df['DIABETES_MELLITUS'].values
    clinical_conditions_df['DENTAL'] = clinical_conditions_df['PERIODONTAL_DISEASE'].values
    clinical_conditions_df['Ear'] = clinical_conditions_df['OTITIS_MEDIA'].values
    clinical_conditions_df['Fatigue'] = clinical_conditions_df['CHRONIC_FATIGUE_SYNDROME'].values
    clinical_conditions_df['Heart_Related_Disorder'] = clinical_conditions_df[HEART_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Hepatitus'] = clinical_conditions_df['HEPATITIS'].values
    clinical_conditions_df['Hiv'] = clinical_conditions_df['HIV_OR_AIDS'].values
    clinical_conditions_df['Hyperlipidemia'] = clinical_conditions_df['HYPERLIPIDEMIA'].values
    clinical_conditions_df['Hypertension'] = clinical_conditions_df['HYPERTENSION'].values
    clinical_conditions_df['Lungs_Related_Disorder'] = clinical_conditions_df[LUNG_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Lyme_Disease'] = clinical_conditions_df['LYME_DISEASE'].values
    clinical_conditions_df['Mental_Disorder'] = clinical_conditions_df[MENTAL_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Neural'] = clinical_conditions_df[NEURAL_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Obesity'] = clinical_conditions_df['OBESITY'].values
    clinical_conditions_df['Orthopedic'] = clinical_conditions_df[ORTHOPEDIC].any(axis=1).astype('int8')
    clinical_conditions_df['Peripheral_Artery_Disease'] = clinical_conditions_df['PERIPHERAL_ARTERY_DISEASE'].values
    clinical_conditions_df['Prostate'] = clinical_conditions_df['BENIGN_PROSTATIC_HYPERTROPHY'].values
    clinical_conditions_df['Renal'] = clinical_conditions_df[RENAL].any(axis=1).astype('int8')
    clinical_conditions_df['Reproductive'] = clinical_conditions_df[REPRODUCTIVE].any(axis=1).astype('int8')
    clinical_conditions_df['SUBSTANCES_RELATED_DISORDERS'] = clinical_conditions_df[SUBSTANCE_RELATED_DISORDERS].any(axis=1).astype('int8')
    clinical_conditions_df['Thyroid_disorder'] = clinical_conditions_df['CHRONIC_THYROID_DISORDERS'].values
    clinical_conditions_df['Vision_Related_Disorder'] = clinical_conditions_df[VISION_RELATED_DISORDERS].any(axis=1).astype('int8')

    clinical_conditions_df['Critical'] = clinical_conditions_df[CRITICAL_CONDITIONS].sum(axis=1).astype('uint8')
    clinical_conditions_df['Total'] = clinical_conditions_df[ALL_CONDITIONS].sum(axis=1).astype('uint8')

    clinical_conditions_df['High_Need_Group'] = ((clinical_conditions_df['Critical'] > 1) |
                                            (clinical_conditions_df['Total'] > 5)
                                            ).astype('int8')
    return clinical_conditions_df