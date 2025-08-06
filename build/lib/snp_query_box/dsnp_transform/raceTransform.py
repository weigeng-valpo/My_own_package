import numpy as np

def get_race_df(pull_race):
    race_df = pull_race.copy()
    race_df['Race_Code'] = np.where(race_df['Race_ID']==1,"White",
        np.where(race_df['Race_ID']==2, "Black",
         np.where(race_df['Race_ID']==5,"Hispanic","Others")))
    return race_df