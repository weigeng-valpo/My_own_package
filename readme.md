Are you tired to look for the most recent queries?

Are you tired to write the same function and code again and again?

I know that feeling...


# DSNP Query Box

![query_box](./query_box2.png)

Keep all DSNP queries one place. Let all the team members can access to the most recent updated queries and functions

#### 1. Queries we use for reports

[Check the query list](./query_list.csv)

#### 2. Helper Functions that can make your life easy

[Check HelperFunction](./snp_query_box/DsnpHelperFunction.py)

#### 3. Constant Values we use frequently

[Check Dsnp Values](./snp_query_box/DsnpVal.py)

#### 4. Also, there are frequently used transformations

[Check dsnp transforms](./snp_query_box/dsnp_transform/)

---
<br>


>How to use it?

1. clone or pull the repo
2. if you are on conda env, deactivate conda env in terminal `conda deactivate`
3. go under the repo `snp_query_box`
4. in your terminal and run `pip install -r requirements.txt`
5. in your terminal `python setup.py clean --all install`
6. `pip install .` to install in your local directory
7. it is ready to use the queries

<br>

Example
```
from snp_query_box import DsnpHelperFunction, populDashQueries  

#use helper functions
DsnpHelperFunction.last_date_of_month("2023-01-29")
>> datetime.date(2023, 1, 31)

# use pull queries
pull_snp_member = populDashQueries.pull_ctm(medicare_number_list, start_date, end_date)
```