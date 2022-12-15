#IN PROGRESS

import pandas as pd
import numpy as np
import datain
import performance as perf
# # # # ## iterate through each row and select
# # 'Name' and 'Stream' column respectively.
# for ind in df.index:
    # print(df['Name'][ind], df['Stream'][ind])

# # # # # iterate through each row and select
# # 'Name' and 'Age' column respectively.
# for i in range(len(df)):
    # print(df.loc[i, "Name"], df.loc[i, "Age"])


# # # # # iterate through each row and select
# # 0th and 2nd index column respectively.
# for i in range(len(df)):
    # print(df.iloc[i, 0], df.iloc[i, 2])

# # # # # iterate through each row and select
# # 'Name' and 'Age' column respectively.
# for index, row in df.iterrows():
    # print(row["Name"], row["Age"])
    
# # # # # iterate through each row and select
# # 'Name' and 'Percentage' column respectively.
# for row in df.itertuples(index=True, name='Pandas'):
    # print(getattr(row, "Name"), getattr(row, "Percentage"))

# # # # # iterate through each row and concatenate
# # 'Name' and 'Percentage' column respectively.
# print(df.apply(lambda row: row["Name"] + " " + 
               # str(row["Percentage"]), axis=1))
#run

import os
import ray
import pyreadstat
from datetime import datetime
import numpy

p = perf.Performance()
datain = datain.Datain()

def household_var(series):
    if series:
        return 1
    else:
        return 0

#strategy for processing multiple edits one person at a time in the household
#spouse or parent pointers plus rrels in the puf?


#https://stackoverflow.com/questions/7837722/what-is-the-most-efficient-way-to-loop-through-dataframes-with-pandas
#a household with at least one person with X characteristic
#   under 18, with disability, with income gt 0
# @p.performance
def row_strategy(strategy,df,strvar,newvar,df_var):
    print("using logic with", strategy, " strategy")
    if strategy == 'people_in_ssuid': #this works
        print("any rdis = 1?",df[strvar].value_counts(dropna=False))
        groups = df.groupby('SSUID')
        ids=[]
        for people in groups:
            if (people[1][strvar] == 1.0).any():
                ids.append(people[0])
        ids = list(dict.fromkeys(ids))
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0)
    elif strategy == 'iter_rows':  #this works
        print("any rdis = 1?",df[strvar].value_counts(dropna=False))
        groups = df.groupby('SSUID')
        ids=[]
        print("first iter through df")
        for people in groups:
            for person in df.iterrows(): 
                if person[1][strvar] == 1.0:
                    print("person[1]['SSUID']=", person[1]['SSUID'], "person[1][strvar],strvar=", strvar, person[1][strvar])
                    ids.append(person[1]['SSUID']) #if we find someone in the household who has the value, append ssuid to the list
                     #and we don't have to keep looking if we find someone
                    continue
        ids = list(dict.fromkeys(ids)) #remove duplicates
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0)
    elif strategy == 'filter_numpy':  #this works
        ids = df.loc[df[strvar] == 1, 'SSUID']
        ids = list(dict.fromkeys(ids)) #remove duplicates
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0) #all rows in household get a 1
            #if someone in the house meets the criteria
    elif strategy == 'objects':
        hh = Household()
        pp = Person()
        # for h in hh:
            # for p in hh.persons:#problem here?
                # if p.rdis == 1:
                    # hh.newvar = 1
                    # break
    elif strategy == 'iter_tuples':  #this works
        groups = df.groupby('SSUID')
        ids = []
        for people in groups:
            for person in df.itertuples():
                if person.df_var == 1.0:
                    ids.append(person.SSUID)
                    continue
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0) #all rows in household get a 1
            #if someone in the house meets the criteria
    elif strategy == 'iter_tuples2': #this works
        ids = []
        for i, row in enumerate(df.itertuples(), 1):
            if row.df_var == 1.0:
                ids.append(row.SSUID)
                continue
        ids = list(dict.fromkeys(ids)) #remove duplicates
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0)
    elif strategy == 'iter_tuples3': #this works
        ids = []
        for row in df.itertuples():
            if getattr(row, 'RDIS') == 1.0:
                ids.append(row.SSUID)
                continue
        ids = list(dict.fromkeys(ids)) #remove duplicates
        df[newvar] = numpy.where(df.SSUID.isin(ids), 1, 0)
    return df



# if __name__ == "__main__":


    # now = datetime.now()

    # current_time = now.strftime("%H:%M:%S")
    # print("Start Time =", current_time)

    # # set MYFOLDR="location:somewhere"
    # # export MYFOLDR="location:somewhere"
    # myfoldr=os.path.join(os.environ['MYFOLDR'])
    # filenam = os.path.join(myfoldr,"pu2021_sas","pu2021.sas7bdat")
    # varlist=['SSUID', 'PNUM', 'MONTHCODE', 'ESEX', 'TPTOTINC', 'TAGE', 'RDIS', 'EXMAR', 'EHEARING','ESEEING', \
    # 'ECOGNIT','EAMBULAT','ESELFCARE','EERRANDS']  
    # drop="ppm" #value possibilities are pp or ppm to keep household or pp respectively
    # chunksize=10
    # iterator=True
    # ITERATIONS=5
    # #####################################
    
    # print()
    # #########NO DROP
    # file1 = open("performance.txt", "w")
    # file1.write("strategy|filename|chunksize|iterations|iterator|varlist|droppedrows|timeelapsed|peakusemb\n")
    # file1.close()
    
    # # #works on local machine
    # df1 = datain.how_read_data("pandas",filenam,chunksize,iterator,varlist,drop)
    # # print("df1 head=", df1.head())
    # # writestuff("pandas-nodrop","hh_dis_mean")
    # print()
    # print(df1["RDIS"].value_counts(dropna=False))
    # # hh_dis_mean(df1)
    # print()
    # # print(pd.crosstab(df1.SSUID,df1.RDIS, rownames=["SSUID"], colnames=["Disability"]))
    # # print()
    # # # df1 = row_strategy("filter_numpy",df1,"RDIS","hh_dis_mean1")
    # # df1 = row_strategy("iter_rows",df1,"RDIS","hh_dis_mean2")
    # # print(df1.head())
    # # print()
    # # print(df1["hh_dis_mean2"].value_counts(dropna=False))
    # # # print()
    # # # print(pd.crosstab(df1.hh_dis_mean1,df1.RDIS))
    # # # print()
    # # # print(pd.crosstab(df1.hh_dis_mean1,df1.hh_dis_mean2))
    # # # print()
    # # # print("count rows:", df1['SSUID'].count())
    # # print()    
    # # print(pd.crosstab(df1.hh_dis_mean2,df1.RDIS))
    # # print()
    # # # df1 = row_strategy("people_in_ssuid",df1,"RDIS","hh_dis_mean3")
    # # # print()   
    # # # print(pd.crosstab(df1.hh_dis_mean3,df1.RDIS))
    # # # print("iter tuples")
    # df1 = row_strategy("iter_tuples",df1,"RDIS","hh_dis_mean4",RDIS)
    # print()   
    # print(pd.crosstab(df1.hh_dis_mean4,df1.RDIS))
    # # df1 = row_strategy("iter_tuples2",df1,"RDIS","hh_dis_mean5")
    # # print()
    # # print(pd.crosstab(df1.hh_dis_mean5,df1.RDIS))
    # # df1 = row_strategy("iter_tuples3",df1,"RDIS","hh_dis_mean6")
    # # print()
    # # print(pd.crosstab(df1.hh_dis_mean6,df1.RDIS))


    # # datain.print_df_info(df1)

