import pandas as pd #cant read in even a subset of the sas7bdat data, getss yuck on 50k rows w the varlist below
# import modin.pandas as mpd #read_sas defaults to pandas. modin cant handle it https://modin.readthedocs.io/en/0.11.0/supported_apis/io_supported.html
#    it makes it sound like it can do read_sas but I'm getting nothing but errors
import modin.pandas as mpd #tried modin[dask] and modin[ray]
import ray
import vaex
import math
import performance as perf

class Datain:
    p = perf.Performance()
    
    def recode_yes_no(self,series):
        if series == 2.0: #no
            return 0
        if series == 1.0: #yes
            return 1.0

    def esex_recode(self,series):
        """docstring"""
        if series == 1:
            return "male"
        if series == 2:
            return "female"

    def print_df_info(self,df): #add strategy
        """docstring"""
        # dask got hung up on df.info and df.memory_usage
        # df.info(memory_usage="deep") 
        # df.memory_usage(index=False, deep=True)
        print("count ssuid =",df['SSUID'].count())
        if ('ESEX' in df.columns):
            print("freq esex =",df['ESEX'].value_counts())
        if ('RDIS' in df.columns):
            print("freq rdis =",df['RDIS'].value_counts(dropna=False))
        print("df head:")
        print(df.head())
        df['rgender'] = df['ESEX'].apply(self.esex_recode)
        # #"vaex-pd" df error "ValueError: If using all scalar values, you must pass an index" for crosstab
        # # https://vaex.readthedocs.io/en/latest/api.html
        a = pd.crosstab(df.rgender,df.RDIS, rownames=["Gender"], colnames=["Disability"])
        print(a)
        # #"vaex-pd" df error "AttributeError: 'GroupBy' object has no attribute 'mean'"
        # # https://vaex.readthedocs.io/en/latest/api.html#vaex.dataframe.DataFrame.mean
        mean_out=df.groupby('rgender').mean('TAGE')
        print(round(mean_out,2))
        # #"vaex-pd" no case for contains
        # # https://towardsdatascience.com/vaex-a-dataframe-with-super-strings-789b92e8d861
        print(df[df['rgender'].str.contains("male", case=False, na=False)]) 
        # #"vaex-pd" no sort_values
        # # https://github.com/vaexio/vaex/issues/1631
        print(df.sort_values(by='TAGE')['TAGE'].head()) 
        #free up some memory
        df = pd.DataFrame()

    def mysort(self,df,drop):
        """docstring"""
        if drop == "ppm":
            return df.sort_values(['SSUID','PNUM','MONTHCODE']).drop_duplicates(subset=['SSUID','PNUM'], keep='last')
            #another way# newdf1 = df.groupby(['SSUID', 'PNUM']).first()
            #another way# return df.sort_values(['SSUID', 'PNUM', 'MONTHCODE']).drop_duplicates(subset=['SSUID', 'PNUM'], keep='last')
        elif drop == "pp":
            return df.sort_values(['SSUID','PNUM']).drop_duplicates(subset=['SSUID'], keep='last')
        else:
            return df

    def do_something(self,strategy,sipp_dat,varlist):
        """docstring"""
        if strategy == "modin":
            return mpd.concat(sipp_dat)[varlist]
        elif strategy == "pandas":
            return pd.concat(sipp_dat)[varlist]
        elif strategy == "vaex-pd":
            df_temp=pd.concat(sipp_dat)[varlist]
            return vaex.from_pandas(df_temp, copy_index=False)
        elif strategy == "vaex-mpd":
            df_temp=mpd.concat(sipp_dat)[varlist]
            return vaex.from_pandas(df_temp, copy_index=False)
        else:
            raise Exception
            
    def is_nan_check(self,series):
        if math.isnan(series):
            return -9
        else:
            return series

    def cast_vars_always(self,df):
        # print("in cast vars always")
        df = df.astype({"SSUID": str})
        df = df.astype({"PNUM": str})
        df = df.astype({"MONTHCODE": str})
        df = df.astype({"ESEX": 'int8'})
        df = df.astype({"TPTOTINC": 'float'})
        df = df.astype({"TAGE": 'int8'})
        df = df.astype({"RDIS": 'float'})
        return df
            
    def cast_vars(self,df):
        # print("in cast vars")
        #adding the is_nan_checks adds a lot of time
        df['EXMAR'] = df['EXMAR'].apply(self.is_nan_check)
        df = df.astype({"EXMAR": 'int8'})
        df['EHEARING'] = df['EHEARING'].apply(self.is_nan_check)
        df = df.astype({"EHEARING": 'int8'})
        df['ESEEING'] = df['ESEEING'].apply(self.is_nan_check)
        df = df.astype({"ESEEING": 'int8'})
        df['ECOGNIT'] = df['ECOGNIT'].apply(self.is_nan_check)
        df = df.astype({"ECOGNIT": 'int8'})
        df['EAMBULAT'] = df['EAMBULAT'].apply(self.is_nan_check)
        df = df.astype({"EAMBULAT": 'int8'})
        df['ESELFCARE'] = df['ESELFCARE'].apply(self.is_nan_check)
        df = df.astype({"ESELFCARE": 'int8'})
        df['EERRANDS'] = df['EERRANDS'].apply(self.is_nan_check)
        df = df.astype({"EERRANDS": 'int8'})
        return df

    @p.performance
    def how_read_data(self,strategy,filenam,chunksize,iterator,varlist,drop):
        """docstring"""
        ITERATIONS=10
        print("using",strategy,"to read in data")
        file1 = open("performance.txt", "a")
        writethis=strategy+"|"+filenam+"|"+str(chunksize)+"|"+str(ITERATIONS)+"|"+str(iterator)+"|"+str(varlist)+"|"+drop
        file1.write(writethis)
        file1.close()
        # "strategy,filename,chunksize,iterations,iterator,varlist,droppedrows")

        itr = pd.read_sas(filenam,chunksize=chunksize,iterator=iterator)
        x=0
        sipp_dat=[] #can't save space by blanking this with each iter and only appending to the dataframe, dataframe.append() is deprecated in favor of concat()
        for chunk in itr:
            # print("x=",x)
            sipp_dat.append(chunk)
            df_temp=self.do_something(strategy,sipp_dat,varlist) #strategy
            # add strategy, if vaex, don't cast
            df_temp = self.cast_vars_always(df_temp)
            df_temp = self.cast_vars(df_temp)
            df = self.mysort(df_temp,drop)
            if x == ITERATIONS:
                break
            x+=1
        #ssuid to str and pnum to str
        return df

    def mpd_read_pipe(self,filenam):
        """docstring"""
        mpd.read_csv(self,filenam)

    def pandas_read_pipe(self,filenam,varlist):
        """docstring"""
        pd.read_csv(self,filenam, usecols=varlist)

    def dask_read_pipe(self,filenam):
        """docstring"""
        pass

    def create_sqlite3(self):
        """docstring"""
        pass

    def sqlite3_read_db(self):
        """docstring"""
        pass
