#run
import datain
import os
import ray
import pyreadstat
from datetime import datetime

if __name__ == "__main__":
    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Start Time =", current_time)

    # set MYFOLDR="location:somewhere"
    # export MYFOLDR="location:somewhere"
    myfoldr=os.path.join(os.environ['MYFOLDR'])
    filenam = os.path.join(myfoldr,"pu2021_sas","pu2021.sas7bdat")
    varlist=['SSUID', 'PNUM', 'MONTHCODE', 'ESEX', 'TPTOTINC', 'TAGE', 'RDIS', 'EXMAR', 'EHEARING','ESEEING', \
    'ECOGNIT','EAMBULAT','ESELFCARE','EERRANDS']  
    drop="" #value possibilities are pp or ppm to keep household or pp respectively
    chunksize=1000
    iterator=True
    ITERATIONS=10
    #####################################
    
    print()
    #########NO DROP
    file1 = open("performance.txt", "w")
    file1.write("strategy|filename|chunksize|iterations|iterator|varlist|droppedrows|timeelapsed|peakusemb\n")
    file1.close()
    
    # #works on local machine
    df1 = datain.how_read_data("pandas",filenam,chunksize,iterator,varlist,"ppm")
    datain.print_df_info(df1)
    
    # #dashboard http://127.0.0.1:8265
    # #ERROR - sort_values not supported by PandasOnRay, defaulting to Pandas implementation
    # #looked online - don't expect this to work on a laptop
    # ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})    
    # df2 = datain.how_read_data("modin",filenam,chunksize,iterator,varlist,"ppm")
    # datain.print_df_info(df2)
    # ray.shutdown()

    # #in readthedocs  "A core feature of Vaex is the extremely efficient calculation of statistics on N-dimensional grids. This is rather useful for making visualisations of large datasets."
    # #error cant drop bc df has no sort_values
    # #it doesnt work if I drop before creating the vaex df either
    # # EXCEPTION - no attribute astype, no attribute sort_values - both of these cause an exception
    # df3 = datain.how_read_data("vaex-pd",filenam,chunksize,iterator,varlist,"ppm")
    # datain.print_df_info(df3)

    # #memory usage error w 6k rows
    # #EXCEPTION - no attribute astype, no attribute sort_values - both of these cause an exception
    # ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})
    # df4 = datain.how_read_data("vaex-mpd",filenam,chunksize,iterator,varlist,"ppm")
    # datain.print_df_info(df4)
    # ray.shutdown()
    
    # #works on local machine
    df5 = datain.how_read_data("pandas",filenam,chunksize,iterator,varlist,"")
    datain.print_df_info(df5)
    
    # # chunksize=500 doesn't help us be able to bring this in on local machine
    # ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})
    # df6 = datain.how_read_data("modin",filenam,chunksize,iterator,varlist,"")
    # datain.print_df_info(df6)
    # ray.shutdown()

    # #EXCEPTION - no attribute astype - also exceptions within print_df_info - find alternatives
    # df7 = datain.how_read_data("vaex-pd",filenam,chunksize,iterator,varlist,"")
    # datain.print_df_info(df7)

    # #don't expect this to run on laptop, see note above
    # #EXCEPTION - no attribute astype - exceptions in print_df_info are assumed to be the same as "vaex-pd"
    # ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})
    # df8 = datain.how_read_data("vaex-mpd",filenam,chunksize,iterator,varlist,"")
    # datain.print_df_info(df8)
    # ray.shutdown()
    
    # #works on local machine
    import time
    rowlim = chunksize * ITERATIONS  
    file1 = open("performance.txt", "a")
    writethis="pyreadstat"+"|"+filenam+"|"+str(rowlim)+"|0|N/A|"+str(varlist)+"|"
    file1.write(writethis)
    file1.close()    
    datain.tracing_start()
    start = time.time()
    df9, meta = pyreadstat.read_sas7bdat(filenam, usecols=varlist, disable_datetime_conversion=True, row_offset=0, row_limit=rowlim)
    end = time.time()
    elapsed = (end-start)*1000
    print("time elapsed {} milli seconds".format(elapsed))
    peak = datain.tracing_memory()
    #put time results in a file
    file1 = open("performance.txt", "a")
    writethis="|"+str(round(elapsed,2))+"|"+str(round(peak,2))+"\n"
    file1.write(writethis)
    file1.close()
    
    datain.print_df_info(df9)

    later = datetime.now()
    later_time = later.strftime("%H:%M:%S")
    print("End Time =", later_time)
