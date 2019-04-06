import numpy as np
import scipy as sp
import pandas as pd
import dask as da
import xarray as xy
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from time import time
import os.path
import cftime
import sys
from scipy.stats import ks_2samp as ksTest

def getCal(fileName):
    temp = xy.open_dataset(fileName,decode_times=False)
    return temp.time.calendar

def addSeasonArray(y):
    return xy.DataArray([GroupbySeason(y,x) for x in np.arange(len(y.time))],name='season',dims=['time'])

def GroupbySeason(df, ind):
    month = int(str(df['time'].isel(time=ind).coords).split('-')[1])
    if month in [12,1,2]:
        return 'DJF'
    elif month in [3,4,5]:
        return 'MAM'
    elif month in [6,7,8]:
        return 'JJA'
    elif month in [9,10,11]:
        return 'SON'

def startslice(startYear,fileName):
    sm = 1
    if getCal(fileName) == '360_day':
        startslice = cftime.Datetime360Day(startYear, sm, 1,0,0,0,0,-1,1)
    else:
        startslice = str(startYear)+str(sm).zfill(2)+'01'

    return startslice

def endslice(endYear,fileName):
    if getCal(fileName) == '360_day':
        endslice = cftime.Datetime360Day(endYear, 12, 30,23,0,0,0,-1,360)
    else:
        endslice = str(endYear)+'1231'

    return endslice

def wetDayPercentile(y,q=85):
    #note hard-coded wet-day threshold of >0.1mm/d
    return y.where(y>0.1).reduce(np.nanpercentile,dim='time',q=q)

#-------------------                                                                                       
if __name__ == "__main__":
        # arguments should be passed in:                                                       
        #"label": plot title: "[analysis scenario rcm gcm location]"                           
        #"obs": input file with observational data (single timeseries, one variable, one point)
        #"cur": same as obs, but for historical period of simulation                           
        #"fut": same as "cur", but for future period of simulation                             
        #"png": name of 'png' file to write the figure                                         
        #"var": name of data variable in input files                                           
        #"txt": name of file to write tab-delimited numerical metric results                   
    #----parse arguments--------------
    txt = sys.argv[-1]
    var = sys.argv[-2]
    png = sys.argv[-3]
    fut = sys.argv[-4]
    cur = sys.argv[-5]
    obs = sys.argv[-6]
    label = ' '.join(sys.argv[1:-6])
    #-------------------------------

    obsData = xy.open_dataset(obs)
    pThreshOne = obsData[var].groupby('time.season').apply(wetDayPercentile,q=85)

    # version for only one point, not set of lats/lons:
    def toDF(data,overThresh=True):
        dataWhere = data.sel(time=data['time.season']==season)
        if overThresh:
            dataWhere = dataWhere.where(dataWhere>pThreshOne.sel(season=season))
        return dataWhere.to_pandas()    

    def callKStest(df1,df2):
        try:
            toReturn = ksTest(df1.dropna().squeeze().values,df2.dropna().squeeze().values)
            return pd.Series([toReturn.statistic, toReturn.pvalue],index=['statistic','pValue'])
        except:
            return pd.Series([np.NaN, np.NaN],index=['statistic','pValue'])

    pHist = xy.open_dataset(cur)
    pFuture = xy.open_dataset(fut)

    if getCal(cur) == '360_day':
        grouperString = 'season'
        pHist.coords['season'] = addSeasonArray(pHist)
        pFuture.coords['season'] = addSeasonArray(pFuture)
    else:
        grouperString = 'time.season'

    pThreshOne = pThreshOne.isel(lat=0,lon=0) #in theory should not be necessary if input file is for just one point

    t1 = time()
    ksOut = pd.DataFrame()
    ksOutFut = pd.DataFrame()
    for i, season in enumerate(['DJF','MAM','JJA','SON']):
        obsDF = toDF(obsData[var].isel(lat=0,lon=0)) #choosing just one point
        histDF = toDF(pHist[var].isel(lat=0,lon=0))
        futureDF = toDF(pFuture[var].isel(lat=0,lon=0))
    
        ksOut[season] = callKStest(histDF,obsDF)
        ksOutFut[season] = callKStest(histDF,futureDF)
    
    t2=time()
    print ('calculation took '+str(np.round(t2-t1,2))+'s')

    ksOut.to_csv(txt)

    #---to read back into pandas----
    # ksOutAndBack = pd.read_csv(txt,index_col=0)

    #-------plotting-------------------------
    binsToUse = np.logspace(-1,2,60)
    plt.figure(figsize=[15,8])
    for i, season in enumerate(['DJF','MAM','JJA','SON']):
        obsDF = toDF(obsData[var].isel(lat=0,lon=0),overThresh=False)
        histDF = toDF(pHist[var].isel(lat=0,lon=0),overThresh=False)
        futureDF = toDF(pFuture[var].isel(lat=0,lon=0),overThresh=False)
    
        ax = plt.subplot(2,2,i+1)
        obsDF.plot.hist(ax=ax,bins=binsToUse,stacked=False,alpha=0.5,label='observations') 
        histDF.plot.hist(ax=ax,bins=binsToUse,stacked=False,alpha=0.5,label='historical') 
        #plot is too busy with this:
        #futureDF.plot.hist(ax=ax,bins=binsToUse,stacked=False,alpha=0.5,label='future') 
        ax.plot(np.zeros(10)+pThreshOne.sel(season=season).data,np.linspace(0,15,10),linestyle='--',color='b',label='85th percentile obs')    

        ax.set(xscale='log')
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda y,pos: ('{{:.{:1d}f}}'.format(int(np.maximum(-np.log10(y),0)))).format(y)))
        if i>1:
            try:
                units = obsData[var].attrs['units']
                plt.xlabel(units)
            except:
                plt.xlabel('default units in file')
        plt.title(season)
        plt.legend(loc='upper left')
        ax.annotate('statistic: '+str(np.round(ksOut[season]['statistic'],2)),xy=(0.7,0.9),
                xycoords='axes fraction')
        ax.annotate('p value:  '+str(np.round(ksOut[season]['pValue'],2)),xy=(0.7,0.85),
                xycoords='axes fraction')

    plt.suptitle(label)
    plt.savefig(png)
