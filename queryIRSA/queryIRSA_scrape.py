## use this script to see what files are available on the IRSA server, 
## apply some simple query, and then download them. 
## Can keep local copies of the files updated with what available at 
## the IRSA (e.g: download new data for as specific field, ecc..)
## 
## it works in three steps:
##    - download the metadata for (all) the files available (if called for the
##      first time), or update the metadata table for the new
##    - apply some filter condition (e.g, airmass, field, date, filter, ecc..)
##    - download the requested data.
##
## updating the metadata table, it can download new files that matches your
## query, keeping your local folders updated. 
##
## refs and docs:
## https://irsa.ipac.caltech.edu/docs/program_interface/ztf_api.html
## https://irsa.ipac.caltech.edu/docs/program_interface/ztf_metadata.html
##
## author: M.Giomi (matteo.giomi@desy.de)



# see also here: http://www.astro.caltech.edu/~tb/ztfops/sky/

import os
import numpy as np
import datetime
from astropy.time import Time
from astropy.io import fits
import astropy.units as u
import pandas as pd
import inspect


import utils.db as dbutils



import logging
logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# download utilities logs only warnings
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

#def updatemeta(which='sci', overwrite=False):
#    """
#        query IRSA for the metadata of all the files that
#        are available at the time of running this command.
#    
#        Parameters:
#        -----------
#        which: `str`
#            select the data type, either 'sci', 'raw', 'cal'.
#        
#        overwrite: `bool`
#            weather to update the metdata file, or to overwrite it.
#    """
#    
#    if not which in ['sci', 'raw', 'cal']:
#        raise ValueError
#    logger.info("Updating %s metdata"%which)
#    
#    # get the collection
#    coll = dbutils.get_collection(which, db_client=None, logger=logger)
#    is_empty = coll.count() == 0
#    
#    # download the metdata to csv temporary file. If it's the 
#    # first time, or you want to overwrite it, then get all of them
#    tmpfile=os.path.join("/tmp/getmeta_%s_%s.tbl"%(which, tstamp("%Y-%m-%d")))
#    if (is_empty) or overwrite:
#        logger.info("downloading all metadata from IRSA. Patience please...")
#        load_file(url+"?WHERE=1=1&ct=csv", outf=tmpfile, auth=(user, pwd), showpbar=True)
#        mtab=pd.read_csv(tmpfile, engine='c')
#    
#    # or you just download the newest ones
#    else:
#        # go read the file and figure out the time of the last datapoint.
#        # download only the newest files
#        oldtab=pd.read_csv(outfile, engine='c')
#        
#        # we convert everything to JD
#        if 'obsjd' in oldtab.columns:
#            maxobsjd=oldtab['obsjd'].max()
#            logger.info("downloading metadata from obs more recent than %s (UTC)"%(
#                Time(maxobsjd, format='jd').iso))
#            querystr="?WHERE=obsjd>%s&ct=csv"%str(maxobsjd)
#            
#        elif 'startobsdate' in oldtab.columns:
#            maxsod=oldtab['startobsdate'].map(
#                lambda x: Time(x[:-3], format='iso')
#                ).max()+0.001*u.second   # add some time for BETWEEN statement
#            future="2042-01-01+00:00:00.000"
#            logger.info("downloading metadata from obs more recent than %s (UTC)"%(maxsod))
#            querystr="?WHERE=startobsdate+BETWEEN+%s+AND+%s&ct=csv"%(
#                "'"+str(maxsod).replace(" ", "+")+"'", "'"+future+"'")
#        
#        # execute the query
#        load_file(url+querystr, outf=tmpfile, auth=(user, pwd), showpbar=True)
#        
#        # add it to the existing table
#        newtab=pd.read_csv(tmpfile, engine='c')
#        if len(newtab)>0:
#            logger.info("%d new observations have been taken"%len(newtab))
#            mtab=pd.concat([oldtab, newtab], ignore_index=True, copy=False)
#        else:
#            logger.info("no new data available")
#            return oldtab
#    
#    # now write the metdata table to file
#    mtab.modified=tstamp()
#    logger.info("writing metadata table to: %s"%outfile)
#    mtab.to_csv(outfile, index=False)
#    return mtab


#def readmetatab(which='sci', update=True):
#    """return an pandas.Dataframe with the metadata for
#    the chosen data products. If none is found, download
#    them using updatemeta.
#    
#    Parameters:
#    -----------
#    which: `str`
#        select the data type, either 'sci', 'raw', 'cal'.
#    update: `bool`
#        if True this function will update the local metadata table 
#        calling updatemeta. Else, it will simply read what's there.
#        
#    Returns:
#    --------
#    datafrme: `pandas.DataFrame`
#        dataframe containing the metadata for the files. 
#    """
#    
#    if not which in ['sci', 'raw', 'cal']:
#        raise ValueError
#        
#    outfile=metadfile(which)
#    mfile=os.path.join(mdatadir, "meta_"+which+".csv")
#    if not os.path.isfile(outfile) or update:
#        mtab=updatemeta(which=which)
#        return mtab
#    else:
#        out=pd.read_csv(mfile, engine='c')
#        logger.info("read %d entries in metadata table %s"%(len(out), mfile))
#        return out



## ---- utitlity functions ---- #



#def metadfile(which):
#    """build the path of the file containing 
#    the metadata for given data products.
#    
#    Parameters:
#    ----------
#    which: `str`
#        identifyer for the data products, either 'sci', 'raw', or 'cal'.
#        
#    Returns:
#    --------
#    mdpath: `str`
#        path of the file containing the metadata for given data products.
#    """
#    
#    if not which in ['sci', 'raw', 'cal']:
#        raise ValueError
#    if not os.path.isdir(mdatadir):
#        os.makedirs(mdatadir)
#    return os.path.join(mdatadir, "meta_"+which+".csv")



def querypos(ra, dec, time=None, cutdim=None):
    """this function will query the IRSA for all the sci data 
    around a given position and for a given time range.
    
    Parameters:
    -----------
        
        ra/dec: `float`
            sky position (in degrees) of the target.
        
        time: `astropy.Quantity`,`astropy.time.Time`, list, or None
            parameter indicating the time range for which the data has to be 
            downloaded:
                - if None, do not query on time, all the data available will be included. 
                - if list of astropy.time.Time objects, data will be downloaded for 
                times BETWEEN time[0] AND time[1].
                - if it's a quantitiy with dimension time, data will be downloaded 
                for times BETWEEN now-time AND now
                - if astropy.time.Time specifing a day all the data for that day 
                are downloaded.
        
        cutdim (NOT IMPLEMENTED YET): `float`, list of `float` or None
            if True, download the entire quadrants, else download a cutout
                - of dimension cutdim[0] x cutdim[1] if cutdim is a list of 2 floats
                - of radius cutdim if cutdim is float.
        
    Returns:
    --------
        
        panda.DataFrame with the metadata for your query.
    """
    
    # define your query string: start with the position
    querystr="?POS=%.4f,%.4f"%(ra, dec)
    logger.info(
        "querying IRSA for data at this position (RA: %.5f, Dec: %.5f)"%(ra, dec))
    
    # and then consider the date
    if type(time)==u.quantity.Quantity:
        tref=(Time.now()-time)
        querystr+="&WHERE=obsjd>%s"%str(tref.jd)
        logger.info(
            "and not older than %f days"%time.to('day').value)
        logger.info(
            "corresponding to time range between %s and %s."%(tref.iso, Time.now().iso))
    
    elif type(time)==Time:
        timestr=time.datetime.strftime("%Y%m%d")
        querystr+="&WHERE=filefracday>=%d+AND+filefracday<=%d"%(int(timestr+"0"*6), int(timestr+"9"*6))
        logger.info("querying IRSA for data taken on %s"%time.iso)
        logger.info(
            "using string %s to query for partial matches in filefracday"%timestr)
    
    elif time is None:
        logger.info("querying IRSA for all the data available for that position")
    
    else:
        try:
            if len(time)==2:
                start, end=time[0].jd, time[1].jd
                querystr+="&WHERE=obsjd+BETWEEN+%.5f+AND+%.5f"%(start, end)
                logger.info("querying IRSA for data taken between %s (%.5f) and %s (%.5f)"%(
                    time[0].iso, start, time[1].iso, end))
        except:
            logging.exception(
                "time argument has to astropy.Quantity`,`astropy.time.Time`, a list of them, or None.",
                "got %s instead"%type(time))
    
    # append table format
    querystr+="&ct=csv"
    
    # download the metadata table
    url=os.path.join(meta_baseurl, 'sci')
    logger.info("downloading metadata using IRSA query: %s"%querystr)
    tmpfile=os.path.join("/tmp/getmeta_ra%.3f_dec%.3f.tbl"%(ra, dec))
    load_file(url+querystr, outf=tmpfile, auth=(user, pwd), showpbar=True)
    
    # read metadata table and get the files
    mtab=pd.read_csv(tmpfile, engine='c')
    logger.info("found %d entries in metadata table"%len(mtab))
    return mtab










#def getdata(tab, where, product="sexcat", 
#        overwrite=False, dry=False, check=True, maxworkers=8, chunks=1024):
#    """download the data whose metadata is specified in the
#    given an astropy table. The files are saved where you want.
#    This use the rules described here:
#    https://irsa.ipac.caltech.edu/docs/program_interface/ztf_metadata.html
#    to build up the download urls.
#    
#    Parameters:
#    -----------
#    tab: 'pandas.DataFrame`
#        table with the metadata for the files you want to download.
#    where: `str`
#        path to the directory where the files will be saved to.
#    product: `str`
#        what kind of data you want to download, for example: log (for sci only), 
#        sciimg, sciimlog, mskimg, sexcat, psfcat, sciimgdao, sciimgdaopsfcent,
#         bias, biascmask, biaslog, hifreqflat, raw, ecc..
#    maxworkers: `int`
#        numer of threads in the pool, to be passed to concurrent_download.
#             
#    Returns:
#    --------
#    ok: `bool`:
#        False is any of the files does not opens as a fits file. True if all
#        the files are ok, or if they are not fits files (e.g. logs), or if
#        check is False
#    """
#    urls=[geturl(t, product) for _, t in tab.iterrows()]
#    if dry:
#        logger.info(
#        "Dry run: use dry=False to download the following file(s):\n"+
#        "\n".join(urls)+"\n")
#        return True
#    else:
#        files=concurrent_download(
#            urls, where, overwrite, dry, chunks=chunks, maxworkers = maxworkers)
#        # check if the files are ok
#        if check and (not dry):
#            logger.info("checking fits files..")
#            for ff in files:
#                if ".fits" in ff:
#                    try:
#                        dd = fits.open(ff)
#                        dd.close()
#                    except:
#                        logger.exception("something is wrong with file %s"%ff)
#                        return False
#        else:
#            return True
#    
## ---------------------------------------------------------- #
## ---------------------- RUN EXAMPLE ----------------------- #
## ---------------------------------------------------------- #

#if __name__ == "__main__":

#    #######################################
#    ###      TEST DOWNLOAD FOR POS      ###
#    #######################################
#    ra, dec=358.3, 25.6
##    time=15*u.day
##    time=Time('2017-12-05')
#    time=[Time('2017-11-05'), Time('2017-12-05')]
#    
#    myfields=querypos(ra=358.3, dec=25.6, time=time, cutdim=None)
#    getdata(myfields, "./autocomplete_test", product="sciimg", dry=False, 
#        overwrite=True, chunks=1024)

#    
#    #######################################
#    ###       TEST OTHER QUERIES        ###
#    #######################################
#    
#    # read in the table containing the metadata for all the science files on IRSA
#    # the table is not there, then create it. Else, either read it or update it.
#    tab=readmetatab('sci', update=False)
#    logger.info("Loaded metadata for %d files on IRSA"%len(tab))
#    logger.info("Use these fields to query IRSA: %s"%"\n".join(tab.columns))

#    ## find out which file you want to download. For example, we want all the
#    ## data for field 612 taken in the last 5 days, at good arimass.
#    ## 
#    ## info on metadata:
#    ## https://irsa.ipac.caltech.edu/docs/program_interface/ztf_metadata.html

#    timejd=Time(tab['obsjd'], format='jd').jd
#    tref=(Time.now()-10*u.d).jd          # last 10 days
#    #tref=Time('2011-11-01').jd         # from beginning of the month
#    fields=[612, 655]
##    query="field == @fields & airmass<1.1 & @timejd>@tref"
#    query="field == @fields"
#    mytab=tab.query(query)
#    logger.info("%d files matches the query: %s"%(len(mytab), query))

#    ## now create name for destination directory and download the files
#    destdir="./data/field612"
#    getdata(mytab[:8], destdir, product="sexcat", overwrite=True, dry=False)
#    
#    # ------------------------- #
#    # now we try with the flats #
#    # ------------------------- #

#    tab=readmetatab('cal', update=False)
#    logger.info("Use these fields to query IRSA: %s"%"\n".join(tab.columns))
#    
#    query="ccdid == 9 & caltype=='hifreqflat'"
#    mytab=tab.query(query)
#    logger.info("%d files matches the query: %s"%(len(mytab), query))

#    destdir="./data/ccd09flats"
#    getdata(mytab[:3], destdir, product="hifreqflat", dry=False)

#    # ------------------------- #
#    # now we try with the bias  #
#    # ------------------------- #

#    query="ccdid == 9 & caltype=='bias'"
#    mytab=tab.query(query)
#    logger.info("%d files matches the query: %s"%(len(mytab), query))

#    destdir="./data/ccd09bias"
#    getdata(mytab[:3], destdir, product="bias", dry=False)
#    
#    # ------------------------------#
#    # now we try with the raw stuff #
#    # ------------------------------#
#    tab=readmetatab('raw', update=False)
#    
#    # raw darks
#    query="ccdid == 9 & imgtype=='d'"
#    mytab=tab.query(query)
#    destdir="./dataccd09raw/dark"
#    getdata(mytab[:3], destdir, product="d", dry=False)
#    
#    # raw bias
#    query="ccdid == 9 & imgtype=='b'"
#    mytab=tab.query(query)
#    destdir="./dataccd09raw/bias"
#    getdata(mytab[:3], destdir, product="b", dry=False)
#    
#    
#    # raw flats
#    query="ccdid == 9 & imgtype=='f'"
#    mytab=tab.query(query)
#    destdir="./dataccd09raw/flats"
#    getdata(mytab[:3], destdir, product="f", dry=False)
#    
#    # raw images
#    tref=(Time.now()-10*u.d).jd
#    fields=[612]
#    query="field == @fields & obsjd>@tref & imgtype=='o'"
#    mytab=tab.query(query)
#    destdir="./dataccd09raw/images"
#    getdata(mytab[:3], destdir, product="o", dry=False)
#    
#    
    

