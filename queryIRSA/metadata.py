#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class to represent IRSA metadata and interface to the corresponding database.
#
# see:
# https://irsa.ipac.caltech.edu/docs/program_interface/ztf_api.html
#
# Author: M. Giomi (matteo.giomi@desy.de)

import tqdm, pymongo
import numpy as np
import pandas as pd
from astropy.time import Time
import astropy.units as u
import concurrent.futures


import logging
module_logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#from utils.download import 
from queryIRSA.utils.db import get_collection, push_url_to_coll
from queryIRSA.utils.time import obsdate_to_time, obsjd_to_time, today_nid
from queryIRSA.utils.download import getdata, url_to_df
from queryIRSA.config import IRSAConfig
conf = IRSAConfig()

class metaDB:
    
    
    def __init__(self, which, db_client=None, wdb=True, logger=None):
        """
            initialize metadata of a certain type e.g.: 'sci', 'raw', or 'cal'
            
            Parameters:
                
                which: `str`
                    type of metdata this instance will handle. Must be in conf.meta_types
                
                db_client: `pymongo.MongoClient`
                    client for the mongo DB. If None, default one will be used.
                
                wdb: `bool`
                    if False, connection to the database will not be established in
                    this method.
                
                logger: `python.logging.logger`
                    logger used by this instance. If None, module level logger will be used.
            
        """
        
        conf.check_meta_type(which)
        self.logger = module_logger if logger is None else logger
        self.type = which
        self.logger.info("Initialized metadata for type: %s"%self.type)
        
        # connect to database and relative collection
        if wdb:
            self.db_client = db_client if not db_client is None else pymongo.MongoClient()
            self.coll = get_collection(self.type, db_client=self.db_client, logger=self.logger)
            self.logger.debug("found %d docs in metadata collection %s"%(
                self.coll.count(), self.type))
        else:
            self.logger.debug("running without database.")
        
        # time parameters and indexes
        if self.type in ['sci', 'raw']:
            self.tkey = 'obsjd'
            self.tkey_formater = obsjd_to_time
        else:
            self.tkey = 'startobsdate'
            self.tkey_formater = obsdate_to_time
            #self.default_indexes = []
        self.default_indexes = [
                                (self.tkey, pymongo.ASCENDING),
                                ('field', pymongo.ASCENDING),
                                ('ccdid', pymongo.ASCENDING),
                                ('fid', pymongo.ASCENDING),
                                ('nid', pymongo.ASCENDING),
                                ('expid', pymongo.ASCENDING)
                            ]

    @property
    def columns(self):
        return self.coll.find_one().keys()
        

#    def get_time_rec(self, meta_record):
#        """
#            return astropy Time object for the metadata record. This 
#            uses jd obsjd for raw and sci metadata, and startobsdate
#            for 
#        """

    def _build_indexes(self, indexes=[]):
        """
            create indexes for the collection.
            
            Parameters:
            -----------
                
                indexes: `list`
                    list of (key, direction) pairs [(k1, type1), (k2, type2), ..]
        """
        
        # add default to requested indexes
        indexes_toadd = self.default_indexes + indexes
        self.logger.debug("creating indexes: %s"%repr(indexes_toadd))
        self.coll.create_index(indexes_toadd, name='default')


    def insert_for_time_range(self, t_start, t_stop):
        """
            download the metadata for the specified time range and update
            the database. 
            
            Parameters:
            -----------
            
                t_start/t_stop: `str` or astropy.time.Time
                    extreme of the time range (UTC) which you want to query.
        """
        
        # parse the time
        t1 = Time(t_start) if type(t_start)==str else t_start
        t2 = Time(t_stop) if type(t_stop)==str else t_stop
        self.logger.info("querying metadata of type %s for time range (%s %s)"%
            (self.type, t1.iso, t2.iso))
        
        # create query on time of metadata
        if self.type in ['sci', 'raw']:
            query_str = "obsjd+BETWEEN+%f+AND+%f"%(t1.jd, t2.jd)
        elif self.type in ['cal', 'ref']:
            query_str = "startobsdate+BETWEEN+'%s'+AND+'%s'"%(
                t1.iso.replace(" ", "+"), t2.iso.replace(" ", "+"))
        
        # query IRSA for the meta and push the result to the database
        full_query = "%s/%s/?WHERE=%s&ct=csv"%(conf.meta_url, self.type, query_str)
        push_url_to_coll(full_query, coll=self.coll, logger=self.logger)


    def insert_for_nid(self, nid):
        """
            download the metadata for the specified night
            
            Parameters:
            -----------
            
                nid: `int`
                    Id of the night, starting from.
        """
        # query IRSA for the meta and push the result to the database
        full_query = "%s/%s/?WHERE=nid=%d&ct=csv"%(conf.meta_url, self.type, nid)
        push_url_to_coll(full_query, coll=self.coll, logger=self.logger)


    def insert_for_nid_range(self, nid_start, nid_stop):
        """
            download the metadata for the specified night:
            nid_start <= nid < nid_stop
            
            Parameters:
            -----------
            
                nid: `int`
                    Id of the night, starting from.
        """
        # query IRSA for the meta and push the result to the database
        full_query = "%s/%s/?WHERE=nid>=+%d+AND+nid<%d&ct=csv"%(conf.meta_url, self.type, nid_start, nid_stop)
        push_url_to_coll(full_query, coll=self.coll, logger=self.logger)


    def last_obs_in_coll(self):
        """
            return the metadata of the latest document in collection. Assume 
            the collection are sorted by date of observation.
        """
        
        if self.coll.count() == 0:
            return None
        else:
            return self.coll.find_one({}, sort=[(self.tkey, pymongo.DESCENDING )])

    def build_database(self, chunk_time_days=7, n_workers=8):
        """
            build the database querying for all night ids.
            
            Parameters:
            -----------
                
                chunk_time_days: `float`
                    duration in nights.
                
                n_workers: `int`
                    number of parallel processes to be used for download.
        """
        
        # index
        self._build_indexes()
        
        # create range for night IDs
        nid_max = today_nid()+2
        dt = int(chunk_time_days)
        nids = [[n, n+dt] for n in range(0, nid_max, dt)]
        if nids[-1][1] > nid_max:
            nids[-1][1] = nid_max
        self.logger.info("building the IRSA metadatabase for product: %s using %d queries"%
            (self.type, len(nids)))
        
        # download the stuff
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            query = self.insert_for_nid_range
            future_to_url = {executor.submit(query, nid[0], nid[1]): nid for nid in nids}
            completed = tqdm.tqdm(concurrent.futures.as_completed(future_to_url), total=len(nids))
            for future in completed:
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.logger.error("%r generated an exception: %s" % (url, exc))
                else:
                    self.logger.debug("query %r completed successfully."%url)
        self.logger.info("finished building the database")


    def update_database(self, chunk_time_days=7, n_workers=4):
        """
            update metadata database for this type of metadata or create it if
            it's empty. The queries are organized in short time intervals of specified
            duration and executed using parallel pools.
            
            Parameters:
            -----------
                
                chunk_time_days: `float`
                    duration in days of the time range covered by each query.
                
                n_workers: `int`
                    number of parallel processes to be used for download.
        """
        
        # define time range for query
        now = Time.now()
        last = self.last_obs_in_coll()
        
        if last is None:
            self.build_database(chunk_time_days=chunk_time_days, n_workers=n_workers)
            return
        
        past =  self.tkey_formater(self.last_obs_in_coll()[self.tkey])
        self.logger.info("updating the database with obs more recent than %s"%past.iso)
        
        # create the time intervals
        dt = chunk_time_days*u.day
        intervals = [[t, t+dt] for t in np.arange(past, now, dt)]
        if intervals[-1][1] > now:
            intervals[-1][1] = now
        self.logger.debug("using %d jobs of %f days long"%(len(intervals), chunk_time_days))
        
        # download the stuff
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            query = self.insert_for_time_range
            future_to_url = {executor.submit(query, tint[0], tint[1]): tint for tint in intervals}
            for future in tqdm.tqdm(concurrent.futures.as_completed(future_to_url), total=len(intervals)):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.logger.error("%r generated an exception: %s" % (url, exc))
                else:
                    self.logger.debug("query %r completed successfully."%url)
        self.logger.info("finished updating the database")


    def query(self, *args, **kwargs):
        """
            query local mongod database and return the results in a dataframe
            
            Parameters:
            -----------
                
                *args, **kwargs: to be passed to self.coll.query()
            
            Returns:
            --------
                
                pandas.DataFrame with query results.
        """
        df = pd.DataFrame(list(self.coll.find(*args, **kwargs)))
        if len(df) == 0:
            self.logger.info("no metadata satisfy query")
            return pd.DataFrame()
        df.drop(columns="_id", inplace=True)
        return df


    def querypos(self, ra, dec, time=None, cutdim=None):
        """
            this function will query the IRSA for all the sci data 
            around a given position and for a given time range.
            
            Parameters:
            -----------
                
                ra/dec: `float`
                    sky position (in degrees) of the target.
                
                time: `astropy.Quantity`,`astropy.time.Time`, list, or None
                    parameter indicating the time range for which the data has to be 
                    downloaded:
                        - if None all the data available will be included. 
                        - if 2-element list of astropy.time.Time objects.
                        will be download data BETWEEN time[0] AND time[1].
                        - if it's a quantitiy with dimension time, data will be 
                        downloaded for times BETWEEN now-time AND now
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
        querystr="?POS=%.5f,%.5f"%(ra, dec)
        self.logger.info(
            "querying IRSA for data at this position (RA: %.5f, Dec: %.5f)"%(ra, dec))
        
        # and then consider the date
        if type(time)==u.quantity.Quantity:
            tref=(Time.now()-time)
            querystr+="&WHERE=obsjd>%s"%str(tref.jd)
            self.logger.info(
                "and not older than %f days"%time.to('day').value)
            self.logger.info(
                "corresponding to time range between %s and %s."%(tref.iso, Time.now().iso))
        elif type(time)==Time:
            timestr=time.datetime.strftime("%Y%m%d")
            querystr+="&WHERE=filefracday>=%d+AND+filefracday<=%d"%(int(timestr+"0"*6), int(timestr+"9"*6))
            self.logger.info("querying IRSA for data taken on %s"%time.iso)
            self.logger.info(
                "using string %s to query for partial matches in filefracday"%timestr)
        elif time is None:
            self.logger.info("querying IRSA for all the data available for that position")
        else:
            try:
                if len(time)==2:
                    
                    # you can have None to leave open time interval bounds
                    time[0] = Time('2016-01-01') if time[0] is None else time[0]
                    time[1] = Time('2026-01-01') if time[1] is None else time[1]
                    
                    # convert to astropy objects
                    tstart = Time(time[0]) if type(time[0])==str else time[0]
                    tstop = Time(time[1]) if type(time[1])==str else time[1]
                    
                    # query using jds
                    start, end= tstart.jd, tstop.jd
                    querystr+="&WHERE=obsjd+BETWEEN+%.5f+AND+%.5f"%(start, end)
                    self.logger.info("querying IRSA for data taken between %s (%.5f) and %s (%.5f)"%(
                        tstart.iso, start, tstop.iso, end))
            except:
                self.logger.exception(
                    "time argument has to astropy.Quantity`,`astropy.time.Time`, a list of them, or None.",
                    "got %s instead"%type(time))
        
        # prepend paths, append format, and download
        full_query = "%s/%s/%s&ct=csv"%(conf.meta_url, self.type, querystr)
        return url_to_df(full_query)
#    logger.info("downloading metadata using IRSA query: %s"%querystr)
#    tmpfile=os.path.join("/tmp/getmeta_ra%.3f_dec%.3f.tbl"%(ra, dec))
#    load_file(url+querystr, outf=tmpfile, auth=(user, pwd), showpbar=True)
#    
#    # read metadata table and get the files
#    mtab=pd.read_csv(tmpfile, engine='c')
#    logger.info("found %d entries in metadata table"%len(mtab))
#    return mtab



    def download(self, what, where, product="psfcat", overwrite=False, dry=False, check=True, n_workers=8, chunks=1024):
        """
            download the data whose metadata is specified in the
            given an astropy table. The files are saved where you want.
            This use the rules described here:
            https://irsa.ipac.caltech.edu/docs/program_interface/ztf_metadata.html
            to build up the download urls.
        
            Parameters:
            -----------
                
                what: `dict` or `pandas.DataFrame`
                    specifies what to download, if 'dict' then this will be used to
                    query the database directly, if 'pandas.DataFrame' then download
                    the files specified by this dataframe.
                
                where: `str`
                    path to the directory where the files will be saved to. It will 
                    be created if not existing.
                
                df: `pandas.DataFrame` (optional)
                    use thhis argument to directly pass a dataframe, this will disable
                    the use of the query_exp.
                
                product: `str`
                    what kind of data you want to download, for example: log (for sci only), 
                    sciimg, sciimlog, mskimg, sexcat, psfcat, sciimgdao, sciimgdaopsfcent,
                     bias, biascmask, biaslog, hifreqflat, raw, ecc..
                 
                n_workers: `int`
                    numer of threads in the pool, to be passed to concurrent_download.
        """
        
        if type(what) == dict:
            df = self.query(query_exp)
        elif type(what) == pd.core.frame.DataFrame:
            df = what
        else:
            raise TypeError("parameter 'what' must be either mongo expression (dict) or DataFrame, not %s"%(type(what)))
        
        # get the data
        if len(df) == 0:
            self.logger.info("nothing to download.")
            return []
        successfull = getdata(
                        df=df, where=where, product=product,
                        overwrite=overwrite, dry=dry, check=check,
                        maxworkers=n_workers, chunks=chunks,
                        logger=self.logger
                    )
        if len(successfull) == len(df):
            self.logger.info("download terminated successfully")
        else:
            self.logger.warning("of the requested files, only %s downloaded successfully."%(len(successfull)))
            return successful
