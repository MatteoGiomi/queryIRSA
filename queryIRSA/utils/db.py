#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# collection of functions to interact with the IRSA metadata databases
#
# Author: M. Giomi (matteo.giomi@desy.de)

import pandas as pd
import pymongo 
from queryIRSA.config import IRSAConfig
from queryIRSA.utils.download import url_to_df

import logging
module_logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

conf = IRSAConfig()

def get_collection(which, db_client=None, logger=None):
    """
        return the pymongo collection for the given type of metadata.
        
        Parameters:
        -----------
        
            which: `str`
                select the data type, either 'sci', 'raw', 'cal'.
            
            db_client: `pymongo.MongoClient`
                database client
        
        Returns:
        --------
            mongo collection.
    """
    
    conf.check_meta_type(which)
    if db_client == None:
        db_client = pymongo.MongoClient()
    db = db_client[conf.db_name]
    return db[which]

def push_url_to_coll(url, coll, logger=None):
    """
        insert the content of a csv file into a mongod collection.
        
        Parameters:
        -----------
            
            url: `str`
                url of the content that has to be inserted in the collection.
            
            coll: `pymongo.MongoCollection`
                database collection in which the entry has tobe stored.
            
            logger: `logging.logger`
                logger instance
    """
    
    if logger is None:
        logger = module_logger
    
    # get the url content into a dataframe
    df = url_to_df(url, logger=logger)
    if len(df) == 0:
        return
    
    # remove '.' from key names
    df.rename(lambda s: s.replace(".", "-"), axis='columns', inplace=True)
    
    # ------- build up the _id field ------ #
    
    if "cid" in df.columns.values: # this is for calibration data products
        df.rename({"cid": "_id"}, axis='columns', inplace=True)
    else:   # for raw and sci, we have to build one unique index ourselves
        df['_id'] = pd.Series((hash(frozenset(row)) for _, row in df.iterrows()))
    
    # insert dataframe as dictionary in collection
    recs = df.to_dict('records')
    logger.debug("got %d records from: %s"%(len(df), url))
    logger.debug("%d documents already present in collection."%coll.count())
    try:
        coll.insert_many(recs, ordered=False)
        logger.debug("inserted %d documents in collection. New count: %d"%(len(recs), coll.count()))
    except pymongo.errors.BulkWriteError:
        logger.warning("no new document to be inserted in collection.")


