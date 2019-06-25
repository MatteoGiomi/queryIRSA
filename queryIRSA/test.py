#from config import IRSAConfig
##from queryIRSA import updatemeta


## test config
#conf = IRSAConfig()



#import utils.db as dbutils
#from utils.download import url_to_df


#test_url = "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/ref?WHERE=field=000400+AND+ccdid=16+AND+qid=1&ct=csv"

#from utils.db import push_url_to_coll
#push_url_to_coll(test_url, None)

#exit()

from queryIRSA.metadata import metaDB
meta = metaDB('sci')
print (meta.coll.count())

meta._insert_for_time_range('2018-07-11', '2018-07-12')


#from metadata import metadata
#meta = metadata('sci')
#meta.update_database()

#df = meta.query({"nid": 287})


#meta = metadata('sci')
#meta._build_indexes()
#meta.update_database()

#meta = metadata('raw')
#meta._build_indexes()

#meta.update_database()
#exit()

#meta = metadata('sci')
#meta._build_indexes()
#meta.update_database()

#df = meta.query({"nid": 287})
#print (df)


#meta.download({"nid": 287}, "./fuffa", product="psfcat")

#exit()


#"https://irsa.ipac.caltech.edu/ibe/data/ztf/products/sci/2018/0705/187928/ztf_20180705187928_000824_zr_c01_o_q1_sexcat.fits"
#push_url_to_coll(test_url, None)

#test_url = "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/ref?WHERE=field=000400+AND+ccdid=16&ct=csv"
#url_to_df(test_url)



#coll = dbutils.get_collection('sci')
#print (coll)

#dbutils.push_url_to_coll(test_url, coll, logger=None)

# test update
#updatemeta(which='sci', overwrite=False)



