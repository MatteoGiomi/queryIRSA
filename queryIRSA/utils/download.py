#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# collection of functions to manage downloads
#
# Author: M. Giomi (matteo.giomi@desy.de)

import os, io, tqdm, requests
import pandas as pd
import numpy as np
import concurrent.futures
from astropy.io import fits

import logging
module_logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from queryIRSA.utils.time import parsefilefracday, parsefilestartdate
from queryIRSA.config import IRSAConfig
conf = IRSAConfig()

def get_cookie():
    """
        Get a cookie from the IPAC login service.
        
        Parameters
        ----------
        
        username: `str`
            The IPAC account username.
        
        password: `str`
            The IPAC account password.
    """
    url = "%s?josso_cmd=login&josso_username=%s&josso_password=%s" %(
        conf.login_url, conf.username, conf.password)
    response = requests.get(url)
    cookies = response.cookies
    return cookies


def get_response(url, logger=None):
    """
    """
    if logger is None:
        logger = module_logger
    logger.debug("requesting data from: %s"%url)
    cookies = get_cookie()
    response = requests.get(url, stream=True, cookies=cookies)
    response.raise_for_status()
    return response


def url_to_df(url, logger=None):
    """
        download the content of a url (pointing to a csv file) directly into a 
        pandas DataFrame
    """
    if logger is None:
        logger = module_logger
    content = get_response(url, logger).content
    df = pd.read_csv(io.StringIO(content.decode('utf-8')), low_memory=False)
    logger.debug("read %d entries into dataframe"%len(df))
    return df


def load_file(url, outf=None, localdir=conf.tmp_dir, chunks=1024, showpbar=False):
    """
        Load a file from the specified URL and save it locally.
        
        Parameters
        ----------
        url : `str`
            The URL from which the file is to be downloaded.
        
        outf : `str` or None
            if not None, the downloaded file will be saved to fname, 
            overwriting the localdir option.
        
        localdir : `str`
            The local directory to which the file is to be saved, if no name is specified.
        
        auth : tuple
            A tuple of (username, password) to access the ZTF archive.
        
        chunks: `int`
            size of chunks (in Bytes) used to write file to disk.
        
        showpbar : `bool`
            if True, use tqdm to display the progress bar for the current download.
    """
    
    response = get_response(url)
    
    if outf is None:
        outf = os.path.join(localdir, url[url.rindex('/') + 1:])
    
    with open(outf, 'wb') as handle:
        iterator = response.iter_content(chunks)
        if showpbar:
            size = int(response.headers['Content-length'])
            iterator = tqdm.tqdm(
                iterator, total=int(size/chunks), unit='KB', unit_scale=False, mininterval=0.2)
        for block in iterator:
            handle.write(block)
    return os.stat(outf).st_size


def downloadurl(url, where, overwrite=False, dry=False, chunks=1024, logger=None):
    """
        Dowload the given url to where it is supposed to go.
        
        Parameters:
        -----------
        
        url: `str`
            valid url to a file of some sort.
        
        where: `str`
            path to a directory where the file will be saved.
        
        overwrite: `bool`
            if True will donload again already existsing files.
        
        dry: `bool`
            if True do not download the files. 
        
        Returns:
        --------
        
        outfile: `str`
            path of the file that has been downloaded
    """
    
    if logger is None:
        logger = module_logger
    
    # build up the outfile name
    os.makedirs(where, exist_ok=True)
    outf=os.path.join(where, os.path.basename(url))
    
    # download the stuff
    if not os.path.isfile(outf) or overwrite:
        if not dry:
            logger.info("downloading: %s"%url)
            size=load_file(url, outf=outf, chunks=chunks, showpbar=False)
    else:
        logger.info("file %s exist and we respect it."%outf)
    return outf


def concurrent_download(urls, where, overwrite=False, dry=False, maxworkers = 8, chunks=1024, logger=None):
    """
        Use a thread pool to manage downloads of the
        given urls. Files will be saved where hey should.
        
        Parameters
        ----------
        
        urls : `list`
             List of urls to be downloaded
        
        where : `str`
            The local directory to which the file is to be saved
        
        overwrite : `bool`
            if True will donload again already existsing files.
        
        dry: `bool`
            if True do not download the files.
        
        maxworkers : `int`
            numer of threads in the pool
        
        Returns:
        --------
        list of downloaded files
    """
    
    if logger is None:
        logger = module_logger
    
    files=[] 
    with concurrent.futures.ThreadPoolExecutor(max_workers = maxworkers) as executor:
        jobs = {
            executor.submit(downloadurl, url, where, overwrite, dry, chunks, logger): url for url in urls}
        for job in concurrent.futures.as_completed(jobs):
            url = jobs[job]
            try:
                outf = job.result()
                files.append(outf)
            except Exception:
                logger.exception('%r generated an exception.'%url)
    logger.info("download complete.")
    return files

def geturl(meta, product):
    """
        concatenate the fields in the metadata table to get
        a valid url that can be used to download the data.
        
        Paramters:
        ----------
        meta: `pandas.DataFrame row or dict`
            a dict-like object containing metadata for the file.
        product: `str`
            either bias, sexcat, sciimg, highfreqflat, for the science image
            and catalogs, or the pre-processed calibration files. Product should 
            not contain the file extension. The list of possible product names is:
            
            =============== SCIENCE (SINGLE EXPOSURE) PRODUCTS =================
            - sciimg.fits: primary science image (see header for "info-bit" summary)
            - mskimg.fits: bit-mask image (see header for bit-definitions)
            - psfcat.fits: PSF-fit photometry catalog from DAOPhot
            - sexcat.fits: nested-aperture photometry catalog from SExtractor
            - sciimgdao.psf: spatially varying PSF estimate in DAOPhot's lookup table format
            - sciimgdaopsfcent.fits: PSF estimate at science image center as a FITS image
            - sciimlog.txt: log output from instrumental calibration pipeline, that created sciimg.fits
            - scimrefdiffimg.fits.fz: difference image (science minus reference); fpack-compressed
            - diffimgpsf.fits: PSF estimate for difference image as a FITS image
            - diffimlog.txt: log output from image subtraction and extraction pipeline
            - log.txt: overall system summary log from realtime pipeline

            =================== CALIBRATION IMAGE PRODUCTS =====================
            - bias.fits: bias calibration image product
            - biasunc.fits: 1-sigma uncertainty image for bias image using trimmed stack StdDev/sqrt(N)
            - biascmask.fits: bad pixel calibration mask tagging bad detector / noisy pixels
            - biaslog.txt: log output from bias calibration pipeline
            - hifreqflat.fits: high-frequency relative pixel-to-pixel responsivity image product
            - hifreqflatunc.fits: 1-sigma unc. image for hifreqflat using trimmed stack StdDev/sqrt(N)
            - hifreqflatlog.txt: log output from high-frequency calibration pipeline

            ==================== RAW CAMERA IMAGE FILES ========================

            - o: on-sky object observation or science exposure
            - b: bias calibration image
            - d: dark calibration image
            - f: dome/screen flatfield calibration image
            - c: focus image
            - g: guider image
            
            --------------------------------------------------------------------
            
        Returns:
        --------
        url: `str`
            url of the file which have the given metadata and product.
    """

    # find out the type of dataproduct
    if 'ref' in product:
        which='ref'
    elif 'bias' in product or 'flat' in product:
        which='cal'
    elif product in ['o', 'b', 'd', 'f', 'c', 'g']:
        which='raw'
    elif 'sci' in product or 'cat' in product or 'im' in product or 'log' in product:
        which='sci'
    else:
        raise ValueError("Unknown data product name %s. Accepted values are:\n %s"%
            (product, inspect.getdoc(geturl).split("file.")[-1].split("Returns")[0]))
        logginf.error(exc_info=True)
    
    # reference images are not indexed by date, so we do them first
    if which=='ref':
        fieldprefix = ("%06d"%meta['field'])[:3]
        url_path = os.path.join(
            'ref',
            fieldprefix,
            'field%06d'%meta['field'],
            meta['filtercode'],
            'ccd%02d'%meta['ccdid'],
            'q%d'%meta['qid'])
        fname = "ztf_%06d_%s_c%02d_q%d_refimg.fits"%(meta['field'], meta['filtercode'], meta['ccdid'], meta['qid'])
        return os.path.join(conf.data_url, url_path, fname)
#        https://irsa.ipac.caltech.edu/ibe/data/ztf/products/ref/'+fieldprefix+'/field'+paddedfield+'/'+filtercode+'/ccd'+paddedccdid+'/q'+qid+'/ztf_'+paddedfield+'_'+filtercode+'_c'+paddedccdid+'_q'+qid+'_refimg.fits
    
    # find year, month, day, and dayfrac depending on which metadata you have
    if 'filestartdate' in meta.axes[0] and not (np.isnan(meta['filestartdate'])):
        y, m, d=parsefilestartdate(str(int(meta['filestartdate'])))
    elif 'filefracday' in meta.axes[0] and not (np.isnan(meta['filefracday'])):
        ffd = str(meta['filefracday'])
        y, m, d, fd=parsefilefracday(ffd)
    elif 'nightdate' in meta.axes[0] and not (np.isnan(meta['nightdate'])):
        y, m, d=parsefilestartdate(str(int(meta['nightdate'])))
    
    # parse the metadata into download url
    if 'bias' in product:
        url="bias/00/ccd%02d/q%d/ztf_%s_00_c%02d_q%d_%s.fits"%(
            meta['ccdid'], meta['qid'], y+m+d,
            meta['ccdid'], meta['qid'], product)
    elif 'flat' in product:
        url="hifreqflat/%s/ccd%02d/q%d/ztf_%s_%s_c%02d_q%d_%s.fits"%(
            meta['filtercode'], meta['ccdid'], meta['qid'], y+m+d,
            meta['filtercode'], meta['ccdid'], meta['qid'], product)
    elif which=='raw':
        if product=='d':
            filtercode='dk'
        elif product=='b':
            filtercode='bi'
        else:
            filtercode=meta['filtercode']
        
        # TODO: thsi is a fix for the non-null fieldid field for the flats
        fieldid = meta['field']
        if product=='f':
            fieldid = 0
        url="%s/ztf_%s_%06d_%s_c%02d_%s.fits.fz"%(fd, ffd, fieldid,  #BEFORE FIX: meta['field'],
            filtercode, meta['ccdid'], product)
    else:
        url="%s/ztf_%s_%06d_%s_c%02d_%s_q%d_%s.fits"%(
            fd, ffd, meta['field'], meta['filtercode'], 
            meta['ccdid'], meta['imgtypecode'], meta['qid'], product)
    
    # check different file extensions
    if 'log' in product:
        url=url.replace('.fits', '.txt')
    elif product=='sciimgdao':
        url=url.replace('.fits', '.psf')
    
    # append root and return
    url=os.path.join(conf.data_url, which, y, m+d, url)
    return url

def getdata(df, where, product="psfcat", overwrite=False, dry=False, check=True, 
    maxworkers=8, chunks=1024, logger=None):
    """
        download the data whose metadata are specified in a DataFrame 
        to a desired directory.
        
        This use the rules described here:
        https://irsa.ipac.caltech.edu/docs/program_interface/ztf_metadata.html
        to build up the download urls.
        
        Parameters:
        -----------
        
            df: 'pandas.DataFrame`
                df with the metadata for the files you want to download.
            
            where: `str`
                path to the directory where the files will be saved to.
            
            product: `str`
                what kind of data you want to download, for example: log (for sci only), 
                sciimg, sciimlog, mskimg, sexcat, psfcat, sciimgdao, sciimgdaopsfcent,
                bias, biascmask, biaslog, hifreqflat, raw, ecc..
            
            maxworkers: `int`
                numer of threads in the pool, to be passed to concurrent_download.
            
            dry: `bool`
                if you are just kidding.
            
            check: `bool`
                if True, try to open all of the files to check for integrity.
        
        Returns:
        --------
            list of files that have been succesfully downloaded
            False is any of the files does not opens as a fits file. True if all
            the files are ok, or if they are not fits files (e.g. logs), or if
            check is False
    """
    
    if logger is None:
        logger = module_logger
    
    # populate list of download urls
    urls=[geturl(t, product) for _, t in df.iterrows()]

    if dry:
        logger.info(
        "Dry run: use dry=False to download the following file(s):\n"+
        "\n".join(urls)+"\n")
        return []
    
    # do it for real
    files=concurrent_download(
        urls, where, overwrite, dry, chunks=chunks, maxworkers = maxworkers)
    
    # check if the files are ok. Return list of failed files. 
    successful = []
    if check:
        logger.debug("checking fits files..")
        for ff in files:
            if ".fits" in ff:
                try:
                    dd = fits.open(ff)
                    dd.close()
                    successful.append(ff)
                except:
                    logger.exception("something is wrong with file %s"%ff)
    else:
        successful = files
    return successful
    
