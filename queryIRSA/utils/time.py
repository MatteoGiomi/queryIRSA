#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# utility functions to manipulate time
#
# Author: M. Giomi (matteo.giomi@desy.de)

from astropy.time import Time
from datetime import datetime

def parsefilefracday(ffd):
    y, m, d, fd=ffd[:4], ffd[4:6], ffd[6:8], ffd[8:14]
    return y, m, d, fd

def parsefilestartdate(fsd):
    y, m, d=fsd[:4], fsd[4:6], fsd[6:8]
    return y, m, d

def tstamp(tformat='%Y-%m-%d %H:%M:%S'):
    """
        current time as timestamp string.
        
        Parameters:
        -----------
        tformat: `str`
            valid datetime time format.
        
        Returns:
        --------
        timestamp: `str`
            current time formatted according to tformat.
    """
    return datetime.now().strftime(tformat)

def obsjd_to_time(jdval):
    """
        create astropy.time.Time object from jd value
    """
    return Time(jdval, format = 'jd')

def obsdate_to_time(obsdate):
    """
        create astropy.time.Time object from obsdate string
        e.g.: 2017-10-14 03:25:33+00
    """
    return Time(obsdate.split("+")[0], format='iso', in_subfmt="date_hms")

def nid_to_fits(nid, complete = False):
    """
    converts night ID to standard fits-timeformat (yyyy-mm-dd)
        nid: 'int'
            night ID for which the proper date is wanted
        complete: 'Bool'
            if True, the function returns the following format: (yyyy-mm-ddThh:mm:ss)
    """
    start_date_jd = Time('2017-01-01').jd
    night_fits = Time(start_date_jd + nid, format = 'jd', out_subfmt = 'iso')
    night_fits.format = 'fits'
    if complete:
        night_fits = str(night_fits)
    else:
        night_fits = str(night_fits)[:10]
    return night_fits

def fits_to_nid(date):
    """
    converts standard fits-timeformat (abbreviated) - str[yyyy-mm-dd] - to ZTF night ID (nights since 2017-01-01 as int)

        date: 'str'
            date that needs to be converted
    """
    date_zp_jd = Time('2017-01-01').jd
    date_actual_jd = Time(date).jd
    nid = int(date_actual_jd - date_zp_jd)
    return nid

def today_nid():
    """
        return the night ID corresponding to today UTC time
    """
    return fits_to_nid(Time.now().iso.split(" ")[0].strip())
    

