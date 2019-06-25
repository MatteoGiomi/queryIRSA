#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class to store config relative to IRSA login and base urls.
#
# Author: M. Giomi (matteo.giomi@desy.de)

import os, getpass

HOME = os.getenv('HOME')

class IRSAConfig:
    
    __conf = {
            'meta_url'  : "https://irsa.ipac.caltech.edu/ibe/search/ztf/products",
            'data_url'  : "https://irsa.ipac.caltech.edu/ibe/data/ztf/products",
            'login_url' : "https://irsa.ipac.caltech.edu/account/signon/login.do",
            'db_name'   : "IRSAmeta",
            'tmp_dir'   : "/tmp",
            'auth_file' : os.path.join(HOME, ".queryIRSA", "auth.txt"),
            'meta_types': ['sci', 'raw', 'cal', 'ref']
          }
    
    
    def __init__(self):
        IRSAConfig.load_auth()
        self.__dict__.update(IRSAConfig.__conf)
    
    def check_meta_type(self, which):
        """
            check if given metatdata type is available.
            
            Parameters:
            -----------
                
                which: `str`
                    type of metdata this instance will handle. Must be in conf.meta_types
        """
        
        if not which in self.meta_types:
            raise ValueError("available metadata types are: %s. Got %s instead"%
                (", ".join(self.meta_types), which))
    
    @staticmethod
    def ask():
        username = input("Enter your IRSA username: ")
        password = getpass.getpass("Enter your IRSA password: ")
        return username, password
    
    @staticmethod
    def save(username, password, auth_file):
        os.makedirs(os.path.dirname(auth_file), exist_ok=True)
        with open(auth_file, 'w') as auth:
            auth.write(username+'\n')
            auth.write(password+'\n')
    
    @staticmethod
    def read(auth_file):
        with open(auth_file, 'r') as auth:
            content = [l.strip() for l in auth.readlines()]
            return {'username': content[0], 'password': content[1]}
    
    @classmethod
    def load_auth(cls):
        auth_file = cls.__conf['auth_file']
        if not os.path.isfile(auth_file):
            username, password = IRSAConfig.ask()
            cls.__conf['username'] = username
            cls.__conf['password'] = password
            IRSAConfig.save(username, password, cls.__conf['auth_file'])
        else:
            IRSAConfig.__conf.update(IRSAConfig.read(cls.__conf['auth_file']))
