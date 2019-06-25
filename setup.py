#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: M. Giomi (matteo.giomi@desy.de)

from setuptools import setup

setup(
    name='queryIRSA',
    version='0.1',
    description='Software to query for and dowload all ZTF data',
    author='Matteo Giomi',
    author_email='matteo.giomi@desy.de',
    packages=['queryIRSA'],
    url = '',
    download_url = '',
    install_requires=['pymongo', 'pandas', 'tqdm'],
    )
