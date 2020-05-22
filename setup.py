#!/usr/bin/env python

from distutils.core import setup

setup(name='Q Alpha Tools',
      version='0.1',
      author='Eduardo Peynetti',
      author_email='lalopey@gmail.com',
      url='https://github.com/lalopey/qalphatools',
      packages=['qalphatools',
                'qalphatools.factors',
                'qalphatools.loaders',
                'qalphatools.utils'],
     )
