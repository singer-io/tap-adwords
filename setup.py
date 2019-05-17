#!/usr/bin/env python

from setuptools import setup

setup(name='tap-adwords',
      version="1.11.3",
      description='Singer.io tap for extracting data from the Adwords api',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_adwords'],
      install_requires=[
          'singer-python==5.1.5',
          'requests==2.20.0',
          'googleads==17.0.0',
          'pytz==2018.4',
          'zeep==3.1.0', # googleads dependency, pinned to 3.1.0 (tested version)
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-adwords=tap_adwords:main
      ''',
      packages=['tap_adwords'],
      package_data = {
          'tap_adwords/schemas': [
              "accounts.json",
              "ad_groups.json",
              "ads.json",
              "campaigns.json",
          ],
          'tap_adwords/metadata': [
              "accounts.json",
              "ad_groups.json",
              "ads.json",
              "campaigns.json",
          ],
      },
      include_package_data=True,
)
