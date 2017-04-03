#!/usr/bin/env python

from setuptools import setup

setup(name='tap-adwords',
      version="0.0.1",
      description='Singer.io tap for extracting data from the Adwords api',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_adwords'],
      install_requires=[
            'singer-python>=0.3.1',
            'requests==2.13.0',
      ],
      entry_points='''
          [console_scripts]
          tap-adwords=tap_adwords:main
      ''',
      packages=['tap_adwords'],
      # package_data = {
      #     'tap_adwords/schemas': [
      #         "clients.json",
      #         "contacts.json",
      #         "expense_categories.json",
      #         "expenses.json",
      #         "invoice_item_categories.json",
      #         "invoice_payments.json"
      #         "invoices.json",
      #         "people.json",
      #         "project_tasks.json",
      #         "project_users.json",
      #         "projects.json",
      #         "tasks.json",
      #         "time_entries.json",
      #     ],
      # },
      # include_package_data=True,
)
