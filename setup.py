#!/usr/bin/env python

__author__ = 'hunt3r'

from setuptools import setup

setup(name='Caesium',
      version='0.3.1',
      license='http://www.apache.org/licenses/LICENSE-2.0',
      description='Caesium document revision and RESTful tools.',
      author='Chris Hunter',
      author_email='hunter.christopher@gmail.com',
      url='http://urbn.github.io/Caesium',
      packages=['caesium'],
      install_requires=[
          'tornado >= 3.1',
          'motor >= 0.3',
          'jsonschema >= 2.3.0'
      ],
      keywords=[
          "mongo", "mongodb", "pymongo", "gridfs", "bson", "motor", "tornado", "motor", "patch", "revision", "scheduler", "REST", "RESTful"
      ],
     )

