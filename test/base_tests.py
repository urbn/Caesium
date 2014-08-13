__author__ = 'hunt3r'

"""
Base test classes, all tests should inherit from one of these classes
"""

import unittest
import logging
from tornado.testing import AsyncTestCase
import tornado.web
import tornado.ioloop

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)

class BaseAsyncTest(AsyncTestCase):

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        super(BaseAsyncTest, self).setUp()

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()
