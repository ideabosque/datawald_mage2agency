#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging, sys, unittest, os, boto3, traceback
from dotenv import load_dotenv
from silvaengine_utility import Utility

load_dotenv()
setting = {}

sys.path.insert(0, "/var/www/projects/datawald_mage2agency")
sys.path.insert(1, "/var/www/projects/datawald_agency")
sys.path.insert(2, "/var/www/projects/datawald_connector")
sys.path.insert(3, "/var/www/projects/mage2_connector")

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

from datawald_mage2agency import Mage2Agent


class Mage2AgentTest(unittest.TestCase):
    def setUp(self):
        self.mage2Agent = Mage2Agent(logger, **setting)
        logger.info("Initiate Mage2AgentTest ...")

    def tearDown(self):
        logger.info("Destory Mage2AgentTest ...")


if __name__ == "__main__":
    unittest.main()
