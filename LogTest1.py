# coding: utf-8
from commonPy.LogUtil import *
import LogTestCommon
# from LogTestCommon import CommonUtil

my_logger = get_logger()
my_logger.debug("LogTest1 - a debug message")

c = LogTestCommon.CommonUtil()
c.test()
