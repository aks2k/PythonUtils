# coding: utf-8
import inspect
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from config import *

# LOG_PATH = '/Applications/MAMP/htdocs/bolwiki2Py/'
FORMATTER = logging.Formatter(u"%(asctime)s—%(filename)s—>%(message)s")
# LOG_FILE = "my_app.log"
main_prog = os.path.basename(str(inspect.stack()[-1][1]))
if main_prog == 'pydevd.py':
    main_prog = 'LogTest2.py'
LOG_FILE = LOG_PATH + "/" + os.path.splitext(main_prog)[0] + ".log"
loggers = {}


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_console_handler():
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_logger(name=None):
    global loggers
    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
        if LOG_MODE == 'FILE':
            logger.addHandler(get_file_handler())
        elif LOG_MODE == 'CONSOLE':
            logger.addHandler(get_console_handler())
        logger.propagate = False
        loggers[name] = logger
        return logger
