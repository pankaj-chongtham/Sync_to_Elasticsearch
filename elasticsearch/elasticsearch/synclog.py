"""
Ramco Log Handlers
"""

__maintainer__ = "NGX Team"
__status__ = "Development"
__author__ = "Chongtham Pankaj"

# System Imports
import os
import sys
import logging
import configparser
from logging.handlers import RotatingFileHandler

if getattr(sys, 'frozen', False):
    # we are running in a bundle
    CURRENT_PATH = os.path.dirname(sys.executable)
else:
    # we are running in a normal Python environment
    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
CURRENT_PATH = os.path.normpath(os.path.join(CURRENT_PATH, '..'))

# CONFIG FILE
config_filename = os.path.join(CURRENT_PATH, 'config.ini')
config_obj = configparser.ConfigParser()
config_obj.read(config_filename)


def setup_logger(filename=None, path=None, size: int = None):
    # LOG PATH CONFIGURATION
    if size is not None:
        LOG_SIZE = int(size)
    else:
        LOG_SIZE = int(config_obj['LOG']['logSize'])
    if path is not None:
        log_path = path
    else:
        log_path = os.path.abspath(os.path.join(CURRENT_PATH, 'Log'))
    if filename is not None:
        log_path_file = os.path.join(log_path, filename)
    else:
        log_path_file = os.path.join(log_path, 'Log.log')
    isExist = os.path.exists(log_path)
    if not isExist:
        os.makedirs(log_path)
    log_formatter = logging.Formatter("%(asctime)s  %(filename)s %(lineno)d [%(levelname)s] %(message)s")
    log_handler = RotatingFileHandler(log_path_file, mode='a', maxBytes=LOG_SIZE * 1024 * 1024,
                                      backupCount=2, encoding=None, delay=0)
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(logging.INFO)
    log_stream_handler = logging.StreamHandler(sys.stdout)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(log_handler)
    app_log.addHandler(log_stream_handler)
    return app_log
