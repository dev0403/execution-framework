import os
import logging
import json
import sys
import settings

class logger(object):
    def __init__(self, log_dir,log_file, moduleName):
        global functionName
        global functionParams
        functionName = ""
        functionParams = {}
        moduleName = moduleName.replace('.log','')
        logger = logging.getLogger(' ModuleName: %s' % moduleName)    # log_namespace can be replaced with your namespace
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            file_name = os.path.join(log_dir, '%s.log' % log_file)    # usually I keep the LOGGING_DIR defined in some global settings file
            handler = logging.FileHandler(file_name)
            handler.flush = sys.stdout.flush
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s: %(message)s', "%Y-%m-%d %H:%M:%S.%s")
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
            print("-----------------------logger-----------------------")
            consoleHandler = logging.StreamHandler(sys.stdout)
            consoleHandler.flush = sys.stdout.flush
            consoleHandler.setFormatter(formatter)
            consoleHandler.setLevel(logging.DEBUG)
            logger.addHandler(consoleHandler)

        self._logger = logger

    def get(self):
        return self

    def addFileHandler(self,log_dir,moduleName):
        logger = logging.getLogger(__name__)            #Added this method for logging in specific folder
        file_name = os.path.join(log_dir, '%s.log' % moduleName)
        print("file name : %s" % file_name)  # usually I keep the LOGGING_DIR defined in some global settings file
        handler = logging.FileHandler(file_name)
        formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        print(self._logger)
        self._logger.addHandler(handler)
        return self

    def info(self,msg):
        #msg = self.concat_msg(msg)
        self._logger.info(msg)

    def debug(self,msg):
        self._logger.debug(msg)

    def error(self,msg):
        self._logger.error(msg)

    def critical(self,msg):
        self._logger.critical(msg)

    def warning(self,msg):
        self._logger.warning(msg)
