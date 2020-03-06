import settings
import pyodbc
import  sys
import base64
import subprocess
import shlex
import json
import os
from lib.logger.logger import logger

class Session:
    def __init__(self,parameters):
        #Import logger
        self.parameters = parameters
        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],'SQLExecutor:'+self.__class__.__name__).get()
        self.logger.info("Session API Started")

        #Decrypting connection info using gpg utility
        self.DB_CONN_NM = self.parameters["DB_CONN_NM"]
        #gpg_cmd = "gpg --yes --batch --output - --passphrase=%s --decrypt %s | grep %s" % (base64.b64decode(settings.GPG_PASSPHRASE).decode(),settings.GPG_CONN_FILE,self.DB_ADT_CONN_NM)
        gpg_cmd = "gpg --yes --batch --output - --passphrase=%s --decrypt %s" % (base64.b64decode(settings.GPG_PASSPHRASE).decode(),settings.GPG_CONN_FILE)
        grep_cmd = "grep %s" % self.DB_CONN_NM

        self.logger.info("gpg command to be executed : %s" % gpg_cmd)

        try:
            process_connstr = subprocess.Popen(shlex.split(gpg_cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            process_conndtl = subprocess.Popen(shlex.split(grep_cmd),stdin=process_connstr.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            conn_dtl, stderr = process_conndtl.communicate()
            if len(conn_dtl.strip()) == 0:
                self.logger.error("Connection Details of CONN_NM are not part of entry. Please give entry")
                raise Exception("Connection Details of CONN_NM are not part of entry. Please give entry")

            conn_info = conn_dtl.decode().split("|")

            self.DB_CONN_NM = conn_info[0]
            self.SERVER_HOST = conn_info[1]
            self.SERVER_PORT = conn_info[2]
            self.DB_NM = conn_info[3]
            self.SCH_NM = conn_info[4]
            self.USER_NM = conn_info[5]
            self.PASS_TX = conn_info[6]
        except Exception as err:
            self.logger.error("Error reading or decrypting connection file")
            self.logger.error("Error stacktrace : %s" % err)
            sys.exit(1)

        # self.DB_ADT_CONN_NM = settings.DB_ADT_CONN_NM
        # self.SERVER_HOST = settings.SERVER_HOST
        # self.SERVER_PORT = settings.SERVER_PORT
        # self.ADT_DB_NM = settings.ADT_DB_NM
        # self.ADT_SCH_NM = settings.ADT_SCH_NM
        # self.USER_NM = settings.USER_NM
        # self.PASS_TX = base64.b64decode(settings.PASS_TX).decode() #settings.PASS_TX

        self.logger.info("Audit Database connection name : %s" % (self.DB_CONN_NM))
        self.logger.info("Audit Database Server URL  : %s" % (self.SERVER_HOST))
        self.logger.info("Audit Database Server Port : %s" % (self.SERVER_PORT))
        self.logger.info("Audit Database name : %s" % (self.DB_NM))
        self.logger.info("Audit Database Schema name : %s" % (self.SCH_NM))
        self.logger.info("Audit Database User name : %s" % (self.USER_NM))

        self.logger.info("Creating a Session to Audit Database")
        try:
            connection_str = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:'+ self.SERVER_HOST +','+ self.SERVER_PORT +';Database='+ self.DB_NM +';Uid='+ self.USER_NM + '@'+self.SERVER_HOST.split(".")[0]+';Pwd='+ self.PASS_TX +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
            connection = pyodbc.connect(connection_str)
            self.cursor = connection.cursor()
            if self.cursor is not None:
                self.logger.info("Cursor object created successfully")
            self.logger.info(self.cursor.execute("SELECT @@VERSION;").fetchone())
        except Exception as err:
            self.logger.error("Error while creating database connection. Please check connection entries")
            self.logger.error("Error Stacktrace : %s" % (err))
            sys.exit(1)

    def getDBConn(self):
        self.logger.info("Returing Cursor")
        return self.cursor

    def executeQuery(self,queryfile=None,args=None):
        # query = query % tuple(args) -- Not needed here
        if not os.path.isfile(queryfile):
            self.logger.error("Query file does not exists")
            sys.exit(1)

        with open(queryfile,"r") as fp:
            query=fp.read()

        self.logger.info("Executing Query : %s" % query)
        try:
            # self.cursor.execute("BEGIN")
            cursor_entries = self.cursor.execute(query)
            self.cursor.execute("commit;")
            self.logger.info("SQL file got executed")
        except Exception as err:
            self.cursor.execute("rollback;")
            self.logger.error("Error in executing queries from the sql file")
            self.logger.error("Error stacktrace : %s" % err)
            sys.exit(1)

    def auditLog(self):
        pass

    def sessionClose(self):
        try:
            self.logger.info("Closing Database connection")
            self.cursor.close()
        except Exception as err:
            self.logger.error("Error in Closing database Connection")
            self.logger.error("Error Stacktrace : %s" % err)
            sys.exit(1)
