import settings
import os
import sys
import subprocess
import shlex
from connection.session import Session
from lib.logger.logger import logger


class BCPRUNNER:
    """
        Prepare command and run
    """

    def __init__(self,parameters):
        self.parameters = parameters
        # self.logger = logger(self.parameters["log_dir"], self.parameters["log_file"], self.__class__.__name__).get()
        self.logger = logger(self.parameters["log_dir"], self.parameters["log_file"],'BCPRunner:'+self.__class__.__name__).get()

        self.logger.info("BCP Runner Started")

    def startTaskFlow(self, parameters):
        self.parameters = parameters
        self.logger.info("Checking whether required parameters have been passed")
        self.checkConnections()

        self.logger.info("Preparing BCP Command Started")
        self.prepareCmd()
        self.logger.info("Preapre BCP Command Completed")

        self.logger.info("BCP commands execution initialized")
        self.runCmd()
        self.logger.info("BCP commands execution completed")

    def startTaskFlow_new(self,parameters):
        self.parameters = parameters
        self.logger.info("Starting the bcp flow of commands")
        self.loadUnload()

    def loadUnload(self):
        loadUnloadTypes = self.parameters["load_type"].split("->")
        # Check if same number of objects and connects are passed
        for index in range(1,len(loadUnloadTypes)+1):
            load_type = loadUnloadTypes[index-1]
            if "OBJ_"+str(index) not in self.parameters or "OBJ_CONN_"+str(index) not in self.parameters or "file_"+str(index) not in self.parameters:
                self.logger.error("Exact same number of Objects and Connections to be passed as loadUnloadTypes")
                self.logger.error("%s or %s or %s are not passed. Please check entries accordingly." % ("OBJ_"+str(index),"OBJ_CONN_"+str(index),"file_"+str(index)))
                sys.exit(1)
            if load_type not in ["out","in","queryout"]:
                self.logger.error("Unknown load type mentioned : %s" % load_type)
                self.logger.error("Please check audit entries")
                sys.exit(1)

            self.parameters["DB_CONN_NM"] = self.parameters["OBJ_CONN_"+str(index)]
            self.session = Session(self.parameters)

            (host,port,db,sch,user,passwd,obj) = (self.session.SERVER_HOST,self.session.SERVER_PORT,self.session.DB_NM,
                    self.session.SCH_NM,self.session.USER_NM,self.session.PASS_TX,self.parameters["OBJ_"+str(index)])

            #Adding single quotes around query for queryout option
            if load_type == "queryout":
                obj = "'" + obj + "'"

            file_path = os.path.join(settings.EXTRACTS_DIR , self.parameters["file_"+str(index)])
            self.logger.info("File Path of the current command : %s" % file_path)
            cmd = self.bcpCmd(obj, load_type, file_path, host, db, user, passwd, self.parameters["delimiter"])

            #Checking any pre-delete conditions/pre-query for targets are defined
            if load_type == "in" and "PRE_QRY_"+str(index) in self.parameters:
                self.logger.info("Pre SQL getting executed : %s" % self.parameters["PRE_QRY_"+str(index)])
                self.session.sessionOpen()
                self.session.executeQuery(self.parameters["PRE_QRY_"+str(index)])
                self.session.sessionClose()

            self.logger.info("Command getting executed curretly : %s" % cmd)
            targetType = "File" if load_type in ["out","queryout"] else "DB"

            process = self.processCmd(cmd,targetType)
        return True

    def checkConnections(self):

        if self.parameters["load_type"] == "both":

            if "SRC_DB_CONN_NM" not in self.parameters or  "TGT_DB_CONN_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both connection names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)
            if "SRC_OBJ_NM" not in self.parameters or  "TGT_OBJ_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both object names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)

            self.parameters["DB_CONN_NM"] = self.parameters["SRC_DB_CONN_NM"]
            self.sourceSession = Session(self.parameters)
            self.parameters["DB_CONN_NM"] = self.parameters["TGT_DB_CONN_NM"]
            self.targetSession = Session(self.parameters)
        elif self.parameters["load_type"] == "out" or self.parameters["load_type"] == "queryout":
            if "SRC_DB_CONN_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both connection names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)
            if "SRC_OBJ_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both object names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)
            self.parameters["DB_CONN_NM"] = self.parameters["SRC_DB_CONN_NM"]
            self.sourceSession = Session(self.parameters)
        else:
            if "TGT_DB_CONN_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both connection names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)
            if "TGT_OBJ_NM" not in self.parameters:
                self.logger.error("Load type mentioned '%s' but one or both object names are not specified in entries" % self.parameters["load_type"])
                sys.exit(1)
            self.parameters["DB_CONN_NM"] = self.parameters["TGT_DB_CONN_NM"]
            self.targetSession = Session(self.parameters)

    def bcpCmd(self, object, load_type, file_path, host, db, user, passwd,delimiter):
        return shlex.split("bcp {0} {1} {2} -q -c -S {3} -d {4} -U {5} -P {6} -t {7}".format(object, load_type, file_path, host, db, user, passwd.rstrip(),delimiter))

    def prepareCmd(self):

        file_path = os.path.join(settings.EXTRACTS_DIR , self.parameters["file_nm"])
        self.cmd={}

        if self.parameters["load_type"] == "both":
            sourceHost = self.sourceSession.SERVER_HOST
            sourcePort = self.sourceSession.SERVER_PORT
            sourceDB = self.sourceSession.DB_NM
            sourceSCH = self.sourceSession.SCH_NM
            sourceUser = self.sourceSession.USER_NM
            sourcePass = self.sourceSession.PASS_TX
            sourceObj = self.parameters["SRC_OBJ_NM"]

            targetHost = self.targetSession.SERVER_HOST
            targetPort = self.targetSession.SERVER_PORT
            targetDB = self.targetSession.DB_NM
            targetSCH = self.targetSession.SCH_NM
            targetUser = self.targetSession.USER_NM
            targetPass = self.targetSession.PASS_TX
            targetObj = self.parameters["TGT_OBJ_NM"]

            self.cmd["unload"] = self.bcpCmd(sourceObj, "out", file_path,sourceHost,sourceDB, sourceUser, sourcePass,self.parameters["delimiter"])
            self.cmd["load"] = self.bcpCmd(targetObj, "in", file_path, targetHost, targetDB, targetUser, targetPass, self.parameters["delimiter"])
            # self.cmd["unload"] = 'bcp {0} {1} {2} -q -c -S {3} -d {4} -U {5} -P {6} -t {7}'.format(sourceObj, "out", file_path,sourceHost,sourceDB, sourceUser, sourcePass,self.parameters["delimiter"])
            # self.cmd["load"] = 'bcp {0} {1} {2} -q -c -S {3} -d {4} -U {5} -P {6} -t {7}'.format(targetObj, "in", file_path, targetHost, targetDB, targetUser, targetPass, self.parameters["delimiter"])
        elif self.parameters["load_type"] == "out" or self.parameters["load_type"] == "queryout":
            sourceHost = self.sourceSession.SERVER_HOST
            sourcePort = self.sourceSession.SERVER_PORT
            sourceDB = self.sourceSession.DB_NM
            sourceSCH = self.sourceSession.SCH_NM
            sourceUser = self.sourceSession.USER_NM
            sourcePass = self.sourceSession.PASS_TX
            sourceObj = self.parameters["SRC_OBJ_NM"]
            self.cmd["unload"] = self.bcpCmd(sourceObj, "out", file_path,sourceHost,sourceDB, sourceUser, sourcePass,self.parameters["delimiter"])
        else:
            targetHost = self.targetSession.SERVER_HOST
            targetPort = self.targetSession.SERVER_PORT
            targetDB = self.targetSession.DB_NM
            targetSCH = self.targetSession.SCH_NM
            targetUser = self.targetSession.USER_NM
            targetPass = self.targetSession.PASS_TX
            targetObj = self.parameters["TGT_OBJ_NM"]
            self.cmd["load"] = self.bcpCmd(targetObj, "in", file_path, targetHost, targetDB, targetUser, targetPass, self.parameters["delimiter"])

    def runCmd(self):
        load_type = self.parameters["load_type"]
        targetType = "File" if load_type == "out" else "DB"

        if self.parameters["load_type"] == "both":
            cmd = self.cmd["unload"]
            targetType = "File"
            process = self.processCmd(cmd,targetType)
            cmd = self.cmd["load"]
            targetType = "DB"
            process = self.processCmd(cmd,targetType)
        elif self.parameters["load_type"] == "out" or self.parameters["load_type"] == "queryout":
            cmd = self.cmd["unload"]
            targetType = "File"
            process = self.processCmd(cmd,targetType)
        else:
            cmd = self.cmd["load"]
            targetType = "DB"
            process = self.processCmd(cmd,targetType)

    def processCmd(self,cmd,targetType):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.logger.info("Loading Data To {0} has been started".format(targetType))
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            self.logger.error("Loading Data To {0} has been failed: please refer logs.".format(targetType))
            self.logger.error(stdout)
            self.logger.error(stderr)
            sys.exit(1)
        else:
            self.logger.info(stdout)
            self.logger.info("Loading Data To {0} has been completed successfully.".format(targetType))

        return process
