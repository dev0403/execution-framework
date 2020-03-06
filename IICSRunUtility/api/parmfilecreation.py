import settings
import os
import sys
from datetime import datetime
import shutil
from lib.logger.logger import logger

class ParmFileCreation:
    """
    Parameter file creation
    """
    def __init__(self,parameters):
        self.parameters = parameters
        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],'IICSCTaskRunner:'+self.__class__.__name__).get()
        self.logger.info("Parameter file creation Started")

    def prepareParmFile(self,parameters=None):
        parm_file = os.path.join(settings.PARM_DIR,self.parameters["parm_file"])

        self.logger.info("Parm File path : %s" % (parm_file))
        #Check if the parm file exists and if yes, delete it
        if os.path.isfile(parm_file):
            self.logger.info("Old Parm file exists and deleting it.")
            os.remove(parm_file)

        #Prepare parameters and dump into the file
        try:
            file_des = open(parm_file,"w+")
            self.logger.info("File opened for writing parameters.")
            parms_not_included = ["job_name","task_name","task_type","parm_file"]

            for parm, value in self.parameters.items():
                if parm not in parms_not_included:
                    file_des.write("$$%s=%s\n" % (parm,value))

            file_des.close()
        except Exception as err:
            self.logger.error(err)
            self.logger.error("Parameter file creation failed. please check for the permissions of folder path")
            sys.exit(1)
        finally:
            pass

    def archiveParmFile(self):
        parm_file = os.path.join(settings.PARM_DIR,self.parameters["parm_file"])
        arch_parm_folder = os.path.join(settings.PARM_DIR,self.parameters["job_name"] + "_wf_run")
        arch_parm_file = os.path.join(arch_parm_folder,self.parameters["job_name"] + "_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".parm")

        self.logger.info("Archival Parmeter folder path : %s" % (arch_parm_folder))
        self.logger.info("Archival Parmeter file name : %s" % (arch_parm_file))
        #Move current parameter file to archive location/path
        if not os.path.exists(arch_parm_folder):
            os.mkdir(arch_parm_folder)

        if os.path.isfile(parm_file):
            try:
                shutil.move(parm_file,arch_parm_file)
                self.logger.info("Archival Parmeter folder path : %s" % (arch_parm_folder))
            except:
                self.logger.error("Archiving process failed for parameter files")
