import settings
import os
import sys
import subprocess
import shlex
from lib.logger.logger import logger

class CommandRunner:
    """
        Prepare command and run IICS task
    """
    def __init__(self,parameters):
        self.parameters = parameters
        self.cmd = []
        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],'IICSCTaskRunner:'+self.__class__.__name__).get()
        self.logger.info("Command Runner API has Started")

    def prepareCmd(self):
        self.program_name = settings.CLI_PRG
        self.cmd.extend(shlex.split(self.program_name)) # changed from append to extend
        self.cmd.extend(["-n",self.parameters["task_name"]])
        self.cmd.extend(["-t",self.parameters["task_type"]])
        self.cmd.extend(["-fp",self.parameters["iics_fp"]])

        self.logger.info("IICS command to run : %s" % (' '.join(self.cmd)))

    def runCmd(self):
        self.prepareCmd()
        # self.cmd=shlex.split("ls -lrt /home/devendra/")
        #self.cmd.append("ls -lrt /home/devendra/")
        process = subprocess.Popen(self.cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        self.logger.info("Waiting for the IICS task to complete")

        if process.returncode != 0:
            self.logger.error("IICS task has been failed: please refer logs")
            self.logger.error(stdout)
            self.logger.error(stderr)
            sys.exit(1)

        self.logger.info(stdout)
        self.logger.info("IICS task has been completed successfully")
