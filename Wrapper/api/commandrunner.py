import settings
import os
import sys
import subprocess
from datetime import datetime
from lib.logger.logger import logger

class CommandRunner:
    """
        Prepare command and run task
    """
    def __init__(self,parameters,audit_entires,session):
        self.parameters = parameters
        self.audit_entires = audit_entires
        self.session = session          # added to handle checkpoints
        self.process_cmd_txt = []

        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],self.__class__.__name__).get()
        self.logger.info("CommandRunner API Started")

    def startTaskFlow(self):
        prev_step_id = -99
        index = 0
        parm_li = []
        cmd_line = []

        self.logger.info("Building command line entries")
        for entry in self.audit_entires:
            curr_step_id = entry["STEP_ID"]
            curr_step_nm = entry["STEP_NM"]
            curr_process_nm = entry["PCS_NM"]
            curr_process_scrpt_nm = entry["PCS_SCRPT_NM"]
            curr_process_type = entry["PCS_TYPE"]
            curr_parm_nm = "--" + entry["PARM_NM"]  # Adding -- as suffix to parmeter names
            curr_parm_val = entry["PARM_VAL"]
            if (prev_step_id != curr_step_id) and (index != 0):
                cmd_line.extend([settings.PROCESS_TYPE_MAP[prev_process_type],prev_process_nm])
                cmd_line.extend(parm_li)
                log_file = "%s_%d.log" % (curr_process_scrpt_nm.split(".")[0],prev_step_id)
                cmd_line.extend(["--log_dir",self.parameters["log_dir"],"--log_file",log_file])
                self.process_cmd_txt.append((prev_entry,cmd_line)) # added to handle checkpoints
                self.logger.info("Command line for step id %d : %s" % (prev_step_id,' '.join(cmd_line)))
                cmd_line = []
                parm_li = []

            parm_li.extend([curr_parm_nm,curr_parm_val])
            index = 1

            prev_entry = entry
            prev_step_id = curr_step_id
            prev_step_nm = curr_step_nm
            prev_process_nm = curr_process_nm
            prev_process_scrpt_nm = curr_process_scrpt_nm
            prev_process_type = curr_process_type
            prev_parm_nm = curr_parm_nm
            prev_parm_val = curr_parm_val

        cmd_line.extend([settings.PROCESS_TYPE_MAP[prev_process_type],prev_process_nm])
        cmd_line.extend(parm_li)
        log_file = "%s_%d.log" % (curr_process_scrpt_nm.split(".")[0],prev_step_id)
        cmd_line.extend(["--log_dir",self.parameters["log_dir"],"--log_file",log_file])

        self.process_cmd_txt.append((prev_entry,cmd_line))
        self.logger.info("Command line for step id %d : %s" % (prev_step_id,' '.join(cmd_line)))

        self.logger.info("Check whether any checkpoints got created")
        chkpoint_stepid = self.checkPoints()

        self.logger.info("Commands execution initialized")
        self.commandExecutor(chkpoint_stepid)
        self.logger.info("Commands execution Completed")

    def logJobStatus(self,statusType):
        if statusType == 'RUNNING':
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            args = [self.parameters["job_name"],self.parameters["RUN_ID"],self.parameters["odate"],start_time,'RUNNING']
            self.session.logEntry(settings.JOB_EXE_STRT_STA_QRY,args)
        elif statusType == 'SUCCESS':
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            args = [end_time,statusType,self.parameters["job_name"],self.parameters["odate"],self.parameters["RUN_ID"]]
            self.session.logEntry(settings.JOB_EXE_END_STA_QRY,args)

    def logJobPcsStatus(self,entry,statusType):
        if statusType == 'RUNNING':
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            args = [self.parameters["job_name"],entry["STEP_ID"],entry["STEP_NM"],entry["PCS_NM"],self.parameters["RUN_ID"],self.parameters["odate"],start_time,'RUNNING']
            self.session.logEntry(settings.PCS_EXE_STRT_STA_QRY,args)
        elif statusType == 'SUCCESS':
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            args = [end_time,statusType,self.parameters["job_name"],entry["STEP_ID"],self.parameters["odate"],self.parameters["RUN_ID"]]
            self.session.logEntry(settings.PCS_EXE_END_STA_QRY,args)

    def commandExecutor(self,chkpoint_stepid):
        self.logger.info("Enterd the command runner block")
        self.logJobStatus('RUNNING')
        for (entry,cmd) in self.process_cmd_txt:  # added to handle checkpoints
            step_id=entry["STEP_ID"]
            if chkpoint_stepid<=step_id:
                print("----------------------------------------------------------------------------------------------------------------------------")
                self.logger.info("Starting Executing Step No : %d" % step_id)
                self.logJobPcsStatus(entry,'RUNNING')
                self.session.logEntry(settings.LOGENTRY_QRY,[self.parameters["job_name"],step_id,self.parameters["odate"],'RUNNING'])
                self.logger.info("Command getting executed : %s" % (' '.join(cmd)))
                self.runCmd(cmd)
                self.session.logEntry(settings.DLTENTRY_QRY,[self.parameters["job_name"],self.parameters["odate"]])
                self.logJobPcsStatus(entry,'SUCCESS')
                self.logger.info("Ended Step No %d successfully" % step_id)
                print("----------------------------------------------------------------------------------------------------------------------------")
        self.logJobStatus('SUCCESS')

    def checkPoints(self):
        chkpoint_entry = self.session.executeQuery(settings.CHECKPOINT_QRY,[self.parameters["job_name"],self.parameters["odate"]])
        if len(chkpoint_entry)!=0:
            self.logger.info("Check Point entry found at step: %s" % str(chkpoint_entry[0]["STEP_ID"]))
            self.logger.info("Deleting the checkpoint before running the task")
            self.session.logEntry(settings.DELCHECKPOINT_QRY,[self.parameters["job_name"],self.parameters["odate"]])
            return chkpoint_entry[0]["STEP_ID"]
        self.logger.info("Check Point entry Not found at step. Proceeding from start step")
        return 0

    def runCmd(self,cmd):

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
        self.logger.info("Waiting for task to complete")
        # stdout, stderr = process.communicate()
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                self.logger.info(output.strip())
            rc = process.poll()

        if process.returncode != 0:
            self.logger.error("Task has been failed: please refer logs")
            # self.logger.error(stdout)
            # self.logger.error(stderr)
            sys.exit(1)

        # self.logger.info(stdout)
        self.logger.info("Task has been completed successfully")
