######################################################################################################
#Program : wrapper.py - A python utility to orchestrate processes and wait for the status
#                              and report messages accordingly. It runs all the process such as
#                               shell scripts/ python script and many more if configured
#          Parameters:  --job_name <Job/interface name>
#                       --odate <Order date>
######################################################################################################
from lib.argumentparsing.argumentparser import ArgumentParser
from api.commandrunner import CommandRunner
from api.parameterevaluation import ParameterEvaluation
from connection.session import Session
import sys
from datetime import datetime
import os
import settings
import logging


if __name__=="__main__":
    argParse = ArgumentParser()
    parameters = argParse.parseArguments(sys.argv,["job_name","odate"])

    log_time = datetime.now().strftime('%Y%m%d%H%M%S%f')
    log_dir = os.path.join(settings.LOG_DIR,"wrapper_"+parameters["job_name"]+"_"+parameters["odate"]+"_"+log_time)
    log_file = "wrapper"

    print("The wrapper log directory is : %s" % (log_dir))
    print("All the log files for different processes are going to be in this directory")

    parameters["log_dir"] = log_dir
    parameters["log_file"] = log_file
    parameters["RUN_ID"] = log_time
    parameters["ENV_NM"] = settings.ENV_NM


    session = Session(parameters)
    audit_entries = session.executeQuery(settings.JOB_FLOW_QRY,[parameters['job_name']])

    parameterEvaluation = ParameterEvaluation(parameters,audit_entries)
    # audit_entries = parameterEvaluation.evaluate()
    audit_entries = parameterEvaluation.evaluate_parmorder()

    commandRunner = CommandRunner(parameters,audit_entries,session)
    commandRunner.startTaskFlow()

    session.sessionClose()
