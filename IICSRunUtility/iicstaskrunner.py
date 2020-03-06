######################################################################################################
#Program : AllianceRunIICS.py - A python utility to orchestrate IICS Jobs and wait for the status
#                              and report messages accordingly. It uses internally either REST APIs
#                               or RunAJob utility to trigger IICS mappings or tasks
#          Parameters:  --job_name <Job/interface name>
#                       --task_name <Task Name>
#                       --task_type <MTT/DSS>
#                       --iics_fp <Folder path>
#                       --odate <Order date>
#                       --parm_file <parameter file name>
#                       --[p_<parameter_names> <parameter_values>]*
######################################################################################################
from lib.argumentparsing.argumentparser import ArgumentParser
from api.parmfilecreation import ParmFileCreation
from api.commandrunner import CommandRunner
import sys

if __name__=="__main__":

    #Parsing arguments for triggering IICS Jobs
    argParse = ArgumentParser()
    parameters = argParse.parseArguments(sys.argv,["job_name","task_name","log_dir","task_type"])

    if "iics_fp" not in parameters:
        print("Assigning default IICS folder path: %s" % ("default"))
        parameters["iics_fp"] = "default"

    #Log file throughout the process is "wrapper"
    if "log_file" not in parameters:
        log_file = "iicstaskrunner"
        parameters["log_file"] = log_file

    parmFileCreation = ParmFileCreation(parameters)
    parmFileCreation.prepareParmFile()
    # #parmFileCreation.archiveParmFile()
    #
    commandRunner = CommandRunner(parameters)
    commandRunner.runCmd()
