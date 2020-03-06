######################################################################################################
#Program : wrapper.py - A python utility to orchestrate processes and wait for the status
#                              and report messages accordingly. It runs all the process such as
#                               shell scripts/ python script and many more if configured
#          Parameters:  --job_name <Job/interface name>
#                       --odate <Order date>
######################################################################################################
from lib.argumentparsing.argumentparser import ArgumentParser
from api.parameterevaluation import ParameterEvaluation
from connection.session import Session
import sys
from datetime import datetime
import os
import settings
import logging

if __name__=="__main__":
    argParse = ArgumentParser()
    parameters = argParse.parseArguments(sys.argv,["sql_file","DB_CONN_NM","log_dir"])

    #Log file throughout the process is "wrapper"
    if "log_file" not in parameters:
        log_file = "sqlexecutor"
        parameters["log_file"] = log_file

    parameterEvaluation = ParameterEvaluation(parameters)
    parameterEvaluation.checkParameters()
    evaluated_renderparameters = parameterEvaluation.evaluate()

    session = Session(evaluated_renderparameters)
    session.executeQuery(queryfile = evaluated_renderparameters["renderd_file"])

    session.sessionClose()
