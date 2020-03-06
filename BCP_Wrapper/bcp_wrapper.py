from lib.argumentparsing.argumentparser import ArgumentParser
from connection.session import Session
import sys
from api.bcp_runner import BCPRUNNER

if __name__=="__main__":
    argParse = ArgumentParser()
    parameters = argParse.parseArguments(sys.argv,["load_type","log_dir"])

    if "delimiter" not in parameters:
        parameters["delimiter"] = "^"

    if "log_file" not in parameters:
        log_file = "bcpwrapper"
        parameters["log_file"] = log_file
    #Log file throughout the process is "wrapper"

    bcp_runner =  BCPRUNNER(parameters)
    bcp_runner.startTaskFlow_new(parameters)
