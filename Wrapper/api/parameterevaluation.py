import settings
import os
import sys
import subprocess
import json
from jinja2 import Template
from lib.logger.logger import logger

class ParameterEvaluation:
    """
        Evaluates the Global parameters and parameters of Parameters
    """
    def __init__(self,parameters,audit_entires):
        self.parameters = parameters
        self.audit_entires = audit_entires
        self.process_cmd_txt = []

        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],self.__class__.__name__).get()
        self.logger.info("ParameterEvaluation API Started")

    def evaluate(self):
        self.logger.info("Filtering out Global parameters and forming a dictionary of parameter names and values")
        global_entries = {  it["PARM_NM"]:it["PARM_VAL"] for it in self.audit_entires if it["STEP_NM"]=="GLOBAL"}
        self.logger.info("Global entries are %s" % json.dumps(global_entries))

        self.logger.info("Filtering out task parameters")
        local_entries = [it for it in self.audit_entires if it["STEP_NM"]!="GLOBAL"]
        self.logger.info("List of local entries: %s" % json.dumps(local_entries))

        self.logger.info("Evaluating and replacing parameters")
        template = Template(json.dumps(local_entries))
        evaluated_entries = json.loads(template.render({**global_entries,**self.parameters}))
        self.logger.info("Evaluated entires : %s" % json.dumps(evaluated_entries))

        return evaluated_entries

    def evaluate_parmorder(self):
        self.logger.info("Filtering out Global parameters and forming a dictionary of parameter names and values")
        global_entries = {  it["PARM_NM"]:it["PARM_VAL"] for it in self.audit_entires if it["STEP_NM"]=="GLOBAL"}
        self.logger.info("Global entries are %s" % json.dumps(global_entries))

        self.logger.info("Filtering out task parameters")
        local_entries = [it for it in self.audit_entires if it["STEP_NM"]!="GLOBAL"]
        self.logger.info("List of local entries: %s" % json.dumps(local_entries))

        self.logger.info("Evaluating and replacing parameters")
        evaluated_incremental_entries = []
        prev_step_id = -99
        parameters_incremental={}
        for entry in local_entries:
            if entry["STEP_ID"] != prev_step_id:
                parameters_incremental = {**global_entries,**self.parameters}
            template = Template(json.dumps(entry))
            evaluated_entry = json.loads(template.render({**parameters_incremental}))
            evaluated_incremental_entries.append(evaluated_entry)
            parameters_incremental[entry["PARM_NM"]]=evaluated_entry["PARM_VAL"]
            prev_step_id=entry["STEP_ID"]

        evaluated_entries = evaluated_incremental_entries
        self.logger.info("Evaluated entires : %s" % json.dumps(evaluated_entries))

        return evaluated_entries
