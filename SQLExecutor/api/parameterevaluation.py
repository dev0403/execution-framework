import settings
import os
import sys
import subprocess
import json
from jinja2 import Template
import re
from datetime import datetime
from lib.logger.logger import logger

class ParameterEvaluation:
    """
        Evaluates the Global parameters and parameters of Parameters
    """
    def __init__(self,parameters):
        self.parameters = parameters

        self.logger = logger(self.parameters["log_dir"],self.parameters["log_file"],'SQLExecutor:'+self.__class__.__name__).get()
        self.logger.info("ParameterEvaluation API Started")

    def checkParameters(self):
        self.logger.info("Check for sql file existence")
        if not os.path.isfile(self.parameters["sql_file"]):
            self.logger.error("SQL file doesn't exist in the specified location. Please check sql file")
            sys.exit(1)

        self.logger.info("Reading contents of sql file : %s" % self.parameters["sql_file"])
        try:
            fp = open(self.parameters["sql_file"])
            sql_content = fp.read()
            fp.close()
            patterns = [ pat.replace("{","").replace("}","") for pat in re.findall('{{.*?}}',sql_content,re.MULTILINE) ]
            self.logger.info("Total Variables found in the file : %s" % json.dumps(patterns))

            parameter_set = set(self.parameters.keys())
            patterns_set = set(patterns)

            difference_set = patterns_set.difference(parameter_set)
            if len(difference_set) != 0:
                self.logger.error("The values for the below patterns/parameters in sql file not passed")
                self.logger.error("Parameters for which values not passed are : %s " % str(difference_set))
                sys.exit(1)
            self.logger.info("All the parameters in sql file are passed as parameters")


        except Exception as err:
            self.logger.error("Unable to read file. Please check stacktrace")
            self.logger.error("Error stacktrace : %s" % err)
            sys.exit(1)

    def evaluate(self):
        self.logger.info("Replacing parameters started.")
        try:
            fp = open(self.parameters["sql_file"],"r")
            sql_content = fp.read()
            fp.close()

            template = Template(sql_content)
            rendered_sqlcontent = template.render(**self.parameters)

            file_tmstamp = datetime.now().strftime('%Y%m%d%H%m%S%f')
            evaluated_sqlfile = os.path.join(self.parameters["log_dir"],"tmp_"+file_tmstamp+".sql")
            self.logger.info("Temporary sql rendered file : %s." % evaluated_sqlfile)

            with open(evaluated_sqlfile,"w+") as fp:
                fp.writelines(rendered_sqlcontent)
            self.logger.info("Contents of rendered file : %s" % rendered_sqlcontent)

            evaluated_renderparameters = self.parameters
            evaluated_renderparameters["renderd_file"] = evaluated_sqlfile
        except Exception as err:
            self.logger.error("Error in Reading or Writing temporary sql file")
            self.logger.error("Error stacktrace : %s " % err)
            sys.exit(1)

        return evaluated_renderparameters
