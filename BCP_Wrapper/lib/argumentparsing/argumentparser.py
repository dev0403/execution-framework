import argparse
import datetime

class ArgumentParser:
    """
        Parse Variable Arguments and seperate out the parameters used to trigger processes
        sequentially
    """
    def __init__(self):
        pass

    def parseArguments(self,all_argv_list,req_args_list):

        parser = argparse.ArgumentParser()
        parser._action_groups.pop()
        required = parser.add_argument_group('required arguments')
        optional = parser.add_argument_group('other arguments')

        for i in range(0,len(req_args_list)):
            required.add_argument("--"+req_args_list[i], required=True)

        #required.add_argument('-j','--job_name', required=True)
        #optional.add_argument('-i','--input')

        args=parser.parse_known_args(all_argv_list)
        known_args = vars(args[0])
        unknown_argsl = args[1]
        unknown_args={unknown_argsl[i+1]: unknown_argsl[i+2] for i in range(0, len(unknown_argsl)-1, 2)} # list to dic conv

        print ("known_args:"+ str(known_args))
        print("unknown_args:"+ str(unknown_args))
        for word in unknown_args:
            optional.add_argument(word) #, nargs='+'
        args=vars(parser.parse_args())

        return args
