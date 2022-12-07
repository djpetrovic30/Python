#!/usr/bin/python3

import sys
import argparse
import json
import os
import re
import subprocess
import uuid
from dotenv import load_dotenv

ARG_TYPES = {
  'integer': int,
  'float': float,
  'boolean': bool,
  'stringOptions': str,
  'stringPattern': str
}

def get_arguments(pipeline_dir, args_list):
    json_finder = argparse.ArgumentParser(add_help=False)
    json_finder.add_argument('--tier', type=str, required=True, help='regulatory tier', choices=['ruo', 'gcp'])
    temp_args, _ = json_finder.parse_known_args(args_list) # avoid errors from unknown nextflow params
    json_tier = temp_args.tier
    parser = argparse.ArgumentParser()
    parser.add_argument('-profile', type=_check_profile, required=False, help='Nextflow profile', default="standard,cc")
    with open(f'{pipeline_dir}/param_jsons/{json_tier}_params.json', 'r') as f:
        j = json.load(f)
        if not 'tier' in j.keys():
            raise Exception('no tier in the json, this will not work')
        for k,v in j["definitions"].items():
            is_required = 'required' in v
            if 'boolean' not in v['type']:
                parser.add_argument(f'--{k}', type=ARG_TYPES[v['type']], required=is_required, help=v['description'])
            else:
                # mirror Nextflow behavior by allowing booleans to be --flag or --flag [true/false]
                parser.add_argument(f'--{k}', nargs='?', default=v['default_value'], const='true', help=v['description'])
    args = parser.parse_args(args_list) # parse everything including --tier
    return args

def _check_profile(prof):
    regex = re.compile("^[a-zA-Z0-9_,]*$")
    if regex.match(prof):
        return str(prof)
    else:
        raise argparse.ArgumentTypeError("the -profile argument can be a word character or comma")

class PipelineCaller:

    def __init__(self, pipeline_dir, pipeline_name, args, arg_strings):
        self.pipeline_dir = pipeline_dir
        self.exe = self.pipeline_dir + '/main_bard.nf'
        self.args = args
        self.arg_strings = arg_strings
        self.nxf_work = None
        self.ea_dc_references = None
        self.ea_dc_data_src = None
        self.nextflow_log = None
        self.syslog = None
        self.nf_runner_config = "/opt/nf-runner/nf-runner.config"
        self.nf_command = None
        self.uuid = pipeline_name + "_" + uuid.uuid4().hex

    load_dotenv()

    def _set_ea_dc_data_src(self):
        self.ea_dc_data_src = os.getenv('EA_DC_DATA_SRC')

        if self.ea_dc_data_src is None:
            raise Exception("The EA_DC_DATA_SRC environment variable is required to set the references directory")

    def _set_nxf_work(self):
        ea_nextflow_work_dir = os.getenv('EA_NEXTFLOW_WORK_DIR')
        ea_dc_data_src = os.getenv('EA_DC_DATA_SRC')

        if ea_nextflow_work_dir:
            self.nxf_work = ea_nextflow_work_dir
        elif ea_dc_data_src:
            self.nxf_work = f"{ea_dc_data_src}/scratch/nextflow/work/"
        else:
            raise Exception("The EA_NEXTFLOW_WORK_DIR or EA_DC_DATA_SRC environment variable is needed to set the work directory scratch space")

    def _is_nf_runner(self):
        if hasattr(self.args, "profile"):
            profile = getattr(self.args, "profile")
            regex = re.compile(".*nf_runner.*")
            if regex.match(profile):
                return(True)
        return False

    def _set_nextflow_log(self):
        ea_nextflow_log_file = os.getenv('EA_NEXTFLOW_LOG_FILE') # note that this env var is always set the ea-nextflow-base container

        if self._is_nf_runner():
            if ea_nextflow_log_file:
                self.nextflow_log = ea_nextflow_log_file
            else:
                raise Exception("When using the NF Runner profile the env variable EA_NEXTFLOW_LOG_FILE must be set")
        else:
            if hasattr(self.args, 'out_dir') and getattr(self.args, 'out_dir'):
                self.nextflow_log = f"{getattr(self.args, 'out_dir')}/nextflow.log"
            else:
                raise Exception("The parameter --out_dir must be set to set the log file location")

    def _set_syslog(self):
        ea_nextflow_syslog = os.getenv('EA_NEXTFLOW_SYSLOG')

        if ea_nextflow_syslog:
            self.syslog = ea_nextflow_syslog
        else:
            self.syslog = ""

    def _set_ea_dc_references(self):
        ea_dc_references_env = os.getenv('EA_DC_REFERENCES')
        ea_dc_data_src = os.getenv('EA_DC_DATA_SRC')

        if ea_dc_references_env:
            self.ea_dc_references = ea_dc_references_env
        elif ea_dc_data_src:
            self.ea_dc_references = f"{ea_dc_data_src}/references/combined"
        else:
            raise Exception("The EA_DC_REFERENCES or EA_DC_DATA_SRC environment variable is needed to set the references directory")

    def _build_nf_command(self):
        cmd_args = ' '.join(self.arg_strings)        
        command_elements = [
            f"nextflow",
            f"-log {self.nextflow_log}",
            f"-syslog {self.syslog}",
            f"run {self.exe}",
            f"-name {self.uuid}",
            f"-c {self.nf_runner_config}",
            f"{cmd_args}"
        ]
        if '-profile' not in self.arg_strings:
            command_elements.append(f"-profile {getattr(self.args, 'profile')}") # if no profile at command line, make a string using its default value
        self.nf_command = " ".join(command_elements)
    
    def _cleanup(self):
        # if this is an ADMS run do nothing
        if not self._is_nf_runner():
            command = f"nextflow clean -quiet {self.uuid} -f"
            proc = subprocess.run(command, cwd=self.pipeline_dir, shell=True, env={**os.environ}, capture_output=True, encoding='utf-8')
            if proc.stdout:
                print(proc.stdout)
            if proc.stderr:
                print(proc.stderr)
            if proc.returncode != 0:
                raise Exception(f'BARD succeeded but cleanup failed. Consider manually cleaning {self.nxf_work} if it is not a scratch space, and escalate this to the pipeline development team if issues with `nextflow clean` persist')

    def _execute_nf_command(self):
        envvars = {'NXF_WORK': self.nxf_work, 'EA_DC_REFERENCES':self.ea_dc_references}
        
        proc = subprocess.run(self.nf_command, cwd=self.pipeline_dir, shell=True, env={**os.environ, **envvars}, capture_output=True, encoding='utf-8')
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr)
        if proc.returncode != 0:
            raise Exception("BARD nextflow run command did not exit with exit code 0")

    def _print_nf_command(self):
        print(self.nf_command)

    def run(self):
        # build the command string
        self._set_ea_dc_data_src()
        #self._set_nextflow_log()
        #self._set_syslog()
        self._set_nxf_work()
        #self._set_ea_dc_references()
        self._build_nf_command()
        print(self.nf_command)

        self._print_nf_command()
        # run the command
        #self._execute_nf_command()
        #self._cleanup()

def main():
    pipeline_dir = '/mnt/c/Users/abc/Desktop/Python_scripts/opt/pipeline'
    pipeline_name = 'BARD'
    args = get_arguments(pipeline_dir, sys.argv[1:])
    bard_caller = PipelineCaller(pipeline_dir, pipeline_name, args, sys.argv[1:])
    bard_caller.run()

if __name__=="__main__":
    main()
