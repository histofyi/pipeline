from typing import Dict, List, Tuple, Optional, Union

import toml
import json
import os
from pathlib import Path


import argparse

# used to obtain repository and version info - the version is the git commit hash
import git

# used to obtain the version number of an installed library
from importlib.metadata import version

# used to parse requirements files
from dparse import parse, filetypes

# used to obtain system info on the machine running the pipeline
import platform

        
import datetime
import hashlib

from rich.console import Console
console = Console()


def create_folder(folder_path:str, verbose:bool) -> str:
    """
    This function creates the folder path if it does not already exist 
    
    Args:
        folder_path (str): the full path to the folder
        verbose (bool): whether the function should echo to the terminal if this argument is set to True
    
    Returns:
        str: the status of the folder e.g. created, already in existence
    """

    # check if the folder exists
    if not os.path.exists(folder_path):
        # if it doesn't exist, set folder_status to `folders_created`
        folder_status = 'folders_created'
        # create the folder and any parent folders needed
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        # if verbose is set to True, send a message to the terminal
        if verbose:  
            console.print (f"{folder_path} created")  
    else:
        # if it does exist, set folder_status to `folders_in_existence`
        folder_status = 'folders_in_existence'

        # if verbose is set to True, send a message to the terminal
        if verbose:
            console.print (f"{folder_path} already exists")  
    return folder_status


def get_current_time() -> str:
    """
    This function simply returns the current datetime in isoformat

    Returns:
        str: the current datetime as an isoformat string
    """
    return datetime.datetime.now().isoformat()


def get_dependencies(filename:str, file_type:str) -> Dict:
    """
    This function reads a specific dependency file e.g. requirements.txt 
    This file is specified in the dependencies.toml config file
    The fun returns a dictionary of the dependencies amd the currently installed versions

    Args:
        filename (str): the filename for the dependencies file e.g. requirements.txt
        file_type (str): the type of dependencies file. This is the keyname from the toml file and so is always uppercased to show it's a configuration constant
    
    Returns:
        Dict : the dictionary of dependencies and their version numbers
    """

    # the file_type is the key name in the dependencies.toml file
    if file_type == 'PIP':
        this_file_type = filetypes.requirements_txt
    elif file_type == 'PIPENV':
        this_file_type = filetypes.pipfile
    elif file_type == 'CONDA':
        this_file_type = filetypes.conda_yml

    # read the dependencies file
    with open(filename,'r') as filehandle:
        dependency_file = parse(filehandle.read(), file_type=this_file_type)
    
    # create an array of the dependency names
    dependencies = [dependency['name'] for dependency in json.loads(dependency_file.json())['dependencies']]
    # create an array of the versions
    versions = [version(dependency) for dependency in dependencies]
    # return a dictionary of the dependencies and versions
    return {k:v for k,v in zip(dependencies,versions)}


def get_repository_info() -> Union[str,str,str]:
    """
    This function retrieves information about the current git repository
    """
    repo = git.Repo(search_parent_directories=True)
    repository_name = repo.remotes.origin.url.split('.git')[0].split('/')[-1]
    pipeline_version = repo.head.object.hexsha
    pipeline_name = repository_name.replace('_',' ').capitalize()
    return repository_name, pipeline_version, pipeline_name


def get_system_info() -> Union[Dict, None]:
    """
    This function returns information on the computer hardware running this particular instance of the pipeline

    Returns:
        Dict: a dictionary of information on the system
    """
    try:
        info={}
        info['platform']=platform.system()
        info['platform-release']=platform.release()
        info['platform-version']=platform.version()
        info['architecture']=platform.machine()
        info['hostname']=platform.node()
        info['processor']=platform.processor()
    except:
        info = None
    return info


class Pipeline():
    """
    This class provides methods and internal variable storage to allow the processing of datasets
    """
    def __init__(self):
        """
        This function loads some initial setup data from configuration and from command line arguments and initialises the pipeline
        """
        self.console = Console()
        self.config = self.load_config()
        self.kwargs = self.parse_cli_args()
        self.verbose = self.kwargs['verbose']
        self.force = self.kwargs['force']
        self.release = self.kwargs['release']
        self.logoutput = True
        self.initialise(**self.kwargs)
        pass


    def load_config(self) -> Dict:
        """ 
        This function loads the configuration file for the pipline, traverses the contained configuration files and returns a dictionary of values

        Returns:
            Dict: a dictionary of configuration variables 

        """
        config = {}
        files = toml.load('config.toml')

        for file in files['MODULES']:
            this_config = toml.load(f"{files[file]}")
            config[file] = {}
            for k,v in this_config.items():
                config[file][k] = v
        #self.console.print("Configuration")
        #self.console.print (config)
        return config


    def parse_cli_args(self) -> Dict:
        """
        This function loads the configuration dictionary, sets up argparse and parses the command line arguments and returns a dictionary of values
        
        Returns:
            Dict: a dictionary of command line arguments and variables
        """
        arguments = [value for key, value in self.config['ARGPARSE']['ARGUMENTS'].items()]

        parser = argparse.ArgumentParser(prog=self.config['ARGPARSE']['PROG'],
                    description=self.config['ARGPARSE']['DESCRIPTION'],
                    epilog=self.config['ARGPARSE']['EPILOG'])    

        for argument in arguments:
            parser.add_argument(f"-{argument['FLAG']}", 
                f"--{argument['VARIABLE_NAME']}", 
                help=argument['HELP'], 
                action=argument['ACTION'])

        parser.set_defaults(**{argument['VARIABLE_NAME']:argument['DEFAULT'] for argument in arguments})
        kwargs = vars(parser.parse_args())
        return kwargs


    def initialise(self, **kwargs):
        """
        This function sets up the action_logs dictionary which is written out to be a log file, creates the intital set of base folders specifited in config

        It is also responsible for setting output folders
        #TODO think about whether some of these steps would be better suited to be split out to individual functions called from __init__()
        """
        started_at = get_current_time()

        self.repository_name, self.pipeline_version, self.pipeline_name = get_repository_info()

        self.console.print ("")
        self.console.rule(title="Initialising...")
        self.console.print ("")
        self.console.print (f"{self.pipeline_name} (commit sha : {self.pipeline_version}) started at {started_at}")
        self.console.print ("")

        if self.release:
            # switch the output directory to the warehouse
            self.output_path = f"{self.config['PATHS']['WAREHOUSE_PATH']}/{self.config['PATHS']['PIPELINE_WAREHOUSE_FOLDER']}"
            self.log_path = f"{self.config['PATHS']['WAREHOUSE_PATH']}/logs/{self.config['PATHS']['PIPELINE_WAREHOUSE_FOLDER']}"
        else:
            self.output_path = self.config['PATHS']['OUTPUT_PATH']
            self.log_path = self.config['PATHS']['LOG_PATH']

        self.action_logs = {
            'started_at': started_at,
            'steps':{},
            'repository_name': self.repository_name,
            'pipeline_name': self.pipeline_name,
            'pipeline_version': self.pipeline_version,
            'system_info': get_system_info()
        }
        
        step_number = 0
        substep_number = None
        step_title_number = self.get_step_title_number(step_number, substep_number)
        action_output = self.create_base_folder_structure()
        action_log = self.build_action_log(step_number, substep_number, started_at, None, action_output, action_name='create_base_folder_structure')
        self.action_logs['steps'][step_title_number] = action_log
    

    def bundle_dependency_list(self) -> Dict:
        """
        This function iterates through the individual sets of dependencies in the configuration and returns a dictionary of them

        Returns:
            Dict: a dictionary of the dependencies
        """
        dependencies = {}
        for dependency_type in self.config['DEPENDENCIES']:
            dependencies[dependency_type.lower()] = get_dependencies(self.config['DEPENDENCIES'][dependency_type], dependency_type)
        return dependencies


    def finalise(self) -> Dict:
        """
        This function logs the dependencies in the action_logs variable, sets a completed_at time and writes the logfile, and outputs it in the return

        Returns:
            Dict: the action_logs log dictionary
        """
        self.action_logs['dependencies'] = self.bundle_dependency_list()
        self.action_logs['completed_at'] = get_current_time()
        start = datetime.datetime.fromisoformat(self.action_logs['started_at'])
        end = datetime.datetime.fromisoformat(self.action_logs['completed_at'])
        delta = end - start
        datehash = hashlib.sha256(self.action_logs['completed_at'].encode('utf-8')).hexdigest()
        logfilename = f"{self.log_path}/{self.repository_name}-{datehash}.json"
        with open(logfilename, 'w') as logfile:
            logfile.write(json.dumps(self.action_logs, sort_keys=True, indent=4))
        self.console.print(f"Pipeline completed at {self.action_logs['completed_at']} : Execution time : {delta}") 
        return self.action_logs


    def load_steps(self, steps:Dict):
        """
        This function takes the steps dictionary as input which contains information about individual steps including the function name to perform
        """
        self.steps = steps
        self.console.rule(title=f"Running {self.pipeline_name}")
        self.console.print ("")
        self.console.print(f"There are {len(self.steps)} steps to this pipeline")
        for step in self.steps:
            self.console.print(f"{step}. {self.steps[step]['list_item']}")
        self.console.print("")
        pass


    def get_kwargs(self) -> Dict:
        """
        This function provides a clean dictionary of keyword arguments from those stored in the self.kwargs variable

        Returns:
            Dict: a dictionary of keyword arguments
        """
        return {k:v for k,v in self.kwargs.items()}


    def build_action_log(self, step_number:int, substep_number:Union[int, None], started_at:str, additional_args:Union[Dict,None], action_output:Dict, action_name:str=None) -> Dict:
        """
        This function builds an action log to a consistent structure from some inputs and outputs

        Args:
            step_number (int): the step number e.g. 1
            substep_number (int): the substep number, None if not a multipart step
            started_at (str): an isoformatted string for the datetime of when the step was started
            additional_args (Dict): a dictionary of additional arguments for multipart steps
            action_output (Dict): the output of a step

        Returns:
            Dict: the structured action log
        """
        if not action_name:
            action_name = self.steps[step_number]['function'].__name__
        return {
            'step': step_number,
            'substep_number': substep_number,
            'step_action': action_name,
            'started_at':started_at,
            'completed_at': get_current_time(),
            'arguments': additional_args,
            'action_output': action_output
        }


    def get_step_title_number(self, step_number:int, substep_number:Union[int, None]) -> str:
        """
        This structure generates a step title number. If there is no substep number it just returns a string of the step_number

        Otherwise it returns a number of the form step_number.substep_number

        Args:
            step_number (int): the step number e.g. 1
            substep_number (int): the substep number, None if not a multipart step
            additional_args (Dict): a dictionary of additional arguments for multipart steps
            kwargs (Dict): the keyword arguments from the CLI with additional variables such as config

        Returns:
            str : step_title_number, the title number for the step, also used as the key for the step in the combined log
        """
        if substep_number:
            step_title_number = f"{step_number}.{substep_number}"
        else:
            step_title_number = str(step_number)
        return step_title_number


    def run(self, step_number, substep_number, additional_args, **kwargs) -> Tuple[str,Dict]:
        """
        A function to perform the execution of a particular step or substep

        Args:
            step_number (int): the step number e.g. 1
            substep_number (int): the substep number, None if not a multipart step

        Returns:
            str: the step_title_number
            Dict: the action_output of the step
        """

        started_at = get_current_time()
        step_title_number = self.get_step_title_number(step_number, substep_number)
        self.console.rule(title=f"{step_title_number}. {self.steps[step_number]['title_template'].format(**kwargs)}")
        
        step_function = self.steps[str(step_number)]['function']
            
        if not self.steps[str(step_number)]['has_progress']:
            with console.status(f"Running step {step_title_number}..."):
                action_output = step_function(**kwargs)
        else:
            action_output = step_function(**kwargs)
        console.print(f"Step {step_title_number} completed.\nOutput:")
        console.print (action_output)
        return step_title_number, self.build_action_log(step_number, substep_number, started_at, additional_args, action_output)


    def run_step(self, step_number:int) -> List:
        """
        This function runs the function associated with the step number, using the paramaters from the keyword arguments.

        In the case of multi part steps, where the function is repeated with different parameters, these additional parameters come from the muli_param and multi_options parameters in the step

        An example of this is processing different loci

        Returns:
            List: a list of the outputs of the step
        """
        # TODO refactor to bring in some of the complexity from the allele pipeline implementation of Pipeline
        kwargs = self.get_kwargs()
        kwargs['config'] = self.config 
        kwargs['output_path'] = self.output_path
        kwargs['console'] = self.console

        action_log_items = []
        if self.steps[str(step_number)]['is_multi']:
            # TODO multistep action logging, refactor all of this when you have a real multistep function to work with
            print ('MULTISTEP')
            print (f"multistep parameter : {self.steps[str(step_number)]['multi_param']}")
            print (f"multistep options : {self.steps[str(step_number)]['multi_options']}")
            for item in self.steps[str(step_number)]['multi_options']:
                kwargs[self.steps[str(step_number)]['multi_param']] = item
                action_log = self.steps[str(step_number)]['function'](**kwargs)
                action_log_items.append(action_log)
        else:
            substep_number = None
            additional_args = None
            step_title_number, action_log = self.run(step_number, substep_number, additional_args, **kwargs)
            self.action_logs['steps'][step_title_number] = action_log
            action_log_items.append(action_log)  
        return action_log_items


    def say_hello(self):
        print ('hello there from Pipeline new version')


    def create_base_folder_structure(self) -> Dict:
        """
        This function creates the folder structure for the outputs of the pipeline

        Returns:
            Dict : a dictionary of actions performed 
        """
        # default action log file for this step
        action_log = {
            'folders_created':[],
            'folders_in_existence':[], 
            'completed_at': None
        }

        folders = ['input','output','tmp','log']
        self.console.print ('Creating base folder structure')
        for folder in folders:
            folder_status = create_folder(folder, self.verbose)
            action_log[folder_status].append(folder)
        return action_log
