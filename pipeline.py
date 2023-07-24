from typing import Dict, List, Optional, Union

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
    return datetime.datetime.now().isoformat()


def get_dependencies(filename:str, file_type:str) -> Dict:
    # the file_type is the key name in the dependencies.toml file

    if file_type == 'PIP':
        this_file_type = filetypes.requirements_txt
    elif file_type == 'PIPENV':
        this_file_type = filetypes.pipfile
    elif file_type == 'CONDA':
        this_file_type = filetype.conda_yml

    with open('Pipfile','r') as file:
        pipfile = parse(file.read(), file_type=this_file_type)
    json_pipfile = json.loads(pipfile.json())
    dependencies = [dependency['name'] for dependency in json_pipfile['dependencies']]
    versions = [version(dependency) for dependency in dependencies]
    return {k:v for k,v in zip(dependencies,versions)}


def get_repository_info():
    repo = git.Repo(search_parent_directories=True)
    repository_name = repo.remotes.origin.url.split('.git')[0].split('/')[-1]
    pipeline_version = repo.head.object.hexsha
    pipeline_name = repository_name.replace('_',' ').capitalize()
    return repository_name, pipeline_version, pipeline_name


class Pipeline():
    """
    This class provides methods and internal variable storage to allow the processing of datasets
    """
    
    def __init__(self):
        """
        """
        print ('__INIT__')
        self.console = Console()
        self.config = self.load_config()
        self.kwargs = self.parse_cli_args()
        self.verbose = self.kwargs['verbose']

        print (self.verbose)
        self.release = self.kwargs['release']
        self.logoutput = True
        self.initialise(**self.kwargs)
        pass


    def load_config(self):
        """ 
        Loads the configuration file for the pipline and returns a dictionary of values

        Returns:
            dict: a dictionary of configuration variables 

        """
        config = {}
        files = toml.load('config.toml')

        for file in files['MODULES']:
            this_config = toml.load(f"{files[file]}")
            config[file] = {}
            for k,v in this_config.items():
                config[file][k] = v
        self.console.print("Configuration")
        self.console.print (config)
        return config


    def parse_cli_args(self):
        """
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
        
        self.console.print(kwargs)
        return kwargs


    def initialise(self, **kwargs):
        """
        """
        print ('Initialising')
        self.repository_name, self.pipeline_version, self.pipeline_name = get_repository_info()

        if self.release:
            # switch the output directory to the warehouse
            self.output_path = f"{self.config['PATHS']['WAREHOUSE_PATH']}/{self.config['PATHS']['PIPELINE_WAREHOUSE_FOLDER']}"
            self.log_path = f"{self.config['PATHS']['WAREHOUSE_PATH']}/logs/{self.config['PATHS']['PIPELINE_WAREHOUSE_FOLDER']}"
        else:
            self.output_path = self.config['PATHS']['OUTPUT_PATH']
            self.log_path = self.config['PATHS']['LOG_PATH']

        started_at = get_current_time()
        self.action_logs = {
            'started_at': started_at,
            'steps':{},
            'repository_name': self.repository_name,
            'pipeline_name': self.pipeline_name,
            'pipeline_version': self.pipeline_version
        }
        
        print (self.action_logs)
        self.action_logs['steps']['create_base_folders'] = self.create_base_folder_structure()
        


    def load_steps(self, steps:Dict):
        self.steps = steps
        self.console.print (steps)
        pass

    def get_kwargs(self):
        print (self.kwargs)
        return {k:v for k,v in self.kwargs.items()}


    def run_step(self, step_number:int):
        """
        """
        kwargs = self.get_kwargs()
        kwargs['config'] = self.config 
        action_log_items = []
        if self.steps[str(step_number)]['is_multi']:
            print ('MULTISTEP')
            print (f"multistep parameter : {self.steps[str(step_number)]['multi_param']}")
            print (f"multistep options : {self.steps[str(step_number)]['multi_options']}")
            for item in self.steps[str(step_number)]['multi_options']:
                kwargs[self.steps[str(step_number)]['multi_param']] = item
                action_log_items.append(self.steps[str(step_number)]['function'](**kwargs))
        else:
            print ('SINGLESTEP')
            action_log_items.append(self.steps[str(step_number)]['function'](**kwargs))
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
        print ('Creating base folder structure')
        action_log = {
            'folders_created':[],
            'folders_in_existence':[], 
            'completed_at': None
        }

        folders = ['input','output','tmp','log']

        for folder in folders:
            folder_status = create_folder(folder, self.verbose)
            action_log[folder_status].append(folder)
        return action_log
