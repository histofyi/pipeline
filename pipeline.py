from typing import Dict, List, Optional, Union

import toml
import json

import argparse

import git
from importlib.metadata import version
from dparse import parse, filetypes
        
import datetime
import hashlib

from rich.console import Console


class Pipeline():
    """
    This class provides methods and internal variable storage to allow the processing of datasets
    """
    
    def __init__(self):
        """
        """
        self.console = Console()
        self.config = self.load_config()
        self.kwargs = self.parse_cli_args()
        pass


    def load_config(self, verbose:bool=True):
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
        if verbose:
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
        print (kwargs)


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
        
        action_log_items = []
        if self.steps[str(step_number)]['is_multi']:
            print ('MULTISTEP')
            print (f"multistep parameter : {self.steps[str(step_number)]['multi_param']}")
            print (f"multistep options : {self.steps[str(step_number)]['multi_options']}")
            for item in self.steps[str(step_number)]['multi_options']:
                kwargs = self.get_kwargs()
                kwargs[self.steps[str(step_number)]['multi_param']] = item
                action_log_items.append(self.steps[str(step_number)]['function'](**kwargs))
        else:
            print ('SINGLESTEP')
            kwargs = self.get_kwargs()
            action_log_items.append(self.steps[str(step_number)]['function'](**kwargs))
        return action_log_items


    def say_hello(self):
        print ('hello there from Pipeline new version')