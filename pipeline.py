from typing import Dict, List, Optional, Union

import toml
import json

import argparse

import git
from importlib.metadata import version
from dparse import parse, filetypes
        
import datetime
import hashlib


class Pipeline():
    """
    """
    
    def __init__(self):
        """
        """
        self.config = self.parse_config()
        pass


    def parse_config(self):
        """
        """
        print ('parsing config files')
        return {}


    def parse_cli_args(self):
        """
        """
        argparse_config = toml.load('arguments.toml')

        arguments = [value for key, value in argparse_config['arguments'].items()]

        parser = argparse.ArgumentParser(prog=argparse_config['prog'],
                    description=argparse_config['description'],
                    epilog=argparse_config['epilog'])    

        for argument in arguments:
            parser.add_argument(f"-{argument['flag']}", 
                f"--{argument['variable_name']}", 
                help=argument['variable_name'], 
                action=argument['action'])

        parser.set_defaults(**{argument['variable_name']:argument['default'] for argument in arguments})
        
        kwargs = vars(parser.parse_args())
        
        return kwargs


    def initialise(self, **kwargs):
        """
        """
        print (kwargs)


    def run(self):
        """
        """
        self.hello()


    def say_hello(self):
        print ('hello there from Pipeline new version')