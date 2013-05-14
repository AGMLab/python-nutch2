#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright 2013, Ahmet Emre Aladağ, AGMLAB

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import subprocess
import logging
import time
from collections import deque
from nutch2.runner import settings

logging.basicConfig()


class NutchJob:
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def get_output(self):
        return self.process.stdout.read()

    def get_pid(self):
        return self.process.pid

    def terminate(self):
        self.process.terminate()


class NutchRunner:
    def __init__(self):
        self.log = logging.getLogger("NutchRunner")
        self.log.setLevel("DEBUG")
        self.cg = NutchCommandGenerator()
        os.chdir(settings.NUTCH_BIN_DIR)
        self.active_jobs = deque()

    def inject_seed_dir(self, seed_dir, **kwargs):
        command = self.cg.inject_seed_dir(seed_dir, **kwargs)

        p = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
        job = NutchJob(command=command, type="inject", time=time.time(), process=p)
        self.active_jobs.append(job)

    def generate(self, **kwargs):
        command = self.cg.generate(**kwargs)
        p = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
        job = NutchJob(command=command, type="generate", time=time.time(), process=p)
        self.active_jobs.append(job)

    def fetch(self, batch_id, **kwargs):
        command = self.cg.fetch(batch_id, **kwargs)
        p = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
        job = NutchJob(command=command, type="fetch", time=time.time(), process=p)
        self.active_jobs.append(job)

    def parse(self, batch_id, **kwargs):
        command = self.cg.parse(batch_id, **kwargs)
        p = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
        job = NutchJob(command=command, type="parse", time=time.time(), process=p)
        self.active_jobs.append(job)

    def update(self, batch_id, **kwargs):
        command = self.cg.update(batch_id, **kwargs)
        p = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
        job = NutchJob(command=command, type="update", time=time.time(), process=p)
        self.active_jobs.append(job)


    """def execute(self, command):
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            # Poll process for new output until finished
            while True:
                nextline = process.stdout.readline()
                if nextline == '' and not process.poll():
                    break
                sys.stdout.write(nextline)
                sys.stdout.flush()

            output = process.communicate()[0]
            exitCode = process.returncode

            if exitCode == 0:
                return output
            else:
                raise Exception(command, exitCode, output)"""


class NutchCommandGenerator:
    """
    Generates Nutch Job commands.
    """

    def __init__(self):
        self.log = logging.getLogger("NutchCommandGenerator")
        self.log.setLevel("DEBUG")

    def log_cmd(self, cmd):
        self.log.debug("Command: {0}".format(cmd))

    def __kwarg_str(self, kwargs):
        """
        Converts the keyword arguments to command line form
        threads=3, topN=4 => -threads 3 -topN 4

        """
        no_value_kwargs = ['resume', 'force', 'all']

        keys = sorted(kwargs.keys())
        l = []
        for key in keys:
            value = kwargs[key]
            # if we don't have value for the kwarg, we set the value to none.
            if not value:
                continue
            if key in no_value_kwargs:
                value = ""
            # If the value is True or False, convert it to lower case string true, false.
            elif type(value) == bool:
                value = str(value).lower()
                # add them to the list in -kwarg value form.
            l.append("-{0} {1}".format(key, value).strip())
            # return the kwargs as a whole string.
        return " ".join(l)

    def inject_seed_dir(self, seed_dir, **kwargs):
        """
        Injects the given seed directory.
        Usage: ./nutch inject <url_dir> [-crawlId <id>]
        """
        kwarg_str = self.__kwarg_str(kwargs)
        command = "./nutch inject {0} {1}".format(seed_dir, kwarg_str).strip()

        self.log_cmd(command)
        return command

    def generate(self, **kwargs):
        """
        Generates the URLs to fetch.
        ./nutch generate [-topN N] [-crawlId id] [-noFilter] [-noNorm] [-adddays numDays]
        """
        kwarg_str = self.__kwarg_str(kwargs)
        command = "./nutch generate {0}".format(kwarg_str).strip()
        self.log_cmd(command)
        return command

    def fetch(self, batch_id, **kwargs):
        """
        Fetches the given batch id.
        """
        kwarg_str = self.__kwarg_str(kwargs)
        command = "./nutch fetch {0} {1}".format(batch_id, kwarg_str).strip()
        self.log_cmd(command)
        return command

    def parse(self, batch_id, **kwargs):
        """
        Parses the given batch id.
        """
        kwarg_str = self.__kwarg_str(kwargs)
        command = "./nutch parse {0} {1}".format(batch_id, kwarg_str).strip()
        self.log_cmd(command)
        return command

    def update(self, batch_id, **kwargs):
        """
        Updates the db.
        """
        kwarg_str = self.__kwarg_str(kwargs)
        command = "./nutch update {0} {1}".format(batch_id, kwarg_str).strip()
        self.log_cmd(command)
        return command