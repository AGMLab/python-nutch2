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
import subprocess


class HadoopJob:
    """
    Holds information about the job.
    """

    def __init__(self, *args):
        self.job_id, \
        self.state, \
        self.start_time, \
        self.user_name, \
        self.priority, \
        self.scheduling_info = args

        self.log = logging.getLogger("Job")
        self.log.setLevel("DEBUG")

    def stop(self):
        self.log.debug("Stopping %s..." % self)

    def restart(self):
        self.log.debug("Restarting %s..." % self)
        self.stop()


    def __unicode__(self):
        return "%(job_id)s by %(user_name)s" % self.__dict__

    def __repr__(self):
        return self.__unicode__()


class HadoopParser:
    def __init__(self):
        pass

    @staticmethod
    def parse_job_list(job_list_str):
        pass


class HadoopJobMonitor:
    """
    Monitors the Nutch Jobs and provides information about them.
    """

    def __init__(self):
        # Populate self.job_list
        self.fetch_hadoop_job_info()
        # Generate Job Names list
        self.job_names = [job.__unicode__() for job in self.job_list]
        # Job Name-> Job matching
        self.job_dict = dict(zip(self.job_names, self.job_list))

    def get_job_with_name(self, name):
        """
        Returns the job object with name name
        """
        return self.job_dict.get(name)

    def fetch_hadoop_job_info(self):

        p = subprocess.Popen(["hadoop", "job", "-list"], stdout=subprocess.PIPE)
        out, err = p.communicate()
        lines = out.split("\n")
        num_jobs = int(lines[0].split()[0])
        job_list = []
        if num_jobs:
            # skip header line.
            for line in lines[2:]:
                if not line:
                    continue
                j = HadoopJob(*line.split())
                job_list.append(j)
        self.num_jobs = num_jobs
        self.job_list = job_list