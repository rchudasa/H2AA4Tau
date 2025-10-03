import subprocess
import sys
import json
from pathlib import Path
import os
from copy import deepcopy
import logging

logger = logging.getLogger(__name__)


class LXPlusVanillaSubmitter:
    """
    A class for submitting jobs on the CERN's LXPlus cluster using HTCondor,
    one job per file in a sample list of an analysis.
    All jobs for a given sample are submitted to the same cluster.
    The constructor creates a directory .higgs_dna_vanilla_lxplus if it does not exist
    and another one called .higgs_dna_vanilla_lxplus/<analysis_name>.
    The date and time (YMD_HMS) is appended to the end of <analysis_name>
    to avoid overwriting previous submissions.
    Inside this directory two subdirectories called <inputs> and <jobs> will be created.
    In the former the split JSON files will be stored, in the latter the HTCondor
    job files will be stored. To send each job to a separate cluster then set
    cluster_per_sample=False in the class constructor.
    Note: This submission method works well on lxplus, but has also been tested on
    other infrastructures like RWTH Aachen because it does not rely on any
    specific EOS or HTCondor features. It is simply a generic HTCondor submitter
    that relies on an automated submission of automatically generated sh and sub files.
    """

    def __init__(
        self,
        analysis_name,
        analysis_dict,
        original_analysis_path,
        sample_dict,
        args_string,
        queue="longlunch",
        memory="10GB",
        files_per_job=1,
        cluster_per_sample=True,
        max_materialize=None,
    ):
        # Basic assignments
        self.analysis_name = f"{analysis_name}_{subprocess.getoutput('date +%Y%m%d_%H%M%S')}"
        self.analysis_dict = analysis_dict
        self.sample_dict = sample_dict
        self.args_string = args_string
        self.queue = queue
        self.memory = memory
        self.cluster_per_sample = cluster_per_sample

        # parse memory string (e.g. "10GB", "8000MB") into MB integer
        mem_str = str(self.memory).upper()
        if mem_str.endswith("GB"):
            self.memory_mb = int(mem_str[:-2]) * 1024  # binary convention
        elif mem_str.endswith("MB"):
            self.memory_mb = int(mem_str[:-2])
        else:
            self.memory_mb = int(mem_str)

        # Early parse of raw max_materialize
        self._max_materialize_raw = max_materialize
        self._parse_max_materialize()

        # Setup directories
        self.current_dir = os.getcwd()
        self.base_dir = os.path.join(self.current_dir, ".higgs_dna_vanilla_lxplus")
        self.analysis_dir = os.path.join(self.base_dir, self.analysis_name)
        self.input_dir = os.path.join(self.analysis_dir, "inputs")
        Path(self.input_dir).mkdir(parents=True, exist_ok=True)

        self.jobs_dir = os.path.join(self.analysis_dir, "jobs")
        Path(self.jobs_dir).mkdir(parents=True, exist_ok=True)
        self.job_files = []

        # Create input JSON files and job submission files
        self._make_input_json(files_per_job)
        self._write_sub_files(original_analysis_path)

    def _parse_max_materialize(self):
        raw = self._max_materialize_raw
        if raw is not None:
            try:
                val = float(raw)
            except ValueError:
                raise ValueError(f"Invalid --vlxp-max-materialize: {raw!r}")
            if val <= 0:
                raise ValueError(f"--vlxp-max-materialize must be > 0, got {val}")
            self._max_materialize_val = val
        else:
            self._max_materialize_val = None

    def _compute_max_materialize(self, n_jobs):
        val = self._max_materialize_val
        if val is None:
            return None
        if val <= 1:
            return max(1, int(val * n_jobs))
        return min(n_jobs, int(val))

    def _make_input_json(self, files_per_job):
        self.json_analysis_files = {}
        self.json_sample_files = {}
        for sample in self.sample_dict:
            self.json_analysis_files[sample] = []
            self.json_sample_files[sample] = []
            for i in range(0, len(self.sample_dict[sample]), files_per_job):
                sub_files = self.sample_dict[sample][i : i + files_per_job]
                sample_json = os.path.join(self.input_dir, f"{sample}-{i}.json")
                with open(sample_json, "w") as jf:
                    json.dump({sample: sub_files}, jf, indent=4)
                self.json_sample_files[sample].append(sample_json)

                analysis_json = os.path.join(self.input_dir, f"AN-{sample}-{i}.json")
                an_to_dump = deepcopy(self.analysis_dict)
                an_to_dump["samplejson"] = sample_json
                with open(analysis_json, "w") as jf:
                    json.dump(an_to_dump, jf, indent=4)
                self.json_analysis_files[sample].append(analysis_json)

    def _write_sub_files(self, original_analysis_path):
        # Proxy setup (only needed if cluster_per_sample=True)
        try:
            _, _ = subprocess.getstatusoutput("voms-proxy-info -e --valid 5:00")
        except:
            logger.exception("voms proxy not found or validity less that 5 hours")
            raise
        try:
            _, out = subprocess.getstatusoutput("voms-proxy-info -p")
            proxy = out.strip().split("\n")[-1]
        except:
            logger.exception("Unable to voms proxy")
            raise

        if self.cluster_per_sample:
            for sample, analysis_list in self.json_analysis_files.items():
                base_name = f"AN-{sample}"
                jobs_dir = os.path.realpath(self.jobs_dir).replace("/eos/home-", "/eos/user/")
                if "/eos/user" in jobs_dir:
                    job_dir = "root://eosuser.cern.ch/" + jobs_dir
                elif "/eos/cms" in jobs_dir:
                    job_dir = "root://eoscms.cern.ch/" + jobs_dir
                else:
                    job_dir = jobs_dir
                n_jobs = len(analysis_list)
                max_mat = self._compute_max_materialize(n_jobs)

                # Executable script
                job_file_executable = os.path.join(jobs_dir, f"{base_name}.sh")
                with open(job_file_executable, "w") as exe:
                    exe.write("#!/bin/sh\n")
                    exe.write(f"export X509_USER_PROXY={proxy}\n")
                    for idx, json_file in enumerate(analysis_list):
                        args_ = self.args_string.replace(original_analysis_path, json_file).replace(" vanilla_lxplus", " iterative")
                        exe.write(f"if [ $1 -eq {idx} ]; then\n")
                        exe.write(f"    /usr/bin/env {sys.prefix}/bin/run_analysis.py {args_} || exit 107\n")
                        exe.write("    exit 0\n")
                        exe.write("fi\n")
                os.chmod(job_file_executable, 0o775)

                # Submit file
                job_file_submit = os.path.join(jobs_dir, f"{base_name}.sub")
                with open(job_file_submit, "w") as sub:
                    sub.write(f"executable = {job_file_executable}\n")
                    sub.write("arguments = $(ProcId)\n")
                    sub.write(f"output = {job_dir}/{base_name}.$(ClusterId).$(ProcId).out\n")
                    sub.write(f"error = {job_dir}/{base_name}.$(ClusterId).$(ProcId).err\n")
                    sub.write(f"log = {job_dir}/{base_name}.$(ClusterId).log\n")
                    sub.write(f"output_destination = {job_dir}\n")
                    sub.write(f"RequestMemory = ifThenElse(isUndefined(MemoryUsage), {self.memory_mb}, int(MemoryUsage * 1.1))\n")
                    sub.write("getenv = True\n")
                    sub.write(f'+JobFlavour = "{self.queue}"\n')
                    sub.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
                    sub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n')
                    sub.write('periodic_hold = ( JobStatus == 7 ) && ((CurrentTime - EnteredCurrentStatus) > 300)\n')
                    sub.write('periodic_hold_reason = "Job stuck suspended >5m — requeueing"\n')
                    sub.write('periodic_release = ( JobStatus == 5 ) && ((CurrentTime - EnteredCurrentStatus) > 60)\n')
                    sub.write('max_retries = 3\n')
                    if max_mat is not None:
                        sub.write(f"max_materialize = {max_mat}\n")
                    sub.write('requirements = Machine =!= LastRemoteHost\n')
                    sub.write(f"queue {n_jobs}\n")
                self.job_files.append(job_file_submit)
        else:
            for sample, analysis_list in self.json_analysis_files.items():
                for analysis_json in analysis_list:
                    base_name = os.path.splitext(os.path.basename(analysis_json))[0]
                    jobs_dir = os.path.realpath(self.jobs_dir).replace("/eos/home-", "/eos/user/")
                    if "/eos/user" in jobs_dir:
                        job_dir = "root://eosuser.cern.ch/" + jobs_dir
                    elif "/eos/cms" in jobs_dir:
                        job_dir = "root://eoscms.cern.ch/" + jobs_dir
                    else:
                        job_dir = jobs_dir
                    job_file_submit = os.path.join(jobs_dir, f"{base_name}.sub")
                    max_mat = self._compute_max_materialize(1)
                    with open(job_file_submit, "w") as sub:
                        args_ = self.args_string.replace(original_analysis_path, analysis_json).replace(" vanilla_lxplus", " iterative")
                        sub.write("executable = /usr/bin/env\n")
                        sub.write(f"arguments = {sys.prefix}/bin/run_analysis.py {args_} || exit 107\n")
                        sub.write(f"output = {job_dir}/{base_name}.out\n")
                        sub.write(f"error = {job_dir}/{base_name}.err\n")
                        sub.write(f"log = {job_dir}/{base_name}.log\n")
                        sub.write(f"output_destination = {job_dir}\n")
                        sub.write(f"RequestMemory = ifThenElse(isUndefined(MemoryUsage), {self.memory_mb}, int(MemoryUsage * 1.1))\n")
                        sub.write("getenv = True\n")
                        sub.write(f'+JobFlavour = "{self.queue}"\n')
                        sub.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
                        sub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n')
                        sub.write('periodic_hold = ( JobStatus == 7 ) && ((CurrentTime - EnteredCurrentStatus) > 300)\n')
                        sub.write('periodic_hold_reason = "Job stuck suspended >5m — requeueing"\n')
                        sub.write('periodic_release = ( JobStatus == 5 ) && ((CurrentTime - EnteredCurrentStatus) > 60)\n')
                        sub.write('max_retries = 3\n')
                        if max_mat is not None:
                            sub.write(f"max_materialize = {max_mat}\n")
                        sub.write('requirements = Machine =!= LastRemoteHost\n')
                        sub.write("queue 1\n")
                    self.job_files.append(job_file_submit)

    def submit(self):
        """
        Submit all the generated .sub files to HTCondor.
        """
        for jf in self.job_files:
            if self.current_dir.startswith("/eos"):
                subprocess.run(["condor_submit", "-spool", jf])
            else:
                subprocess.run(["condor_submit", jf])