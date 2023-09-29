#!/usr/bin/env python3
import subprocess
import sys
import logging

from datetime import datetime
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Work Directory Setup -----------------------------------------------------
RUN_ID = "local-hierarchy-sharedfs-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Properties ---------------------------------------------------------------
props = Properties()

props["pegasus.dir.storage.deep"] = "false"
props["pegasus.data.configuration"] = "nonsharedfs"

props.write()

# --- Sites --------------------------------------------------------------------
sites = """
pegasus: "5.0"
sites:
 -
  name: "CCG"
  arch: "x86_64"
  os.type: "linux"
  os.release: "rhel"
  os.version: "7"
  directories:
   -
    type: "sharedScratch"
    path: "/nfs/bamboo/scratch-90-days/CCG/scratch/{run_id}"
    fileServers:
     -
      operation: "all"
      url: "scp://bamboo@slurm-pegasus.isi.edu:2222/nfs/bamboo/scratch-90-days/CCG/scratch/{run_id}"
   -
    type: "localStorage"
    path: "/nfs/bamboo/scratch-90-days/CCG/outputs"
    fileServers:
     -
      operation: "all"
      url: "scp://bamboo@slurm-pegasus.isi.edu:2222/nfs/bamboo/scratch-90-days/CCG/outputs/{run_id}"
  grids: 
   - 
    type: "batch" 
    contact: "slurm-pegasus.isi.edu" 
    scheduler: "slurm" 
    jobtype: "compute" 
   - 
    type: "batch" 
    contact: "slurm-pegasus.isi.edu" 
    scheduler: "slurm" 
    jobtype: "compute" 
  profiles: 
    env: 
      PEGASUS_HOME: "{cluster_pegasus_home}" 
    pegasus: 
      # SSH is the style to use for Bosco SSH submits. 
      style: ssh 
      # Works around bug in the HTCondor GAHP, that does not 
      # set the remote directory 
      change.dir: 'true' 
      # the key to use for scp transfers 
      SSH_PRIVATE_KEY: /scitech/shared/home/bamboo/.ssh/workflow_id_rsa 
 -
  name: "local"
  arch: "x86_64"
  os.type: "linux"
  os.release: "rhel"
  os.version: "7"
  directories:
   -
    type: "sharedScratch"
    path: "{work_dir}/local-site/scratch"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/local-site/scratch"
   -
    type: "localStorage"
    path: "{work_dir}/outputs/local-site"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/outputs/local-site"
""".format(
    run_id=RUN_ID, work_dir=str(WORK_DIR), cluster_pegasus_home="/usr"
)

with open("sites.yml", "w") as f:
    f.write(sites)

# --- Transformations ----------------------------------------------------------

try:
    pegasus_config = subprocess.run(
        ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
except FileNotFoundError as e:
    print("Unable to find pegasus-config")

assert pegasus_config.returncode == 0

PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()

transformations = """
pegasus: "5.0"
transformations:
 -
  namespace: "diamond"
  name: "analyze"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "diamond"
  name: "findrange"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "diamond"
  name: "preprocess"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "diamond"
  name: "post-analyze"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"

 -
  namespace: "diamond"
  name: "pre-preprocess"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
""".format(
    pegasus_bin_dir=PEGASUS_BIN_DIR
)

with open("transformations.yml", "w") as f:
    f.write(transformations)

# --- Input Directory Setup ----------------------------------------------------
try:
    Path.mkdir(Path("input"))
except FileExistsError:
    pass

# --- Blackdiamond Subworkflow -------------------------------------------------
with open("input/f.input", "w") as f:
    f.write("Sample input file\n")

finput= File("f.input")
fa = File("f.a")
fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")
fe = File("f.e")
wf = (
    Workflow("blackdiamond")
    .add_jobs(
        Job("preprocess", namespace="diamond", version="4.0")
        .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)
        .add_inputs(fa)
        .add_outputs(fb1, fb2, register_replica=True),
        Job("findrange", namespace="diamond", version="4.0")
        .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)
        .add_inputs(fb1)
        .add_outputs(fc1, register_replica=True),
        Job("findrange", namespace="diamond", version="4.0")
        .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)
        .add_inputs(fb2)
        .add_outputs(fc2, register_replica=True),
        Job("analyze", namespace="diamond", version="4.0")
        .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)
        .add_inputs(fc1, fc2)
        .add_outputs(fd, register_replica=False, stage_out=False),
    )
    .write(str(TOP_DIR / "input/blackdiamond.yml"))
)

# --- Top Level Workflow -------------------------------------------------------
wf = Workflow("local-hierarchy")

pre_preprocess_job = Job("pre-preprocess", namespace="diamond", version="4.0")\
                   .add_args("-a", "post-analyze", "-T", "60", "-i", finput, "-o", fa)\
                   .add_inputs(finput)\
                   .add_outputs(fa, register_replica=True, stage_out=True)

blackdiamond_wf = SubWorkflow("blackdiamond.yml", False).add_args(
    "--input-dir", "input", "--output-sites", "local", "-vvv", "--force"
).add_inputs(fa).add_outputs(fd)

sleep_wf = SubWorkflow("sleep.yml", False).add_args("--output-sites", "local", "-vvv")

post_analyze_job = Job("post-analyze", namespace="diamond", version="4.0")\
                   .add_args("-a", "post-analyze", "-T", "60", "-i", fd, "-o", fe)\
                   .add_inputs(fd)\
                   .add_outputs(fe, register_replica=True, stage_out=True)

wf.add_jobs(pre_preprocess_job, blackdiamond_wf, post_analyze_job)
wf.add_dependency(pre_preprocess_job, children=[blackdiamond_wf])
wf.add_dependency(blackdiamond_wf, children=[post_analyze_job])

try:
    wf.plan(
        site=["CCG"],
        dir=str(WORK_DIR),
        relative_dir=RUN_ID,
        input_dirs=["input"],
        submit=True,
        verbose=3
    )
except PegasusClientError as e:
    print(e.output)
