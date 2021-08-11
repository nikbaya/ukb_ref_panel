#!/usr/bin/env bash

raise_error() {
  >&2 echo -e "Error: $1. Exiting." && exit 1
}

elapsed_time() {
  if [ ! -z $1 ]; then echo "elapsed time: $( echo "scale=2; $1/3600" | bc -l ) hrs "; fi
}

print_update() {
  local _message=$1
  set +o nounset # disable check for unbound variables temporarily
  local _duration=$2
  echo "${_message} $( elapsed_time ${_duration} )(job id: ${JOB_ID}.${SGE_TASK_ID} $( date ))"
  set -o nounset
}

set_up_conda() {
  local __conda_setup="$('/apps/eb/skylake/software/Anaconda3/2020.07/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
  if [ $? -eq 0 ]; then
      set +o nounset # use to avoid "PS1: unbound variable" error
      eval "$__conda_setup"
      set -o nounset
  else
      if [ -f "/apps/eb/skylake/software/Anaconda3/2020.07/etc/profile.d/conda.sh" ]; then
          . "/apps/eb/skylake/software/Anaconda3/2020.07/etc/profile.d/conda.sh"
      else
          export PATH="/apps/eb/skylake/software/Anaconda3/2020.07/bin:$PATH"
      fi
  fi
  unset __conda_setup
}

get_hail_memory() {
  if [[ -z ${QUEUE} || -z ${NSLOTS} ]]; then
    raise_error "QUEUE and NSLOTS must both be defined"
  fi
  if [[ "${QUEUE}" = *".qe" || "${QUEUE}" = *".qc" ]]; then
    local _mem_per_slot=10
  elif [[ "${QUEUE}" = *".qf" ]]; then
    local _mem_per_slot=3
  else
    raise_error "QUEUE must end in either \".qe\", \".qc\", or \".qf\""
  fi
  echo $(( ${_mem_per_slot}*${NSLOTS} ))
}

set_up_hail() {
  mkdir -p ${spark_dir} # directory for Hail's temporary Spark output files
  module load Anaconda3/2020.07
  module load java/1.8.0_latest
  set_up_conda
  conda activate hail # Requires conda environment with Hail installed
  local _mem=$( get_hail_memory )
  if [ ! -z ${_mem} ]; then
    export PYSPARK_SUBMIT_ARGS="--conf spark.local.dir=${spark_dir} --conf spark.executor.heartbeatInterval=1000000 --conf spark.network.timeout=1000000  --driver-memory ${_mem}g --executor-memory ${_mem}g pyspark-shell"
  fi
}
