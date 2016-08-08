#!/bin/bash -e

# Copyright 2016 OrgSync.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

flink=$FLINK_HOME/bin/flink

function check_error() {
  if [[ $@ == *"program finished with the following exception"* ]]; then
    echo "ERROR: external command failed"
    echo "DEBUG: $@"
    exit 1
  fi
}

function get_job_id() {
  echo "INFO: checking for running jobs"
  local jobs=$($flink list --jobmanager $JOBMANAGER_CONNECT 2>&1)
  check_error $jobs

  JOB_ID=$(echo "$jobs" | awk "/$JOB_NAME/ {print \$4}")
  if [[ ! -z $JOB_ID ]]; then return 0; else return 1; fi
}

function run() {
  declare -A options
  options[jobmanager]=$JOBMANAGER_CONNECT
  options[parallelism]=$PARALLELISM
  options[detached]=

  declare -A args
  args[allowedLateness]=$ALLOWED_LATENESS
  args[checkpointInterval]=$CHECKPOINT_INTERVAL
  args[dedupeCacheTime]=$DEDUPE_CACHE_TIME
  args[emailChannelDelay]=$EMAIL_CHANNEL_DELAY
  args[groupingGap]=$GROUPING_GAP
  args[jobName]=$JOB_NAME
  args[kafkaBootstrap]=$KAFKA_BOOTSTRAP
  args[kafkaEmailDeliveryTopic]=$KAFKA_EMAIL_DELIVERY_TOPIC
  args[kafkaEventTopic]=$KAFKA_EVENT_TOPIC
  args[kafkaGroup]=$KAFKA_GROUP
  args[kafkaPartTopic]=$KAFKA_PART_TOPIC
  args[kafkaPushDeliveryTopic]=$KAFKA_PUSH_DELIVERY_TOPIC
  args[kafkaSmsDeliveryTopic]=$KAFKA_SMS_DELIVERY_TOPIC
  args[kafkaStorageDeliveryTopic]=$KAFKA_STORAGE_DELIVERY_TOPIC
  args[kafkaWebDeliveryTopic]=$KAFKA_WEB_DELIVERY_TOPIC
  args[maxDeliveryTime]=$MAX_DELIVERY_TIME
  args[maxEventLag]=$MAX_EVENT_LAG
  args[maxEventOutOfOrder]=$MAX_EVENT_OUT_OF_ORDER
  args[maxPartLag]=$MAX_PART_LAG
  args[maxPartOutOfOrder]=$MAX_PART_OUT_OF_ORDER
  args[pushChannelDelay]=$PUSH_CHANNEL_DELAY
  args[smsChannelDelay]=$SMS_CHANNEL_DELAY
  args[storageChannelDelay]=$STORAGE_CHANNEL_DELAY
  args[stateBackendPath]=$STATE_BACKEND_PATH
  args[watermarks]=$WATERMARKS
  args[webChannelDelay]=$WEB_CHANNEL_DELAY

  if [ -z $1 ]; then
    echo "INFO: running job"
  else
    echo "INFO: running job from savepoint $1"
    options[fromSavepoint]=$1
  fi

  local opt=""
  for k in "${!options[@]}"; do opt="$opt --$k ${options[$k]}"; done

  local arg=""
  for k in "${!args[@]}"; do arg="$arg --$k ${args[$k]}"; done

  local result=$($flink run $opt $JARFILE $arg 2>&1)
  check_error $result

  JOB_ID=$(echo "$result" | awk "match(\$0,/submitted with JobID [0-9a-f]+/) {print \$NF}")
  if [[ -z $JOB_ID ]]; then
    echo "ERROR: $JOB_NAME job ID not found"
    exit 1
  else
    echo "INFO: $JOB_NAME job running, ID: $JOB_ID"
  fi
}

function savepoint() {
  echo "INFO: triggering savepoint"

  declare -A options
  options[jarfile]=$JARFILE
  options[jobmanager]=$JOBMANAGER_CONNECT

  local opt=""
  for k in "${!options[@]}"; do opt="$opt --$k ${options[$k]}"; done

  local result=$($flink savepoint $opt $JOB_ID 2>&1)
  check_error $result

  SAVEPOINT_PATH=$(echo "$result" | awk "/Savepoint completed. Path:/ {print \$NF}")
  if [[ -z SAVEPOINT_PATH ]]; then
    echo "ERROR: savepoint path not found"
    exit 1
  else
    echo "INFO: savepoint saved to $SAVEPOINT_PATH"
  fi
}

function stop() {
  echo "INFO: cancelling job $JOB_ID"
  local result=$($flink cancel --jobmanager $JOBMANAGER_CONNECT $JOB_ID 2>&1)
  check_error $result
}

if get_job_id; then
  echo "INFO: $JOB_NAME job found, ID: $JOB_ID"
  savepoint $JOB_ID
  stop
  run $SAVEPOINT_PATH
else
  echo "INFO: $JOB_NAME not found"
  run
fi
