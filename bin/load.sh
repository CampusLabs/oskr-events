#!/bin/bash

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

declare -A ARGS
ARGS[allowedLateness]=$ALLOWED_LATENESS
ARGS[checkpointInterval]=$CHECKPOINT_INTERVAL
ARGS[dedupeCacheTime]=$DEDUPE_CACHE_TIME
ARGS[emailChannelDelay]=$EMAIL_CHANNEL_DELAY
ARGS[groupingGap]=$GROUPING_GAP
ARGS[kafkaBootstrap]=$KAFKA_BOOTSTRAP
ARGS[kafkaEmailDeliveryTopic]=$KAFKA_EMAIL_DELIVERY_TOPIC
ARGS[kafkaEventTopic]=$KAFKA_EVENT_TOPIC
ARGS[kafkaGroup]=$KAFKA_GROUP
ARGS[kafkaPartTopic]=$KAFKA_PART_TOPIC
ARGS[kafkaPushDeliveryTopic]=$KAFKA_PUSH_DELIVERY_TOPIC
ARGS[kafkaSmsDeliveryTopic]=$KAFKA_SMS_DELIVERY_TOPIC
ARGS[kafkaWebDeliveryTopic]=$KAFKA_WEB_DELIVERY_TOPIC
ARGS[maxDeliveryTime]=$MAX_DELIVERY_TIME
ARGS[maxEventLag]=$MAX_EVENT_LAG
ARGS[maxEventOutOfOrder]=$MAX_EVENT_OUT_OF_ORDER
ARGS[maxPartLag]=$MAX_PART_LAG
ARGS[maxPartOutOfOrder]=$MAX_PART_OUT_OF_ORDER
ARGS[pushChannelDelay]=$PUSH_CHANNEL_DELAY
ARGS[smsChannelDelay]=$SMS_CHANNEL_DELAY
ARGS[stateBackendPath]=$STATE_BACKEND_PATH
ARGS[watermarks]=$WATERMARKS
ARGS[webChannelDelay]=$WEB_CHANNEL_DELAY

for K in "${!ARGS[@]}"; do echo $K --- ${ARGS[$K]}; done
