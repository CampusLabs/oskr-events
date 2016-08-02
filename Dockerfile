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

FROM quay.io/orgsync/flink:latest

WORKDIR /code
COPY . /code/

RUN apt-get update \
  && apt-get install -y maven \
  && mvn package -DskipTests \
  && mkdir -p /opt/oskr-events \
  && cp /code/target/oskr-events.jar /opt/oskr-events/ \
  && apt-get remove --purge -y maven \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -Rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.m2 /code

COPY ./bin/load.sh /usr/local/bin/

WORKDIR /opt/oskr-events

ENV CHECKPOINT_INTERVAL        PT5S
ENV STATE_BACKEND_PATH         file:///tmp/state.db
ENV KAFKA_BOOTSTRAP            kafka:9092
ENV KAFKA_GROUP                oskr-events
ENV KAFKA_PART_TOPIC           Communications.MessageParts
ENV KAFKA_EVENT_TOPIC          Communications.Events
ENV WATERMARKS                 periodic
ENV MAX_PART_OUT_OF_ORDER      PT5S
ENV MAX_EVENT_OUT_OF_ORDER     PT5S
ENV MAX_PART_LAG               PT5S
ENV MAX_EVENT_LAG              PT5S
ENV WEB_CHANNEL_DELAY          PT0S
ENV PUSH_CHANNEL_DELAY         PT5S
ENV SMS_CHANNEL_DELAY          PT30S
ENV EMAIL_CHANNEL_DELAY        PT1M
ENV DEDUPE_CACHE_TIME          PT1H
ENV ALLOWED_LATENESS           PT1H
ENV GROUPING_GAP               PT5M
ENV MAX_DELIVERY_TIME          PT168H
ENV KAFKA_WEB_DELIVERY_TOPIC   Communications.Deliveries.Web
ENV KAFKA_PUSH_DELIVERY_TOPIC  Communications.Deliveries.Push
ENV KAFKA_SMS_DELIVERY_TOPIC   Communications.Deliveries.Sms
ENV KAFKA_EMAIL_DELIVERY_TOPIC Communications.Deliveries.Email

CMD ["/usr/local/bin/load.sh"]
