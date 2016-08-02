# oskr-events

A Flink-based service for more humane notifications. Work in progress.

## Configuration

Use the following commandline arguments to configure the job:

| Program Argument          | Environment Variable         | Default Value                     | Description                                                                                     |
|---------------------------|------------------------------|-----------------------------------|-------------------------------------------------------------------------------------------------|
| `allowedLateness`         | `ALLOWED_LATENESS`           | `PT1H`                            | maximum allowed grouped message lateness in (ISO 8601 duration)                                 |
| `checkpointInterval`      | `CHECKPOINT_INTERVAL`        | `PT5S`                            | interval between state snapshots (ISO 8601 duration)                                            |
| `dedupeCacheTime`         | `DEDUPE_CACHE_TIME`          | `PT1H`                            | size of the message deduplication cache (ISO 8601 duration)                                     |
| `emailChannelDelay`       | `EMAIL_CHANNEL_DELAY`        | `PT1M`                            | default delay before sending a message to email channel (ISO 8601 duration)                     |
| `groupingGap`             | `GROUPING_GAP`               | `PT5M`                            | default grouped message session gap (ISO 8601 duration)                                         |
| `kafkaBootstrap`          | `KAFKA_BOOTSTRAP`            | `kafka:9092`                      | Kafka broker bootstrap                                                                          |
| `kafkaEmailDeliveryTopic` | `KAFKA_EMAIL_DELIVERY_TOPIC` | `Communications.Deliveries.Email` | Kafka topic for email delivery events                                                           |
| `kafkaEventTopic`         | `KAFKA_EVENT_TOPIC`          | `Communications.Events`           | Kafka topic for incoming events                                                                 |
| `kafkaGroup`              | `KAFKA_GROUP`                | `oskr-events`                     | Kafka consumer group                                                                            |
| `kafkaPartTopic`          | `KAFKA_PART_TOPIC`           | `Communications.MessageParts`     | Kafka topic for incoming messages                                                               |
| `kafkaPushDeliveryTopic`  | `KAFKA_PUSH_DELIVERY_TOPIC`  | `Communications.Deliveries.Push`  | Kafka topic for push delivery events                                                            |
| `kafkaSmsDeliveryTopic`   | `KAFKA_SMS_DELIVERY_TOPIC`   | `Communications.Deliveries.Sms`   | Kafka topic for sms delivery events                                                             |
| `kafkaWebDeliveryTopic`   | `KAFKA_WEB_DELIVERY_TOPIC`   | `Communications.Deliveries.Web`   | Kafka topic for web delivery events                                                             |
| `maxDeliveryTime`         | `MAX_DELIVERY_TIME`          | `PT168H`                          | maximum time to wait for delivery to arrive (ISO 8601 duration)                                 |
| `maxEventLag`             | `MAX_EVENT_LAG`              | `PT5S`                            | for `periodic` strategy, maximum lag before an event is considered late (ISO 8601 duration)     |
| `maxEventOutOfOrder`      | `MAX_EVENT_OUT_OF_ORDER`     | `PT5S`                            | for `bounded` strategy, maximum amount of time events can be out of order (ISO 8601 duration)   |
| `maxPartLag`              | `MAX_PART_LAG`               | `PT5S`                            | for `periodic` strategy, maximum lag before a message is considered late (ISO 8601 duration)    |
| `maxPartOutOfOrder`       | `MAX_PART_OUT_OF_ORDER`      | `PT5S`                            | for `bounded` strategy, maximum amount of time messages can be out of order (ISO 8601 duration) |
| `pushChannelDelay`        | `PUSH_CHANNEL_DELAY`         | `PT5S`                            | default delay before sending a message to push channel (ISO 8601 duration)                      |
| `smsChannelDelay`         | `SMS_CHANNEL_DELAY`          | `PT30S`                           | default delay before sending a message to SMS channel (ISO 8601 duration)                       |
| `stateBackendPath`        | `STATE_BACKEND_PATH`         | `file:///tmp/state.db`            | path to state database (file or HDFS)                                                           |
| `watermarks`              | `WATERMARKS`                 | `periodic`                        | event time watermarking strategy, either `periodic` or `bounded`                                |
| `webChannelDelay`         | `WEB_CHANNEL_DELAY`          | `PT0S`                            | default delay before sending a message to web channel (ISO 8601 duration)                       |

## License

Copyright 2016 OrgSync.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
