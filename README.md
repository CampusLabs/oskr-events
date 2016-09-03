# oskr-events

A Flink-based service for more humane notifications. Work in progress.

## Configuration

### Job Options

| Environment Variable | Default Value | Description                                    |
|----------------------|---------------|------------------------------------------------|
| `JOBMANAGER_CONNECT` | `jobmanager`  | Flink jobmanager connect string                |
| `PARALLELISM`        | `8`           | Job parallelism (must not change after deploy) |


### Program Arguments

| Environment Variable           | Default Value                       | Description                                                                                     |
|--------------------------------|-------------------------------------|-------------------------------------------------------------------------------------------------|
| `ALLOWED_LATENESS`             | `PT1H`                              | maximum allowed grouped message lateness in (ISO 8601 duration)                                 |
| `CHECKPOINT_INTERVAL`          | `PT5S`                              | interval between state snapshots (ISO 8601 duration)                                            |
| `DEDUPE_CACHE_TIME`            | `PT1H`                              | size of the message deduplication cache (ISO 8601 duration)                                     |
| `EMAIL_CHANNEL_DELAY`          | `PT1M`                              | default delay before sending a message to email channel (ISO 8601 duration)                     |
| `GROUPING_GAP`                 | `PT5M`                              | default grouped message session gap (ISO 8601 duration)                                         |
| `JOB_NAME`                     | `oskr event processing`             | Flink job name                                                                                  |
| `KAFKA_BOOTSTRAP`              | `kafka:9092`                        | Kafka broker bootstrap                                                                          |
| `KAFKA_EMAIL_DELIVERY_TOPIC`   | `Communications.Deliveries.Email`   | Kafka topic for email delivery messages                                                         |
| `KAFKA_DELIVERY_EVENT_TOPIC`   | `Communications.Events.Deliveries`  | Kafka topic for incoming delivery events                                                        |
| `KAFKA_GROUP`                  | `oskr-events`                       | Kafka consumer group                                                                            |
| `KAFKA_PART_TOPIC`             | `Communications.MessageParts`       | Kafka topic for incoming messages                                                               |
| `KAFKA_PUSH_DELIVERY_TOPIC`    | `Communications.Deliveries.Push`    | Kafka topic for push delivery messages                                                          |
| `KAFKA_SMS_DELIVERY_TOPIC`     | `Communications.Deliveries.Sms`     | Kafka topic for sms delivery messages                                                           |
| `KAFKA_STORAGE_DELIVERY_TOPIC` | `Communications.Deliveries.Storage` | Kafka topic for delivery storage messages                                                       |
| `KAFKA_WEB_DELIVERY_TOPIC`     | `Communications.Deliveries.Web`     | Kafka topic for web delivery messages                                                           |
| `MAX_EVENT_LAG`                | `PT5S`                              | for `periodic` strategy, maximum lag before an event is considered late (ISO 8601 duration)     |
| `MAX_EVENT_OUT_OF_ORDER`       | `PT5S`                              | for `bounded` strategy, maximum amount of time events can be out of order (ISO 8601 duration)   |
| `MAX_PART_LAG`                 | `PT5S`                              | for `periodic` strategy, maximum lag before a message is considered late (ISO 8601 duration)    |
| `MAX_PART_OUT_OF_ORDER`        | `PT5S`                              | for `bounded` strategy, maximum amount of time messages can be out of order (ISO 8601 duration) |
| `PUSH_CHANNEL_DELAY`           | `PT5S`                              | default delay before sending a message to push channel (ISO 8601 duration)                      |
| `SMS_CHANNEL_DELAY`            | `PT30S`                             | default delay before sending a message to SMS channel (ISO 8601 duration)                       |
| `STORAGE_CHANNEL_DELAY`        | `PT0S`                              | default delay before sending a message to storage channel (ISO 8601 duration)                   |
| `STATE_BACKEND_PATH`           | `file:///tmp/state.db`              | path to state database (file or HDFS)                                                           |
| `UNREAD_TIME`                  | `PT168H`                            | unread message count window duration                                                            |
| `UNREAD_SLIDE`                 | `PT24H`                             | unread message count window update interval                                                     |
| `WATERMARKS`                   | `periodic`                          | event time watermarking strategy, either `periodic` or `bounded`                                |
| `WEB_CHANNEL_DELAY`            | `PT0S`                              | default delay before sending a message to web channel (ISO 8601 duration)                       |

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
