# oskr-events

A Flink-based service for more humane notifications. Work in progress.

## Configuration

Use the following commandline arguments to configure the job:

| Argument  | Default Value | Description |
|-----------|---------------|-------------|
| `checkpointInterval` | `PT5S` | interval between state snapshots (ISO 8601 duration) |
| `stateBackendPath` | `file:///tmp/state.db` | path to state database (file or HDFS) |
| `kafkaBootstrap` | `kafka:9092` | Kafka broker bootstrap |
| `kafkaGroup` | `oskr-events` | Kafka consumer group |
| `kafkaPartTopic` | `Communications.MessageParts` | Kafka topic for incoming messages |
| `kafkaEventTopic` | `Communications.Events` | Kafka topic for incoming events |
| `watermarks` | `periodic` | event time watermarking strategy, either `periodic` or `bounded` |
| `maxPartOutOfOrder` | `PT5S` | for `bounded` strategy, maximum amount of time messages can be out of order (ISO 8601 duration) |
| `maxEventOutOfOrder` | `PT5S` | for `bounded` strategy, maximum amount of time events can be out of order (ISO 8601 duration) |
| `maxPartLag` | `PT5S` | for `periodic` strategy, maximum lag before a message is considered late (ISO 8601 duration) |
| `maxEventLag` | `PT5S` | for `periodic` strategy, maximum lag before an event is considered late (ISO 8601 duration) |
| `webChannelDelay` | `PT0S` | default delay before sending a message to web channel (ISO 8601 duration) |
| `pushChannelDelay` | `PT5S` | default delay before sending a message to push channel (ISO 8601 duration) |
| `smsChannelDelay` | `PT30S` | default delay before sending a message to SMS channel (ISO 8601 duration) |
| `emailChannelDelay` | `PT1M` | default delay before sending a message to email channel (ISO 8601 duration) |
| `dedupeCacheTime` | `PT1H` | size of the message deduplication cache (ISO 8601 duration) |
| `allowedLateness` | `PT1H` | maximum allowed grouped message lateness in (ISO 8601 duration) |
| `groupingGap` | `PT5M` | default grouped message session gap (ISO 8601 duration) |
| `maxDeliveryTime` | `PT168H` | maximum time to wait for delivery to arrive (ISO 8601 duration) |
| `kafkaWebDeliveryTopic` | `Communications.Deliveries.Web` | Kafka topic for web delivery events |
| `kafkaPushDeliveryTopic` | `Communications.Deliveries.Push` | Kafka topic for push delivery events |
| `kafkaSmsDeliveryTopic` | `Communications.Deliveries.Sms` | Kafka topic for sms delivery events |
| `kafkaEmailDeliveryTopic` | `Communications.Deliveries.Email` | Kafka topic for email delivery events |

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
