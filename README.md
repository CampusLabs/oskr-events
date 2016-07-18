# oskr-events

A Flink-based service for more humane notifications. Work in progress.

## Configuration

Use the following commandline arguments to configure the job:

| Argument  | Default Value | Description |
|-----------|---------------|-------------|
| `checkpointInterval` | `5000` | interval between state snapshots (ms) |
| `stateBackendPath` | `file:///tmp/state.db` | path to state database (file or HDFS) |
| `kafkaBootstrap` | `kafka:9092` | Kafka broker bootstrap |
| `kafkaGroup` | `oskr-events` | Kafka consumer group |
| `kafkaPartTopic` | `MessageParts` | Kafka topic for incoming messages |
| `kafkaEventTopic` | `Events` | Kafka topic for incoming events |
| `watermarks` | `periodic` | event time watermarking strategy, either `periodic` or `bounded` |
| `maxPartOutOfOrder` | `1000` | for `bounded` strategy, maximum amount of time messages can be out of order (ms) |
| `maxEventOutOfOrder` | `1000` | for `bounded` strategy, maximum amount of time events can be out of order (ms) |
| `maxPartLag` | `5000` | for `periodic` strategy, maximum lag before a message is considered late (ms) |
| `maxEventLag` | `5000` | for `periodic` strategy, maximum lag before an event is considered late (ms) |
| `webChannelDelay` | `0` | default delay before sending a message to web channel (ms) |
| `pushChannelDelay` | `5000` | default delay before sending a message to push channel (ms) |
| `smsChannelDelay` | `30000` | default delay before sending a message to SMS channel (ms) |
| `emailChannelDelay` | `60000` | default delay before sending a message to email channel (ms) |
| `dedupeCacheTime` | `60` | size of the message deduplication cache (minutes) |
| `allowedLateness` | `60` | maximum allowed grouped message lateness in (minutes) |
| `groupingGap` | `5` | default grouped message session gap (minutes) |

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
