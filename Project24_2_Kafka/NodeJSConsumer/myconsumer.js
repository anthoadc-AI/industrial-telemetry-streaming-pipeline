/* Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * =============================================================================
 *
 * Consume messages from Confluent Cloud
 * Using the node-rdkafka client for Apache Kafka
 *
 * =============================================================================
 */
/*
 * Consume messages from Kafka using kafkajs (pure JavaScript, no native build)
 */
const { Kafka } = require('kafkajs');

const myconsumer = function myconsumer(topic) {
    const kafka = new Kafka({
        clientId: 'node-example',
        brokers: ['localhost:9092']
    });

    const consumer = kafka.consumer({ groupId: 'node-example-group-1' });

    const run = async () => {
        console.log(`Consuming records from ${topic.topic}`);
        await consumer.connect();
        await consumer.subscribe({ topic: topic.topic, fromBeginning: true });

        let seen = 0;
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                console.log(`value ${value} of partition ${partition} @ offset ${message.offset}. Updated total count to ${++seen}`);
            }
        });
    };

    run().catch((err) => {
        console.error(`Something went wrong:\n${err}`);
    });

    return "";
}

exports.myconsumer = myconsumer;