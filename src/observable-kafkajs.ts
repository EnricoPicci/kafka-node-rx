import { from, forkJoin, Observable, Subscriber, TeardownLogic } from 'rxjs';
import { map, expand, filter, delay, first, tap, concatMap } from 'rxjs/operators';

import { KafkaConfig, ITopicConfig, Kafka, Admin, ResourceConfigQuery, Consumer, KafkaMessage } from 'kafkajs';

export const connectAdminClient = (config: KafkaConfig) => {
    const kafka = new Kafka(config);
    const admin = kafka.admin();
    return from(admin.connect()).pipe(map(() => admin));
};

export const disconnectAdminClient = (adminClient: Admin) => from(adminClient.disconnect());

export const createTopics = (adminClient: Admin, topics: ITopicConfig[]) => {
    let _topicsToCreate: ITopicConfig[];
    return existingTopics(
        adminClient,
        topics.map(t => t.topic),
    ).pipe(
        tap(_existingTopics => {
            if (_existingTopics.length > 0) {
                console.log(
                    `These topics "${_existingTopics.map(t => t.name)}" do already exist and therefore are not created`,
                );
            }
        }),
        map(_existingTopics => {
            const _existingTopicNames = _existingTopics.map(t => t.name);
            _topicsToCreate = topics.filter(t => !_existingTopicNames.includes(t.topic));
            return _topicsToCreate;
        }),
        concatMap(topicsToCreate => from(adminClient.createTopics({ topics: topicsToCreate }))),
        map(topicsCreated => {
            if (topicsCreated) {
                return _topicsToCreate.map(t => t.topic);
            } else {
                const errorMessage =
                    topics.length === 1
                        ? `The topic ${topics[0].topic} has not been created`
                        : `Some topics have not been created`;
                throw new Error(errorMessage);
            }
        }),
    );
};

export const deleteTopics = (adminClient: Admin, topics: string[], timeout?: number) => {
    const _delay = 100;
    const maxAttempts = timeout ? Math.floor(timeout / _delay) : 10;
    let _topicsToBeDeleted: string[];
    return nonExistingTopics(adminClient, topics).pipe(
        tap(_nonExistingTopics => {
            if (_nonExistingTopics.length > 0) {
                console.log(`These topics ${_nonExistingTopics} do not exist and therefore are not cancelled`);
            }
        }),
        map(_nonExistingTopics => topics.filter(t => !_nonExistingTopics.includes(t))),
        concatMap(topicsToBeDeleted => {
            _topicsToBeDeleted = topicsToBeDeleted;
            const options = timeout ? { topics: topicsToBeDeleted, timeout } : { topics: topicsToBeDeleted };
            return from(adminClient.deleteTopics(options)).pipe(map(() => topicsToBeDeleted));
        }),
        expand((topicsToBeDeleted, index) => {
            console.log('try delete', index, topicsToBeDeleted);
            if (index > maxAttempts) {
                throw new Error(
                    `Topics ${topicsToBeDeleted} not deleted yet after ${_delay * maxAttempts} milliseconds`,
                );
            }
            return existingTopics(adminClient, topics).pipe(delay(_delay));
        }),
        filter(_existingTopics => {
            return _existingTopics.length === 0;
        }),
        first(),
        map(() => _topicsToBeDeleted),
    );
};

export const deleteCreateTopics = (adminClient: Admin, topics: ITopicConfig[], timeout?: number) => {
    return deleteTopics(
        adminClient,
        topics.map(t => t.topic),
        timeout,
    ).pipe(
        tap(topicsDeleted => console.log('Topics deleted', topicsDeleted)),
        concatMap(() => createTopics(adminClient, topics)),
    );
};

export const existingTopics = (adminClient: Admin, topicNames: string[]) => {
    return fetchTopicMetadata(adminClient).pipe(map(({ topics }) => topics.filter(t => topicNames.includes(t.name))));
};
export const nonExistingTopics = (adminClient: Admin, topicNames: string[]) => {
    return fetchTopicMetadata(adminClient).pipe(
        map(({ topics }) => {
            const existingTopicNames = topics.map(t => t.name);
            return topicNames.filter(t => !existingTopicNames.includes(t));
        }),
    );
};

export const fetchConsumerGroupOffsets = (adminClient: Admin, groupId: string, topic: string) => {
    return from(adminClient.fetchOffsets({ groupId, topic }));
};

export const fetchTopicOffsets = (adminClient: Admin, topic: string) => {
    return from(adminClient.fetchTopicOffsets(topic));
};

export const fetchTopicMetadata = (adminClient: Admin, topics?: string[]) => {
    return from(adminClient.fetchTopicMetadata({ topics }));
};

export const describeConfigs = (adminClient: Admin, resources: ResourceConfigQuery[], includeSynonyms = false) => {
    return from(adminClient.describeConfigs({ resources, includeSynonyms }));
};

export const connectProducer = (config: KafkaConfig) => {
    const kafka = new Kafka(config);
    const producer = kafka.producer();
    return from(producer.connect()).pipe(map(() => producer));
};

export const connectConsumer = (config: KafkaConfig, groupId: string) => {
    const kafka = new Kafka(config);
    const consumer = kafka.consumer({ groupId });
    return from(consumer.connect()).pipe(map(() => consumer));
};

export const subscribeConsumerToTopic = (consumer: Consumer, topics: string[]) => {
    consumer.subscribe({ topic: topics[0] });
    return forkJoin(topics.map(topic => consumer.subscribe({ topic })));
};

export type ConsumerMessage = { topic: string; partition: any; message: KafkaMessage };
export const consumerMessages = (consumer: Consumer) => {
    return new Observable<ConsumerMessage>(
        (subscriber: Subscriber<ConsumerMessage>): TeardownLogic => {
            consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    console.log({
                        key: message.key.toString(),
                        value: message.value.toString(),
                        headers: message.headers,
                        topic,
                        partition,
                    });
                    let g = 1;
                    console.log('Start');
                    for (let i = 0; i < 1000000000; i++) {
                        g = (g / i) * (i + 1);
                    }
                    subscriber.next({ topic, partition, message });
                },
            });
        },
    );
};
