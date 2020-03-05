import { ITopicConfig, Admin } from 'kafkajs';
import { connectAdminClient, deleteTopics, createTopics } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap } from 'rxjs/operators';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

const topicName1 = 'kafkajs-topic-1-1';
const topicNameN = 'kafkajs-topic-2-1';
const topics: ITopicConfig[] = [
    {
        topic: topicName1,
    },
    {
        topic: topicNameN,
    },
];

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(() =>
            deleteTopics(
                _adminClient,
                topics.map(t => t.topic),
            ),
        ),
        tap(topicsDeleted => console.log('Topics deleted', topicsDeleted)),
        concatMap(() => createTopics(_adminClient, topics)),
    )
    .subscribe({
        next: data => console.log('Topics created', data),
        error: err => {
            console.error(err);
            _adminClient.disconnect();
        },
        complete: () => {
            console.log('DONE');
            _adminClient.disconnect();
        },
    });
