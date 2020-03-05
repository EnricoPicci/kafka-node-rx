import { concatMap, tap } from 'rxjs/operators';
import { Admin } from 'kafkajs';
import { connectAdminClient, existingTopics } from '../observable-kafkajs/observable-kafkajs';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

const topicNames = ['kafkajs-topic-1-1', 'kafkajs-topic-2-1'];

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(() => existingTopics(_adminClient, topicNames)),
    )
    .subscribe({
        next: data =>
            console.log(
                'existing topics',
                data.map(t => t.name),
            ),
        error: err => {
            console.error(err);
            _adminClient.disconnect();
        },
        complete: () => {
            console.log('DONE');
            _adminClient.disconnect();
        },
    });
