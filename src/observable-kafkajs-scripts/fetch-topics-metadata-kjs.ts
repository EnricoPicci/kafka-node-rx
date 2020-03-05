import { concatMap, tap } from 'rxjs/operators';
import { Admin } from 'kafkajs';
import { connectAdminClient, fetchTopicMetadata } from '../observable-kafkajs/observable-kafkajs';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(() => fetchTopicMetadata(_adminClient)),
    )
    .subscribe({
        next: ({ topics }) =>
            console.log(
                'next in subscribe',
                topics.map(t => t.name),
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
