import { concatMap, tap } from 'rxjs/operators';
import { ResourceTypes, Admin } from 'kafkajs';
import { describeConfigs, connectAdminClient } from '../observable-kafkajs/observable-kafkajs';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

const resources = [
    {
        type: ResourceTypes.TOPIC,
        name: 'topic-name',
    },
];

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(() => describeConfigs(_adminClient, resources)),
    )
    .subscribe({
        next: data => console.log('next in subscribe', data),
        error: err => {
            console.error(err);
            _adminClient.disconnect();
        },
        complete: () => {
            console.log('DONE');
            _adminClient.disconnect();
        },
    });
