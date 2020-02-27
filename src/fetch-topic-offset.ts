import { concatMap, tap } from 'rxjs/operators';
import { Admin } from 'kafkajs';
import { connectAdminClient, fetchTopicOffsets } from './observable-kafkajs';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(() => fetchTopicOffsets(_adminClient, 'TopicForProd')),
    )
    .subscribe({
        next: data => data.forEach(d => console.log('next in subscribe', d)),
        error: err => {
            console.error(err);
            _adminClient.disconnect();
        },
        complete: () => {
            console.log('DONE');
            _adminClient.disconnect();
        },
    });
