import { SourceForOneTopic } from './source-for-one-topic';
import { finalize } from 'rxjs/operators';

const producer = new SourceForOneTopic(
    'First Prod',
    {
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    },
    {
        topic: 'TopicForProd',
    },
);

producer
    .sendMessages()
    .pipe(finalize(() => producer.disconnect()))
    .subscribe({
        next: data => console.log('Data from prod', data[0]),
        error: console.error,
        complete: () => {
            console.log('DONE with Prod');
        },
    });
