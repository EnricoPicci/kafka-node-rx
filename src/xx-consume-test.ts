import { SinkForOneTopic } from './sink-for-one-topic';
import { ConsumerMessage } from './observable-kafkajs';
import { of } from 'rxjs';
import { delay, map } from 'rxjs/operators';

const consumer = new SinkForOneTopic(
    'First Consumer',
    {
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    },
    'Group 1',
    'TopicForProd',
);

// consumer.message().subscribe({
//     next: msg => {
//         console.log('C Data 1', msg.message.value.toString());
//         // msg.done();
//     },
//     error: console.error,
//     complete: () => console.log('Consumer Done'),
// });

// consumer.sequentialMessage().subscribe({
//     next: msg => {
//         console.log('ddddd<<<<<<<', msg.message.message.value.toString());
//         // msg.done();
//     },
//     error: console.error,
//     complete: () => console.log('Consumer Done'),
// });

// consumer.concurrentMessage(3).subscribe({
//     next: msg => {
//         console.log('ddddd<<<<<<<', JSON.stringify(msg.message['message'].key.toString()));
//         // msg.done();
//     },
//     error: console.error,
//     complete: () => console.log('Consumer Done'),
// });

const processor = (message: ConsumerMessage) => {
    return of(message).pipe(
        delay(5000),
        map(message => ({ message, result: 'Processed' })),
    );
};
consumer.processMessages(processor, 2).subscribe({
    next: msg => {
        console.log('ddddd<<<<<<<', msg.message.message.value.toString());
        // msg.done();
    },
    error: console.error,
    complete: () => console.log('Consumer Done'),
});
