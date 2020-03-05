import { SinkForOneTopic } from './sink-for-one-topic';
import { ConsumerMessage } from './observable-kafkajs/observable-kafkajs';
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
    'ControllerTopic',
);

const processor = (message: ConsumerMessage) => {
    return of(message).pipe(
        delay(5000),
        map(message => ({ message, result: 'Processed' })),
    );
};
consumer.processMessages(processor, 2).subscribe({
    next: msg => {
        console.log('ddddd<<<<<<<', msg.message.kafkaMessage.value.toString());
    },
    error: console.error,
    complete: () => console.log('Consumer Done'),
});
