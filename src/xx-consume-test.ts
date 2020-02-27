import { SinkForOneTopic } from './sink-for-one-topic';

const consumer = new SinkForOneTopic(
    'First Consumer',
    {
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    },
    'Group 1',
    'TopicForProd',
);

consumer.message().subscribe({
    next: msg => console.log('C Data 1', JSON.stringify(msg)),
    error: console.error,
    complete: () => console.log('Consumer Done'),
});
