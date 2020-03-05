import { KafkaClient } from 'kafka-node';
import { createTopicsObs } from './_observable-kafka-node';

const client = new KafkaClient({ kafkaHost: 'localhost:9092' });

const topicsToCreate = [
    {
        topic: 'topic1',
        partitions: 1,
        replicationFactor: 1,
    },
    {
        topic: 'topic2',
        partitions: 5,
        replicationFactor: 1,
        // Optional set of config entries
        configEntries: [
            {
                name: 'compression.type',
                value: 'gzip',
            },
            {
                name: 'min.compaction.lag.ms',
                value: '50',
            },
        ],
    },
];

createTopicsObs(topicsToCreate, client).subscribe(
    topics => console.log('topics created', JSON.stringify(topics)),
    error => console.error(JSON.stringify(error)),
    () => console.log('DONE'),
);
