import { KafkaClient } from 'kafka-node';

const client = new KafkaClient({ kafkaHost: 'localhost:9092' });

const topicsToCreate = [
    {
        topic: 'topic10',
        partitions: 1,
        // there are no 10 brokers so this should fail
        replicationFactor: 10,
    },
];

client.createTopics(topicsToCreate, (error, createTopicResponses) => {
    if (error) {
        console.error('Unexpected Error', error);
        process.exit(0);
    } else {
        if (createTopicResponses.length > 0) {
            createTopicResponses.forEach(r =>
                console.error(`Error creating topic ${r.topic} - error message ${r.error}`),
            );
        } else {
            console.log('Success');
        }
        process.exit(0);
    }
});

// process.exit(0);
