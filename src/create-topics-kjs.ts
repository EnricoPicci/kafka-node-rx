import { ITopicConfig, Admin } from 'kafkajs';
import { createTopics, connectAdminClient } from './observable-kafkajs';
import { concatMap, tap } from 'rxjs/operators';

// import { createTopics} from './observable-kafkajs'
// import { tap, finalize } from 'rxjs/operators';

const kafkaConfig = {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
};

const topicName1 = 'kafkajs-topic-1-1';
const topicNameN = 'kafkajs-topic-2-1';
const topics: ITopicConfig[] = [
    {
        topic: topicName1,
    },
    {
        topic: topicNameN,
    },
];

let _adminClient: Admin;
connectAdminClient(kafkaConfig)
    .pipe(
        tap(adminClient => (_adminClient = adminClient)),
        concatMap(adminClient => createTopics(adminClient, topics)),
    )
    .subscribe({
        next: data => console.log('topics created', data),
        error: err => {
            console.error(err);
            _adminClient.disconnect();
        },
        complete: () => {
            console.log('DONE');
            _adminClient.disconnect();
        },
    });

// const kafka = new Kafka(kafkaConfig);
// const admin = kafka.admin();
// admin
//     .connect()
//     .then(
//         () => {
//             return admin.createTopics({ topics });
//         },
//         err => {
//             console.error('Error in connecting to admin client', err);
//         },
//     )
//     .then(
//         created => {
//             if (created) {
//                 console.log(`Topic created`);
//             } else {
//                 console.log(`Topic NOT created`);
//             }
//         },
//         err => {
//             console.error('Error in creating topic', err);
//         },
//     )
//     .finally(() => admin.disconnect());
