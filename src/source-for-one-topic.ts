import { KafkaConfig, Producer, ITopicConfig } from 'kafkajs';
import { Observable, interval } from 'rxjs';
import { connectProducer, connectAdminClient, deleteCreateTopics } from './observable-kafkajs/observable-kafkajs';
import { concatMap, tap, take, map } from 'rxjs/operators';

export class SourceForOneTopic {
    private _producer$: Observable<Producer>;
    private _producer: Producer;

    constructor(
        private name: string,
        config: KafkaConfig,
        private outputTopic: ITopicConfig,
        private options = { interval: 100, numberOfMessages: 10, recreateTopic: false },
    ) {
        this._producer$ = options.recreateTopic
            ? connectAdminClient(config).pipe(
                  concatMap(adminClient =>
                      deleteCreateTopics(adminClient, [this.outputTopic]).pipe(
                          map(topicsCreated => ({ topicsCreated, adminClient })),
                      ),
                  ),
                  tap(({ topicsCreated, adminClient }) => {
                      topicsCreated.map(t => console.log(`Topic ${t} created`));
                      adminClient.disconnect();
                  }),
                  concatMap(() => connectProducer(config)),
                  tap(producer => (this._producer = producer)),
              )
            : connectProducer(config).pipe(tap(producer => (this._producer = producer)));
    }

    sendMessages() {
        return this._producer$.pipe(
            concatMap(producer => interval(this.options.interval).pipe(map(i => ({ i, producer })))),
            concatMap(({ i, producer }) =>
                producer.send({
                    topic: this.outputTopic.topic,
                    messages: [{ key: `key-${i}`, value: `Massage from ${this.name} - ${i}` }],
                }),
            ),
            take(this.options.numberOfMessages),
        );
    }

    disconnect() {
        this._producer.disconnect();
    }
}
