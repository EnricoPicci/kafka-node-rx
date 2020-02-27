import { Observable } from 'rxjs';
import { tap, concatMap, map } from 'rxjs/operators';

import { Consumer, KafkaConfig } from 'kafkajs';
import { connectConsumer, subscribeConsumerToTopic, consumerMessages, ConsumerMessage } from './observable-kafkajs';

export class SinkForOneTopic {
    // private _consumer$: Observable<Consumer>;
    private _consumer: Consumer;
    private _message$: Observable<ConsumerMessage & { consumerName: string }>;

    constructor(private name: string, config: KafkaConfig, private groupId: string, private topicName: string) {
        this._message$ = connectConsumer(config, this.groupId).pipe(
            tap(consumer => (this._consumer = consumer)),
            concatMap(consumer => subscribeConsumerToTopic(consumer, [this.topicName])),
            concatMap(() => consumerMessages(this._consumer)),
            map(message => ({ ...message, consumerName: this.name })),
        );
    }

    message() {
        return this._message$;
    }

    disconnect() {
        this._consumer.disconnect();
    }
}
