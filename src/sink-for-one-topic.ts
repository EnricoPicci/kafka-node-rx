import { Observable, of } from 'rxjs';
import { tap, map, delay, switchMap } from 'rxjs/operators';

import { Consumer, KafkaConfig } from 'kafkajs';
import {
    connectConsumer,
    subscribeConsumerToTopic,
    consumerMessages,
    ConsumerMessage,
    consumerSequentiallyProcessedMessages,
    consumerConcurrentlyProcessedMessages,
    Processor,
} from './observable-kafkajs';

export class SinkForOneTopic {
    // private _consumer$: Observable<Consumer>;
    private _consumer: Consumer;
    private _connectAndSubscribe$: Observable<void[]>;

    constructor(private name: string, config: KafkaConfig, private groupId: string, private topicName: string) {
        this._connectAndSubscribe$ = connectConsumer(config, this.groupId).pipe(
            tap(consumer => (this._consumer = consumer)),
            switchMap(consumer => subscribeConsumerToTopic(consumer, [this.topicName])),
        );
    }

    message() {
        return this._connectAndSubscribe$.pipe(
            switchMap(() => consumerMessages(this._consumer)),
            map(message => ({ ...message, consumerName: this.name })),
        );
    }
    sequentialMessage() {
        const processor = (message: ConsumerMessage) => {
            return of(message).pipe(
                delay(5000),
                map(message => ({ message, result: 'Processed' })),
            );
        };
        return this._connectAndSubscribe$.pipe(
            switchMap(() => consumerSequentiallyProcessedMessages(this._consumer, processor)),
            map(message => ({ ...message, consumerName: this.name })),
        );
    }
    concurrentMessage(concurrency: number) {
        const processor = (message: ConsumerMessage) => {
            return of(message).pipe(
                delay(5000),
                map(message => ({ message, result: 'Processed' })),
            );
        };
        return this._connectAndSubscribe$.pipe(
            switchMap(() => consumerConcurrentlyProcessedMessages(this._consumer, processor, concurrency)),
            map(message => ({ ...message, consumerName: this.name })),
        );
    }

    processMessages<T>(processor: Processor<T>, concurrency: number) {
        return this._connectAndSubscribe$.pipe(
            switchMap(() => consumerConcurrentlyProcessedMessages<T>(this._consumer, processor, concurrency)),
            map(message => ({ ...message, consumerName: this.name })),
        );
    }

    disconnect() {
        this._consumer.disconnect();
    }
}
