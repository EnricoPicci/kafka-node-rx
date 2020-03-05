import { Observable } from 'rxjs';
import { tap, map, switchMap, filter, share, takeUntil, finalize, mergeMap } from 'rxjs/operators';

import { Consumer, KafkaConfig } from 'kafkajs';
import {
    connectConsumer,
    subscribeConsumerToTopic,
    consumerMessages,
    ConsumerMessage,
} from './observable-kafkajs/observable-kafkajs';
import { COMMANDS } from './command';

export type MessageProcessingResult<T> = { message: ConsumerMessage; result: T };
export type Processor<T> = (message: ConsumerMessage) => Observable<MessageProcessingResult<T>>;
export class SinkForOneTopic {
    private _consumer: Consumer;
    private _connectAndSubscribe$: Observable<void[]>;
    private _messages$: Observable<ConsumerMessage>;
    private _exit$: Observable<any>;

    constructor(
        private name: string,
        config: KafkaConfig,
        private groupId: string,
        private topicName: string,
        private controllerTopicName: string,
    ) {
        this._connectAndSubscribe$ = connectConsumer(config, this.groupId).pipe(
            tap(consumer => (this._consumer = consumer)),
            switchMap(consumer => subscribeConsumerToTopic(consumer, [this.topicName, this.controllerTopicName])),
            share(),
        );

        this._messages$ = this._connectAndSubscribe$.pipe(
            switchMap(() => consumerMessages(this._consumer)),
            share(),
        );

        this._exit$ = this._messages$.pipe(
            filter(message => message.topic === controllerTopicName),
            filter(message => message.kafkaMessage.value.toString() === COMMANDS.exit),
        );
    }

    processMessages<T>(processor: Processor<T>, concurrency: number) {
        const _mergeMap = concurrency
            ? mergeMap((message: ConsumerMessage) => processor(message), concurrency)
            : mergeMap((message: ConsumerMessage) => processor(message));
        return this._messages$.pipe(
            filter(message => message.topic === this.topicName),
            _mergeMap,
            map(procResult => ({ ...procResult, consumerName: this.name })),
            tap(resp => {
                resp.message.done();
            }),
            takeUntil(this._exit$),
            finalize(() => process.exit(10)),
        );
    }
    processMessagesSequentially<T>(processor: Processor<T>) {
        return this.processMessages<T>(processor, 1);
    }

    disconnect() {
        this._consumer.disconnect();
    }
}
