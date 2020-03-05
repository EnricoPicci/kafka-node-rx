import { Observable } from 'rxjs';
import { tap, concatMap } from 'rxjs/operators';

import { KafkaConfig, ITopicConfig, Producer } from 'kafkajs';
import { connectProducer } from './observable-kafkajs/observable-kafkajs';
import { COMMANDS } from './command';

export class ComponentController {
    private _controller$: Observable<Producer>;
    private _controller: Producer;

    constructor(config: KafkaConfig, private controllerTopic: ITopicConfig) {
        this._controller$ = connectProducer(config).pipe(tap(controller => (this._controller = controller)));
    }

    sendCommand(command: COMMANDS) {
        return this._controller$.pipe(
            concatMap(controller =>
                controller.send({
                    topic: this.controllerTopic.topic,
                    messages: [{ value: command }],
                }),
            ),
        );
    }

    disconnect() {
        this._controller.disconnect();
    }
}
