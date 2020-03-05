import { ComponentController } from './component-controller';
import { COMMANDS } from './command';
import { finalize } from 'rxjs/operators';

const controller = new ComponentController(
    {
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    },
    {
        topic: 'ControllerTopic',
    },
);

controller
    .sendCommand(COMMANDS.exit)
    .pipe(finalize(() => controller.disconnect()))
    .subscribe({
        next: data => console.log('Exit command issued', data),
        error: console.error,
        complete: () => {
            console.log('DONE with Issuing Exit command');
        },
    });
