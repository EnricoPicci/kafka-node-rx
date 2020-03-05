import { Observable, TeardownLogic, Observer } from 'rxjs';
import { KafkaClient, CreateTopicRequest } from 'kafka-node';

// returns and Observable which emits the names of the topics created or errors if problems are encountered
export const createTopicsObs = (topicsToCreate: CreateTopicRequest[], client: KafkaClient) => {
    return new Observable(
        (observer: Observer<string[]>): TeardownLogic => {
            client.createTopics(topicsToCreate, (error, createTopicResponses) => {
                if (error) {
                    observer.error(error);
                } else if (createTopicResponses.length > 0) {
                    observer.error(createTopicResponses);
                } else {
                    observer.next(topicsToCreate.map(t => t.topic));
                    observer.complete();
                }
            });
        },
    );
};
