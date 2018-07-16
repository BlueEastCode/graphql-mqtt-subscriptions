export interface PubSubEngine {
    publish(triggerName: string, payload: any): boolean;
    subscribe(triggerName: string, onMessage: Function, options, model): Promise<number>;
    unsubscribe(subId: number): any;
    asyncIterator(model, options): any;
}
