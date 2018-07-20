import { PubSubEngine } from './pubsub-engine';
import { connect, Client, ISubscriptionGrant, IClientSubscribeOptions, IClientPublishOptions } from 'mqtt';
import { PubSubAsyncIterator } from './pubsub-async-iterator';
import {QoS} from 'mqtt-packet';

export interface PubSubMQTTOptions {
  brokerUrl?: string;
  client?: Client;
  connectionListener?: (err: Error) => void;
  publishOptions?: PublishOptionsResolver;
  subscribeOptions?: SubscribeOptionsResolver;
  onMQTTSubscribe?: (id: number, granted: ISubscriptionGrant[]) => void;
  triggerTransform?: TriggerTransform;
  parseMessageWithEncoding?: string;
}

export class MQTTPubSub implements PubSubEngine {

  private triggerTransform: TriggerTransform;
  private onMQTTSubscribe: SubscribeHandler;
  private subscribeOptionsResolver: SubscribeOptionsResolver;
  private publishOptionsResolver: PublishOptionsResolver;
  private mqttConnection: Client;

  private subscriptionMap: { [subId: number]: [string, Function] };
  private subsRefsMap: { [trigger: string]: Array<number> };
  private currentSubscriptionId: number;
  private parseMessageWithEncoding: string;
  private myQoS: QoS;

  constructor(options: PubSubMQTTOptions = {}) {
    this.triggerTransform = options.triggerTransform || (trigger => trigger as string);

    if (options.client) {
      this.mqttConnection = options.client;
    } else {
      const brokerUrl = options.brokerUrl || 'mqtt://localhost:3881';
      this.mqttConnection = connect(brokerUrl);
    }

    this.mqttConnection.on('message', this.onMessage.bind(this));

    if (options.connectionListener) {
      this.mqttConnection.on('connect', options.connectionListener);
      this.mqttConnection.on('error', options.connectionListener);
    } else {
      this.mqttConnection.on('error', console.error);
    }

    this.myQoS = 0;
    this.subscriptionMap = {};
    this.subsRefsMap = {};
    this.currentSubscriptionId = 0;
    this.onMQTTSubscribe = options.onMQTTSubscribe || (() => null);
    this.subscribeOptionsResolver = options.subscribeOptions || (() => Promise.resolve({ qos: this.myQoS}));
    this.publishOptionsResolver = options.publishOptions || (() => Promise.resolve({  qos: this.myQoS }));
    this.parseMessageWithEncoding = options.parseMessageWithEncoding;
  }

  public publish(trigger: string, payload: any): boolean {
    this.publishOptionsResolver(trigger, payload).then(publishOptions => {
      const message = Buffer.from(JSON.stringify(payload), this.parseMessageWithEncoding);

      this.mqttConnection.publish(trigger, message, publishOptions);
    });
    return true;
  }

  public subscribe(trigger: string, onMessage: Function, options, model): Promise<number> {
    const me = this;
    let triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    triggerName += id;
    this.subscriptionMap[id] = [triggerName, onMessage];
    let refs = this.subsRefsMap[triggerName];

    const { create, update, remove } = options;
    model.createChangeStream(options.options, function (err, stream) {
      if (!err) {
        stream.on('data', function (data) {
          switch (data.type) {
            case 'create':
              if (create) {
                if (me.subscriptionMap[id]) {
                  me.onNewMessage(id, 'create', data, triggerName);
                }
              }
              break;
            case 'update':
              if (update) {
                if (me.subscriptionMap[id]) {
                  me.onUpdateMessage(id, 'update', data, model, triggerName);
                }
              }
              break;
            case 'remove':
              if (remove) {
                if (me.subscriptionMap[id]) {
                  me.onNewMessage(id, 'remove', data, triggerName);
                }
              }
              break;
            default:
              break;
          }
        });
      }
      stream.on('end', function () { return me.unsubscribe(id); });
      stream.on('error', function () { return me.unsubscribe(id); });
    });

    if (refs && refs.length > 0) {
      const newRefs = [...refs, id];
      this.subsRefsMap[triggerName] = newRefs;
      return Promise.resolve(id);

    } else {
      return new Promise<number>((resolve, reject) => {
        // 1. Resolve options object
        this.subscribeOptionsResolver(trigger, options).then(subscriptionOptions => {

          // 2. Subscribing using MQTT
          this.mqttConnection.subscribe(triggerName, { qos: 0, ...subscriptionOptions }, (err, granted) => {
            if (err) {
              reject(err);
            } else {

              // 3. Saving the new sub id
              const subscriptionIds = this.subsRefsMap[triggerName] || [];
              this.subsRefsMap[triggerName] = [...subscriptionIds, id];

              // 4. Resolving the subscriptions id to the Subscription Manager
              resolve(id);

              // 5. Notify implementor on the subscriptions ack and QoS
              this.onMQTTSubscribe(id, granted);
            }
          });
        }).catch(err => reject(err));
      });
    }
  }

  public unsubscribe(subId: number) {
    const [triggerName = null] = this.subscriptionMap[subId] || [];
    const refs = this.subsRefsMap[triggerName];

    if (!refs) {
      throw new Error(`There is no subscription of id "${subId}"`);
    }

    let newRefs;
    if (refs.length === 1) {
      this.mqttConnection.unsubscribe(triggerName);
      newRefs = [];

    } else {
      const index = refs.indexOf(subId);
      if (index !== -1) {
        newRefs = [...refs.slice(0, index), ...refs.slice(index + 1)];
      }
    }

    this.subsRefsMap[triggerName] = newRefs;
    delete this.subscriptionMap[subId];
  }

  public asyncIterator(model, options) {
    return new PubSubAsyncIterator(this, model.name, model, options);
  }

  private onMessage(topic: string, message: Buffer) {
    const subscribers = this.subsRefsMap[topic];

    // Don't work for nothing..
    if (!subscribers || !subscribers.length) {
      return;
    }

    const messageString = message.toString(this.parseMessageWithEncoding);
    let parsedMessage;
    try {
      parsedMessage = JSON.parse(messageString);
    } catch (e) {
      parsedMessage = messageString;
    }

    for (const subId of subscribers) {
      const listener = this.subscriptionMap[subId][1];
      listener(parsedMessage);
    }
  }

  private onUpdateMessage(subId, event, object, model, triggerName) {
    const me = this;
    model.findById(object.target).then(function (obj) {
      const payload = {
        subscriptionId: subId,
        event: event,
        object: { data: obj },
      };
      me.publish(triggerName, payload);
    });
  }

  private onNewMessage(subId, event, object, triggerName) {
    const payload = {
      subscriptionId: subId,
      event: event,
      object: object,
    };
    this.publish(triggerName, payload);
  }


}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;
export type SubscribeOptionsResolver = (trigger: Trigger, channelOptions?: Object) => Promise<IClientSubscribeOptions>;
export type PublishOptionsResolver = (trigger: Trigger, payload: any) => Promise<IClientPublishOptions>;
export type SubscribeHandler = (id: number, granted: ISubscriptionGrant[]) => void;
