'use strict'

const Rx = require('rxjs');
const uuidv4 = require('uuid/v4');

class PubSubBroker {

    constructor({ projectId, gatewayRepliesTopic, gatewayRepliesTopicSubscription, gatewayEventsTopic, gatewayEventsTopicSubscription, replyTimeOut, materializedViewTopic, materializedViewTopicSubscription }) {
        this.projectId = projectId
        this.gatewayRepliesTopic = gatewayRepliesTopic;
        this.gatewayRepliesTopicSubscription = gatewayRepliesTopicSubscription;
        this.gatewayEventsTopic = gatewayEventsTopic;
        this.gatewayEventsTopicSubscription = gatewayEventsTopicSubscription;
        this.materializedViewTopic = materializedViewTopic;
        this.materializedViewTopicSubscription = materializedViewTopicSubscription;
        this.replyTimeOut = replyTimeOut;

        /**
         * Rx Subject for every message reply
         */
        this.replies$ = new Rx.Subject();
        this.senderId = uuidv4();
        /**
         * Map of verified topics
         */
        this.verifiedTopics = {};

        const PubSub = require('@google-cloud/pubsub');
        this.pubsubClient = new PubSub({
            projectId: this.projectId,
        });
        //lets start listening to messages
        this.startMessageListener();
    }

    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param { {root,args,jwt} } message payload {root,args,jwt}
     * @param {Object} ops {correlationId, messageId} 
     */
    forward$(topic, type, payload, ops = {}) {
        return this.getTopic$(topic)
            .switchMap(topic => this.publish$(topic.name, type, payload, ops));
    }

    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param { {root,args,jwt} } message payload {root,args,jwt}
     * @param {number} timeout wait timeout millis
     * @param {boolean} ignoreSelfEvents ignore messages comming from this clien
     * @param {Object} ops {correlationId, messageId}
     * 
     * Returns an Observable that resolves the message response
     */
    forwardAndGetReply$(topic, type, payload, timeout = this.replyTimeout, ignoreSelfEvents = true, ops) {
        return this.forward$(topic, type, payload, ops)       
            .switchMap((messageId) => this.getMessageReply$(messageId, timeout, ignoreSelfEvents));
    }


    /**
    * Returns an observable that waits for the message response or throws an error if timeout is exceded
    * The observable extract the message.data and resolves to it
    * @param {string} correlationId 
    * @param {number} timeout 
    */
    getMessageReply$(correlationId, timeout = this.replyTimeout, ignoreSelfEvents = true) {
        //console.log('getMessageReply$ => data: ', new Date());
        return this.replies$
            //.do(val =>  console.log('getMessageReply **** '))
            .filter(msg => msg)
            .filter(msg => msg.topic === this.gatewayRepliesTopic)
            .filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            //.do(msg => console.log("msg.correlationId => ",msg.correlationId, " Correlation => ", correlationId))
            .filter(msg => msg && msg.correlationId === correlationId)
            .map(msg => msg.data)
            .timeout(timeout)
            .first();
    }

    /**
     * Returns an observable listen to events, and returns the entire message
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getEvents$(types, ignoreSelfEvents = true) {
        return this.replies$
            .filter(msg => msg)
            .filter(msg => msg.topic === this.gatewayEventsTopic)
            .filter(msg => types ? types.indexOf(msg.type) !== -1 : true)
            .filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            ;
    }

    /**
     * Publish data throught a topic
     * Returns an Observable that resolves to the sent message ID
     * @param {Topic} topicName 
     * @param {string} type message(payload) type
     * @param {Object} data 
     * @param {Object} ops {correlationId, messageId} 
     */
    publish$(topicName, type, data, { correlationId, messageId } = {}) {
        const dataBuffer = Buffer.from(JSON.stringify(data));

        return this.getTopic$(topicName)
            .mergeMap(topic => {
                return Rx.Observable.fromPromise(
                    topic.publisher().publish(dataBuffer,
                        {
                            senderId: this.senderId,
                            correlationId,
                            type,
                            replyTo: this.gatewayRepliesTopic
                        }));
            })
            .do(messageId => console.log(`Message published through ${topicName}, MessageId=${messageId}`, new Date()))
            ;
    }


    /**
     * Returns an observable listen to messages from MaterializedViewsUpdate topic.
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getMaterializedViewsUpdates$(types, ignoreSelfEvents = true) {
        //console.log('getMaterializedViewsUpdates$1 ', types);
        return this.replies$
            .filter(msg => msg)
            .filter(msg => msg.topic === this.materializedViewTopic)
            .filter(msg => types ? types.indexOf(msg.type) !== -1 : true)
            .filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId);
    }



    /**
     * Gets an observable that resolves to the topic object
     * @param {string} topicName 
     */
    getTopic$(topicName) {
        //Tries to get a cached topic
        const cachedTopic = this.verifiedTopics[topicName];
        if (!cachedTopic) {
            //if not cached, then tries to know if the topic exists
            const topic = this.pubsubClient.topic(topicName);
            return Rx.Observable.fromPromise(topic.exists())
                .map(data => data[0])
                .switchMap(exists => {
                    if (exists) {
                        //if it does exists, then store it on the cache and return it
                        this.verifiedTopics[topicName] = topic;
                        console.log(`Topic ${topicName} already existed and has been set into the cache`);
                        return Rx.Observable.of(topic);
                    } else {
                        //if it does NOT exists, then create it, store it in the cache and return it
                        return this.createTopic$(topicName);
                    }
                })
                ;
        }
        //return cached topic
        return Rx.Observable.of(cachedTopic);
    }

    /**
     * Creates a Topic and return an observable that resolves to the created topic
     * @param {string} topicName 
     */
    createTopic$(topicName) {
        return Rx.Observable.fromPromise(this.pubsubClient.createTopic(topicName))
            .switchMap(data => {
                this.verifiedTopics[topicName] = this.pubsubClient.topic(topicName);
                console.log(`Topic ${topicName} have been created and set into the cache`);
                return Rx.Observable.of(this.verifiedTopics[topicName]);
            });
    }



    /**
     * Returns an Observable that resolves to the subscription
     * @param {string} topicName 
     * @param {string} subscriptionName 
     */
    getSubscription$(topicName, subscriptionName) {
        return this.getTopic$(topicName)
            //.do(topic => console.log('getTopic => ', topic.name))
            .mergeMap(topic => Rx.Observable.fromPromise(
                topic.subscription(subscriptionName)
                    .get({ autoCreate: true }))
            ).map(results => {
                return {
                    subscription: results[0],
                    topicName,
                    subscriptionName
                };
            });
    }

    /**
     * Starts to listen messages
     */
    startMessageListener() {
        Rx.Observable.from([
            { topicName: this.gatewayRepliesTopic, topicSubscriptionName: this.gatewayRepliesTopicSubscription },
            { topicName: this.gatewayEventsTopic, topicSubscriptionName: this.gatewayEventsTopicSubscription },
            { topicName: this.materializedViewTopic, topicSubscriptionName: this.materializedViewTopicSubscription },])
            .mergeMap(({ topicName, topicSubscriptionName }) => this.getSubscription$(topicName, topicSubscriptionName))
            .subscribe(
                ({ subscription, topicName, subscriptionName }) => {
                    subscription.on(`message`, message => {
                        console.log('Received message', new Date(), topicName, message.attributes.correlationId, JSON.parse(message.data));
                        this.replies$.next(
                            {
                                topic: topicName,
                                id: message.id,
                                type: message.attributes.type,
                                data: JSON.parse(message.data),
                                attributes: message.attributes,
                                correlationId: message.attributes.correlationId
                            }
                        );
                        message.ack();
                        console.log('****** ACK MESSAGE ', message.attributes.correlationId);
                    });
                    console.log(`PubSubBroker is listening to ${topicName} under the subscription ${subscriptionName}`);
                },
                (err) => {
                    console.error(`Failed to obtain subscription `, err);
                },
                () => {
                    //console.log('GatewayReplies listener has completed!');
                }
            );
    }

    /**
     * Stops broker 
     */
    disconnectBroker() {
        this.getSubscription$(this.gatewayRepliesTopic, this.gatewayRepliesTopicSubscription).subscribe(
            ({ topicName, topicSubscriptionName }) => subscription.removeListener(`message`),
            (error) => console.error(`Error disconnecting Broker`, error),
            () => console.log('Broker disconnected')
        );
    }
}

module.exports = PubSubBroker;