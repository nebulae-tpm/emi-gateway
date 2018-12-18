'use strict'

var mqtt = require('mqtt');
const { map, switchMap, filter, first, timeout } = require('rxjs/operators');
const { of, BehaviorSubject } = require('rxjs');
const uuidv4 = require('uuid/v4');

class MqttBroker {

    constructor({ gatewayRepliesTopic, mqttServerUrl, replyTimeout, gatewayEventsTopic, materializedViewTopic }) {
        this.gatewayRepliesTopic = gatewayRepliesTopic;
        this.gatewayEventsTopic = gatewayEventsTopic;
        this.materializedViewTopic = materializedViewTopic;
        this.mqttServerUrl = mqttServerUrl;
        this.senderId = uuidv4();
        this.replyTimeout = replyTimeout;
        /**
         * Rx Subject for every message reply
         */
        this.replies$ = new BehaviorSubject();

        /**
         * MQTT Client
         */
        this.mqttClient = mqtt.connect(this.mqttServerUrl);
        this.configMessageListener();
    }



    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param {Object} message payload {root,args,jwt}
     * @param {Object} ops {correlationId, messageId} 
     */
    forward$(topic, type, payload, ops = {}) {
        return this.publish$(topic, type, payload, ops);
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
        .pipe(
            switchMap((messageId ) => this.getMessageReply$(messageId, timeout, ignoreSelfEvents))
        );
    }


    /**
     * Returns an observable that waits for the message response or throws an error if timeout is exceded
     * The observable extract the message.data and resolves to it
     * @param {string} correlationId 
     * @param {number} timeoutLimit 
     */
    getMessageReply$(correlationId, timeoutLimit = this.replyTimeout, ignoreSelfEvents = true) {
        return this.replies$
        .pipe(
            filter(msg => msg),
            filter(msg => msg.topic === this.gatewayRepliesTopic),
            filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId),
            filter(msg => msg && msg.correlationId === correlationId),
            map(msg => msg.data),
            timeout(timeoutLimit),
            first()
        );
            
    }

    /**
     * Returns an observable listen to events, and returns the entire message
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getEvents$(types, ignoreSelfEvents = true) {
        return this.replies$
            .pipe(
                filter(msg => msg),
                filter(msg => msg.topic === this.gatewayEventsTopic),
                filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            )

    }

    /**
     * Returns an observable listen to messages from MaterializedViewsUpdate topic.
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getMaterializedViewsUpdates$(types, ignoreSelfEvents = true) {
        return this.replies$
        .pipe(
            filter(msg => msg),
            filter(msg => msg.topic === this.materializedViewTopic),
            filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
            filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
        )            
    }

    /**
     * Publish data throught a topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {string} type message(payload) type
     * @param {Object} data 
     * @param {Object} ops {correlationId, messageId} 
     */
    publish$(topicName, type, data, { correlationId, messageId } = {}) {
        const uuid = messageId || uuidv4();
        const dataBuffer = JSON.stringify(
            {
                id: uuid,
                type: type,
                data,
                attributes: {
                    senderId: this.senderId,
                    correlationId,
                    replyTo: this.gatewayRepliesTopic
                }
            }
        );

        return of({})
            .pipe(
                map(() => {
                    this.mqttClient.publish(`${topicName}`, dataBuffer, { qos: 0 });
                    return uuid;
                })
            );
    }


    /**
     * Configure to listen messages
     */
    configMessageListener() {

        const that = this;

        this.mqttClient.on('connect', function () {
            that.mqttClient.subscribe(`${that.gatewayRepliesTopic}`);
            that.mqttClient.subscribe(`${that.gatewayEventsTopic}`);
            that.mqttClient.subscribe(`${that.materializedViewTopic}`);
            console.log(`Mqtt client subscribed to ${that.gatewayRepliesTopic},  ${that.gatewayEventsTopic} and ${that.materializedViewTopic}`);
        });

        this.mqttClient.on('message', function (topic, message) {
            const envelope = JSON.parse(message);
            // message is Buffer
            that.replies$.next(
                {
                    topic: topic,
                    id: envelope.id,
                    type: envelope.type,
                    data: envelope.data,
                    attributes: envelope.attributes,
                    correlationId: envelope.attributes.correlationId
                }
            );
        })
    }

    /**
     * Stops broker 
     */
    disconnectBroker() {
        this.mqttClient.end();
    }
}

module.exports = MqttBroker;