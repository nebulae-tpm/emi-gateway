'use strict';

let instance = null;
class BrokerFactory {
  constructor() {
    //Service Port
    const BROKER_TYPE = process.env.BROKER_TYPE || 'PUBSUB';

    switch (BROKER_TYPE) {
      case 'PUBSUB':
      
        const PubSubBroker = require('./PubSubBroker');
        this.broker = new PubSubBroker({
          replyTimeOut: process.env.REPLY_TIMEOUT || 2000,
          projectId: process.env.GCLOUD_PROJECT_ID,
          gatewayRepliesTopic: process.env.GATEWAY_REPLIES_TOPIC,
          gatewayRepliesTopicSubscription: process.env.GATEWAY_REPLIES_TOPIC_SUBSCRIPTION,
          gatewayEventsTopic: process.env.GATEWAY_EVENTS_TOPIC,
          gatewayEventsTopicSubscription: process.env.GATEWAY_EVENTS_TOPIC_SUBSCRIPTION,
          materializedViewTopic : process.env.GATEWAY_MATERIALIZED_VIEW_UPDATES_TOPIC,
          materializedViewTopicSubscription : process.env.GATEWAY_MATERIALIZED_VIEW_UPDATES_TOPIC_SUBSCRIPTION,
        });
        break;
      case 'MQTT':
        const MqttBroker = require('./MqttBroker');
        this.broker = new MqttBroker({
          gatewayRepliesTopic: process.env.GATEWAY_REPLIES_TOPIC,
          gatewayEventsTopic: process.env.GATEWAY_EVENTS_TOPIC,
          materializedViewTopic : process.env.GATEWAY_MATERIALIZED_VIEW_UPDATES_TOPIC,
          projectId: process.env.GCLOUD_PROJECT_ID,
          mqttServerUrl: process.env.MQTT_SERVER_URL,
          replyTimeout: process.env.REPLY_TIMEOUT ||  2000
        });
        break;
    }
  }
  /**
   * Get the broker instance
   */
  getBroker() {
    return broker;
  }
}

module.exports = () => {
  if (!instance) {
    instance = new BrokerFactory();
    console.log('NEW instance!!');
  }
  return instance.broker;
};