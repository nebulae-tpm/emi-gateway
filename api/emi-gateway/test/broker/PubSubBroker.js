// TEST LIBS
const assert = require('assert');
const Rx = require('rxjs');

//LIBS FOR TESTING
const PubSubBroker = require('../../broker/PubSubBroker');

//GLOABAL VARS to use between tests
let pubsubBroker = {};
let payload = { a: 1, b: 2, c: 3 };

process.env.GOOGLE_APPLICATION_CREDENTIALS = '/Users/sebastianmolano/NebulaE/Projects/TPM/emi-gateway/etc/gcloud-service-key.json';


describe('PUBSUB BROKER', function () {
    describe('Prepare PUBSUB broker', function () {
        it('instance PubSubBroker', function (done) {
            pubsubBroker = new PubSubBroker({
                replyTimeOut: 10000,
                projectId: 'ne-tpm-prod',
                gatewayRepliesTopic: 'emi-gateway-replies',
                gatewayRepliesTopicSubscription: 'emi-gateway-replies-test'
            });
            assert.ok(true, 'PubSubBroker constructor worked');
            return done();
        });
    });
    describe('Publish and listent on PubSub', function () {
        it('Publish and recive response using forward$ + getMessageReply$', function (done) {
            this.timeout(10000);
            pubsubBroker.forward$('Test','Test', payload)
                .switchMap((sentMessageId) => Rx.Observable.forkJoin(
                    //listen for the reply
                    pubsubBroker.getMessageReply$(sentMessageId, 9500, false),

                    //send a dummy reply, but wait a litle bit before send it so the listener is ready
                    Rx.Observable.of({})
                        .delay(1000)
                        .switchMap(() => pubsubBroker.forward$('emi-gateway-replies','Test', { x: 1, y: 2, z: 3 }, { correlationId: sentMessageId }))

                )).subscribe(
                    ([response, sentResponseMessageId]) => {
                        assert.deepEqual(response, { x: 1, y: 2, z: 3 });
                    },
                    error => {
                        return done(new Error(error));
                    },
                    () => {
                        return done();
                    }
                );
        });
        // it('Publish and recive response using forwardAndGetReply$', function (done) {

        //     const messageId = uuidv4();
        //     Rx.Observable.forkJoin(
        //         //send payload and listen for the reply
        //         mqttBroker.forwardAndGetReply$('Test', payload, 1800, false, { messageId }),

        //         //send a dummy reply, but wait a litle bit before send it so the listener is ready
        //         Rx.Observable.of({})
        //             .delay(200)
        //             .switchMap(() => mqttBroker.forward$('emi-gateway-replies-test', { x: 1, y: 2, z: 3 }, { correlationId: messageId }))
        //     ).subscribe(
        //         ([response, sentResponseMessageId]) => {
        //             assert.deepEqual(response, { x: 1, y: 2, z: 3 });
        //         },
        //         error => {
        //             return done(new Error(error));
        //         },
        //         () => {
        //             return done();
        //         }
        //     );
        // });
    });
    describe('de-prepare PubSub broker', function () {
        it('stop PubSubBroker', function (done) {
            pubsubBroker.disconnectBroker();
            assert.ok(true, 'PubSubBroker stoped');
            return done();
        });
    });
});
