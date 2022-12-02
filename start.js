const { GoogleAuth } = require('google-auth-library');
const { PubSub } = require('@google-cloud/pubsub');

//Set up the pubsub client
const pubsub = new PubSub({ keyFilename: 'key.json' });

(async () => {
    const auth = new GoogleAuth({ keyFilename: 'key.json' });
    const startFunction = 'https://europe-west2-cloudcomputingcw-368814.cloudfunctions.net/start';
    const client = await auth.getIdTokenClient(startFunction);
    const res = await client.request({ url: startFunction });
    console.info(res.data);

    // References an existing subscription
    const subscriptionName = 'projects/cloudcomputingcw-368814/subscriptions/FinishedTopic-sub';
    const subscription = pubSubClient.subscription(subscriptionName);

    // Create an event handler to handle messages
    const messageHandler = message => {
        console.log(`Received message ${message.id}:`);
        console.log(`\tData: ${message.data}`);
        console.log(`\tAttributes: ${message.attributes}`);
        message.ack();
        subscription.removeListener('message', messageHandler);
    };

    // Listen for new messages until timeout is hit
    subscription.on('message', messageHandler);
})();
