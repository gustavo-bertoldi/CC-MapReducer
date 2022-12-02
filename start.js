const { GoogleAuth } = require('google-auth-library');
const { PubSub } = require('@google-cloud/pubsub');
const { exit } = require('process');

// Project configuration
const keyFile = './key.json';
const subscriptionName = 'projects/cloudcomputingcw-368814/subscriptions/FinishedTopic-sub';
const startFunctionTrigger = 'https://europe-west2-cloudcomputingcw-368814.cloudfunctions.net/start';
const bucketOutput = 'gs://mrcw/output/';
const httpBucket = 'https://storage.googleapis.com/mrcw/output/';


//Set up the pubsub client
const pubsub = new PubSub({ keyFilename: keyFile });

(async () => {
    const startTime = Date.now();
    const auth = new GoogleAuth({ keyFilename: keyFile });
    const client = await auth.getIdTokenClient(startFunctionTrigger);
    const res = await client.request({ url: startFunctionTrigger });
    console.info(res.data);

    // Subscribe to the finished topic to track pipeline progress
    const subscription = pubsub.subscription(subscriptionName);

    // Create an event handler to handle messages
    const messageHandler = message => {
        console.log(`Pipeline finished successfully after ${Date.now() - startTime}ms`);
        const outputFile = message.data.toString().split(' ')[1];
        console.log(`Result in ${bucketOutput}${outputFile}.txt`);
        console.log(`HTTP access: ${httpBucket}${outputFile}.txt`);
        subscription.removeListener('message', messageHandler);
        message.ack();
        exit();
    };

    // Listen for new messages until timeout is hit
    subscription.on('message', messageHandler);

    // 3 min timeout
    setTimeout(() => {
        subscription.removeListener('message', messageHandler);
        console.error("MapReduce timed out");
    }, 180000);
})();
