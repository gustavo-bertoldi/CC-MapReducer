const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub'); 

//Set up the storage client and read input files
const storage = new Storage();
const bucket = storage.bucket('mrcw');

//Set up the pubsub client and topic
const pubsub = new PubSub();
const topic = pubsub.topic('projects/cloudcomputingcw-368814/topics/MapperInput');

//Download stop words from Google Cloud Storage
async function getStopWords() {
    const stopWords = await bucket.file('config/StopWords').download();
    return stopWords.toString().split(',');
}

bucket.getFiles({prefix: 'input/'}).then(async (files) => {
    const stopWords = await getStopWords();
    files[0].forEach((file) => {
        file.download((err, contents) => {
            if (err) {
                console.error(err);
                return;
            }

            const json = {
                stopWords: stopWords,
                contents: contents.toString()
            };
            topic.publishMessage({ json });
        })
    });
});
