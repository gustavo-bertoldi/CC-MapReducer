const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub'); 

//Set up the storage client and read input files
const storage = new Storage();
const bucket = storage.bucket('mrcw');

//Set up the pubsub client and topic
const pubsub = new PubSub();
const topic = pubsub.topic('MapperInput');

//Download stop words from Google Cloud Storage
async function getStopWords() {
    const stopWords = await bucket.file('config/StopWords').download();
    return stopWords.toString().split(',');
}

exports.read = async (req, res) => {
    const stopWords = await getStopWords();
    const files = await bucket.getFiles({prefix: 'input/'});
    let count = 0;
    files[0].forEach(file => {
        file.download((err, contents) => {
            if (err) {
                console.error(err);
                return;
            } else count += 1;

            topic.publishMessage({ data: contents }).then(() => {
                if (count == files[0].length) 
                    res.status(200).send("Done");
                console.log(`Message for file [${i}] published`);
            }).catch(err => {
                console.error(err);
            })
        });
    });
};
  
