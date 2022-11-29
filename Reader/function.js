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
    return new Set(stopWords.toString().split(','));
}

function isValidWord(word) {
    return word.length > 0 
        && word.match(/[a-z]+/);
    
}

exports.read = async (req, res) => {
    const stopWords = await getStopWords();
    const files = await bucket.getFiles({prefix: 'input/'});
    files[0].forEach((file, idx) => {
        file.download((err, contents) => {
            if (err) {
                console.error(err);
                return;
            }

            //Filter the words by the following criteria
            //1. Remove stop words
            //2. Remove words with non-alphabetic characters
            //3. Remove words with length less than 1
            const wordsOutput = contents.toString().split(' ')
                .map(word => word.toLowerCase())
                .filter(word => isValidWord(word) && !stopWords.has(word))
                .join(',');
            
            //Write to Google Storage
            const fileName = 'mapper_input_' + idx;
            bucket.file(`reader_output/${fileName}`).save(wordsOutput, {
                resumable: false,
                timeout: 30000
            }).then(() => {
                //Check finished
                if (idx === files[0].length - 1) res.status(200).send('DONE');
            });
        });
    });
};
  
