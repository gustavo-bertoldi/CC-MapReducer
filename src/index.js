const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub'); 
const crypto = require('crypto');
require('dotenv').config();

if (!process.env.PROJECT_ID) throw new Error("PROJECT_ID environment variable not set");
if (!process.env.BUCKET_NAME) throw new Error("BUCKET_NAME environment variable not set");
if (!process.env.MAPPER_INPUT_TOPIC) throw new Error("MAPPER_INPUT_TOPIC environment variable not set");
if (!process.env.SHUFFLER_INPUT_TOPIC) throw new Error("SHUFFLER_INPUT_TOPIC environment variable not set");
if (!process.env.REDUCER_INPUT_TOPIC) throw new Error("REDUCER_INPUT_TOPIC environment variable not set");
if (!process.env.STOP_WORDS_PATH) throw new Error("STOP_WORDS_PATH environment variable not set");
if (!process.env.INPUT_PATH) throw new Error("INPUT_PATH environment variable not set");
if (!process.env.OUTPUT_PATH) throw new Error("OUTPUT_PATH environment variable not set");
if (!process.env.SHUFFLER_HASH_MODULO) process.env.SHUFFLER_HASH_MODULO = "10";

const config = {
    projectId: process.env.PROJECT_ID,
    keyFilename: process.env.KEY_FILE
}

//Set up the storage client and read input files
const storage = new Storage(config);
const bucket = storage.bucket(process.env.BUCKET_NAME);

//Set up the pubsub client and topics
const pubsub = new PubSub(config);
const mapperTopic = pubsub.topic(process.env.MAPPER_INPUT_TOPIC);
const shufflerTopic = pubsub.topic(process.env.SHUFFLER_INPUT_TOPIC);
const reducerTopic = pubsub.topic(process.env.REDUCER_INPUT_TOPIC);

// Download stop words from Google Cloud Storage
// Uses a set for efficient lookups
async function getStopWords() {
    const stopWords = await bucket.file(process.env.STOP_WORDS_PATH).download();
    return new Set(stopWords.toString().split(','));
}

// Filters the input string to remove stop words and non-alphabetic characters.
// Returns a comma separated string of valid words to be used as input of the mappers.
function _read(str, stopWords) {
    return str.toLowerCase()
        .replace(/'/,'') // Remove apostrophes
        .replace(/[^a-z]+/g, ' ') // Replace non-alphabetic characters with spaces
        .split(' ') 
        .filter(word => word.length > 1 && !stopWords.has(word))
        .join(',');
}

// Return a comma separated string containing a pair of words in the format
// word1:word2. Where word1 is the alphabetically sorted word and word2 is the
// original word. To be used as input for the shuffler.
function _map(input) {
    return input.split(',').map(word => {
        const alphabeticalOrder = [...word].sort().join('');
        return `${alphabeticalOrder}:${word}`;
    }).join(',');
}

// Processes an input in the format of a comma separated string of pairs of words
// in the format word1:word2. Where word1 is the alphabetically sorted word and
// word2 is the original word. Using the hash of the sorted word, groups the words
// into buckets, where the same words always go to the same bucket. The output is
// an array of length nbOutputs where each element is a comma separated string in the
// same format as the input.
function _shuffle(input, nbOutputs = process.env.SHUFFLER_HASH_MODULO) {
    return input.split(',').reduce((acc, pair, idx) => {
        const [sorted, _] = pair.split(':');
        const hash = crypto.createHash('md5').update(sorted).digest('hex'); 
        const hashIdx = parseInt(hash, 16) % nbOutputs;
        if (!acc[hashIdx]) acc[hashIdx] = '';
        if (idx != 0) acc[hashIdx] += ',';
        acc[hashIdx] += pair;
        return acc;
    }, new Array(nbOutputs));
};

function _reduce(input) {
    const map = input.split(',').reduce((acc, pair) => {
        if (!pair) return acc;
        const [sorted, word] = pair.split(':');
        if (!acc[sorted]) acc[sorted] = new Set();
        acc[sorted].add(word);
        return acc;
    }, {});

    return Object.keys(map).reduce((acc, key) => {
        if (map[key].size > 1) {
            acc += `${key}: { ${[...map[key]].join(', ')} }\n`;
        }
        return acc;
    }, "");
};

// Reads all files from the input directory, filters the words and writes the 
// output in format as a comma separated string of valid words to the output 
// directory to be used as input for the mappers.
exports.read = async (req, res) => {
    const stopWords = await getStopWords();
    const files = (await bucket.getFiles({prefix: process.env.INPUT_PATH}))[0]
        .filter(file => file.name.endsWith('.txt'));
    files.forEach((file, idx) => {
        file.download((err, data) => {
            if (err) {
                console.error(err);
                res.status(500).send(err);
                return;
            }

            const output = _read(data.toString(), stopWords);
            const outputFileName = `map_${idx}`;
            const outputFilePath = `${process.env.OUTPUT_PATH}${outputFileName}`;
            bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 })
                .then(() => {
                    const jsonMessage = {
                        targetFile: outputFileName,
                        nbInputs: files.length
                    }
                    mapperTopic.publishMessage({json: jsonMessage})
                        .catch(err => console.error(err));
                    if (idx === files.length - 1) 
                        res.status(200).send(`Reading completed. Generated ${idx + 1} mapper inputs.`);
                });
        });
    });
};

// Triggered by a message to the mapper input topic containing the name of the file
// to be mapped. On conclusion, published a message to the shuffler input topic
// containing the file with the mapped content.
exports.map = (message, context, callback) => {
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const fileName = _message.targetFile;
    console.log("Mapping file: ", fileName);
    bucket.file(`${process.env.OUTPUT_PATH}${fileName}`).download((err, data) => {
        if (err) {
            console.error(err);
            return - 1;
        }

        const output = _map(data.toString());
        const outputFileName = `shuf_${fileName.split('_')[1]}`;
        const outputFilePath = `${process.env.OUTPUT_PATH}${outputFileName}`;
        bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 })
            .then(() => {
                const jsonMessage = {
                    targetFile: outputFileName,
                    nbInputs: _message.nbInputs
                } 
                shufflerTopic.publishMessage({json: jsonMessage})
                .then(() => {
                    return 1;
                })
                .catch(err => {
                    console.error(err);
                    return -1;
                })
            });
    });
};


exports.shuffle = (message, context, callback) => {
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const fileName = _message.targetFile;
    console.log("Shuffling file: ", fileName);
    bucket.file(`${process.env.OUTPUT_PATH}${fileName}`).download((err, data) => {
        if (err) {
            console.error(err);
            return;
        }

        const outputs = _shuffle(data.toString());
        outputs.forEach((output, idx) => {
            const outputFilePrefix = `red_${fileName.split('_')[1]}_`;
            const outputFileName = outputFilePrefix + idx;
            const outputFilePath = `${process.env.OUTPUT_PATH}${outputFileName}`;
            bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 })
                .then(async () => {
                    console.log(`Shuffled to ${outputFileName}`);

                    // Verify if all shufflers have finished to trigger the reducers
                    const shufflerOutputsPrefix = `${process.env.OUTPUT_PATH}red_`;
                    const nbShufflerOutputs = (await bucket.getFiles({prefix: shufflerOutputsPrefix}))[0].length;
                    const expectedShufflerOutputs = _message.nbInputs * process.env.SHUFFLER_HASH_MODULO;
                    if (idx === outputs.length - 1 && nbShufflerOutputs === expectedShufflerOutputs) {
                        // Publish message to trigger all the reducers
                        for (let i = 0; i < _message.nbInputs; i++) {
                            const jsonMessage = {
                                targetPrefix: outputFilePrefix,
                                nbInputs: _message.nbInputs
                            }
                            reducerTopic.publishMessage({json: jsonMessage})
                                .catch(err => console.error(err));
                        }
                    }
                });
        });
    });
};

exports.reduce = async (message, context, callback) => {
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const targetPrefix = _message.targetPrefix;
    const files = (await bucket.getFiles({prefix: targetPrefix}))[0];

    console.log("Reducing files: ", `${targetPrefix}_x`);

    // Download and concatenate all the files
    const downloads = files.map(file => file.download());
    const data = (await Promise.all(downloads)).map(d => d[0].toString()).join('');

    // Reduce the data
    const output = _reduce(data);

    // Write the output to the output directory in Google Cloud Storage
    const outputFileName = `result_${targetPrefix.split('_')[1]}`;
    const outputFilePath = `${process.env.OUTPUT_PATH}${outputFileName}`;
    bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 })
        .then(() => {
            console.log(`Reduced to ${outputFileName}`);
            return 1;
        }).catch(err => {
            console.error("Failed to save output file: ", err);
            return -1;
        });
}

exports.clean = async (message, context, callback) => {
    console.log("Cleaning up...");
    
}