const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub'); 
const crypto = require('crypto');
require('dotenv').config();

if (!process.env.PROJECT_ID) throw new Error("PROJECT_ID environment variable not set");
if (!process.env.BUCKET_NAME) throw new Error("BUCKET_NAME environment variable not set");
if (!process.env.MAPPER_INPUT_TOPIC) throw new Error("MAPPER_INPUT_TOPIC environment variable not set");
if (!process.env.SHUFFLER_INPUT_TOPIC) throw new Error("SHUFFLER_INPUT_TOPIC environment variable not set");
if (!process.env.REDUCER_INPUT_TOPIC) throw new Error("REDUCER_INPUT_TOPIC environment variable not set");
if (!process.env.CLEANER_TOPIC) throw new Error("CLEANER_TOPIC environment variable not set");
if (!process.env.STOP_WORDS_PATH) throw new Error("STOP_WORDS_PATH environment variable not set");
if (!process.env.INPUT_PATH) throw new Error("INPUT_PATH environment variable not set");
if (!process.env.OUTPUT_PATH) throw new Error("OUTPUT_PATH environment variable not set");
if (!process.env.SHUFFLER_HASH_MODULO) process.env.SHUFFLER_HASH_MODULO = "5";

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
const cleanerTopic = pubsub.topic(process.env.CLEANER_TOPIC);

/**
 * Download stop words from Google Cloud Storage. Uses a set for efficient lookups
 * @return {Set<string>} The set of stop words
 */
async function getStopWords() {
    const stopWords = await bucket.file(process.env.STOP_WORDS_PATH).download();
    return new Set(stopWords.toString().split(','));
}

/**
 * Filters the input string to remove stop words and non-alphabetic characters.
 * Returns a comma separated string of valid words to be used as input of the mappers.
 * @param {string} str the input file in string format
 * @param {Set<string>} stopWords the set of stop words
 * @returns {string} the filtered string
 */
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
/**
 * Return a comma separated string containing a pair of words in the format
 * word1:word2. Where word1 is the alphabetically sorted word and word2 is the
 * original word. To be used as input for the shuffler.
 * @param {string} input mapper input in the format of a comma separated string of words
 * @returns {string} 
 */
function _map(input) {
    return input.split(',').map(word => {
        const alphabeticalOrder = [...word].sort().join('');
        return `${alphabeticalOrder}:${word}`;
    }).join(',');
}

/**
 * Processes an input in the format of a comma separated string of pairs of words
 * in the format word1:word2. Where word1 is the alphabetically sorted word and
 * word2 is the original word. Using the hash of the sorted word, groups the words
 * into buckets, where the same words always go to the same bucket. The output is
 * an array of length nbOutputs where each element is a comma separated string in the
 * same format as the input.
 * @param {string} input 
 * @param {number} nbOutputs 
 * @returns {string[]}
 */
function _shuffle(input, nbOutputs = parseInt(process.env.SHUFFLER_HASH_MODULO)) {
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

/**
 * 
 * @param {string} input 
 * @returns {{string: Set<string>}}
 */
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
    // Create temporary output directory for this run
    console.log('Starting pipeline...');
    const tmpOutputPath = crypto.randomBytes(4).toString("hex") + '/';
    console.log(`Pipeline tmp output path: ${tmpOutputPath}`);

    // Download stop words from Google Cloud Storage
    const stopWords = await getStopWords();

    // Read all files from the input directory
    const files = (await bucket.getFiles({prefix: process.env.INPUT_PATH}))[0]
        .filter(file => file.name.endsWith('.txt'));

    files.forEach(async (file, idx) => {
        const data = await file.download();
        const output = _read(data[0].toString(), stopWords);
        const outputFileName = `map_${idx}`;
        const outputFilePath = tmpOutputPath + outputFileName;

        bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 })
                .then(() => {
                    const jsonMessage = {
                        targetFile: outputFilePath,
                        nbInputs: files.length
                    }
                    mapperTopic.publishMessage({json: jsonMessage})
                        .catch(err => console.error(err));
                    if (idx === files.length - 1) 
                        res.status(200).send(`Reading completed. Generated ${idx + 1} mapper inputs.`);
                });
    });
};

// Triggered by a message to the mapper input topic containing the name of the file
// to be mapped. On conclusion, published a message to the shuffler input topic
// containing the file with the mapped content.
exports.map = async (message, context, callback) => {
    // Get trigger parameters
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const targetFile = _message.targetFile;

    console.log("Mapping file: ", targetFile);
    const data = (await bucket.file(targetFile).download())[0].toString();
    const output = _map(data);
    const outputFilePath = `${targetFile.split('/')[0]}/shuf_${targetFile.split('_')[1]}`;
    await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });
    const jsonMessage = {
        targetFile: outputFilePath,
        nbInputs: _message.nbInputs
    }
    await shufflerTopic.publishMessage({json: jsonMessage});
    console.log("File mapped and published to shuffler: ", outputFilePath);
};


exports.shuffle = async (message, context, callback) => {
    // Get trigger parameters
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const targetFile = _message.targetFile;

    console.log("Shuffling file: ", targetFile);
    const data = (await bucket.file(targetFile).download())[0].toString();
    const outputs = _shuffle(data);
    const outputFilePrefix = `${targetFile.split('/')[0]}/red_${targetFile.split('_')[1]}_`;
    
    // Save all outputs to a separate file in Google Cloud Storage
    await Promise.all(outputs.map((output, idx) => 
        bucket.file(outputFilePrefix + idx).save(output, { resumable: false, timeout: 30000 })));
    console.log('File shuffled: ', outputFilePrefix + '_x');

    //Verify all shufflers have finished
    const shufflerOutputPrefix = `${targetFile.split('/')[0]}/red_`;
    const expectedShufflerOutputs = _message.nbInputs * process.env.SHUFFLER_HASH_MODULO;
    const shufflerOutputs = (await bucket.getFiles({prefix: shufflerOutputPrefix}))[0].length;
    
    if (shufflerOutputs === expectedShufflerOutputs) {
        // Trigger reducers
        for (let i = 0; i < _message.nbInputs; i++) {
            const jsonMessage = {
                targetPrefix:  `${targetFile.split('/')[0]}/red_${i}`,
                nbInputs: _message.nbInputs
            }
            await reducerTopic.publishMessage({json: jsonMessage});
        }
        console.log('All shufflers finished and published to reducer.');
    }
};

exports.reduce = async (message, context, callback) => {
    // Get trigger parameters
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const files = (await bucket.getFiles({prefix: _message.targetPrefix}))[0];

    console.log("Reducing files with prefix: ", `${_message.targetPrefix}`);

    // Download and concatenate all the files
    const downloads = files.map(file => file.download());
    const data = (await Promise.all(downloads)).map(d => d[0].toString()).join('');

    // Reduce the data
    const output = _reduce(data);

    // Write the output to the output directory in Google Cloud Storage
    const outputFilePath = `${_message.targetPrefix.split('/')[0]}/result_${_message.targetPrefix.split('_')[1]}`;
    await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });
    console.log('File reduced: ', outputFilePath);

    //Check all reducers finished
    const reducerOutputPrefix = `${_message.targetPrefix.split('/')[0]}/result_`;
    await bucket.getFiles({prefix: reducerOutputPrefix});
    if (files.length === _message.nbInputs) {
        console.log('All reducers finished. Starting cleanup...');
        const jsonMessage = {
            targetPrefix: _message.targetPrefix.split('/')[0]
        }
        await cleanerTopic.publishMessage({json: jsonMessage});
    }
}

exports.clean = async (message, context, callback) => {
    // Get trigger parameters
    const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
    const reducerOutputPrefix = `${_message.targetPrefix}/result_`;
    const files = (await bucket.getFiles({prefix: reducerOutputPrefix}))[0];
    const finalResult = (await Promise.all(files.map(file => file.download())))
        .map(d => d[0].toString()).join('');

    // Write the output to the output directory in Google Cloud Storage
    const outputName = `${_message.targetPrefix}.txt`;
    const outputFilePath = process.env.OUTPUT_PATH + outputName;
    await bucket.file(outputFilePath).save(finalResult, { resumable: false, timeout: 30000 });
    console.log('Final result saved to: ', outputFilePath);

    // Delete all temporary files
    console.log("Cleaning up...");
    await bucket.deleteFiles({prefix: _message.targetPrefix});
}