const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
const crypto = require('crypto');
require('dotenv').config();

if (!process.env.PROJECT_ID) throw new Error("PROJECT_ID environment variable not set");
if (!process.env.BUCKET_NAME) throw new Error("BUCKET_NAME environment variable not set");
if (!process.env.READER_INPUT_TOPIC) throw new Error("READER_INPUT_TOPIC environment variable not set");
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

//Set up the pubsub client
const pubsub = new PubSub(config);

/**
 * Filters the input string to remove stop words and non-alphabetic characters.
 * Returns a comma separated string of valid words to be used as input of the mappers.
 * @param {string} str the input file in string format
 * @param {Set<string>} stopWords the set of stop words
 * @returns {string} the filtered string
 */
function _read(str, stopWords) {
    return str.toLowerCase()
        .replace(/'/, '') // Remove apostrophes
        .replace(/[^a-z]+/g, ' ') // Replace non-alphabetic characters with spaces
        .split(' ')
        .filter(word => word.length > 1 && !stopWords.has(word))
        .join(',');
}

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
    console.log('Shuffling with hash modulo', nbOutputs);
    const res = input.split(',').reduce((acc, pair, idx) => {
        const [sorted, _] = pair.split(':');
        const hash = crypto.createHash('md5').update(sorted).digest('hex');
        const hashIdx = parseInt(hash, 16) % nbOutputs;
        if (!acc[hashIdx]) acc[hashIdx] = '';
        if (idx != 0) acc[hashIdx] += ',';
        acc[hashIdx] += pair;
        return acc;
    }, new Array(nbOutputs));
    console.log(res);
    return res;
};

/**
 * 
 * @param {string} input 
 * @returns {string}
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
            acc += `${key}: { ${[...map[key]].sort().join(', ')} }\n`;
        }
        return acc;
    }, "");
};

exports.start = async (req, res) => {
    try {
        console.log('Starting MapReduce pipeline...');

        // Initialize the topic to publish messages to the reader
        const readerTopic = pubsub.topic(process.env.READER_INPUT_TOPIC);
    
        // Generate an unique id for this pipeline run and create a temporary output directory
        const tmpOutputDir = crypto.randomBytes(4).toString("hex") + '/';
        console.log(`Pipeline temporary output path: ${tmpOutputDir}`);
    
        //Download stop words file
        const stopWords = (await bucket.file(process.env.STOP_WORDS_PATH).download({ timeout: 30000 })).toString();
    
        //Trigger one reader for each file by publishing a message to the reader topic
        const inputs = (await bucket.getFiles({ prefix: process.env.INPUT_PATH }))[0]
            .filter(file => file.name.endsWith('.txt'));
        inputs.forEach(async file => {
                const jsonMessage = {
                    targetFile: file.name,
                    outputDir: tmpOutputDir,
                    nbInputs: inputs.length,
                    stopWords: stopWords
                };
                await readerTopic.publishMessage({ json: jsonMessage });
            });
        res.status(200).send(`Pipeline started for ${inputs.length} files\nPipeline temporary output path: ${tmpOutputDir}`);
    } catch (err) {
        console.error(err);
        res.status(500).send(err);
    }
    
};

/**
 * Reads all files from the input directory, filters the words and writes the 
 * output in format as a comma separated string of valid words to the output 
 * directory to be used as input for the mappers.
 * @param {*} message 
 * @param {*} context 
 * @param {*} callback 
 */
exports.read = async (message, context, callback) => {
    try {
        // Initialize the topic to publish messages to the mapper
        const mapperTopic = pubsub.topic(process.env.MAPPER_INPUT_TOPIC);

        // Parse message from start function
        const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
        const stopWords = new Set(_message.stopWords.split(',')); // Set for better lookup performance

        // Download target file
        const data = (await bucket.file(_message.targetFile).download({ timeout: 30000 }))[0].toString();

        // Generate mapper input and save it to the output directory
        const output = _read(data, stopWords);
        const outputFileName = `map_${_message.targetFile.split('/').pop().split('.')[0]}`;
        const outputFilePath = _message.outputDir + outputFileName;
        await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });

        // Trigger the mapper by publishing a message
        const jsonMessage = {
            targetFile: outputFilePath,
            nbInputs: _message.nbInputs,
            outputDir: _message.outputDir
        };
        await mapperTopic.publishMessage({ json: jsonMessage });
        console.log('Finished read for file: ', _message.targetFile);
        callback();
    } catch (err) {
        console.error(err);
        callback(err);
    }
};


/**
 * Triggered by a message to the mapper input topic containing the name of the file
 * to be mapped. On conclusion, published a message to the shuffler input topic
 * containing the file with the mapped content.
 * @param {*} message 
 * @param {*} context 
 * @param {*} callback 
 */
exports.map = async (message, context, callback) => {
    try {
        // Parse message from reader
        const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());

        console.log("Mapping file: ", _message.targetFile);

        // Download target file
        const data = (await bucket.file(_message.targetFile).download({ timeout: 30000 }))[0].toString();

        // Generate shuffler input and save it to the output directory
        const output = _map(data);
        const outputFilePath = _message.targetFile.replace('map_', 'shuf_');
        await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });

        // Trigger the shuffler by publishing a message
        const jsonMessage = {
            targetFile: outputFilePath,
            nbInputs: _message.nbInputs,
            outputDir: _message.outputDir
        }
        const shufflerTopic = pubsub.topic(process.env.SHUFFLER_INPUT_TOPIC);
        await shufflerTopic.publishMessage({ json: jsonMessage });

        console.log("Finished mapping for file ", _message.targetFile);
        callback();
    } catch (err) {
        console.error(err);
        callback(err);
    }
};

/**
 * 
 * @param {*} message 
 * @param {*} context 
 * @param {*} callback 
 */
exports.shuffle = async (message, context, callback) => {
    try {
        // Parse message from mapper
        const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());

        console.log("Shuffling file: ", _message.targetFile);

        // Download target file
        const data = (await bucket.file(_message.targetFile).download({ timeout: 30000 }))[0].toString();

        // Generate reducer inputs
        const outputs = _shuffle(data);

        const outputFilesPrefix = _message.targetFile.replace('shuf_', 'red_') + '_';

        // Save all outputs to a separate file in Google Cloud Storage
        await Promise.all(outputs.map((output, idx) =>
            bucket.file(outputFilesPrefix + idx).save(output, { resumable: false, timeout: 30000 })));
        console.log('Finished shuffling for file: ', _message.targetFile);

        //Verify all shufflers have finished
        const expectedShufflerOutputs = _message.nbInputs * process.env.SHUFFLER_HASH_MODULO;
        const shufflerOutputs = (await bucket.getFiles({ prefix: outputFilesPrefix.split('_')[0] }))[0].length;
        console.log('Shuffler outputs: ', shufflerOutputs, ' Expected: ', expectedShufflerOutputs);
        if (shufflerOutputs === expectedShufflerOutputs) {
            // All shufflers have finished, trigger the reducers
            const reducerTopic = pubsub.topic(process.env.REDUCER_INPUT_TOPIC);
            for (let i = 0; i < process.env.SHUFFLER_HASH_MODULO; i++) {
                const jsonMessage = {
                    targetIdx: i,
                    nbInputs: _message.nbInputs,
                    outputDir: _message.outputDir
                }
                await reducerTopic.publishMessage({ json: jsonMessage });
            }
            console.log('All shufflers finished, triggered reducers');
        }
        callback();
    } catch (err) {
        console.error(err);
        callback(err);
    }
};

/**
 * 
 * @param {*} message 
 * @param {*} context 
 * @param {*} callback 
 */
exports.reduce = async (message, context, callback) => {
    try {
        // Get trigger parameters
        const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
        const reducerPrefix = _message.outputDir + 'red_';
        const files = (await bucket.getFiles({ prefix: reducerPrefix }))[0]
            .filter(file => file.name.endsWith('_' + _message.targetIdx));

        console.log("Reducing for hash index: ", `${_message.targetIdx}`);

        // Download and concatenate all the files
        const downloads = files.map(file => file.download({ timeout: 30000 }));
        const data = (await Promise.all(downloads)).map(d => d[0].toString()).join('');

        // Reduce the data
        const output = _reduce(data);

        // Write the output to the output directory in Google Cloud Storage
        const outputFilePath = `${_message.outputDir}result_${_message.targetIdx}`;
        await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });
        console.log('Finished reducing for hash index: ', _message.targetIdx);

        //Check all reducers finished
        const reducerOutputPrefix = `${_message.outputDir}result_`;
        const reducerOutputs = (await bucket.getFiles({ prefix: reducerOutputPrefix }))[0];
        if (reducerOutputs.length === parseInt(process.env.SHUFFLER_HASH_MODULO)) {
            console.log('All reducers finished. Starting cleanup...');
            const jsonMessage = {
                outputDir: _message.outputDir
            }
            const cleanerTopic = pubsub.topic(process.env.CLEANER_TOPIC);
            await cleanerTopic.publishMessage({ json: jsonMessage });
        }
        callback();
    } catch (err) {
        console.error(err);
        callback(err);
    }

}

/**
 * 
 * @param {*} message 
 * @param {*} context 
 * @param {*} callback 
 */
exports.clean = async (message, context, callback) => {
    try {
        // Get trigger parameters
        const _message = JSON.parse(Buffer.from(message.data, 'base64').toString());
        const reducerOutputPrefix = `${_message.outputDir}result_`;
        const files = (await bucket.getFiles({ prefix: reducerOutputPrefix }))[0];
        const data = (await Promise.all(files.map(file => file.download({ timeout: 30000 }))));
        const output = data.map(d => d[0].toString()).join('');


        // Write the output to the output directory in Google Cloud Storage
        const outputName = `${_message.outputDir.split('/')[0]}.txt`;
        const outputFilePath = process.env.OUTPUT_PATH + outputName;
        await bucket.file(outputFilePath).save(output, { resumable: false, timeout: 30000 });
        console.log('Pipeline finished. Output saved to: ', outputFilePath);

        // Delete all temporary files
        console.log("Cleaning up...");
        await bucket.deleteFiles({ prefix: _message.outputDir });
        callback();
    } catch (err) {
        console.error(err);
        callback(err);
    }
}