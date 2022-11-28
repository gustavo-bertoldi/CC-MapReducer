const {Storage} = require('@google-cloud/storage');

const storage = new Storage();

const fetchStopWords = async (bucketName = 'mrcw_input', fileName = "StopWords") => {
  const fileContent = await storage.bucket(bucketName).file(fileName).download();
  return new Set(fileContent.toString().split(','));
}

const isValidWord = (word, stopWords) => {
  return !stopWords.has(word) &&
    [...word].every(char => char.match(/[a-zA-Z]+/));
}

const mapper = async (words) => {
  const stopWords = await fetchStopWords();
  return words.split(' ')
    .filter(word => word.length > 0 && isValidWord(word, stopWords))
    .map(word => {
      word = word.toLowerCase();
      return `${[...word].sort().join('')}: ${word}`;
    });
}

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.mapper = (event, context, callback) => {
  console.log("Data: ", event.data);
  const message = event.data
    ? Buffer.from(event.data, 'base64').toString()
    : "";
  
  console.log("message: ", message);
  mapper("all all all");
}

