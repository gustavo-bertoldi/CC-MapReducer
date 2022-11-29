const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub'); 

//Set up the storage client and read input files
const storage = new Storage();
const bucket = storage.bucket('mrcw');


exports.mapper = (message, context, callback) => {
  console.log(message);
};

