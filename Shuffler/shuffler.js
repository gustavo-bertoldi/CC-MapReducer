const crypto = require('crypto');
const fs = require('fs');

const shuffler = (fileName = "../output/10001.map", nbOutputs = 5) => {
    const files = fs.readFileSync(fileName, 'utf8').split('\n')
        .reduce((acc, line) => {
            const [sorted, _] = line.split(': ');
            const hash = crypto.createHash('md5').update(sorted).digest('hex');
            const idx = parseInt(hash, 16) % nbOutputs;
            if (!acc[idx]) acc[idx] = [];
            acc[idx].push(line);
            return acc;
        }, new Array(nbOutputs));
    console.log(files);
}

shuffler();