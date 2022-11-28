const fs = require('fs');

const reducer = (fileName = '../output/10001.shuf0') => {
    const map = fs.readFileSync(fileName, 'utf8').split('\n')
        .reduce((acc, pair) => {
            if (!pair) return acc;
            const [sorted, word] = pair.split(': ');
            if (!acc[sorted]) acc[sorted] = new Set();
            acc[sorted].add(word);
            return acc;
        }, {});

    console.log(map);
}

reducer();