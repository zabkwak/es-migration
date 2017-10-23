const args = require('args-parser')(process.argv);
const ProgressBar = require('progress');
const request = require('request');
const async = require('async');

import Process from './process';

const __error = (err) => {
    console.error(err);
    process.exit(1);
}

const __start = (p, cb) => {
    const bar = new ProgressBar(`${p.srcIndex} -> ${p.destIndex} [:bar] :percent :etas`, {
        complete: '=',
        incomplete: ' ',
        width: 40,
        total: 100
    });

    p.start((progress) => bar.update(progress), cb);
}

const __done = (err) => {
    if (err) {
        __error(err);
        return;
    }
    console.log('DONE');
}

if (!args.src) {
    throw new Error('No source database defined');
}

if (!args.srcIndex) {
    throw new Error('No source index defined');
}

// TODO different versions of elastic search
// TODO npm cli

request.get({
    url: `http://${args.src}/${args.srcIndex}/_aliases`,
    gzip: true,
    json: true
}, (err, res, body) => {
    if (err) {
        __error(err);
        return;
    }
    const indexes = Object.keys(body);
    async.eachSeries(Object.keys(body), (index, callback) => {
        const p = new Process({
            src: args.src,
            dest: args.dest || args.src,
            srcIndex: index,
            postfix: args.postfix,
            clear: args.clear,
            size: args.size,
            commands: args.commands,
            prefix: args.prefix
        });
        __start(p, callback);
    }, __done);
});