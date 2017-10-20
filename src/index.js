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

const pr = new Process(args);
// TODO multi automatically
if (!args.multi) {
    __start(pr, __done);
} else {
    request.get({
        url: `http://${pr.src.uri}/${pr.srcIndex}/_aliases`,
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
                src: pr.src.uri,
                dest: pr.dest.uri,
                srcIndex: index,
                destIndex: index
            });
            __start(p, callback);
        }, __done);
    });
}

/*
var i = 0, steps = [0.1, 0.25, 0.6, 0.8, 0.4, 0.5, 0.6, 0.2, 0.8, 1.0];

(function next() {
  if (i >= steps.length) {
  } else {
    bar.update(Math.random());
    setTimeout(next, 500);
  }
})();
/*

const bar = new Progress(':bar');
p.start((progress) => bar.tick(progress), (err) => console.error(err));*/