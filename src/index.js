const request = require('request');
const async = require('async');

import Process from './process';

class Pool {
    constructor(options) {
        if (!options.src) {
            throw new Error('No source database defined');
        }
        if (!options.srcIndex) {
            throw new Error('No source index defined');
        }
        this._options = options;
    }
    execute(cb) {
        this._loadAliases((err, indexes) => {
            if (err) {
                cb(err);
                return;
            }
            console.log(`Processing ${indexes.length} indexes`);
            async.eachSeries(indexes, (index, callback) => {
                const args = this._options;
                this._call('start', { index });
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
                p.start((d) => {
                    return this._call('update', d) || d;
                }, (progress) => this._call('progress', { index, progress }), (err) => {
                    if (err) {
                        this._call('error', { index, err });
                        callback(err);
                        return;
                    }
                    this._call('done', { index });
                    callback();

                });
            }, cb);
        });
    }
    _loadAliases(cb) {
        request.get({
            url: `http://${this._options.src}/${this._options.srcIndex}/_aliases`,
            gzip: true,
            json: true
        }, (err, res, body) => {
            if (err) {
                cb(err);
                return;
            }
            cb(null, Object.keys(body));
            return;
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
    }
    _call(method, params) {
        const m = this._options[method];
        if (typeof m !== 'function') {
            return;
        }
        return m(params);
    }
}

module.exports = (options, cb) => {
    new Pool(options).execute(cb);
}