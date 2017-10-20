import es from 'es';
import async from 'async';

const SCROLL = '5m';
const SIZE = 1000;
const COMMANDS = 100;

export default class Process {
    constructor(args) {
        this._init(args);
        this.es = {
            src: es.createClient({ server: this.src }),
            dest: es.createClient({ server: this.dest })
        }
    }
    start(onProgress, onFinish) {
        if (this.srcIndex.indexOf('*') >= 0) {
            throw new Error('Cannot migrate multiple indexes. Use --multi command.');
        }
        if (!onFinish) {
            onFinish = onProgress;
            onProgress = null;
        }
        if (typeof onProgress !== 'function') {
            onProgress = () => { };
        }
        if (typeof onFinish !== 'function') {
            onProgress = () => { };
        }
        this._clear((err) => {
            if (err) {
                onFinish(err);
                return;
            }
            this._create((err) => {
                if (err) {
                    onFinish(err);
                    return;
                }
                this._reindex(onProgress, onFinish);
            })
        });

    }

    _clear(onFinish) {
        if (!this.clear) {
            return onFinish();
        }
        console.log('Clearing destination index.');
        this._indexExists(this.destIndex, (err, exists) => {
            if (err) {
                onFinish(err);
                return;
            }
            if (!exists) {
                console.log('Destination index doesn\' exist. Clearing skipped.');
                onFinish();
                return;
            }
            this.es.dest.delete({ _index: this.destIndex }, onFinish);
        });
    }

    _create(onFinish) {
        console.log('Creating destination index');
        this._indexExists(this.destIndex, (err, exists) => {
            if (err) {
                onFinish(err);
                return;
            }
            if (exists) {
                console.log('Destination index already exists. Mapping skipped. Use --clear option for clear index.');
                onFinish();
                return;
            }
            this.es.dest.indices.createIndex({ _index: this.destIndex }, {}, (err) => {
                if (err) {
                    onFinish(err);
                    return;
                }
                this.es.src.indices.mappings({ _index: this.srcIndex }, (err, data) => {
                    if (err) {
                        onFinish(err);
                        return;
                    }
                    const mappings = data[this.srcIndex].mappings;
                    async.eachSeries(Object.keys(mappings), (type, callback) => {
                        this.es.dest.indices.putMapping({ _index: this.destIndex, _type: type }, mappings[type], callback);
                    }, onFinish);
                });
            });
        });
    }

    _reindex(onProgress, onFinish) {
        const map = {
            search: 0
        };
        this.es.src.search({
            _index: this.srcIndex,
            search_type: 'scan',
            scroll: SCROLL
        }, { size: this.size }, (err, data) => {
            if (err) {
                onFinish(err);
                return;
            }
            let out = [];
            const count = data.hits.total;
            console.log(`Documents count: ${count}`);
            let done = 0;
            if (count === 0) {
                onProgress(this._progress(map, 'search', 1));
                onFinish();
                return;
            }
            const scrollCallback = (err, data) => {
                if (err) {
                    onFinish(err);
                    return;
                }
                const hits = data.hits.hits;
                done += hits.length;
                this._index(hits, ((progress) => { }), (err) => {
                    if (err) {
                        onFinish(err);
                        return;
                    }
                    onProgress(this._progress(map, 'search', done / count));
                    if (done === count) {
                        onFinish();
                        // DB.es.indices.flush({}, callback);
                        return;
                    }
                    this.es.src.scroll({ scroll: SCROLL }, data._scroll_id, scrollCallback);
                });
            }
            this.es.src.scroll({ scroll: SCROLL }, data._scroll_id, scrollCallback);
        });
    }

    _index(data, onProgress, onFinish) {
        const bulk = [];
        while (data.length > 0) {
            const commands = [];
            const spliced = data.splice(0, this.commands);
            for (let i = 0; i < spliced.length; i++) {
                const d = spliced[i];
                commands.push({ index: { _index: this.destIndex, _type: d._type, _id: d._id } });
                commands.push(d._source);
            }
            bulk.push(commands);
        }
        let i = 0;
        async.eachSeries(bulk, (commands, callback) => {
            this.es.dest.bulk({}, commands, (err, data) => {
                if (err) {
                    callback(err);
                    return;
                }
                i++;
                onProgress(i / bulk.length);
                callback();
            });
        }, onFinish);
    }

    _indexExists(index, cb) {
        this.es.dest.exists({ _index: index }, (err, exists) => cb(err, exists ? exists.exists : false));
    }
    _progress(map, key, progress) {
        map[key] = progress;
        let sum = 0;
        const count = Object.keys(map).length;
        for (let k in map) {
            sum += map[k];
        }
        return sum / count;
    }
    _init(args) {
        let { src, dest, srcIndex, destIndex, size, commands, clear } = args;
        if (!src) {
            throw new Error('No source database defined');
        }

        if (!srcIndex) {
            throw new Error('No source index defined');
        }

        if (!dest) {
            dest = 'localhost:9200';
        }

        if (!destIndex) {
            destIndex = srcIndex;
        }
        if (!size) {
            size = SIZE;
        }
        if (!commands) {
            commands = COMMANDS;
        }
        this.src = this._parse(src);
        this.dest = this._parse(dest);
        this.srcIndex = srcIndex;
        this.destIndex = destIndex;
        this.size = size;
        this.commands = commands;
        this.clear = Boolean(clear);
    }
    _parse(url) {
        let [host, port] = url.split(':');
        return {
            host: host,
            port: port || 80,
            uri: url
        }
    }
}