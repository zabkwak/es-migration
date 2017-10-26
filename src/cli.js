const args = require('args-parser')(process.argv);
const ProgressBar = require('progress');


// TODO different versions of elastic search
// TODO npm cli
const m = require('./');

let p;

args.start = ({ index }) => {
    p = new ProgressBar(`${index} [:bar] :percent :etas`, {
        complete: '=',
        incomplete: ' ',
        width: 40,
        total: 100
    });
    console.log(new Date(), `Index '${index}' started`);
};
args.progress = ({ index, progress }) => p.update(progress);
args.done = ({ index }) => console.log(new Date(), `Index '${index}' done`);

m(args, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
        return;
    }
    console.log('DONE');
});