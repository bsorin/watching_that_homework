const commander = require('commander');
const fs = require('fs-extra');
const zlib = require("node:zlib");
const tar = require('tar-stream');
const helpers = require('./helpers');

commander
    .version('1.0.0', '-v, --version')
    .usage('[OPTIONS]...')
    .requiredOption('-u, --remote-file <value>', 'The remote CSV gzipped file.')
    .parse(process.argv);

const options = commander.opts();
const {
    remoteFile
} = {
    ...options
};

const runPipeline = async (remoteFile) => {
    // get remote file name
    const remoteFileName = helpers.getBasenameFromUrl(remoteFile);
    // create the tar extract stream
    const extract = tar.extract();
    // entry = file stream
    extract.on('entry', function(header, stream, next) {
        if (header.type === 'file' && header.name.endsWith('.csv')) {
            stream
                .pipe(helpers.csvParser())
                .pipe(helpers.filterRows())
                .pipe(helpers.filterColumns())
                .pipe(fs.createWriteStream(`./downloads/parsed_${header.name}`));
        }

        stream.on('end', function() {
            console.log('[+] Tar stream entry end.');
            // ready for next entry
            next();
        })
        // just auto drain the stream
        stream.resume();
    })

    extract.on('finish', function() {
        // all entries read
        console.log('[+] All tar stream entries read');
    })

    // create remote http stream
    const httpStream = await helpers.getHttpStream(remoteFile);
    // create gunzipped stream
    const unzip = zlib.createGunzip();
    // remote file -> unzip -> extract 
    // extract = (file read stream -> csv parse -> csv transform -> csv stringify -> output write stream)
    httpStream.pipe(unzip).pipe(extract)
        .on('finish', async () => {
            console.log(`[+] Processed the ${remoteFileName} file!`);
            console.log('[+] Finished pipeline');
        });
}

// run the entire pipeline
runPipeline(remoteFile);