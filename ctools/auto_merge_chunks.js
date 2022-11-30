/*
 *
 * Script to merge all mergeable chunks for a given collection.
 *
 * !!! THIS SCRIPT CAN ONLY BE RUN IN MONGODB VERSIONS >= 6.0.3
 * !!! BALANCING NEEDS TO BE DISABLED IN ORDER TO RUN THE SCRIPT
 *
 */

const NS = 'test.coll';
const MAX_CHUNKS_PER_MERGE = 1000;
const SLEEP_TIME_MS = 1000;

/* ============= DO NOT MODIFY NEXT LINES ===================== */

function logLine(str) {
    print('----------------------- [AUTO-MERGER] ' + str);
}

if (db.version() < "6.0.3") {
    logLine("CAN'T RUN THE SCRIPT WITH A MONGODB BIN VERSION <= 6.0.3");
    return;
}

if (db.adminCommand({balancerStatus: 1}).mode !== 'off') {
    logLine("CAN'T RUN AUTO-MERGER WITH BALANCING ENABLED");
    return;
}

logLine('Starting for collection ' + NS + ' - Max chunks to squash per merge request: ' +
        MAX_CHUNKS_PER_MERGE + ' - Sleep between each merge request: ' + SLEEP_TIME_MS + 'ms');

const config = db.getSiblingDB('config');
const collectionDoc = config.collections.findOne({_id: NS});
const collUUID = collectionDoc.uuid;
const chunks = config.chunks.find({uuid: collUUID}).sort({min: 1}).noCursorTimeout();

var chunksToMerge = [];
var numChunksScanned = 0;

function issueMergeRequest() {
    if (chunksToMerge.length > 1) {
        const min = chunksToMerge[0].min;
        const max = chunksToMerge[chunksToMerge.length - 1].max;
        const shard = chunksToMerge[0].shard;
        db.getSiblingDB("admin").runCommand({mergeChunks: NS, bounds: [min, max]});
        logLine('MERGED ' + chunksToMerge.length + ' CHUNKS FROM ' + JSON.stringify(min) + ' TO ' +
                JSON.stringify(max) + ' ON SHARD ' + shard);
        logLine('CHUNKS PROCESSED SO FAR: ' + numChunksScanned);
        sleep(SLEEP_TIME_MS);
    }
    chunksToMerge = [];
}

while (chunks.hasNext()) {
    var chunk = chunks.next();

    numChunksScanned++;

    if (chunksToMerge.length == MAX_CHUNKS_PER_MERGE ||
        (chunksToMerge.length > 0 &&
         chunksToMerge[chunksToMerge.length - 1].shard != chunk.shard)) {
        issueMergeRequest();
    }

    chunksToMerge.push(chunk);
}

issueMergeRequest();

logLine(' TOTAL CHUNKS PROCESSED: ' + numChunksScanned);
