/*
 *
 * Script to merge all mergeable chunks for a given collection.
 *
 * !!! THIS SCRIPT CAN ONLY BE RUN IN MONGODB VERSIONS >= 6.0.3
 * !!! BALANCING NEEDS TO BE DISABLED IN ORDER TO RUN THE SCRIPT
 *
 */

'use strict';
(() => {
    const NS = 'test.coll';
    const MAX_CHUNKS_PER_MERGE = 1000;
    const SLEEP_TIME_MS = 1000;

    /* ============= DO NOT MODIFY NEXT LINES ===================== */

    function logLine(str) {
        print('[AUTO-MERGER] ' + str);
    }

    if (db.version() < "6.0.3") {
        logLine("ERROR: Incompatible mongodb version (" + db.version() +
                "). The auto merger script is compatible only with versions >= 6.0.3");
        return;
    }

    if (db.adminCommand({balancerStatus: 1}).mode !== 'off') {
        logLine(
            "ERROR: The balancer is enabled. Disable the balancer before to run the auto merger script");
        return;
    }

    logLine('   Auto merging collection: ' + NS);
    logLine('   Max chunks to squash per merge request: ' + MAX_CHUNKS_PER_MERGE);
    logLine('   Sleep between each merge request: ' + SLEEP_TIME_MS + 'ms');
    print();

    const config = db.getSiblingDB('config');
    const collectionDoc = config.collections.findOne({_id: NS});
    const collUUID = collectionDoc.uuid;

    const totalNumChunks = config.chunks.countDocuments({uuid: collUUID});
    logLine('Total number of chunks before merging: ' + totalNumChunks);

    const chunks = config.chunks.find({uuid: collUUID}).sort({min: 1}).noCursorTimeout();

    var chunksToMerge = [];
    var numChunksScanned = 0;
    var numRangesMerged = 0;
    var numChunksMerged = 0;

    function issueMergeRequest() {
        if (chunksToMerge.length > 1) {
            const min = chunksToMerge[0].min;
            const max = chunksToMerge[chunksToMerge.length - 1].max;
            const shard = chunksToMerge[0].shard;
            db.getSiblingDB("admin").runCommand({mergeChunks: NS, bounds: [min, max]});
            numRangesMerged++;
            numChunksMerged = numChunksMerged + (chunksToMerge.length - 1);
            logLine('Merged ' + chunksToMerge.length + ' chunks in range [min:' +
                    JSON.stringify(min) + ', max:' + JSON.stringify(max) + '] on shard ' + shard);
            logLine('Remaining chunks to process: ' + (totalNumChunks - numChunksScanned));
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

    print();
    logLine("* Completed *");
    logLine('Total chunks processed: ' + numChunksScanned);
    logLine('Total chunks merged: ' + numChunksMerged);
    logLine('Total merge operations: ' + numRangesMerged);
    logLine('Total resulting chunks: ' + config.chunks.countDocuments({uuid: collUUID}));
})()