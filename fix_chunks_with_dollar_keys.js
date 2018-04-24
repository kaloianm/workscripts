(function() {
    'use strict';

    var configDb = db.getSiblingDB('config');

    // Recursively checks whether a given BSON object contains '$' as a key name
    function isBSONOKForStorage(obj) {
        assert(typeof(obj) === 'object', obj + ' is not an object');
        for (var key in obj) {
            if (key.startsWith('$')) {
                print('Key', key, 'from object', tojson(obj), 'is illegal for sharding');
                return false;
            }

            if (typeof(obj[key]) === 'object' && !isBSONOKForStorage(obj[key])) {
                return false;
            }
        }

        return true;
    }

    function generateNextActionForChunksCollection(shardedCollName) {
        var firstChunkToMerge;
        var lastChunkToMerge;

        let configDocs = configDb.chunks.find({ns: shardedCollName}).sort({min: 1});
        while (configDocs.hasNext()) {
            let configDoc = configDocs.next();

            if (!lastChunkToMerge) {
                if (isBSONOKForStorage(configDoc)) {
                    // We haven't found invalid document, so no lower chunk for the merge has been
                    // established -- continue moving the lower bound
                    firstChunkToMerge = configDoc;
                    continue;
                }
            }

            // We have found an invalid chunk, check that everying starting from the lower chunk for
            // the merge is on the same shard and if not, issue moveChunk commands
            if (configDoc.shard !== firstChunkToMerge.shard) {
                print('Discovered non-contiguous invalid chunks: ' + tojson(configDoc) +
                      ' is not on the same shard as the chunk to use as a lower bound ' +
                      tojson(firstChunkToMerge));
                return {
                    moveChunk: shardedCollName,
                    bounds: [configDoc.min, configDoc.max],
                    to: firstChunkToMerge.shard
                };
            }

            if (isBSONOKForStorage(configDoc.max)) {
                lastChunkToMerge = configDoc;
                break;
            }
        }

        if (lastChunkToMerge === undefined) {
            // No invalid docs found
            return null;
        }

        assert(isBSONOKForStorage(firstChunkToMerge.min), tojson(firstChunkToMerge) + ' is wrong');
        assert(isBSONOKForStorage(lastChunkToMerge.max), tojson(lastChunkToMerge) + ' is wrong');

        return {
            mergeChunks: shardedCollName,
            bounds: [firstChunkToMerge.min, lastChunkToMerge.max]
        };
    }

    function getBalancerStatus() {
        var balancerStatus = assert.commandWorked(db.adminCommand({balancerStatus: 1}));
        if (balancerStatus.mode !== 'off') {
            throw Error('Balancer is still enabled');
        }

        return balancerStatus;
    }

    // Stop the sharding balancer and make sure any current
    sh.stopBalancer();
    print('Balancer stopped. Status is: ', tojson(getBalancerStatus()));

    // Fix all collections
    configDb.collections.find({dropped: {$ne: true}}).forEach(collObj => {
        print('Fixing chunks for collection', collObj._id);
        let nextAction = generateNextActionForChunksCollection(collObj._id);
        if (nextAction === null) {
            print('No illegal chunks found, moving on to next collection.');
            return;
        }

        print('Running command', tojson(nextAction));
        assert.commandWorked(db.adminCommand(nextAction));
    });
})();
