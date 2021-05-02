/**
 * Number.prototype.format(n, x)
 * 
 * @param integer n: length of decimal
 * @param integer x: length of sections
 */
Number.prototype.format = function(n, x) {
    var re = '\\d(?=(\\d{' + (x || 3) + '})+' + (n > 0 ? '\\.' : '$') + ')';
    return this.toFixed(Math.max(0, ~~n)).replace(new RegExp(re, 'g'), '$&,');
};

/**
 * Generate a random integer between min+max inclusive
 * 
 * @param integer min: Starting range
 * @param integer max: End range
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

sh._debugMode = true;
sh._oldhelp=sh.help
sh._configDB = db.getSiblingDB("config");

sh.enable_debug = function() {
    sh._debugMode = true;
    sh.is_debug_enabled();
}

sh.disable_debug = function() {
    sh._debugMode = false;
    sh.is_debug_enabled();
}

sh.is_debug_enabled = function() {
    print("Debug Mode: ", sh._debugMode);
}

sh._retryDelay = function() {
    let rc = 10*1000;
    if (this._debugMode) rc = 1000;
    return rc;
}

sh._sameChunk = function(a, b) {
	// print("_sameChunk(",a._id,",", b._id, ")");
	lhs={ id: a._id };
	rhs={ id: b._id };
    if (bsonWoCompare(lhs, rhs) === 0) return true;
    return false;
}

sh._isMaxChunk = function(chunk) {
    //print("isMaxChunk:", tojsononeline(chunk));
    var max = chunk.max;
    for (p in max) {
        if (max[p] === MaxKey) return true;
    }
    return false;
}

sh._isMinChunk = function(chunk) {
    //print("isMinChunk:", tojsononeline(chunk));
    var min = chunk.min;
    for (p in min) {
        if (min[p] === MinKey) return true;
    }
    return false;
}

sh._contiguousChunks = function(a, b) {
    if (bsonWoCompare(a.max, b.min) === 0) return true;
    print("Non-contiguous Chunks(",tojsononeline(a.max),",", tojsononeline(b.min));
    return false;
}

sh._chunkSize = function() {
    var rc = this._configDB.settings.findOne({_id: "chunksize"});
    if (rc) {
        rc = rc.value;
    }
    else {
        rc = 64;
    }
    rc = rc*1024*1024;
    //print("Chunk Size: ", sh._dataFormat(rc))
    return rc;
}

sh.data_size = function(ns, key, kmin, kmax, est = true) {
    var rc = undefined;
    if ( this._debugMode === true ) {
        rc = { ok : getRandomInt(0, 4), size: getRandomInt(0, 1024*1024*1024) };
    }
    else {
        rc = sh._adminCommand(
            { dataSize: ns, keyPattern: key, min: kmin, max: kmax, estimate: est }
        );
    }
    return rc;
}

sh.move_chunk = function(ns, min, dst) {
    const fn = function() {
        if ( sh._debugMode === true ) {
            return { ok : getRandomInt(0,4), msg : "Debug Mode" , millis: getRandomInt(100,1000)};
        }
        return sh.moveChunk(ns, min, dst);
    };

    // Retry an error up to 6 times
    let retries = 6;
    let rc = undefined;

    while (retries > 0) {
        retries--;
        rc = fn();
        if (rc.ok !== 0) break;
        print("move_chunk retrying(", retries, "):", tojsononeline(rc));
        sleep(sh._retryDelay());
    } 

    return rc;
}

sh._moveChunk = function(chunk, dst) {
    return sh.move_chunk(chunk.ns, chunk.min, dst);
}

sh.split_chunk = function(ns, at, in_half=true) {
    const fn = function() {
        if ( sh._debugMode === true ) {
            return { ok : getRandomInt(0,4), msg : "Debug Mode" };
        }
        if (in_half) return sh.splitFind(ns, at);
        return sh.splitAt(ns, at);
    };

    // Retry an error up to 6 times
    let retries = 6;
    let rc = undefined;

    while (retries > 0) {
        retries--;
        rc = fn();
        if (rc.ok !== 0) break;
        print("_split retrying(", retries, "):", tojsononeline(rc));
        sleep(sh._retryDelay());
    } 

    return rc;
}

sh._splitChunk = function(chunk, at=undefined) {
    return sh.split_chunk(chunk.ns, chunk.min, (at === undefined));
}

sh._chunkDataSize = function(key, chunk, est = true) {
    var result = sh.data_size(chunk.ns, key, chunk.min, chunk.max, est);
    if ( result.ok === 0 ) {
        printjson(result);
        return -1;
    }
    return result.size;
}

sh.merge_chunks = function(ns, lowerBound, upperBound) {
    var rc = undefined;
    if ( sh._debugMode === true ) {
        rc = { ok : getRandomInt(0,4), msg : "Debug Mode" }
    }
    else {
        rc = sh._adminCommand( { mergeChunks: ns, bounds: [ lowerBound, upperBound ] });
    }
    return rc;
}

sh._mergeChunks = function(firstChunk, lastChunk) {
    var rc = { ok : 1 };
    if ( firstChunk && lastChunk && !sh._sameChunk(firstChunk, lastChunk) ) {
        rc = sh.merge_chunks(firstChunk.ns, firstChunk.min, lastChunk.max);
        print("Merge Attempt:", firstChunk._id, ",", lastChunk._id, "=", rc.ok);
    } else {
        print("Merge Aborted:");
        rc.ok = 0;
        rc.firstChunk = firstChunk && firstChunk._id || undefined;
        rc.lastChunk = lastChunk && lastChunk._id || undefined;
        if (firstChunk && lastChunk) {
            rc.sameChunk = sh._sameChunk(firstChunk, lastChunk);
        };
    }
    if (rc.ok !== 1) printjson(rc);
    return rc;
}

sh._findMinKeyChunk = function(ns) {
    return sh._configDB.chunks.find({"ns": ns}).sort({min:1}).limit(1).next();
}

sh._findMaxKeyChunk = function(ns) {
    return sh._configDB.chunks.find({"ns": ns}).sort({min:-1}).limit(1).next();
}

function sendtoscreen(obj) {
	printjson(obj.toArray())
}

sh.help = function() {
	this._oldhelp()
	print("\tsh.op_count()                            Number of operations")
	print("\tsh.ops_by_hour()                         Operations by hour")
	print("\tsh.ops_by_hour_not_aborted()             Unaborted operations by hour")
	print("\tsh.ops_by_hour_not_aborted_condensed()   Condensed view")
	print("\tsh.ops_by_ns()                           Operations by namespace")
	print("\tsh.splits_and_migrations()               Operations by namespace")
	print("\tsh.errors_by_phase()                     Errors by phase")
	print("\tsh.covered_period()                      Period covered by changelog")
	print("\tsh.first_last_migration()                First and last successful migrations")
	print("\tsh.moves_by_donor()                      Shard moves sorted by donor")
	print("\tsh.rates_and_volumes()                   Successful migration rates and volumes")
	print("\tsh.print_sizes()                         Print data sizes")
	print("\tsh.move_data(ns, from, to, bytes)        Move chunks in ns from -> to (shards) until 'bytes' are moved")
	print("\tsh.split_to_max(ns)                      Split namespace chunks until they are below the max if possible")
	print("\tsh.print_bounds(ns)                      Print namespace sharding boundary chunks")
	print("\tsh.split_topchunk(ns)                    Split the top chunk (maxkey) down the middle")
}

sh.op_count = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $group : { _id : { what : "$what", note : "$details.note" }, total : { $sum : 1  } } } 
		])
	)
}

sh.ops_by_hour = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $project : { day : { $dayOfYear : "$time" }, time : { $hour : "$time" }, what : "$what", note : "$details.note" } }, 
			{ $group : { _id : { day : "$day", time : "$time", what : "$what", note : "$note" }, count : { $sum : 1 } } }, 
			{ $sort : { "_id.day" : 1, "_id.time" : 1 } } 
		])
	)
}

sh.ops_by_hour_not_aborted = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $match : { "details.note" : { $ne : 'aborted' } } },
			{ $project : { day : { $dayOfYear : "$time" }, time : { $hour : "$time" }, what : "$what" } },
			{ $group : { _id : { day : "$day", time : "$time", what : "$what" }, count : { $sum : 1 } } },
			{ $sort : { "_id.day" : 1, "_id.time" : 1 } }
		])
	)
}

sh.ops_by_hour_not_aborted_condensed = function() {
	this._configDB.changelog.aggregate([
		{ $match : { "details.note" : { $ne : 'aborted' } } },
		{ $project : { day : { $dayOfYear : "$time" }, time : { $hour : "$time" }, what : "$what" } },
		{ $group : { _id : { day : "$day", time : "$time", what : "$what" }, count : { $sum : 1 } } },
		{ $sort : { "_id.day" : 1, "_id.time" : 1 } }
	]).forEach(function(cl){ printjsononeline(cl);});
}

sh.ops_by_ns = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $group : { _id : { what : "$what", ns : "$ns", note : "$details.note" }, total : { $sum : 1  } } },
			{ $sort : { "_id.ns" : 1, "_id.what" : 1 } } 
		])
	)
}

sh.splits_and_migrations = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{$group: {
				_id:{ "ns":"$ns","server":"$server"},
				multiSplits:{$sum:{$cond:[{$eq:["$what","multi-split"]},1,0]}},
				splits:{$sum:{$cond:[{$eq:["$what","split"]},1,0]}},
				migrationAttempts:{$sum:{$cond:[{$eq:["$what","moveChunk.from"]},1,0]}},
				migrationFailures:{$sum:{$cond:[ {$eq:["$details.note","aborted" ]} ,1,0]}},
				migrations:{$sum:{$cond:[{$eq:["$what","moveChunk.commit"]},1,0]}}
			} },
			{ $sort: { _id:1, multiSplits: -1, splits: -1 } }
		])
	)
}

sh.errors_by_phase = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $match : { "details.note" : 'aborted' } },
			{ $group : { _id : { what : "$what", errmsg : "$details.errmsg" }, count : { $sum : 1 } } },
			{ $sort : { "_id.what" : 1, count : -1 } }
		])
	)
}

sh.covered_period = function() {
	sendtoscreen( this._configDB.changelog.find({},{_id:0, time:1}).limit(1) )
	sendtoscreen( this._configDB.changelog.find({},{_id:0, time:1}).sort({$natural:-1}).limit(1) )
}

sh.first_last_migration = function() {
	sendtoscreen( this._configDB.changelog.find({what:"moveChunk.commit"},{_id:0, time:1}).limit(1) )
	sendtoscreen( this._configDB.changelog.find({what:"moveChunk.commit"},{_id:0, time:1}).sort({$natural:-1}).limit(1) )
}

sh.moves_by_donor = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $match: { "what" : "moveChunk.start" }},
			{ $group : { _id: { from: "$details.from", ns : "$ns"}, count: { $sum : 1 } } },
			{ $sort : { "count" : -1 } }
		])
	)
}

sh.rates_and_volumes = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{ $match: { what: { "$in": [ "moveChunk.commit", "moveChunk.start" ] } } },
			{ $project: { _id: 0,
				what: "$what", time: "$time",
				uniqueDetails: {
					from: "$details.from", to: "$details.to",
					mink: "$details.min", maxk: "$details.max",
					ns: "$ns" },
				sizeInfo: {
					cloned: "$details.cloned",
					bytes: "$details.clonedBytes" }, } },
			{ $group: {
				_id: "$uniqueDetails",
				start: { "$min": "$time" },
				commit: { "$max": "$time" },
				count: { "$sum": 1 },
				cloned: { "$max": "$sizeInfo.cloned"},
				bytes: { "$max": "$sizeInfo.bytes" } } },
			{ $project: { _id: "$_id",
				whenStart: "$start", whenDone: "$commit",
				bytesMoved: "$bytes", docsMoved: "$cloned",
				moveTime_ms: { "$subtract": [ "$commit", "$start" ] } } },
			{ $match: { bytesMoved: { "$ne": null }, moveTime_ms: { "$ne": 0 } } },
			{ $project: { _id: "$_id",
				whenStart: "$whenStart", whenDone: "$whenDone",
				moveTime_ms: "$moveTime_ms",
				bytesMoved: "$bytesMoved", docsMoved: "$docsMoved",
				bytesPer_ms: { "$divide": [ "$bytesMoved", "$moveTime_ms" ] },
				docsPer_ms: { "$divide": [ "$docsMoved", "$moveTime_ms" ] } } },
			// outputs stats for each chunk moved ...
			{ $project: { _id: "$_id",
				whenStart: "$whenStart", whenDone: "$whenDone",
				moveTime_ms: "$moveTime_ms",
				bytesMoved: "$bytesMoved", docsMoved: "$docsMoved",
				docsPer_sec: { "$multiply": [ "$docsPer_ms", 1000.0 ] },
				MBper_sec: { "$divide": [ "$bytesPer_ms", 1048.576 ] } } }
			// optionally limit to date range, or etc...
			// , { $match: {
			//    whenStart: { "$gte": ISODate("2017-08-09T00:00:00.000Z") },
			//    whenDone: { "$lt": ISODate("2017-08-10T00:00:00.000Z") } } }
			// optionally get averages per shard-pair, or per sending shard or receiving shard or per collection, etc.
			// , { $group: {
			//    _id: "$_id.from",  // example: from-shard stats
			//    // _id: { "$_id.from", "$_id.to" }, // example: shard pair stats,
			//    // _id: { "$_id.ns" }, // example: collection chunk stats
			//    minMoveTime_ms: { "$min": "$moveTime_ms" },
			//    avgMoveTime_ms: { "$avg": "$moveTime_ms" },
			//    maxMoveTime_ms: { "$max": "$moveTime_ms" },
			//    stdevMoveTime_ms: { "$stdDevPop": "$moveTime_ms" },
			//    minMBper_sec: { "$min": "$MBper_sec" },
			//    avgMBper_sec: { "$avg": "$MBper_sec" },
			//    maxMBper_sec: { "$max": "$MBper_sec" },
			//    stdevMBper_sec: { "$stdDevPop": "$MBper_sec" } } }
			// optionally, sort for time charting or hot shards by long-time or large volume, etc., output to collection, etc.
			//, { $sort: { whenDone: 1 } }
			//, { $out: "__chunkMoveStats__" }
		])
	)
}

sh.hot_shard = function() {
	sendtoscreen(
		this._configDB.changelog.aggregate([
			{$group: {
				_id:{ "ns":"$ns","server":"$server"},
				multiSplits:{$sum:{$cond:[{$eq:["$what","multi-split"]},1,0]}},
				splits:{$sum:{$cond:[{$eq:["$what","split"]},1,0]}},
				migrationAttempts:{$sum:{$cond:[{$eq:["$what","moveChunk.from"]},1,0]}},
				migrationFailures:{$sum:{$cond:[ {$eq:["$details.note","aborted" ]} ,1,0]}},
				migrations:{$sum:{$cond:[{$eq:["$what","moveChunk.commit"]},1,0]}}
			} },
			{ $sort: { _id:1, multiSplits: -1, splits: -1 } }
		])
	)
}

sh.checkInBalancerWindow = function() {
    let balancer = this._configDB.settings.findOne({_id: "balancer"}, {activeWindow: 1});
    if (!balancer || balancer.activeWindow === undefined || balancer.activeWindow.start === undefined) {
        return true;
    }
    let start = balancer.activeWindow.start;
    let stop = balancer.activeWindow.stop;
    let currentDate = new Date();
    let currentHourMinute = `${currentDate.getHours()}:${currentDate.getMinutes()}`;
    if (start < currentHourMinute && stop > currentHourMinute) {
        return true;
    }
    return false;
}


sh.consolidate_ns_chunks = function(ns) {

    if (!sh.checkInBalancerWindow()) {
        print(`**Not In Balancer Window**`);
        return;
    }

    let maxSize = sh._chunkSize();
    let halfSize = maxSize / 2;
    let coll = sh._configDB.collections.findOne({_id: ns});
    let chunksProcessed = 0;
    let breaks = 0;
    let merges = 0;

    print("Collection: ", tojsononeline(coll))
    print("Max Size: ", sh._dataFormat(halfSize))
    print(`Start: ${new Date().toISOString()}`);

    // Process chunks in each shard
    let shards = sh._configDB.shards.find({state: 1}, {_id:1}).toArray();
    print(`Found ${shards.length} shards`);
    shards.forEach(function(shard) {
        print();
        print(`------- Shard: ${shard._id} --------`);
        var start = new Date().toISOString();
        print(`------- Start: ${start}`);
        print();
        var stats = sh._consolidate_ns_chunks_shard(shard, ns, coll, maxSize, halfSize);
        chunksProcessed += stats.chunksProcessed;
        breaks += stats.breaks;
        merges += stats.merges;
        print(`------- Shard: ${shard._id} --------`);
        print(`------- Chunks :${stats.chunksProcessed}`);
        print(`------- Breaks :${stats.breaks}`);
        print(`------- Merges :${stats.merges}`);
        print(`------- Start: ${start}`);
        print(`------- End: ${new Date().toISOString()}`);

    });
    print("----------------------------------------");
    print(`End: ${new Date().toISOString()}`);
    print("Chunks :", chunksProcessed);
    print("Breaks :", breaks);
    print("Merges :", merges);        
}

sh.consolidate_ns_chunksShard = function(ns, shardName) {

    if (!sh.checkInBalancerWindow()) {
        print(`**Not In Balancer Window**`);
        return;
    }

    let shard = sh._configDB.shards.findOne({state: 1, _id: shardName}, {_id:1});

    let maxSize = sh._chunkSize();
    let halfSize = maxSize / 2;
    let coll = sh._configDB.collections.findOne({_id: ns});

    print("Collection: ", tojsononeline(coll))
    print("Max Size: ", sh._dataFormat(halfSize))

    print();
    print(`------- Shard: ${shard._id} --------`);
    var start = new Date().toISOString();
    print(`------- Start: ${start}`);
    print();
    var stats = sh._consolidate_ns_chunks_shard(shard, ns, coll, maxSize, halfSize);
    print(`------- Shard: ${shard._id} --------`);
    print(`------- Chunks :${stats.chunksProcessed}`);
    print(`------- Breaks :${stats.breaks}`);
    print(`------- Merges :${stats.merges}`);
    print(`------- Start: ${start}`);
    print(`------- End: ${new Date().toISOString()}`);
}

// Torsten: just a placeholder, adobe wants a resume function
sh.resume_consolidate = function() {
    db.getSiblingDB("shardUtils").state.find().forEach(function(saveState) {
        printjson(saveState);
    });
}

sh._consolidate_ns_chunks_shard = function(shard, ns, coll, maxSize, halfSize) {
    const localChunkMerge = function(a, b) {
        // print(`localChunkMerge:\n ${JSON.stringify(a)}\n${JSON.stringify(b)}`);
        let rc = sh._mergeChunks(a, b);
        if (rc.ok === 1) {
            merges++;
        }
        return rc;
    };

    let chunksProcessed = 0;
    let breaks = 0;
    let merges = 0;

    var startingChunk = undefined;  // Drop anchor
    var prevChunk = undefined;      // Previous chunk (current chunk is part of function)
    var runningSize = 0;            // Trailing aggregate chunk size
    
    db.getSiblingDB("config").chunks.find({"ns": ns, "shard": shard._id}).sort({min: 1}).noCursorTimeout().forEach(function(chunk) {

        chunksProcessed++;

        // Additional debug output and state saves
        if (chunksProcessed % 100 == 0) {
            print(`Total Merges: ${merges} after ${chunksProcessed}, currently on ${shard._id}`);
            // let's save every 100s chunk for later resume
            // Torsten: disabled for now. Adobe wants a resume function
            // the min from the chunk can be used for the find() over chunks
            // db.getSiblingDB("shardUtils").state.updateOne({"_id": shard._id}, {$set: {"ns": ns, "chunk": chunk}}, {upsert: true});
        }

        // Start processing
        if ( startingChunk === undefined ) {
            startingChunk = prevChunk = chunk;
            runningSize = 0;
        }

        print("Chunk:", chunk._id, "Running:", sh._dataFormat(runningSize));

        // Stop on non-contiguous range and reset to current chunk
        if ( !sh._sameChunk(prevChunk, chunk) && !sh._contiguousChunks(prevChunk, chunk) ) {
            breaks++;
            localChunkMerge(startingChunk, prevChunk);
            startingChunk = prevChunk = chunk;
            runningSize = 0;
        }

        // Gather chunk info
        var dataSize = sh._chunkDataSize(coll.key, chunk);
        print("Size:", sh._dataFormat(dataSize));

        // Failed to get the size for the chunk or the chunk 
        // is already big enough so we coalesce what we have
        // until now and skip this chunk
        if ( dataSize < 0 || dataSize > halfSize ) {
            print(`Failed to get size for chunk or chunk big enough`);
            localChunkMerge(startingChunk, prevChunk);
            startingChunk = undefined;
            return;
        }

        // Commulative chunks must be merged
        // startingChunk + prevChunk < halfSize and currentChunk is big then c1+c2+c3 > maxSize?
        // 31 x 1 MiB chunks + 1 x 31 MiB = 62 MiB < maxSize
        if ( runningSize > halfSize ) {
            print(`cumulative chunks to be merged`);
            localChunkMerge(startingChunk, prevChunk);
            startingChunk = chunk;
            runningSize = 0;
        }

        prevChunk = chunk;
        runningSize += dataSize;

    });

    // Merge any leftovers
    localChunkMerge(startingChunk, prevChunk);
    return {chunksProcessed: chunksProcessed, breaks: breaks, merges: merges};
}

var indentStr = function(indent, s) {
	if (typeof(s) === "undefined") {
		s = indent;
		indent = 0;
	}
	if (indent > 0) {
		indent = (new Array(indent + 1)).join(" ");
		s = indent + s.replace(/\n/g, "\n" + indent);
	}
	return s;
};

sh._shardingStatusStrX = function(indent, s) {
	// convert from logical indentation to actual num of chars
	if (indent == 0) {
		indent = 0;
	} else if (indent == 1) {
		indent = 2;
	} else {
		indent = (indent - 1) * 8;
	}
	return indentStr(indent, s) + "\n";
};

sh.print_sizes = function(configDB) {
	// configDB is a DB object that contains the sharding metadata of interest.
	// Defaults to the db named "config" on the current connection.
	if (configDB === undefined)
		configDB = db.getSisterDB('config');

	var version = configDB.getCollection("version").findOne();
	if (version == null) {
		print("printShardingSizes : not a shard db!");
		return;
	}

	var raw = "";
	var raw_lines = 0;
	var output = function(indent, s) {
		raw += sh._shardingStatusStrX(indent, s);
		raw_lines++;
		if (raw_lines > 1000) {
			print(raw);
			raw = "";
			raw_lines = 0;
		}
	};

	output(0, "--- Sharding Sizes --- ");
	output(1, "sharding version: " + tojson(configDB.getCollection("version").findOne()));

	output(1, "shards:");
	configDB.shards.find().forEach(function(z) {
		output(2, tojson(z));
	});

	var saveDB = db;
	output(1, "databases:");
	configDB.databases.find().sort({name: 1}).noCursorTimeout().forEach(function(db) {
		output(2, tojson(db, "", true));

		if (db.partitioned) {
			configDB.collections.find({_id: new RegExp("^" + RegExp.escape(db._id) + "\.")})
				.noCursorTimeout()
				.sort({_id: 1})
				.forEach(function(coll) {
					output(3, coll._id + " chunks:");
					configDB.chunks.find({"ns": coll._id}).sort({min: 1}).forEach(function(chunk) {
						var out = saveDB.adminCommand({
							dataSize: coll._id,
							keyPattern: coll.key,
							min: chunk.min,
							max: chunk.max
						});
						delete out.millis;
						delete out.ok;

						output(4,
							tojson(chunk.min) + " -->> " + tojson(chunk.max) + " on : " +
							chunk.shard + " " + tojson(out));

					});
				});
		}
	});

	print(raw);
}

sh.move_data = function(ns, srcShard, dstShard, bytesRequested) {
    print("--------------------------------------------------------------------------------");
    print("Move", ns, sh._dataFormat(bytesRequested), "from", srcShard, "to", dstShard);
    print("--------------------------------------------------------------------------------");

    var maxSize = sh._chunkSize();
    var coll = sh._configDB.collections.findOne({_id: ns});

    if (!coll) {
		print("sh.move_data: namespace", ns, "not found!");
		return;
    }

    if (!bytesRequested || typeof(bytesRequested) !== 'number' || bytesRequested < maxSize) {
		print("sh.move_data: Minimum move size is: ", sh._dataFormat(maxSize));
		return;
    }

	if (sh._configDB.shards.findOne({_id: srcShard, state: 1}) === null) {
		print("sh.move_data: Source shard not found!");
		return;
	}

	if (sh._configDB.shards.findOne({_id: dstShard, state: 1}) === null) {
		print("sh.move_data: Destination shard not found!");
		return;
	}


    // Process chunks
    var itr = sh._configDB.chunks.find({"ns": ns, shard: srcShard}).sort({min: 1});
    //var chunkCount = itr.count();
    var chunksProcessed = 0;
    var bytesMoved = 0;
    var failedMoves = 0;
    var failedSizes = 0;
    var zeroChunks = 0;

    while (itr.hasNext() && bytesMoved < bytesRequested) {
        chunksProcessed++;
        var chunk = itr.next();

        if ( sh._isMinChunk(chunk) || sh._isMaxChunk(chunk)) {
            print("Skipping", chunk._id, chunk.min, chunk.max);
            continue;
        }

        var dataSize = sh._chunkDataSize(coll.key, chunk);
        //print("Size:", sh._dataFormat(dataSize));

        if ( dataSize < 0 ) {
            print("Skipping", chunk._id, "due to an invalid data size");
            failedSizes++;
            continue;
        }

        if ( dataSize === 0 ) {
            //print("Skipping ZERO chunk", chunk._id);
            zeroChunks++;
            continue;
        }

        var moveResult = sh._moveChunk(chunk, dstShard);
        if (moveResult.ok === 0) {
            print("Skipping", chunk._id, ":", tojsononeline(moveResult));
            failedMoves++;
            continue;
        }

        print("Moved", sh._dataFormat(dataSize), "from", chunk._id, "in ", moveResult.millis, "ms");
        bytesMoved += dataSize;
    }

    // Close the cursor
    itr.close();

    print("--------------------------------------------------------------------------------");
    print("Chunks processed:", chunksProcessed.format());
    print("Zero chunks:", zeroChunks.format());
    print("Failed sizes:", failedSizes.format());
    print("Failed moves:", failedMoves.format());
    print("Bytes moved:", sh._dataFormat(bytesMoved));
    print();
}

sh.split_to_max = function(ns) {
    const maxSize = sh._chunkSize();
    print("--------------------------------------------------------------------------------");
    print("Split", ns, "chunks until they are below", sh._dataFormat(maxSize));
    print("--------------------------------------------------------------------------------");

    const coll = sh._configDB.collections.findOne({_id: ns});

    if (!coll) {
		print("sh.split_to_max: namespace", ns, "not found!");
		return;
    }

    // Process chunks
    let chunksProcessed = 0;
    let failedSplits = 0;
    let failedSizes = 0;
    let zeroChunks = 0;
    let fitChunks = 0;
    let splitChunks = 0;
    let query = {"ns": ns};
    let itr = sh._configDB.chunks.find(query).sort({_id:1});

     while (itr.hasNext()) {
        //print("Starting with", tojsononeline(query));

        while (itr.hasNext()) {
            chunksProcessed++;
            const chunk = itr.next();
            //print("Processing:", chunk._id);

            if ( sh._isMinChunk(chunk) || sh._isMaxChunk(chunk)) {
                print("Skipping", chunk._id, tojsononeline(chunk.min), tojsononeline(chunk.max));
                continue;
            }

            const dataSize = sh._chunkDataSize(coll.key, chunk);

            if ( dataSize < 0 ) {
                print("Skipping", chunk._id, "due to an invalid data size");
                failedSizes++;
                continue;
            }

            if ( dataSize === 0 ) {
                //print("Skipping", chunk._id, "due to ZERO size");
                zeroChunks++;
                continue;
            }

            if ( dataSize < maxSize ) {
                //print("Skipping", chunk._id, "due to FIT size");
                fitChunks++;
                continue;
            }

            const splitResult = sh._splitChunk(chunk);
            if (splitResult.ok === 0) {
                print("Skipping", chunk._id, ":", tojsononeline(splitResult));
                failedSplits++;
                continue;
            }

            splitChunks++;
            print("Split", chunk._id, "sized", sh._dataFormat(dataSize));

            // We need to restart the query from this point
            // such that we process both of these chunks again (current
            // and newly born)
            itr.close();
            query = {_id: { $gte: chunk._id}, "ns": ns };
            itr = sh._configDB.chunks.find(query).sort({_id:1});
            break;
        }
    }

    itr.close();

    print("--------------------------------------------------------------------------------");
    print("Chunks processed:", chunksProcessed.format());
    print("Zero chunks:", zeroChunks.format());
    print("Fit chunks:", fitChunks.format());
    print("Split chunks:", splitChunks.format());
    print("Failed sizes:", failedSizes.format());
    print("Failed splits:", failedSplits.format());
    print();
}

sh.print_bounds = function(ns) {
    const coll = sh._configDB.collections.findOne({_id: ns});

    if (!coll) {
		print("sh.print_bounds: namespace", ns, "not found!");
		return;
    }

    printjson(sh._findMinKeyChunk(ns));
    printjson(sh._findMaxKeyChunk(ns));
}

sh.split_topchunk = function(ns) {
    print("split_topchunk", ns);

    let maxChunk = sh._findMaxKeyChunk(ns);

    if (!maxChunk) {
		print("sh.split_maxkey_at: maxKey not found!");
		return;
    }

    return sh._splitChunk(maxChunk);
}

/**
 * presplit GridFS
 *
 * Splits the GridFS namespace chunks and distributes them randomly amongst the named shards
 * 
 * @param ns:           Namespace
 * @param shards:       Array of shard id strings used as the round-robin target for chunks
 * @param startDate:    Date object or string for the split start
 * @param endDate:      Date object or string to serve as the upper bound for the split
 * @param inc:          Number of seconds between splits (Defaults to one hour)
 */
sh.presplit_gridfs = function(ns, shards, startDate, endDate, inc = 60*60) {
    print("--------------------------------------------------------------------------------");
    print("Pre-split GridFS", ns, startDate, endDate, inc);
    print("--------------------------------------------------------------------------------");
    if (shards.length <= 0) throw Error("Shard list is invalid");
    if (typeof(startDate) === "string") startDate = new Date(startDate);
    if (typeof(endDate)   === "string") endDate   = new Date(endDate);
    let   startSeconds = startDate.getTime() / 1000 | 0;
    const endSeconds = endDate.getTime() / 1000 | 0;
    let   prevShardName = "";
    const nextShardName = function() {
        let next = ""
        do {
            next = shards[getRandomInt(0, shards.length-1)];
        } while (next === prevShardName);
        prevShardName = next;
        return next;
    };
    
    for (startSeconds; startSeconds < endSeconds; startSeconds += inc) {
        const oid = ObjectId(startSeconds.toString(16).pad(24,true,0));
        const min = {files_id: oid, n:0};

        const splitResult = sh.split_chunk(ns,  min, false);
        if (splitResult.ok === 0) {
            print("Splitting", min, "failed:", tojsononeline(splitResult));
            continue;
        }

        const shardName = nextShardName();
        const moveResult = sh.move_chunk(ns, min, shardName);
        if (moveResult.ok === 0) {
            print("Moving", min, "to", shardName, "failed:", tojsononeline(moveResult));
        }

        print(oid.getTimestamp().toISOString(), ":", tojsononeline(min), "at", shardName);
    }
}
