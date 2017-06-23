const BlockMap = require('blockmap');
const sha256 = require('js-sha256').sha256;

function getNotDiscardedChunks(disk, blockSize, capacity) {
	const chunks = [];
	const discardedChunks = disk.getDiscardedChunks();
	let lastStart = 0;
	for (let discardedChunk of discardedChunks) {
		chunks.push([lastStart, discardedChunk.start - 1]);
		lastStart = discardedChunk.end + 1;
	}
	if (lastStart < capacity) {
		chunks.push([lastStart, capacity - 1]);
	}
	return chunks;
}

function mergeBlocks(blocks) {
	if (blocks.length > 1) {
		let last = blocks[0];
		for (let i = 1; i < blocks.length; i++) {
			let block = blocks[i];
			if ((block[0] >= last[0]) && (block[0] <= last[1] + 1)) {
				last[1] = block[1];
				blocks.splice(i, 1);
				i--;
			} else {
				last = block;
			}
		}
	}
}

function streamSha256(stream, callback) {
	const hash = sha256.create();
	stream.on('data', function(data) {
		hash.update(data);
	});
	stream.on('error', callback);
	stream.on('end', function() {
		callback(null, hash.hex());
	});
}

function getRanges(disk, blocks, blockSize, callback) {
	const result = new Array(blocks.length);
	let wait = blocks.length;
	let block, i, start, length;
	let error = false;
	for (i = 0; i < blocks.length; i++) {
		block = blocks[i];
		start  = block[0] * blockSize;
		length = (block[1] - block[0] + 1) * blockSize;
		(function(i, start, length, block) {
			disk.getStream(start, length, function(err, stream) {
				if (err) {
					if (!error) {
						error = true;
						callback(err);
					}
					wait -= 1;
					return;
				}
				streamSha256(stream, function(err, hex) {
					if (err) {
						if (!error) {
							error = true;
							callback(err);
						}
						wait -= 1;
						return;
					}
					result[i] = {
						start: block[0],
						end: block[1],
						checksum: hex
					};
					wait -= 1;
					if (wait === 0) {
						callback(null, result);
					}
				});
			});
		})(i, start, length, block);
	}
}

function roundChunksToBlockSize(chunks, blockSize) {
	return chunks.map(function(chunk) {
		let start = chunk[0];
		const remainder = start % blockSize;
		if (remainder !== 0) {
			start -= remainder;
		}
		let end = chunk[1];
		let endExcluded = end + 1;
		if (endExcluded % blockSize !== 0) {
			end = (Math.floor(endExcluded / blockSize) + 1) * blockSize;
		}
		return [start, endExcluded - 1];
	});
}

function calculateBmapSha256(bmap){
	bmap.checksum = Array(64).join('0');
	bmap.checksum = sha256(bmap.toString());
}

exports.getBlockMap = function(disk, blockSize, capacity, callback) {
	const chunks = getNotDiscardedChunks(disk, blockSize, capacity);
	let blocks = roundChunksToBlockSize(chunks, blockSize);
	blocks = blocks.map(function(block) {
		return [ block[0] / blockSize, Math.floor((block[1] / blockSize)) ];
	});
	mergeBlocks(blocks);
	const mappedBlockCount = blocks.map(function(block) {
		return block[1] - block[0] + 1;
	}).reduce(function(a, b) {
		return a + b;
	});
	getRanges(disk, blocks, blockSize, function(err, ranges) {
		if (err) {
			callback(err);
			return;
		}
		const bmap = new BlockMap({
			imageSize: capacity,
			blockSize: blockSize,
			blockCount: Math.ceil(capacity / blockSize),
			mappedBlockCount: mappedBlockCount,
			ranges: ranges
		});
		calculateBmapSha256(bmap);
		callback(null, bmap);
	});
};
