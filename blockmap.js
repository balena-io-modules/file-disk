'use strict';

const BlockMap = require('blockmap');
const crypto = require('crypto');

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
	// Merges adjacent blocks in place (helper for getBlockMap).
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
	const hash = crypto.createHash('sha256');
	hash.setEncoding('hex');
	stream.pipe(hash);
	stream.on('error', callback);
	stream.on('end', function() {
		hash.end();
		callback(null, hash.read());
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

function calculateBmapSha256(bmap){
	bmap.checksum = Array(64).join('0');
	const hash = crypto.createHash('sha256');
	hash.update(bmap.toString());
	bmap.checksum = hash.digest('hex');
}

exports.getBlockMap = function(disk, blockSize, capacity, callback) {
	const chunks = getNotDiscardedChunks(disk, blockSize, capacity);
	const blocks = chunks.map(function(chunk) {
		return chunk.map(function(pos) {
			return Math.floor(pos / blockSize);
		});
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
