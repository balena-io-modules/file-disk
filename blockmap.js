'use strict';

const BlockMap = require('blockmap');
const Promise = require('bluebird');
const crypto = require('crypto');

const getNotDiscardedChunks = (disk, capacity) => {
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
};

function* mergeBlocks(blocks) {
	// Merges adjacent and overlapping blocks (helper for getBlockMap).
	let current;
	for (let block of blocks) {
		if (current === undefined) {
			current = block.slice();  // slice for copying
		} else if (block[0] > current[1] + 1) {
			// There's a gap
			yield current;
			current = block.slice();  // slice for copying
		} else {
			// No gap
			current[1] = block[1];
		}
	}
	if (current !== undefined) {
		yield current;
	}
}

const streamSha256 = (stream) => {
	const hash = crypto.createHash('sha256');
	return new Promise((resolve, reject) => {
		stream.on('error', reject);
		hash.on('error', reject);
		hash.on('finish', () => {
			resolve(hash.read().toString('hex'));
		});
		stream.pipe(hash);
	});
};

const getRanges = (disk, blocks, blockSize, calculateChecksums) => {
	const getStreamAsync = Promise.promisify(disk.getStream, { context: disk });
	const result = blocks.map((block) => {
		return { start: block[0], end: block[1], checksum: null };
	});
	if (!calculateChecksums) {
		return Promise.resolve(result);
	}
	return Promise.each(blocks, (block, i) => {
		const start  = block[0] * blockSize;
		const length = (block[1] - block[0] + 1) * blockSize;
		return getStreamAsync(start, length)
		.then((stream) => {
			return streamSha256(stream)
			.then((hex) => {
				result[i].checksum = hex;
			});
		});
	})
	.return(result);
};

const calculateBmapSha256 = (bmap) => {
	bmap.checksum = Array(64).join('0');
	const hash = crypto.createHash('sha256');
	hash.update(bmap.toString());
	bmap.checksum = hash.digest('hex');
};

exports.getBlockMap = (disk, blockSize, capacity, calculateChecksums, callback) => {
	const chunks = getNotDiscardedChunks(disk, capacity);
	let blocks = chunks.map((chunk) => {
		return chunk.map((pos) => {
			return Math.floor(pos / blockSize);
		});
	});
	blocks = Array.from(mergeBlocks(blocks));
	const mappedBlockCount = blocks.map((block) => {
		return block[1] - block[0] + 1;
	}).reduce((a, b) => {
		return a + b;
	});
	getRanges(disk, blocks, blockSize, calculateChecksums)
	.then((ranges) => {
		const bmap = new BlockMap({
			imageSize: capacity,
			blockSize: blockSize,
			blockCount: Math.ceil(capacity / blockSize),
			mappedBlockCount: mappedBlockCount,
			ranges: ranges
		});
		calculateBmapSha256(bmap);
		callback(null, bmap);
	})
	.catch(callback);
};
