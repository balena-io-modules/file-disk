import * as BlockMap from 'blockmap';
import * as Bluebird from 'bluebird';
import { createHash } from 'crypto';
import { Readable } from 'stream';

import { Disk } from './index';
import { Interval } from './interval-intersection';

const getNotDiscardedChunks = (disk: Disk, capacity: number): Interval[] => {
	const chunks: Interval[] = [];
	const discardedChunks = disk.getDiscardedChunks();
	let lastStart = 0;
	for (const discardedChunk of discardedChunks) {
		chunks.push([lastStart, discardedChunk.start - 1]);
		lastStart = discardedChunk.end + 1;
	}
	if (lastStart < capacity) {
		chunks.push([lastStart, capacity - 1]);
	}
	return chunks;
};

function* mergeBlocks(blocks: Interval[]): Iterable<Interval> {
	// Merges adjacent and overlapping blocks (helper for getBlockMap).
	let current: Interval | undefined;
	for (const block of blocks) {
		if (current === undefined) {
			current = block.slice() as Interval;  // slice for copying
		} else if (block[0] > current[1] + 1) {
			// There's a gap
			yield current;
			current = block.slice() as Interval;  // slice for copying
		} else {
			// No gap
			current[1] = block[1];
		}
	}
	if (current !== undefined) {
		yield current;
	}
}

const streamSha256 = (stream: Readable): Bluebird<string> => {
	const hash = createHash('sha256');
	return new Bluebird((resolve, reject) => {
		stream.on('error', reject);
		hash.on('error', reject);
		hash.on('finish', () => {
			resolve((hash.read() as Buffer).toString('hex'));
		});
		stream.pipe(hash);
	});
};

interface BlockMapRange {
	start: number;
	end: number;
	checksum: string | null;
}

const getRanges = (disk: Disk, blocks: Interval[], blockSize: number, calculateChecksums: boolean): Bluebird<BlockMapRange[]> => {
	const result: BlockMapRange[] = blocks.map((block) => {
		return { start: block[0], end: block[1], checksum: null };
	});
	if (!calculateChecksums) {
		return Bluebird.resolve(result);
	}
	return Bluebird.each(blocks, (block, i) => {
		const start  = block[0] * blockSize;
		const length = (block[1] - block[0] + 1) * blockSize;
		return disk.getStream(start, length)
		.then((stream) => {
			return streamSha256(stream)
			.then((hex) => {
				result[i].checksum = hex;
			});
		});
	})
	.return(result);
};

const calculateBmapSha256 = (bmap: any): void => {
	bmap.checksum = Array(64).join('0');
	const hash = createHash('sha256');
	hash.update(bmap.toString());
	bmap.checksum = hash.digest('hex');
};

export const getBlockMap = (disk: Disk, blockSize: number, capacity: number, calculateChecksums: boolean): any => {
	const chunks: Interval[] = getNotDiscardedChunks(disk, capacity);
	let blocks: Interval[] = chunks.map((chunk: Interval): Interval => {
		return [
			Math.floor(chunk[0] / blockSize),
			Math.floor(chunk[1] / blockSize),
		];
	});
	blocks = Array.from(mergeBlocks(blocks));
	const mappedBlockCount = blocks.map((block) => {
		return block[1] - block[0] + 1;
	}).reduce((a, b) => {
		return a + b;
	});
	return getRanges(disk, blocks, blockSize, calculateChecksums)
	.then((ranges) => {
		const bmap = new (BlockMap as any)({  // Ugly hack to avoid `Cannot use 'new' with an expression whose type lacks a call or construct signature.`
			imageSize: capacity,
			blockSize,
			blockCount: Math.ceil(capacity / blockSize),
			mappedBlockCount,
			ranges,
		});
		calculateBmapSha256(bmap);
		return bmap;
	});
};
