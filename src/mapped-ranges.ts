import { Disk } from './index';
import { Interval } from './interval-intersection';

import { DiskChunk } from './diskchunk';

function* mapIterable<TInput, TOutput>(
	source: Iterable<TInput>,
	transform: (input: TInput) => TOutput,
): Iterable<TOutput> {
	for (const value of source) {
		yield transform(value);
	}
}

async function getNotDiscardedIntervals(
	disk: Disk,
): Promise<Iterable<Interval>> {
	return notDiscardedIntervalsIterator(
		disk.getDiscardedChunks(),
		await disk.getCapacity(),
	);
}

function* notDiscardedIntervalsIterator(
	discardedChunks: Iterable<DiskChunk>,
	capacity: number,
): Iterable<Interval> {
	let lastStart = 0;
	for (const discardedChunk of discardedChunks) {
		yield [lastStart, discardedChunk.start - 1];
		lastStart = discardedChunk.end + 1;
	}
	if (lastStart < capacity) {
		yield [lastStart, capacity - 1];
	}
}

function* mergeIntervals(intervals: Iterable<Interval>): Iterable<Interval> {
	// Merges adjacent and overlapping intervals (helper for getRanges)
	let current: Interval | undefined;
	for (const interval of intervals) {
		if (current === undefined) {
			current = interval.slice() as Interval; // slice for copying
		} else if (interval[0] > current[1] + 1) {
			// There's a gap
			yield current;
			current = interval.slice() as Interval; // slice for copying
		} else {
			// No gap
			current[1] = interval[1];
		}
	}
	if (current !== undefined) {
		yield current;
	}
}

export interface Range {
	offset: number;
	length: number;
}

export async function getRanges(
	disk: Disk,
	blockSize: number,
): Promise<Iterable<Range>> {
	const intervals = await getNotDiscardedIntervals(disk);
	const blockSizedIntervals = mapIterable(
		intervals,
		(chunk: Interval): Interval => [
			Math.floor(chunk[0] / blockSize),
			Math.floor(chunk[1] / blockSize),
		],
	);
	const mergedBlockSizedIntervals = mergeIntervals(blockSizedIntervals);
	return mapIterable(
		mergedBlockSizedIntervals,
		(interval: Interval): Range => {
			const offset = interval[0] * blockSize;
			const length = (interval[1] - interval[0] + 1) * blockSize;
			return { offset, length };
		},
	);
}
