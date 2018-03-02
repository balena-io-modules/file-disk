import { intervalIntersection, Interval } from './interval-intersection';

/*
A `DiskChunk` is a part of a `Disk` for which we already know the contents.
It may be used for storing parts:
 * that we've written on the disk;
 * that we've already read from the disk;
 * that are discarded.
It has 2 properties:
 * `start` which is the position of the first byte of this chunk in the `Disk`
 * `end` which is the position of the last byte of this chunk in the `Disk`
 and oine method:
 * `cut(other)`, other must be an overlapping `DiskChunk`. This method returns
 a list of 1 or 2 `DiskChunk`s created by cutting of other from this
 `DiskChunk`. It relies on subclasses `slice(start, end)` method.
data.

`DiskChunk` is abstract and must not be used directly.
Subclasses must implement 2 methods:
 * `data()`: it must return a buffer representing the contents of this
 `DiskChunk`. This `Buffer`'s length must be end - start + 1.
 * `slice(start, end)`: it must return a slice of this buffer from `start` to
 `end` (included). `start` and `end` are relative to the `Disk` that contains
 this `DiskChunk`.
*/
export abstract class DiskChunk {
	constructor(
		public readonly start: number,  // position in file
		public readonly end: number,    // position of the last byte in file (included)
	) {}

	abstract slice(start: number, end: number): DiskChunk;

	abstract data(): Buffer;

	interval(): Interval {
		return [ this.start, this.end ];
	}

	intersection(other: DiskChunk): Interval | null {
		return intervalIntersection(this.interval(), other.interval());
	}

	intersects(other: DiskChunk): boolean {
		return (this.intersection(other) !== null);
	}

	includedIn(other: DiskChunk): boolean {
		return ((this.start >= other.start) && (this.end <= other.end));
	}

	cut(other: DiskChunk): DiskChunk[] {
		// `other` must be an overlapping `DiskChunk`
		const result: DiskChunk[] = [];
		const intersection = this.intersection(other);
		if (intersection === null) {
			throw new Error('`other` must be an overlapping `DiskChunk`');
		}
		if (intersection[0] > this.start) {
			result.push(this.slice(this.start, intersection[0] - 1));
		}
		if (this.end > intersection[1]) {
			result.push(this.slice(intersection[1] + 1, this.end));
		}
		return result;
	}
}

/*
`BufferDiskChunk` is a `DiskChunk` backed by a `Buffer`
*/
export class BufferDiskChunk extends DiskChunk {
	private readonly buffer: Buffer;

	constructor(buffer: Buffer, offset: number, copy: boolean = true) {
		super(offset, offset + buffer.length - 1);
		if (copy) {
			this.buffer = Buffer.from(buffer);
		} else {
			this.buffer = buffer;
		}
	}

	data(): Buffer {
		return this.buffer;
	}

	slice(start: number, end: number): BufferDiskChunk {
		// start and end are relative to the Disk
		const startInBuffer = start - this.start;
		return new BufferDiskChunk(
			this.buffer.slice(startInBuffer, startInBuffer + end - start + 1),
			start,
			false,
		);
	}
}

/*
`DiscardDiskChunk` is a `DiskChunk` containing only zeros. These zeros are not stored anywhere.
`DiscardDiskChunk.data()` allocates a `Buffer` of the size of the chunk filled with zeros.
*/
export class DiscardDiskChunk extends DiskChunk {
	constructor(offset: number, length: number) {
		super(offset, offset + length - 1);
	}

	data(): Buffer {
		return Buffer.alloc(this.end - this.start + 1);
	}

	slice(start: number, end: number): DiscardDiskChunk {
		// start and end are relative to the Disk
		return new DiscardDiskChunk(start, end - start + 1);
	}
}
