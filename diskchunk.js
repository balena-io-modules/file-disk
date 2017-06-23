'use strict';

const iisect = require('interval-intersection');

class DiskChunk {
	constructor(start, end) {
		this.start = start;  // position in file
		this.end = end;      // position of the last byte in file (included)
	}

	interval() {
		return [this.start, this.end];
	}

	intersection(other) {
		return iisect(this.interval(), other.interval());
	}

	intersects(other) {
		return (this.intersection(other) !== null);
	}

	includedIn(other) {
		return ((this.start >= other.start) && (this.end <= other.end));
	}

	cut(other) {
		// `other` must be an overlapping `DiskChunk`
		const result = [];
		const intersection = this.intersection(other);
		if (intersection[0] > this.start) {
			result.push(this.slice(this.start, intersection[0] - 1));
		}
		if (this.end > intersection[1]) {
			result.push(this.slice(intersection[1] + 1, this.end));
		}
		return result;
	}
}

class BufferDiskChunk extends DiskChunk {
	constructor(buffer, offset, copy) {
		copy = (copy === undefined) ? true : copy;
		super(offset, offset + buffer.length - 1);
		if (copy) {
			this.buffer = Buffer.from(buffer);
		} else {
			this.buffer = buffer;
		}
	}

	data() {
		return this.buffer;
	}

	slice(start, end) {
		// start and end are relative to the Disk
		const startInBuffer = start - this.start;
		return new BufferDiskChunk(
			this.buffer.slice(startInBuffer, startInBuffer + end - start + 1),
			start,
			false
		);
	}
}

class DiscardDiskChunk extends DiskChunk {
	constructor(offset, length) {
		super(offset, offset + length - 1);
	}

	data() {
		return Buffer.alloc(this.end - this.start + 1);
	}

	slice(start, end) {
		// start and end are relative to the Disk
		return new DiscardDiskChunk(start, end - start + 1);
	}
}

exports.DiskChunk = DiskChunk;
exports.BufferDiskChunk = BufferDiskChunk;
exports.DiscardDiskChunk = DiscardDiskChunk;
