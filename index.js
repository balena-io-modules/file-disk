'use strict';

const Promise = require('bluebird');
const Readable = require('stream').Readable;
const iisect = require('interval-intersection');

const blockmap = require('./blockmap');
const diskchunk = require('./diskchunk');
const fs = require('./fs');

const MIN_HIGH_WATER_MARK = 16;
const DEFAULT_HIGH_WATER_MARK = 16384;

class DiskStream extends Readable {
	constructor(disk, capacity, highWaterMark, start) {
		super({highWaterMark: Math.max(highWaterMark, MIN_HIGH_WATER_MARK)});
		this.disk = disk;
		this.capacity = capacity;
		this.position = start;
	}

	_read(size) {
		const length = Math.min(size, this.capacity - this.position);
		if (length <= 0) {
			this.push(null);
			return;
		}
		this.disk.read(
			Buffer.allocUnsafe(length),
			0,  // buffer offset
			length,
			this.position  // disk offset
		)
		.then(({ bytesRead, buffer }) => {
			this.position += bytesRead;
			if (this.push(buffer)) {
				this._read(size);
			}
		})
		.catch((err) => {
			this.emit('error', err);
		});
	}
}

const openFile = (path, flags, mode) => {
	// Opens a file and closes it when you're done using it.
	// Arguments are the same that for `fs.open()`
	// Use it with Bluebird's `using`, example:
	// Promise.using(openFile('/some/path', 'r+'), (fd) => {
	//   doSomething(fd);
	// });
	return fs.open(path, flags, mode)
	.disposer((fd) => {
		return fs.close(fd);
	});
};

class Disk {
	// Subclasses need to implement:
	// * _getCapacity(): Promise<Number>
	// * _read(buffer, bufferOffset, length, fileOffset): Promise<{ bytesRead: Number, buffer: Buffer }>
	// * _write(buffer, bufferOffset, length, fileOffset): Promise<{ bytesWritten: Number, buffer: Buffer }> [only for writable disks]
	// * _flush(): Promise<undefined> [only for writable disks]
	// * _discard(offset, length): Promise<undefined> [only for writable disks]
	//
	// Users of instances of subclasses can use:
	// * getCapacity(): Promise<Number>
	// * read(buffer, bufferOffset, length, fileOffset): Promise<{ bytesRead: Number, buffer: Buffer }>
	// * write(buffer, bufferOffset, length, fileOffset): Promise<{ bytesWritten: Number, buffer: Buffer }>
	// * flush(): Promise<undefined>
	// * discard(offset, length): Promise<undefined>
	// * getStream([position, [length, [highWaterMark]]]): Promise<stream.Readable>
	//   * position: start reading from this offset (defaults to zero)
	//   * length: read that amount of bytes (defaults to (disk capacity - position))
	//   * highWaterMark: size of chunks that will be read (default 16384, minimum 16)
	constructor(readOnly, recordWrites, recordReads, discardIsZero) {
		discardIsZero = (discardIsZero === undefined) ? true : discardIsZero;
		this.readOnly = readOnly;
		this.recordWrites = recordWrites;
		this.recordReads = recordReads;
		this.discardIsZero = discardIsZero;
		this.knownChunks = [];  // sorted list of non overlapping DiskChunks
		this.capacity = null;
	}

	read(buffer, bufferOffset, length, fileOffset) {
		const plan = this._createReadPlan(fileOffset, length);
		return this._readAccordingToPlan(buffer, plan);
	}

	write(buffer, bufferOffset, length, fileOffset) {
		if (this.recordWrites) {
			const chunk = new diskchunk.BufferDiskChunk(
				buffer.slice(bufferOffset, bufferOffset + length),
				fileOffset
			);
			this._insertDiskChunk(chunk);
		} else {
			// Special case: we do not record writes but we may have recorded
			// some discards. We want to remove any discard overlapping this
			// write.
			// In order to do this we do as if we were inserting a chunk: this
			// will slice existing discards in this area if there are any.
			const chunk = new diskchunk.DiscardDiskChunk(fileOffset, length);
			// The `false` below means "don't insert the chunk into knownChunks"
			this._insertDiskChunk(chunk, false);
		}
		if (this.readOnly) {
			return Promise.resolve({ bytesWritten: length, buffer });
		} else {
			return this._write(buffer, bufferOffset, length, fileOffset);
		}
	}

	flush() {
		if (this.readOnly) {
			return Promise.resolve();
		} else {
			return this._flush();
		}
	}
	
	discard(offset, length) {
		this._insertDiskChunk(new diskchunk.DiscardDiskChunk(offset, length));
		return Promise.resolve();
	}

	getCapacity() {
		if (this.capacity !== null) {
			return Promise.resolve(this.capacity);
		}
		return this._getCapacity()
		.then((capacity) => {
			this.capacity = capacity;
			return capacity;
		});
	}

	getStream(position=0, length=null, highWaterMark=DEFAULT_HIGH_WATER_MARK) {
		return this.getCapacity()
		.then((end) => {
			if (Number.isInteger(length)) {
				end = Math.min(position + length, end);
			}
			return new DiskStream(this, end, highWaterMark, position);
		});
	}

	getDiscardedChunks() {
		return this.knownChunks.filter((chunk) => {
			return (chunk instanceof diskchunk.DiscardDiskChunk);
		});
	}

	getBlockMap(blockSize, calculateChecksums) {
		return this.getCapacity()
		.then((capacity) => {
			return blockmap.getBlockMap(this, blockSize, capacity, calculateChecksums);
		});
	}

	_insertDiskChunk(chunk, insert) {
		insert = (insert === undefined) ? true : insert;
		let other, i;
		let insertAt = 0;
		for (i = 0; i < this.knownChunks.length; i++) {
			other = this.knownChunks[i];
			if (other.start > chunk.end) {
				break;
			}
			if (other.start < chunk.start) {
				insertAt = i + 1;
			} else {
				insertAt = i;
			}
			if (!chunk.intersects(other)) {
				continue;
			} else if (other.includedIn(chunk)) {
				// Delete other
				this.knownChunks.splice(i, 1);
				i--;
			} else {
				// Cut other
				const newChunks = other.cut(chunk);
				const args = [i, 1].concat(newChunks);
				this.knownChunks.splice.apply(this.knownChunks, args);
				i += newChunks.length - 1;
			}
		}
		if (insert) {
			this.knownChunks.splice(insertAt, 0, chunk);
		}
	}

	_createReadPlan(offset, length) {
		const end = offset + length - 1;
		const interval = [offset, end];
		let chunks = this.knownChunks;
		if (!this.discardIsZero) {
			chunks = chunks.filter((chunk) => {
				return !(chunk instanceof diskchunk.DiscardDiskChunk);
			});
		}
		const intersections = chunks.map((chunk) => {
			const inter = iisect(interval, chunk.interval());
			return (inter !== null) ? chunk.slice(inter[0], inter[1]) : null;
		}).filter((chunk) => {
			return (chunk !== null);
		});
		if (intersections.length === 0) {
			return [ [ offset, end ] ];
		}
		const readPlan = [];
		let chunk;
		for (chunk of intersections) {
			if (offset < chunk.start) {
				readPlan.push([offset, chunk.start - 1]);
			}
			readPlan.push(chunk);
			offset = chunk.end + 1;
		}
		if (chunk && (end > chunk.end)) {
			readPlan.push([chunk.end + 1, end]);
		}
		return readPlan;
	}

	_readAccordingToPlan(buffer, plan) {
		let offset = 0;
		return Promise.each(plan, (entry) => {
			if (entry instanceof diskchunk.DiskChunk) {
				const data = entry.data();
				const length = Math.min(data.length, buffer.length - offset);
				data.copy(buffer, offset, 0, length);
				offset += length;
			} else {
				const length = entry[1] - entry[0] + 1;
				return this._read(buffer, offset, length, entry[0])
				.then(() => {
					if (this.recordReads) {
						const chunk = new diskchunk.BufferDiskChunk(
							Buffer.from(buffer.slice(offset, offset + length)),
							entry[0]
						);
						this._insertDiskChunk(chunk);
					}
					offset += length;
				});
			}
		})
		.then(() => {  // Using .return() here wouldn't work because offset would be 0
			return { bytesRead: offset, buffer };
		});
	}
}

class FileDisk extends Disk {
	constructor(fd, readOnly, recordWrites, recordReads) {
		super(readOnly, recordWrites, recordReads);
		this.fd = fd;
	}

	_getCapacity() {
		return fs.fstat(this.fd).get('size');
	}

	_read(buffer, bufferOffset, length, fileOffset) {
		return fs.read(this.fd, buffer, bufferOffset, length, fileOffset);
	}

	_write(buffer, bufferOffset, length, fileOffset) {
		return fs.write(this.fd, buffer, bufferOffset, length, fileOffset);
	}

	_flush() {
		return fs.fdatasync(this.fd);
	}
}

class S3Disk extends Disk {
	constructor(s3, bucket, key, recordReads, discardIsZero) {
		discardIsZero = (discardIsZero === undefined) ? true : discardIsZero;
		super(true, true, recordReads, discardIsZero);
		this.s3 = s3;
		this.bucket = bucket;
		this.key = key;
	}

	_getS3Params() {
		return { Bucket: this.bucket, Key: this.key };
	}

	_getCapacity() {
		return this.s3.headObject(this._getS3Params()).promise().get('ContentLength');
	}

	_read(buffer, bufferOffset, length, fileOffset) {
		const params = this._getS3Params();
		params.Range = `bytes=${fileOffset}-${fileOffset + length - 1}`;
		return this.s3.getObject(params).promise()
		.then((data) => {
			data.Body.copy(buffer, bufferOffset);
			return { bytesRead: data.contentLength, buffer };
		});
	}
}

class DiskWrapper {
	constructor(disk) {
		this.disk = disk;
	}

	getCapacity() {
		return this.disk.getCapacity();
	}

	getStream(highWaterMark) {
		return this.disk.getStream(highWaterMark);
	}
}

exports.DiskStream = DiskStream;
exports.openFile = openFile;
exports.Disk = Disk;
exports.FileDisk = FileDisk;
exports.S3Disk = S3Disk;
exports.DiskWrapper = DiskWrapper;
