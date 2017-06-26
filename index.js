'use strict';

const Promise = require('bluebird');
const Readable = require('stream').Readable;
const fs = Promise.promisifyAll(require('fs'));
const iisect = require('interval-intersection');

const blockmap = require('./blockmap');
const diskchunk = require('./diskchunk');

const MIN_HIGH_WATER_MARK = 16;
const DEFAULT_HIGH_WATER_MARK = 16384;

class DiskStream extends Readable {
	constructor(disk, capacity, highWaterMark, start) {
		super({highWaterMark: Math.max(highWaterMark, MIN_HIGH_WATER_MARK)});
		this.disk = disk;
		this.capacity = capacity;
		this.position = start;
	}

	_read() {
		const self = this;
		const length = Math.min(
			self._readableState.highWaterMark,
			self.capacity - self.position
		);
		if (length <= 0) {
			self.push(null);
			return;
		}
		const buffer = Buffer.allocUnsafe(length);
		self.disk.read(
			buffer,
			0,  // buffer offset
			length,
			self.position,  // disk offset
			function(err, bytesRead, buf) {
				if (err) {
					self.emit('error', err);
					return;
				}
				self.position += length;
				self.push(buf);
			}
		);
	}
}

function openFile(path, flags, mode) {
	// Opens a file and closes it when you're done using it.
	// Arguments are the same that for `fs.open()`
	// Use it with Bluebird's `using`, example:
	// Promise.using(openFile('/some/path', 'r+'), function(fd) {
	//   doSomething(fd);
	// });
	return fs.openAsync(path, flags, mode)
	.disposer(function(fd) {
		return fs.closeAsync(fd);
	});
}

class Disk {
	// Subclasses need to implement:
	// * _getCapacity(callback)
	// * _read(buffer, bufferOffset, length, fileOffset, callback(err, bytesRead, buffer))
	// * _write(buffer, bufferOffset, length, fileOffset, callback(err, bytesWritten)) [only for writable disks]
	// * _flush(callback(err)) [only for writable disks]
	// * _discard(offset, length, callback(err)) [only for writable disks]
	//
	// Users of instances of subclasses can use:
	// * getCapacity(callback(err, size))
	// * read(buffer, bufferOffset, length, fileOffset, callback(err, bytesRead, buffer))
	// * write(buffer, bufferOffset, length, fileOffset, callback(err, bytesWritten))
	// * flush(callback(err))
	// * discard(offset, length, callback(err))
	// * getStream(highWaterMark, callback(err, stream))
	//   * highWaterMark [optional] is the size of chunks that will be read (default 16384, minimum 16)
	//   * `stream` will be a readable stream of the disk
	constructor(readOnly, recordWrites, recordReads, discardIsZero) {
		discardIsZero = (discardIsZero === undefined) ? true : discardIsZero;
		this.readOnly = readOnly;
		this.recordWrites = recordWrites;
		this.recordReads = recordReads;
		this.discardIsZero = discardIsZero;
		this.knownChunks = [];  // sorted list of non overlapping DiskChunks
		this.capacity = null;
	}

	read(buffer, bufferOffset, length, fileOffset, callback) {
		const plan = this._createReadPlan(fileOffset, length);
		this._readAccordingToPlan(buffer, plan, callback);
	}

	write(buffer, bufferOffset, length, fileOffset, callback) {
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
			callback(null, length, buffer);
		} else {
			this._write(buffer, bufferOffset, length, fileOffset, callback);
		}
	}

	flush(callback) {
		if (this.readOnly) {
			callback(null);
		} else {
			this._flush(callback);
		}
	}
	
	discard(offset, length, callback) {
		this._insertDiskChunk(new diskchunk.DiscardDiskChunk(offset, length));
		callback(null);
	}

	getCapacity(callback) {
		if (this.capacity !== null) {
			callback(null, this.capacity);
			return;
		}
		const self = this;
		this._getCapacity(function(err, capacity) {
			if (err) {
				callback(err);
				return;
			}
			self.capacity = capacity;
			callback(null, capacity);
		});
	}

	getStream(position, length, highWaterMark, callback) {
		position = Number.isInteger(position) ? position : 0;
		if ((typeof highWaterMark === 'function') && (callback === undefined)) {
			callback = highWaterMark;
			highWaterMark = DEFAULT_HIGH_WATER_MARK;
		}
		if (Number.isInteger(length)) {
			callback(
				null,
				new DiskStream(this, position + length, highWaterMark, position)
			);
			return;
		}
		const self = this;
		self.getCapacity(function(err, capacity) {
			if (err) {
				callback(err);
				return;
			}
			callback(
				null,
				new DiskStream(self, capacity, highWaterMark, position)
			);
		});
	}

	getDiscardedChunks() {
		return this.knownChunks.filter(function(chunk) {
			return (chunk instanceof diskchunk.DiscardDiskChunk);
		});
	}

	getBlockMap(blockSize, callback) {
		const self = this;
		this.getCapacity(function(err, capacity) {
			if (err) {
				callback(err);
				return;
			}
			blockmap.getBlockMap(self, blockSize, capacity, callback);
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
			chunks = chunks.filter(function(chunk) {
				return !(chunk instanceof diskchunk.DiscardDiskChunk);
			});
		}
		const intersections = chunks.map(function(chunk) {
			const inter = iisect(interval, chunk.interval());
			return (inter !== null) ? chunk.slice(inter[0], inter[1]) : null;
		}).filter(function(chunk) {
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

	_readAccordingToPlan(buffer, plan, callback) {
		const self = this;
		let chunksLeft = plan.length;
		let offset = 0;
		let failed = false;
		function done() {
			if (!failed && (chunksLeft === 0)) {
				callback(null, offset, buffer);
			}
		}
		for (let entry of plan) {
			if (entry instanceof diskchunk.DiskChunk) {
				const data = entry.data();
				const length = Math.min(data.length, buffer.length - offset);
				data.copy(buffer, offset, 0, length);
				offset += length;
				chunksLeft--;
			} else {
				const length = entry[1] - entry[0] + 1;
				(function(entry, offset, length) {
					self._read(buffer, offset, length, entry[0], function(err) {
						if (err) {
							if (!failed) {
								callback(err.errno);
								failed = true;
							}
						} else {
							if (self.recordReads) {
								const slice = Buffer.from(
									buffer.slice(offset, offset + length)
								);
								const chunk = new diskchunk.BufferDiskChunk(
									slice,
									entry[0]
								);
								self._insertDiskChunk(chunk);
							}
							chunksLeft--;
							done();
						}
					});
				})(entry, offset, length);
				offset += length;
			}
		}
		done();
	}
}

class FileDisk extends Disk {
	constructor(fd, readOnly, recordWrites, recordReads) {
		super(readOnly, recordWrites, recordReads);
		this.fd = fd;
	}

	_getCapacity(callback) {
		fs.fstat(this.fd, function (err, stat) {
			if (err) {
				callback(err);
				return;
			}
			callback(null, stat.size);
		});
	}

	_read(buffer, bufferOffset, length, fileOffset, callback) {
		fs.read(this.fd, buffer, bufferOffset, length, fileOffset, callback);
	}

	_write(buffer, bufferOffset, length, fileOffset, callback) {
		fs.write(this.fd, buffer, bufferOffset, length, fileOffset, callback);
	}

	_flush(callback) {
		fs.fdatasync(this.fd, callback);
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

	_getCapacity(callback) {
		this.s3.headObject(this._getS3Params(), function(err, data) {
			if (err) {
				callback(err);
				return;
			}
			callback(null, data.ContentLength);
		});
	}

	_read(buffer, bufferOffset, length, fileOffset, callback) {
		const params = this._getS3Params();
		params.Range = `bytes=${fileOffset}-${fileOffset + length - 1}`;
		this.s3.getObject(params, function(err, data) {
			if (err) {
				callback(err);
				return;
			}
			data.Body.copy(buffer, bufferOffset);
			callback(null, data.ContentLength, buffer);
		});
	}
}

class DiskWrapper {
	constructor(disk) {
		this.disk = disk;
	}

	getCapacity(callback) {
		this.disk.getCapacity(callback);
	}

	getStream(highWaterMark, callback) {
		this.disk.getStream(highWaterMark, callback);
	}
}

exports.DiskStream = DiskStream;
exports.openFile = openFile;
exports.Disk = Disk;
exports.FileDisk = FileDisk;
exports.S3Disk = S3Disk;
exports.DiskWrapper = DiskWrapper;
