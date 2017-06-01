'use strict';

const Promise = require('bluebird');
const Readable = require('stream').Readable;
const fs = Promise.promisifyAll(require('fs'));
const iisect = require('interval-intersection');

const MIN_HIGH_WATER_MARK = 16;
const DEFAULT_HIGH_WATER_MARK = 16384;

class DiskStream extends Readable {
	constructor(disk, capacity, highWaterMark) {
		super({highWaterMark: Math.max(highWaterMark, MIN_HIGH_WATER_MARK)});
		this.disk = disk;
		this.capacity = capacity;
		this.position = 0;
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
		const buffer = Buffer.allocUnsafe(self._readableState.highWaterMark);
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
				self.push(buf.slice(0, length));
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

class DiskWrite {
	constructor(buffer, offset) {
		this.buffer = Buffer.from(buffer);
		this.start = offset;
		this.end = offset + buffer.length - 1;
	}

	interval() {
		return [this.start, this.end];
	}

	intersection(other) {
		return iisect(this.interval(), other.interval());
	}

	merge(other) {
		// `other` must be an overlapping `DiskWrite`
		const buffers = [];
		if (other.start < this.start) {
			buffers.push(other.buffer.slice(0, this.start - other.start));
			this.start = other.start;
		}
		buffers.push(this.buffer);
		if (other.end > this.end) {
			buffers.push(
				other.buffer.slice(
					other.buffer.length - other.end + this.end
				)
			);
			this.end = other.end;
		}
		if (buffers.length > 1) {
			this.buffer = Buffer.concat(buffers, this.end - this.start + 1);
		}
	}
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
	constructor(readOnly, recordWrites) {
		this.readOnly = readOnly;
		this.recordWrites = recordWrites;
		this.writes = [];  // sorted list of non overlapping DiskWrites
	}

	read(buffer, bufferOffset, length, fileOffset, callback) {
		if (this.readOnly && this.recordWrites) {
			const plan = this._createReadPlan(buffer, fileOffset, length);
			this._readAccordingToPlan(buffer, plan, callback);
		} else {
			this._read(buffer, bufferOffset, length, fileOffset, callback);
		}
	}

	write(buffer, bufferOffset, length, fileOffset, callback) {
		if (this.recordWrites) {
			const end = bufferOffset + length;
			this._insertDiskWrite(buffer.slice(bufferOffset, end), fileOffset);
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
		console.log('UNIMPLEMENTED: discarding', length, 'bytes at offset', offset);  // eslint-disable-line no-console
		callback(null);
	}

	getCapacity(callback) {
		this._getCapacity(callback);
	}

	getStream(highWaterMark, callback) {
		if ((typeof highWaterMark === 'function') && (callback === undefined)) {
			callback = highWaterMark;
			highWaterMark = DEFAULT_HIGH_WATER_MARK;
		}
		const self = this;
		self.getCapacity(function(err, capacity) {
			if (err) {
				callback(err);
				return;
			}
			callback(null, new DiskStream(self, capacity, highWaterMark));
		});
	}

	_insertDiskWrite(buffer, offset) {
		const write = new DiskWrite(buffer, offset);
		if (this.writes.length === 0) {
			// Special case for empty list: insert and return.
			this.writes.push(write);
			return;
		}
		let firstFound = false;
		let other;
		for (let i = 0; i < this.writes.length; i++) {
			other = this.writes[i];
			if (!firstFound) {
				if (write.intersection(other) !== null) {
					// First intersection found: merge write and replace it.
					write.merge(other);
					this.writes[i] = write;
					firstFound = true;
				} else if (write.end < other.start) {
					// Our write is before the other: insert here and return.
					this.writes.splice(i, 0, write);
					return;
				}
			} else {
				if (write.intersection(other) !== null) {
					// Another intersection found: merge write and remove it
					write.merge(other);
					this.writes.splice(i, 1);
					i--;
				} else {
					// No intersection found, we're done.
					return;
				}
			}
		}
		if (!firstFound) {
			// No intersection and end of loop: our write is the last.
			this.writes.push(write);
		}
	}

	_createReadPlan(buf, offset, length) {
		const end = offset + length - 1;
		const interval = [offset, end];
		const intersections = this.writes.filter(function(w) {
			return (iisect(interval, w.interval()) !== null);
		});
		if (intersections.length === 0) {
			return [ [ offset, end ] ];
		}
		const readPlan = [];
		let w;
		for (w of intersections) {
			if (offset < w.start) {
				readPlan.push([offset, w.start - 1]);
			}
			readPlan.push(w);
			offset = w.end + 1;
		}
		if (w && (end > w.end)) {
			readPlan.push([w.end + 1, end]);
		}
		return readPlan;
	}

	_readAccordingToPlan(buffer, plan, callback) {
		let chunksLeft = plan.length;
		let offset = 0;
		let failed = false;
		function done() {
			if (!failed && (chunksLeft === 0)) {
				callback(null, offset, buffer);
			}
		}
		for (let entry of plan) {
			if (entry instanceof DiskWrite) {
				entry.buffer.copy(buffer, offset);
				offset += entry.buffer.length;
				chunksLeft--;
			} else {
				const length = entry[1] - entry[0] + 1;
				this._read(buffer, offset, length, entry[0], function(err) {
					if (err) {
						if (!failed) {
							callback(err.errno);
							failed = true;
						}
					} else {
						chunksLeft--;
						done();
					}
				});
				offset += length;
			}
		}
		done();
	}
}

class FileDisk extends Disk {
	constructor(fd, readOnly, recordWrites) {
		super(readOnly, recordWrites);
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
	constructor(s3, bucket, key) {
		super(true, true);
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
