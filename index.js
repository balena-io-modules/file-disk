'use strict';

const aws = require('aws-sdk');
const EventEmitter = require('events');
const fs = require('fs');
const iisect = require('interval-intersection');

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
			)
			this.end = other.end;
		}
		if (buffers.length > 1) {
			this.buffer = Buffer.concat(buffers, this.end - this.start + 1);
		}
	}
}

class Disk extends EventEmitter {
	// Subclasses need to implement:
	// * constructor() that should call `this._ready()` when the disk is ready;
	// * _getCapacity(callback)
	// * _read(buffer, bufferOffset, length, fileOffset, callback)
	// * _write(buffer, bufferOffset, length, fileOffset, callback)
	// * _flush(callback)
	// * _discard(offset, length, buffer, callback)
	constructor(mapping, readOnly, recordWrites) {
		super();
		this._prepareMapping(mapping);
		this._err = null;
		this._isReady = false;
		this.readOnly = readOnly;
		this.recordWrites = recordWrites;
		this.writes = []  // sorted list of non overlapping DiskWrites
	};

	_ready() {
		this._isReady = true;
		this.emit('ready');
	}

	_callWhenReady(method, args) {
		method = method.bind(this);
		if (!this._isReady) {
			this.once('ready', function() {
				if (this._err) {
					const callback = args[args.length - 1];
					callback(this._err);
					return;
				}
				method.apply(this, args);
			});
			return;
		}
		method.apply(this, args);
	}

	_prepareMapping(mapping) {
		this.mapping = {}
		for (let action of [ 'Read', 'Write', 'Flush', 'Discard' ]) {
			let value = mapping[action.toLowerCase()];
			if (value !== undefined) {
				if (Array.isArray(value)) {
					for (let x of value) {
						this.mapping[x] = this['_request' + action];
					}
				} else {
					this.mapping[value] = this['_request' + action];
				}
			}
		}
	}

	_requestRead(offset, length, buffer, callback) {
		// Reads `length` bytes starting at `offset` into `buffer`
		if (this.readOnly && this.recordWrites) {
			const plan = this._createReadPlan(buffer, offset, length);
			this._readAccordingToPlan(buffer, plan, callback);
		} else {
			this._read(buffer, 0, length, offset, callback);
		}
	};

	_requestWrite(offset, length, buffer, callback) {
		// Writes the first `length` bytes of `buffer` at `offset`
		if (this.recordWrites) {
			this._insertDiskWrite(new DiskWrite(buffer.slice(0, length), offset));
		}
		if (this.readOnly) {
			callback(null, length, buffer);
		} else {
			this._write(buffer, 0, length, offset, callback);
		}
	};

	_requestFlush(offset, length, buffer, callback) {
		if (!this.readOnly) {
			this._flush(callback);
		} else {
			callback(null);
		}
	};
	
	_requestDiscard(offset, length, buffer, callback) {
		console.log('UNIMPLEMENTED: discarding', length, 'bytes at offset', offset);
		callback(null);
	};

	request(type, offset, length, buffer, callback) {
		this._callWhenReady(
			this._request,
			[type, offset, length, buffer, callback]
		);
	}

	_request(type, offset, length, buffer, callback) {
		const method = this.mapping[type];
		if (method) {
			method.bind(this)(offset, length, buffer, callback);
		} else {
			callback(new Error("Unknown request type: " + type));
		}
	};

	getCapacity(callback) {
		this._callWhenReady(this._getCapacity, [callback]);
	}

	_insertDiskWrite(w) {
		if (this.writes.length === 0) {
			// Special case for empty list: insert and return.
			this.writes.push(w);
			return;
		}
		let firstFound = false;
		let other;
		for (let i = 0; i < this.writes.length; i++) {
			other = this.writes[i];
			if (!firstFound) {
				if (w.intersection(other) !== null) {
					// First intersection found: merge write and replace it.
					w.merge(other);
					this.writes[i] = w;
					firstFound = true;
				} else if (w.end < other.start) {
					// Our write is before the other: insert here and return.
					this.writes.splice(i, 0, w);
					return;
				}
			} else {
				if (w.intersection(other) !== null) {
					// Another intersection found: merge write and remove it
					w.merge(other);
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
			this.writes.push(w);
		}
	};

	_createReadPlan(buf, offset, length) {
		const end = offset + length - 1
		const interval = [offset, end];
		const intersections = this.writes.filter(function(w) {
			return (iisect(interval, w.interval()) !== null);
		});
		if (intersections.length === 0) {
			return [ [ offset, end ] ];
		}
		const readPlan = []
		let w;
		for (w of intersections) {
			if (offset < w.start) {
				readPlan.push([offset, w.start - 1])
			}
			readPlan.push(w)
			offset = w.end + 1
		}
		if (w && (end > w.end)) {
			readPlan.push([w.end + 1, end])
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
	constructor(path, mapping, readOnly, recordWrites) {
		super(mapping, readOnly, recordWrites);
		this.path = path;
		this._fd = null;

		const self = this;
		const mode = this.readOnly ? 'r' : 'r+';
		fs.open(this.path, mode, function(err, fd) {
			if (err) {
				self._err = err;
			} else {
				self._fd = fd;
			}
			self._ready();
		});
	};

	_getCapacity(callback) {
		fs.fstat(this._fd, function (err, stat) {
			if (err) {
				callback(err);
				return;
			}
			callback(null, stat.size);
		});
	};

	_read(buffer, bufferOffset, length, fileOffset, callback) {
		fs.read(this._fd, buffer, bufferOffset, length, fileOffset, callback);
	};

	_write(buffer, bufferOffset, length, fileOffset, callback) {
		fs.write(this._fd, buffer, bufferOffset, length, fileOffset, callback);
	};

	_flush(callback) {
		fs.fdatasync(this._fd, callback);
	};
}

class S3Disk extends Disk {
	constructor(
		mapping,
		bucket,
		key,
		accessKey,
		secretKey,
		endpoint=null,
		sslEnabled=true,
		s3ForcePathStyle=true,
		signatureVersion='v4'
	) {
		super(mapping, true, true);
		this.bucket = bucket;
		this.key = key;
		this.s3 = new aws.S3({
			accessKeyId: accessKey,
			secretAccessKey: secretKey,
			endpoint: endpoint,
			sslEnabled: sslEnabled,
			s3ForcePathStyle: s3ForcePathStyle,
			signatureVersion: signatureVersion
		});
		this._ready();
	};

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
	};

	_read(buffer, bufferOffset, length, fileOffset, callback) {
		const params = this._getS3Params()
		params.Range = `bytes=${fileOffset}-${fileOffset + length - 1}`;
		this.s3.getObject(params, function(err, data) {
			if (err) {
				callback(err);
				return;
			}
			data.Body.copy(buffer, bufferOffset);
			callback(null, data.ContentLength, buffer);
		});
	};
}

exports.FileDisk = FileDisk;
exports.S3Disk = S3Disk;
