'use strict';

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

class FileDisk extends EventEmitter {
	constructor(path, mapping, options) {
		options = (options === undefined) ? {} : options;
		super();
		this.path = path;
		this._prepareMapping(mapping);
		this._fd = null;
		this._err = null;
		this.readOnly = options.readOnly || false;
		this.recordWrites = options.recordWrites || false;
		this.writes = []  // sorted list of non overlapping DiskWrites
		this._open();
	};

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

	_open() {
		const self = this;
		const mode = this.readOnly ? 'r' : 'r+';
		fs.open(this.path, mode, function(err, fd) {
			if (err) {
				self._fd = -1;
				self._err = err;
			} else {
				self._fd = fd;
			}
			self.emit('open')
		});
	};

	_requestRead(offset, length, buffer, callback) {
		// Reads `length` bytes starting at `offset` into `buffer`
		if (this.readOnly && this.recordWrites) {
			const plan = this._createReadPlan(buffer, offset, length);
			this._readAccordingToPlan(buffer, plan, function(err, count, buf) {
				callback(err, count, buf);
			});
		} else {
			fs.read(this._fd, buffer, 0, length, offset, callback);
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
			fs.write(this._fd, buffer, 0, length, offset, callback);
		}
	};

	_requestFlush(offset, length, buffer, callback) {
		fs.fdatasync(this._fd, callback);
	};
	
	_requestDiscard(offset, length, buffer, callback) {
		console.log('UNIMPLEMENTED: discarding', length, 'bytes at offset', offset);
		callback(null);
	};

	request(type, offset, length, buffer, callback) {
		if (typeof this._fd !== 'number') {
			this.once('open', function() {
				if (this._err) {
					callback(this._err);
					return;
				}
				// We want to call this request method, not one defined by a
				// subclass.
				const thisMethod = FileDisk.prototype.request.bind(this);
				thisMethod(type, offset, length, buffer, callback);
			});
			return;
		}
		if (this._err) {
			callback(this._err);
			return;
		}
		const method = this.mapping[type];
		if (method) {
			method.bind(this)(offset, length, buffer, callback);
		} else {
			callback(new Error("Unknown request type: " + type));
		}
	};

	getCapacity(callback) {
		if (typeof this._fd !== 'number') {
			this.once('open', function() {
				if (this._err) {
					callback(this._err);
					return;
				}
				// We want to call this getCapacity method, not one defined by
				// a subclass.
				FileDisk.prototype.getCapacity.bind(this)(callback);
			});
			return;
		}
		if (this._err) {
			callback(this._err);
			return;
		}
		fs.fstat(this._fd, function (err, stat) {
			if (err) {
				callback(err);
				return;
			}
			callback(null, stat.size);
		});
	};

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
				fs.read(this._fd, buffer, offset, length, entry[0], function(err) {
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

exports.FileDisk = FileDisk;
