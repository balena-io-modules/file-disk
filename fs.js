'use strict';

const Promise = require('bluebird');
const fs = require('fs');

const read = Promise.promisify(fs.read, { context: fs, multiArgs: true })
const write = Promise.promisify(fs.write, { context: fs, multiArgs: true })

exports.open = Promise.promisify(fs.open, { context: fs })
exports.close = Promise.promisify(fs.close, { context: fs })
exports.fstat = Promise.promisify(fs.fstat, { context: fs })
exports.fdatasync = Promise.promisify(fs.fdatasync, { context: fs })

exports.read = (...args) => {
	return read(...args)
	.spread((bytesRead, buffer) => {
		return { bytesRead, buffer }
	})
}

exports.write = (...args) => {
	return write(...args)
	.spread((bytesWritten, buffer) => {
		return { bytesWritten, buffer }
	})
}
