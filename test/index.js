/*global it describe beforeEach afterEach*/
/*eslint no-undef: "error"*/

'use strict';

const Promise = require('bluebird');
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const aws = require('aws-sdk');

const filedisk = Promise.promisifyAll(require('../'), { multiArgs: true });

const DISK_PATH = path.join(__dirname, 'fixtures', 'zeros');
const TMP_DISK_PATH = DISK_PATH + '-tmp';
const S3 = new aws.S3('access_key', 'secret_key', 'http://0.0.0.0:9042', false);

function createDisk() {
	const disk = new filedisk.FileDisk(TMP_DISK_PATH);
	return disk.openAsync();
}

function createCowDisk() {
	const disk = new filedisk.FileDisk(
		DISK_PATH,
		true,  // readOnly
		true  // recordWrites
	);
	return disk.openAsync();
}

function createS3CowDisk() {
	const disk = new filedisk.S3Disk(S3, 'bucket', 'zeros');
	return disk.openAsync();
}

function testOnAllDisks(fn) {
	const disks = [ createDisk(), createCowDisk(), createS3CowDisk() ];
	return Promise.all(disks)
	.spread(function(d) {
		return d.map(fn);
	});
}

describe('file-disk', function() {
	beforeEach(function(done) {
		// Make a copy of the disk image
		fs.createReadStream(DISK_PATH)
		.pipe(fs.createWriteStream(TMP_DISK_PATH))
		.on('close', done);
	});

	afterEach(function() {
		fs.unlinkSync(TMP_DISK_PATH);
	});

	it('should errback when the disk file does not exist', function() {
		const disk = new filedisk.FileDisk('no_such_file');
		let gotError = false;
		return disk.openAsync()
		.catch(function(err) {
			gotError = true;
			assert.strictEqual(err.errno, -2);
			assert.strictEqual(err.code, 'ENOENT');
		})
		.then(function() {
			assert(gotError);
		});
	});

	function testGetCapacity(disk) {
		return disk.getCapacityAsync()
		.spread(function(size) {
			assert.strictEqual(size, 10240);
		});
	}

	it('getCapacity should return the disk size', function() {
		return testOnAllDisks(testGetCapacity);
	});

	function readRespectsLength(disk) {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.readAsync(buf, 0, 10, 0)
		.spread(function(count, buf) {
			const firstTenBytes = Buffer.allocUnsafe(10);
			firstTenBytes.fill(0);
			// first ten bytes were read: zeros
			assert(buf.slice(0, 10).equals(firstTenBytes));
			const rest = Buffer.alloc(1024 - 10);
			rest.fill(1);
			// the rest was not updated: ones
			assert(buf.slice(10).equals(rest));
		});
	}

	it('read should respect the length parameter', function() {
		return testOnAllDisks(readRespectsLength);
	});

	function writeRespectsLength(disk) {
		const buf = Buffer.allocUnsafe(1024);
		const buf2 = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.writeAsync(buf, 0, 10, 0)
		.then(function() {
			return disk.readAsync(buf2, 0, 1024, 0);
		})
		.spread(function(count, buf2) {
			const firstTenBytes = Buffer.allocUnsafe(10);
			firstTenBytes.fill(1);
			// first ten bytes were written: ones
			assert(buf2.slice(0, 10).equals(firstTenBytes));
			const rest = Buffer.alloc(1024 - 10);
			rest.fill(0);
			// the rest was not written: zeros
			assert(buf2.slice(10).equals(rest));
		});
	}

	it('write should respect the length parameter', function() {
		return testOnAllDisks(writeRespectsLength);
	});

	function shouldReadAndWrite(disk) {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.writeAsync(buf, 0, buf.length, 0)
		.spread(function(count) {
			assert.strictEqual(count, buf.length);
			const buf2 = Buffer.allocUnsafe(1024);
			return disk.readAsync(buf2, 0, buf2.length, 0);
		})
		.spread(function(count, buf2) {
			assert.strictEqual(count, buf2.length);
			assert(buf.equals(buf2));
			return disk.flushAsync();
		});
	}

	it('should write and read', function() {
		return testOnAllDisks(shouldReadAndWrite);
	});

	function overlappingWrites(disk) {
		const buf = Buffer.allocUnsafe(8);
		buf.fill(1);
		return disk.writeAsync(buf, 0, buf.length, 0)
		.then(function() {
			buf.fill(2);
			return disk.writeAsync(buf, 0, buf.length, 4);
		})
		.then(function() {
			buf.fill(3);
			return disk.writeAsync(buf, 0, buf.length, 8);
		})
		.then(function() {
			buf.fill(4);
			return disk.writeAsync(buf, 0, buf.length, 24);
		})
		.then(function() {
			buf.fill(5);
			return disk.writeAsync(buf, 0, 2, 3);
		})
		.then(function() {
			const data = Buffer.allocUnsafe(32);
			return disk.readAsync(data, 0, data.length, 0);
		})
		.spread(function(count, data) {
			// The final result should be '11155222333333330000000044444444'
			const expected = [
				1, 1, 1,
				5, 5,
				2, 2, 2,
				3, 3, 3, 3, 3, 3, 3, 3,
				0, 0, 0, 0, 0, 0, 0, 0,
				4, 4, 4, 4, 4, 4, 4, 4
			];
			assert(data.equals(Buffer.from(expected)));
		});
	}

	it('copy on write mode should properly record overlapping writes', function() {
		return testOnAllDisks(overlappingWrites);
	});
});
