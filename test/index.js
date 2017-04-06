'use strict';

const Promise = require('bluebird')
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const filedisk = Promise.promisifyAll(require('../'), { multiArgs: true });

const DISK_PATH = path.join(__dirname, 'fixtures', 'zeros');
const TMP_DISK_PATH = DISK_PATH + '-tmp';

const READ = 0;
const WRITE = 1;
const FLUSH = 2;
const DISCARD = 3;
const mapping = {
	read: READ,
	write: WRITE,
	flush: FLUSH,
	discard: DISCARD
};

function createDisk() {
	return new filedisk.FileDisk(TMP_DISK_PATH, mapping);
}

function createCowDisk() {
	return new filedisk.FileDisk(
		DISK_PATH,
		mapping,
		true,  // readOnly
		true  // recordWrites
	);
}

function createS3CowDisk() {
	return new filedisk.S3Disk(
		mapping,
		'bucket',
		'zeros',
		'access_key',
		'secret_key',
		'http://0.0.0.0:9042',
		false
	);
}

function testOnAllDisks(fn) {
	const disks = [ createDisk(), createCowDisk(), createS3CowDisk() ];
	return Promise.all(disks.map(fn));
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

	it('should errback when the disk file does not exist', function(done) {
		const disk = new filedisk.FileDisk('no_such_file', mapping);
		const buf = Buffer.allocUnsafe(1024);
		disk.request(READ, 0, buf.length, buf, function(err) {
			assert.strictEqual(err.errno, -2);
			assert.strictEqual(err.code, 'ENOENT');
			done();
		})
	});

	function testGetCapacity(disk) {
		return disk.getCapacityAsync()
		.spread(function(size) {
			assert.strictEqual(size, 10240);
		})
	}

	it('getCapacity should return the disk size', function() {
		return testOnAllDisks(testGetCapacity);
	});

	function readRespectsLength(disk) {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.requestAsync(READ, 0, 10, buf)
		.spread(function(count, buf) {
			const firstTenBytes = Buffer.allocUnsafe(10);
			firstTenBytes.fill(0);
			// first ten bytes were read: zeros
			assert(buf.slice(0, 10).equals(firstTenBytes));
			const rest = Buffer.alloc(1024 - 10);
			rest.fill(1);
			// the rest was not updated: ones
			assert(buf.slice(10).equals(rest));
		})
	}

	it('read should respect the length parameter', function() {
		return testOnAllDisks(readRespectsLength);
	});

	function writeRespectsLength(disk) {
		const buf = Buffer.allocUnsafe(1024);
		const buf2 = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.requestAsync(WRITE, 0, 10, buf)
		.then(function() {
			return disk.requestAsync(READ, 0, 1024, buf2);
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
		return disk.requestAsync(WRITE, 0, buf.length, buf)
		.spread(function(count) {
			assert.strictEqual(count, buf.length);
			const buf2 = Buffer.allocUnsafe(1024);
			return disk.requestAsync(READ, 0, buf2.length, buf2);
		})
		.spread(function(count, buf2) {
			assert.strictEqual(count, buf2.length);
			assert(buf.equals(buf2));
			return disk.requestAsync(FLUSH, null, null, null);
		})
	}

	it('should write and read', function() {
		return testOnAllDisks(shouldReadAndWrite);
	});

	function overlappingWrites(disk) {
		const buf = Buffer.allocUnsafe(8);
		buf.fill(1);
		return disk.requestAsync(WRITE, 0, buf.length, buf)
		.then(function() {
			buf.fill(2);
			return disk.requestAsync(WRITE, 4, buf.length, buf);
		})
		.then(function() {
			buf.fill(3);
			return disk.requestAsync(WRITE, 8, buf.length, buf);
		})
		.then(function() {
			buf.fill(4);
			return disk.requestAsync(WRITE, 24, buf.length, buf);
		})
		.then(function() {
			buf.fill(5);
			return disk.requestAsync(WRITE, 3, 2, buf);
		})
		.then(function() {
			const data = Buffer.allocUnsafe(32);
			return disk.requestAsync(READ, 0, data.length, data);
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
		})
	}

	it('copy on write mode should properly record overlapping writes', function() {
		return testOnAllDisks(overlappingWrites);
	});
});
