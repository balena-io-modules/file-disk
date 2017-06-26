/*global it describe beforeEach afterEach*/
/*eslint no-undef: "error"*/

'use strict';

const Promise = require('bluebird');
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const aws = require('aws-sdk');
const streamToArrayAsync = Promise.promisifyAll(require('stream-to-array'));
const sha256 = require('js-sha256').sha256;

const filedisk = Promise.promisifyAll(require('../'), { multiArgs: true });
const diskchunk = require('../diskchunk');

const DISK_PATH = path.join(__dirname, 'fixtures', 'zeros');
const TMP_DISK_PATH = DISK_PATH + '-tmp';
const DISK_SIZE = 10240;
const S3 = new aws.S3({
	accessKeyId: 'access_key',
	secretAccessKey: 'secret_key',
	endpoint: 'http://0.0.0.0:9042',
	s3ForcePathStyle: true,
	sslEnabled: false
});

function createDisk(fd) {
	// read write
	// don't record reads
	// don't record writes
	// discarded chunks are zeros
	return new filedisk.FileDisk(fd);
}

function createCowDisk(fd) {
	// read only
	// record writes
	// record reads
	// discarded chunks are zeros
	return new filedisk.FileDisk(fd, true, true, true, true);
}

function createCowDisk2(fd) {
	// read only
	// record writes
	// don't record reads
	// read discarded chunks from the disk anyway
	return new filedisk.FileDisk(fd, true, true, false, false);
}

function createS3CowDisk() {
	// read only
	// record reads
	// record writes
	// discarded chunks are zeros
	return new filedisk.S3Disk(S3, 'bucket', 'zeros', true, true);
}

function testOnAllDisks(fn) {
	const files = [
		filedisk.openFile(DISK_PATH, 'r'),
		filedisk.openFile(TMP_DISK_PATH, 'r+')
	];
	return Promise.using(files, function(fds) {
		const disks = [
			createCowDisk(fds[0]),
			createCowDisk2(fds[0]),
			createDisk(fds[1]),
			createS3CowDisk()
		];
		return Promise.all(disks.map(fn));
	});
}

describe('BufferDiskChunk', function() {
	describe('slice', function() {
		it('0-3, slice 0-2', function() {
			const chunk = new diskchunk.BufferDiskChunk(Buffer.alloc(4), 0);
			const slice = chunk.slice(0, 2);
			assert.strictEqual(slice.start, 0);
			assert.strictEqual(slice.end, 2);
			assert.strictEqual(slice.buffer.length, 3);
		});

		it('4-7, slice 5-6', function() {
			const chunk = new diskchunk.BufferDiskChunk(Buffer.alloc(4), 4);
			const slice = chunk.slice(5, 6);
			assert.strictEqual(slice.start, 5);
			assert.strictEqual(slice.end, 6);
			assert.strictEqual(slice.buffer.length, 2);
		});
	});
});

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

	function testGetCapacity(disk) {
		return disk.getCapacityAsync()
		.spread(function(size) {
			assert.strictEqual(size, DISK_SIZE);
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

	function createBuffer(size, pattern) {
		// Helper for checking disk contents.
		const buffer = Buffer.alloc(size);
		Buffer.from(Array.from(pattern).map(Number)).copy(buffer);
		return buffer;
	}

	function checkDiskContains(disk, pattern) {
		// Helper for checking disk contents.
		return function() {
			const size = 32;
			const expected = createBuffer(size, pattern);
			return disk.readAsync(Buffer.allocUnsafe(size), 0, size, 0)
			.spread(function(count, real) {
				assert(real.equals(expected));
			});
		};
	}

	function overlappingWrites(disk) {
		const buf = Buffer.allocUnsafe(8);
		buf.fill(1);
		return disk.discardAsync(0, DISK_SIZE)
		.then(function() {
			return disk.writeAsync(buf, 0, buf.length, 0);
		})
		.then(checkDiskContains(disk, '11111111'))
		.then(function() {
			buf.fill(2);
			return disk.writeAsync(buf, 0, buf.length, 4);
		})
		.then(checkDiskContains(disk, '111122222222'))
		.then(function() {
			buf.fill(3);
			return disk.writeAsync(buf, 0, buf.length, 8);
		})
		.then(checkDiskContains(disk, '1111222233333333'))
		.then(function() {
			buf.fill(4);
			return disk.writeAsync(buf, 0, buf.length, 24);
		})
		.then(checkDiskContains(disk, '11112222333333330000000044444444'))
		.then(function() {
			buf.fill(5);
			return disk.writeAsync(buf, 0, 2, 3);
		})
		.then(checkDiskContains(disk, '11155222333333330000000044444444'))
		// Test disk readable stream:
		.then(function() {
			return disk.getStreamAsync(null, null);
		})
		.spread(function(stream) {
			return streamToArrayAsync(stream);
		})
		.then(function(arr) {
			const expectedFull = createBuffer(
				DISK_SIZE,
				'11155222333333330000000044444444'
			);
			assert(Buffer.concat(arr).equals(expectedFull));
		})
		.then(function() {
			buf.fill(6);
			return disk.writeAsync(buf, 0, 5, 2);
		})
		.then(checkDiskContains(disk, '11666662333333330000000044444444'))
		.then(function() {
			buf.fill(7);
			return disk.writeAsync(buf, 0, 2, 30);
		})
		.then(checkDiskContains(disk, '11666662333333330000000044444477'))
		.then(function() {
			buf.fill(8);
			return disk.writeAsync(buf, 0, 8, 14);
		})
		.then(checkDiskContains(disk, '11666662333333888888880044444477'))
		.then(function() {
			buf.fill(9);
			return disk.writeAsync(buf, 0, 8, 6);
		})
		.then(checkDiskContains(disk, '11666699999999888888880044444477'))
		.then(function() {
			const discarded = disk.getDiscardedChunks();
			assert.strictEqual(discarded.length, 2);
			assert.strictEqual(discarded[0].start, 22);
			assert.strictEqual(discarded[0].end, 23);
			assert.strictEqual(discarded[1].start, 32);
			assert.strictEqual(discarded[1].end, 10239);
			return disk.getBlockMapAsync(1);
		})
		.spread(function(blockmap) {
			const firstRange = '1166669999999988888888';
			const secondRange = '44444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			assert.strictEqual(
				blockmap.ranges[1].checksum,
				sha256(createBuffer(secondRange.length, secondRange))
			);
			return disk.getBlockMapAsync(2);
		})
		.spread(function(blockmap) {
			const firstRange = '1166669999999988888888';
			const secondRange = '44444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			assert.strictEqual(
				blockmap.ranges[1].checksum,
				sha256(createBuffer(secondRange.length, secondRange))
			);
			return disk.getBlockMapAsync(3);
		})
		.spread(function(blockmap) {
			const firstRange = '116666999999998888888800444444770';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(4);
		})
		.spread(function(blockmap) {
			const firstRange = '11666699999999888888880044444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(5);
		})
		.spread(function(blockmap) {
			const firstRange = '11666699999999888888880044444477000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(6);
		})
		.spread(function(blockmap) {
			const firstRange = '116666999999998888888800444444770000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(7);
		})
		.spread(function(blockmap) {
			const firstRange = '11666699999999888888880044444477000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(8);
		})
		.spread(function(blockmap) {
			const firstRange = '11666699999999888888880044444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(9);
		})
		.spread(function(blockmap) {
			const firstRange = '116666999999998888888800444444770000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(10);
		})
		.spread(function(blockmap) {
			const firstRange = '1166669999999988888888004444447700000000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(11);
		})
		.spread(function(blockmap) {
			const firstRange = '116666999999998888888800444444770';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange.length, firstRange))
			);
			return disk.getBlockMapAsync(11);
		});
	}

	it('copy on write mode should properly record overlapping writes', function() {
		return testOnAllDisks(overlappingWrites);
	});
});
