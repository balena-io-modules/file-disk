import * as assert from 'assert';
import * as aws from 'aws-sdk';
import * as Promise from 'bluebird';
import { createHash } from 'crypto';
import * as fs from 'fs';
import { afterEach, beforeEach, describe } from 'mocha';
import * as path from 'path';

import { FileDisk, openFile, S3Disk } from '../src';
import { BufferDiskChunk } from '../src/diskchunk';

const BUCKET_NAME = 'fixtures';
const FILE_NAME = 'zeros';
const DISK_PATH = path.join(__dirname, BUCKET_NAME, FILE_NAME);
const TMP_DISK_PATH = DISK_PATH + '-tmp';
const DISK_SIZE = 10240;
const S3 = new aws.S3({
	accessKeyId: 'access_key',
	secretAccessKey: 'secret_key',
	endpoint: 'http://0.0.0.0:9042',
	s3ForcePathStyle: true,
	sslEnabled: false,
});

const streamToBuffer = (stream) => {
	return new Promise((resolve, reject) => {
		const chunks = [];
		stream.on('error', reject);
		stream.on('data', chunks.push.bind(chunks));
		stream.on('end', () => {
			resolve(Buffer.concat(chunks));
		});
	});
};

const sha256 = (buffer) => {
	const hash = createHash('sha256');
	hash.update(buffer);
	return hash.digest('hex');
};

const createDisk = (fd) => {
	// read write
	// don't record reads
	// don't record writes
	// discarded chunks are zeros
	return new FileDisk(fd);
};

const createCowDisk = (fd) => {
	// read only
	// record writes
	// record reads
	// discarded chunks are zeros
	return new FileDisk(fd, true, true, true, true);
};

const createCowDisk2 = (fd) => {
	// read only
	// record writes
	// don't record reads
	// read discarded chunks from the disk anyway
	return new FileDisk(fd, true, true, false, false);
};

const createS3CowDisk = () => {
	// read only
	// record reads
	// record writes
	// discarded chunks are zeros
	return new S3Disk(S3, BUCKET_NAME, FILE_NAME, true, true);
};

const testOnAllDisks = (fn) => {
	const files = [
		openFile(DISK_PATH, 'r'),
		openFile(TMP_DISK_PATH, 'r+'),
	];
	return Promise.using(files, (fds) => {
		const disks = [
			createCowDisk(fds[0]),
			createCowDisk2(fds[0]),
			createDisk(fds[1]),
			createS3CowDisk(),
		];
		return Promise.all(disks.map(fn));
	});
};

describe('BufferDiskChunk', () => {
	describe('slice', () => {
		it('0-3, slice 0-2', () => {
			const chunk = new BufferDiskChunk(Buffer.alloc(4), 0);
			const slice = chunk.slice(0, 2);
			assert.strictEqual(slice.start, 0);
			assert.strictEqual(slice.end, 2);
			assert.strictEqual(slice.buffer.length, 3);
		});

		it('4-7, slice 5-6', () => {
			const chunk = new BufferDiskChunk(Buffer.alloc(4), 4);
			const slice = chunk.slice(5, 6);
			assert.strictEqual(slice.start, 5);
			assert.strictEqual(slice.end, 6);
			assert.strictEqual(slice.buffer.length, 2);
		});
	});
});

describe('file-disk', () => {
	beforeEach((done) => {
		// Make a copy of the disk image
		fs.createReadStream(DISK_PATH)
		.pipe(fs.createWriteStream(TMP_DISK_PATH))
		.on('close', done);
	});

	afterEach(() => {
		fs.unlinkSync(TMP_DISK_PATH);
	});

	const testGetCapacity = (disk) => {
		return disk.getCapacity()
		.then((size) => {
			assert.strictEqual(size, DISK_SIZE);
		});
	};

	it('getCapacity should return the disk size', () => {
		return testOnAllDisks(testGetCapacity);
	});

	const readRespectsLength = (disk) => {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.read(buf, 0, 10, 0)
		.then((result) => {
			const firstTenBytes = Buffer.allocUnsafe(10);
			firstTenBytes.fill(0);
			// first ten bytes were read: zeros
			assert(result.buffer.slice(0, 10).equals(firstTenBytes));
			assert.strictEqual(result.bytesRead, 10);
			const rest = Buffer.alloc(1024 - 10);
			rest.fill(1);
			// the rest was not updated: ones
			assert(result.buffer.slice(10).equals(rest));
		});
	};

	it('read should respect the length parameter', () => {
		return testOnAllDisks(readRespectsLength);
	});

	const writeRespectsLength = (disk) => {
		const buf = Buffer.alloc(1024);
		buf.fill(1);
		return disk.write(buf, 0, 10, 0)
		.then(() => {
			return disk.read(Buffer.allocUnsafe(1024), 0, 1024, 0);
		})
		.then((result) => {
			const firstTenBytes = Buffer.allocUnsafe(10);
			firstTenBytes.fill(1);
			// first ten bytes were written: ones
			assert(result.buffer.slice(0, 10).equals(firstTenBytes));
			assert.strictEqual(result.bytesRead, 1024);
			const rest = Buffer.alloc(1024 - 10);
			// the rest was not written: zeros
			assert(result.buffer.slice(10).equals(rest));
		});
	};

	it('write should respect the length parameter', () => {
		return testOnAllDisks(writeRespectsLength);
	});

	const shouldReadAndWrite = (disk) => {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		return disk.write(buf, 0, buf.length, 0)
		.then((data) => {
			assert.strictEqual(data.bytesWritten, buf.length);
			const buf2 = Buffer.allocUnsafe(1024);
			return disk.read(buf2, 0, buf2.length, 0);
		})
		.then((data) => {
			assert.strictEqual(data.bytesRead, data.buffer.length);
			assert(buf.equals(data.buffer));
			return disk.flush();
		});
	};

	it('should write and read', () => {
		return testOnAllDisks(shouldReadAndWrite);
	});

	const createBuffer = (pattern, size) => {
		// Helper for checking disk contents.
		size = (size === undefined) ? pattern.length : size;
		const buffer = Buffer.alloc(size);
		Buffer.from(Array.from(pattern).map(Number)).copy(buffer);
		return buffer;
	};

	const checkDiskContains = (disk, pattern) => {
		// Helper for checking disk contents.
		return () => {
			const size = 32;
			const expected = createBuffer(pattern, size);
			return disk.read(Buffer.allocUnsafe(size), 0, size, 0)
			.then(({ buffer }) => {
				assert(buffer.equals(expected));
			});
		};
	};

	const overlappingWrites = (disk) => {
		const buf = Buffer.allocUnsafe(8);
		buf.fill(1);
		return disk.discard(0, DISK_SIZE)
		.then(() => {
			return disk.write(buf, 0, buf.length, 0);
		})
		.then(checkDiskContains(disk, '11111111'))
		.then(() => {
			buf.fill(2);
			return disk.write(buf, 0, buf.length, 4);
		})
		.then(checkDiskContains(disk, '111122222222'))
		.then(() => {
			buf.fill(3);
			return disk.write(buf, 0, buf.length, 8);
		})
		.then(checkDiskContains(disk, '1111222233333333'))
		.then(() => {
			buf.fill(4);
			return disk.write(buf, 0, buf.length, 24);
		})
		.then(checkDiskContains(disk, '11112222333333330000000044444444'))
		.then(() => {
			buf.fill(5);
			return disk.write(buf, 0, 2, 3);
		})
		.then(checkDiskContains(disk, '11155222333333330000000044444444'))
		// Test disk readable stream:
		.then(() => {
			return disk.getStream();
		})
		.then((stream) => {
			return streamToBuffer(stream);
		})
		.then((buffer) => {
			const expectedFull = createBuffer(
				'11155222333333330000000044444444',
				DISK_SIZE,
			);
			assert(buffer.equals(expectedFull));
		})
		// Test getStream with start position
		.then(() => {
			return disk.getStream(3);
		})
		.then((stream) => {
			return streamToBuffer(stream);
		})
		.then((buffer) => {
			const expectedFull = createBuffer(
				'55222333333330000000044444444',
				DISK_SIZE - 3,
			);
			assert(buffer.equals(expectedFull));
		})
		// Test getStream with start position and length
		.then(() => {
			return disk.getStream(3, 4);
		})
		.then((stream) => {
			return streamToBuffer(stream);
		})
		.then((buffer) => {
			const expectedFull = createBuffer('5522');
			assert(buffer.equals(expectedFull));
		})
		//
		.then(() => {
			buf.fill(6);
			return disk.write(buf, 0, 5, 2);
		})
		.then(checkDiskContains(disk, '11666662333333330000000044444444'))
		.then(() => {
			buf.fill(7);
			return disk.write(buf, 0, 2, 30);
		})
		.then(checkDiskContains(disk, '11666662333333330000000044444477'))
		.then(() => {
			buf.fill(8);
			return disk.write(buf, 0, 8, 14);
		})
		.then(checkDiskContains(disk, '11666662333333888888880044444477'))
		.then(() => {
			buf.fill(9);
			return disk.write(buf, 0, 8, 6);
		})
		.then(checkDiskContains(disk, '11666699999999888888880044444477'))
		.then(() => {
			const discarded = disk.getDiscardedChunks();
			assert.strictEqual(discarded.length, 2);
			assert.strictEqual(discarded[0].start, 22);
			assert.strictEqual(discarded[0].end, 23);
			assert.strictEqual(discarded[1].start, 32);
			assert.strictEqual(discarded[1].end, 10239);
			return disk.getBlockMap(1, true);
		})
		.then((blockmap) => {
			const firstRange = '1166669999999988888888';
			const secondRange = '44444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			assert.strictEqual(
				blockmap.ranges[1].checksum,
				sha256(createBuffer(secondRange)),
			);
			return disk.getBlockMap(2, true);
		})
		.then((blockmap) => {
			const firstRange = '1166669999999988888888';
			const secondRange = '44444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			assert.strictEqual(
				blockmap.ranges[1].checksum,
				sha256(createBuffer(secondRange)),
			);
			return disk.getBlockMap(3, true);
		})
		.then((blockmap) => {
			const firstRange = '116666999999998888888800444444770';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(4, true);
		})
		.then((blockmap) => {
			const firstRange = '11666699999999888888880044444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(5, true);
		})
		.then((blockmap) => {
			const firstRange = '11666699999999888888880044444477000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(6, true);
		})
		.then((blockmap) => {
			const firstRange = '116666999999998888888800444444770000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(7, true);
		})
		.then((blockmap) => {
			const firstRange = '11666699999999888888880044444477000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(8, true);
		})
		.then((blockmap) => {
			const firstRange = '11666699999999888888880044444477';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(9, true);
		})
		.then((blockmap) => {
			const firstRange = '116666999999998888888800444444770000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(10, true);
		})
		.then((blockmap) => {
			const firstRange = '1166669999999988888888004444447700000000';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
			return disk.getBlockMap(11, true);
		})
		.then((blockmap) => {
			const firstRange = '116666999999998888888800444444770';
			assert.strictEqual(
				blockmap.ranges[0].checksum,
				sha256(createBuffer(firstRange)),
			);
		});
	};

	it('copy on write mode should properly record overlapping writes', () => {
		return testOnAllDisks(overlappingWrites);
	});
});
