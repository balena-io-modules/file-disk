import * as assert from 'assert';
import * as aws from 'aws-sdk';
import * as Bluebird from 'bluebird';
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
	return new Bluebird((resolve, reject) => {
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
	return Bluebird.using(files, (fds) => {
		const disks = [
			createCowDisk(fds[0]),
			createCowDisk2(fds[0]),
			createDisk(fds[1]),
			createS3CowDisk(),
		];
		return Bluebird.all(disks.map(fn));
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

	const testGetCapacity = async (disk) => {
		const size = await disk.getCapacity();
		assert.strictEqual(size, DISK_SIZE);
	};

	it('getCapacity should return the disk size', async () => {
		await testOnAllDisks(testGetCapacity);
	});

	const readRespectsLength = async (disk) => {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		const result = await disk.read(buf, 0, 10, 0);
		const firstTenBytes = Buffer.allocUnsafe(10);
		firstTenBytes.fill(0);
		// first ten bytes were read: zeros
		assert(result.buffer.slice(0, 10).equals(firstTenBytes));
		assert.strictEqual(result.bytesRead, 10);
		const rest = Buffer.alloc(1024 - 10);
		rest.fill(1);
		// the rest was not updated: ones
		assert(result.buffer.slice(10).equals(rest));
	};

	it('read should respect the length parameter', async () => {
		await testOnAllDisks(readRespectsLength);
	});

	const writeRespectsLength = async (disk) => {
		const buf = Buffer.alloc(1024);
		buf.fill(1);
		await disk.write(buf, 0, 10, 0);
		const result = await disk.read(Buffer.allocUnsafe(1024), 0, 1024, 0);
		const firstTenBytes = Buffer.allocUnsafe(10);
		firstTenBytes.fill(1);
		// first ten bytes were written: ones
		assert(result.buffer.slice(0, 10).equals(firstTenBytes));
		assert.strictEqual(result.bytesRead, 1024);
		const rest = Buffer.alloc(1024 - 10);
		// the rest was not written: zeros
		assert(result.buffer.slice(10).equals(rest));
	};

	it('write should respect the length parameter', async () => {
		await testOnAllDisks(writeRespectsLength);
	});

	const shouldReadAndWrite = async (disk) => {
		const buf = Buffer.allocUnsafe(1024);
		buf.fill(1);
		const writeResult = await disk.write(buf, 0, buf.length, 0);
		assert.strictEqual(writeResult.bytesWritten, buf.length);
		const buf2 = Buffer.allocUnsafe(1024);
		const readResult = await disk.read(buf2, 0, buf2.length, 0);
		assert.strictEqual(readResult.bytesRead, readResult.buffer.length);
		assert(buf.equals(readResult.buffer));
		await disk.flush();
	};

	it('should write and read', async () => {
		await testOnAllDisks(shouldReadAndWrite);
	});

	const createBuffer = (pattern, size) => {
		// Helper for checking disk contents.
		size = (size === undefined) ? pattern.length : size;
		const buffer = Buffer.alloc(size);
		Buffer.from(Array.from(pattern).map(Number)).copy(buffer);
		return buffer;
	};

	const checkDiskContains = async (disk, pattern) => {
		// Helper for checking disk contents.
		const size = 32;
		const expected = createBuffer(pattern, size);
		const { buffer } = await disk.read(Buffer.allocUnsafe(size), 0, size, 0);
		assert(buffer.equals(expected));
	};

	const overlappingWrites = async (disk) => {
		const buf = Buffer.allocUnsafe(8);
		await disk.discard(0, DISK_SIZE);

		buf.fill(1);
		await disk.write(buf, 0, buf.length, 0);
		await checkDiskContains(disk, '11111111');

		buf.fill(2);
		await disk.write(buf, 0, buf.length, 4);
		await checkDiskContains(disk, '111122222222');

		buf.fill(3);
		await disk.write(buf, 0, buf.length, 8);
		await checkDiskContains(disk, '1111222233333333');

		buf.fill(4);
		await disk.write(buf, 0, buf.length, 24);
		await checkDiskContains(disk, '11112222333333330000000044444444');

		buf.fill(5);
		await disk.write(buf, 0, 2, 3);
		await checkDiskContains(disk, '11155222333333330000000044444444');

		// Test disk readable stream:
		const buffer1 = await streamToBuffer(await disk.getStream());
		const expected1 = createBuffer('11155222333333330000000044444444', DISK_SIZE);
		assert(buffer1.equals(expected1));

		// Test getStream with start position
		const buffer2 = await streamToBuffer(await disk.getStream(3));
		const expected2 = createBuffer('55222333333330000000044444444', DISK_SIZE - 3);
		assert(buffer2.equals(expected2));

		// Test getStream with start position and length
		const buffer3 = await streamToBuffer(await disk.getStream(3, 4));
		const expected3 = createBuffer('5522');
		assert(buffer3.equals(expected3));

		buf.fill(6);
		await disk.write(buf, 0, 5, 2);
		await checkDiskContains(disk, '11666662333333330000000044444444');

		buf.fill(7);
		await disk.write(buf, 0, 2, 30);
		await checkDiskContains(disk, '11666662333333330000000044444477');

		buf.fill(8);
		await disk.write(buf, 0, 8, 14);
		await checkDiskContains(disk, '11666662333333888888880044444477');

		buf.fill(9);
		await disk.write(buf, 0, 8, 6);
		await checkDiskContains(disk, '11666699999999888888880044444477');

		const discarded = disk.getDiscardedChunks();
		assert.strictEqual(discarded.length, 2);
		assert.strictEqual(discarded[0].start, 22);
		assert.strictEqual(discarded[0].end, 23);
		assert.strictEqual(discarded[1].start, 32);
		assert.strictEqual(discarded[1].end, 10239);

		const blockmap1 = await disk.getBlockMap(1, true);
		assert.strictEqual(blockmap1.ranges.length, 2);
		assert.strictEqual(blockmap1.ranges[0].checksum, sha256(createBuffer('1166669999999988888888')));
		assert.strictEqual(blockmap1.ranges[1].checksum, sha256(createBuffer('44444477')));

		const blockmap2 = await disk.getBlockMap(2, true);
		assert.strictEqual(blockmap2.ranges.length, 2);
		assert.strictEqual(blockmap2.ranges[0].checksum, sha256(createBuffer('1166669999999988888888')));
		assert.strictEqual(blockmap2.ranges[1].checksum, sha256(createBuffer('44444477')));

		const blockmap3 = await disk.getBlockMap(3, true);
		assert.strictEqual(blockmap3.ranges.length, 1);
		assert.strictEqual(blockmap3.ranges[0].checksum, sha256(createBuffer('116666999999998888888800444444770')));

		const blockmap4 = await disk.getBlockMap(4, true);
		assert.strictEqual(blockmap4.ranges.length, 1);
		assert.strictEqual(blockmap4.ranges[0].checksum, sha256(createBuffer('11666699999999888888880044444477')));

		const blockmap5 = await disk.getBlockMap(5, true);
		assert.strictEqual(blockmap5.ranges.length, 1);
		assert.strictEqual(blockmap5.ranges[0].checksum, sha256(createBuffer('11666699999999888888880044444477000')));

		const blockmap6 = await disk.getBlockMap(6, true);
		assert.strictEqual(blockmap6.ranges.length, 1);
		assert.strictEqual(blockmap6.ranges[0].checksum, sha256(createBuffer('116666999999998888888800444444770000')));

		const blockmap7 = await disk.getBlockMap(7, true);
		assert.strictEqual(blockmap7.ranges.length, 1);
		assert.strictEqual(blockmap7.ranges[0].checksum, sha256(createBuffer('11666699999999888888880044444477000')));

		const blockmap8 = await disk.getBlockMap(8, true);
		assert.strictEqual(blockmap8.ranges.length, 1);
		assert.strictEqual(blockmap8.ranges[0].checksum, sha256(createBuffer('11666699999999888888880044444477')));

		const blockmap9 = await disk.getBlockMap(9, true);
		assert.strictEqual(blockmap9.ranges.length, 1);
		assert.strictEqual(blockmap9.ranges[0].checksum, sha256(createBuffer('116666999999998888888800444444770000')));

		const blockmap10 = await disk.getBlockMap(10, true);
		assert.strictEqual(blockmap10.ranges.length, 1);
		assert.strictEqual(blockmap10.ranges[0].checksum, sha256(createBuffer('1166669999999988888888004444447700000000')));

		const blockmap11 = await disk.getBlockMap(11, true);
		assert.strictEqual(blockmap11.ranges.length, 1);
		assert.strictEqual(blockmap11.ranges[0].checksum, sha256(createBuffer('116666999999998888888800444444770')));
	};

	it('copy on write mode should properly record overlapping writes', async () => {
		await testOnAllDisks(overlappingWrites);
	});
});
