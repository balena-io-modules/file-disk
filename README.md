# file-disk
Handles reads / writes on disk image files.

[![Build Status](https://travis-ci.org/resin-io-modules/file-disk.svg?branch=master)](https://travis-ci.org/resin-io-modules/file-disk)

## API

**Warning: The API exposed by this library is still forming and can change at
any time!**

### FileDisk

`new FileDisk(fd, readOnly, recordWrites, recordReads, discardIsZero=true)`

 - `fd` is a file descriptor returned by `fs.open`
 - `readOnly` a boolean (default `false`)
 - `recordWrites`, a boolean (default `false`); if you use `readOnly` without
 `recordWrites`, all write requests will be lost.
 - `recordReads`, a boolean (default `false`): cache reads in memory
 - `discardIsZero`, a boolean (default `true`): don't read discarded regions,
 return zero filled buffers instead.

`FileDisk.getCapacity()`: `Promise<Number>`

`FileDisk.read(buffer, bufferOffset, length, fileOffset)`: `Promise<{ bytesRead: Number, buffer: Buffer }>`

 - behaves like [fs.read](https://nodejs.org/api/fs.html#fs_fs_read_fd_buffer_offset_length_position_callback)

`FileDisk.write(buffer, bufferOffset, length, fileOffset)`: `Promise<{ bytesWritten: Number, buffer: Buffer }>`

 - behaves like [fs.write](https://nodejs.org/api/fs.html#fs_fs_write_fd_buffer_offset_length_position_callback)

`FileDisk.flush()`: `Promise<undefined>`

 - behaves like [fs.fdatasync](https://nodejs.org/api/fs.html#fs_fs_fdatasync_fd_callback)

`FileDisk.discard(offset, length)`: `Promise<undefined>`

`FileDisk.getStream([position, [length, [highWaterMark]]])`: `Promise<stream.Readable>`
 - `position` start reading from this offset (defaults to 0)
 - `length` read that amount of bytes (defaults to (disk capacity - position))
 - `highWaterMark` (defaults to 16384, minimum 16) is the size of chunks that
 will be read

`FileDisk.getDiscardedChunks()` returns the list of discarded chunks. Each chunk
has a `start` and `end` properties. `end` position is inclusive.

`FileDisk.getBlockMap(blockSize, calculateChecksums`: `Promise<blockmap.BlockMap>`
 - using the disk's discarded chunks and the given blockSize, it returns a Promise
of a [`BlockMap`](https://github.com/resin-io-modules/blockmap).
Be careful to how you use `Disk`'s `discardIsZero` option as it may change the
blockmap ranges checksums if discarded regions not aligned with `blockSize`
contain anything else than zeros on the disk.

### S3Disk

`S3Disk` acts like `FileDisk` except it reads the image file from S3 instead of
the filesystem. `S3Disk` has `readOnly` and `recordWrites` enabled. This can
not be changed.

`new S3Disk(s3, bucket, key, recordReads, discardIsZero=true)`

 - `s3` is an s3 connection.
 - `bucket` is the S3 bucket to use.
 - `key` is the key (file name) to use in the bucket.

For more information about S3Disk parameters see
[the aws documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

## Examples

### Read 1024 first bytes, write them starting at position 1024 then flush.

```javascript

const Promise = require('bluebird');
const filedisk = require('file-disk');

Promise.using(filedisk.openFile('/path/to/some/file', 'r+'), (fd) => {
	const disk = new filedisk.FileDisk(fd)

	// get file size
	return disk.getCapacity()
	.then((size) => {
		console.log("size:", size);
		const buf = Buffer.alloc(1024);
		// read `buf.length` bytes starting at 0 from the file into `buf`
		return disk.read(buf, 0, buf.length, 0);
	})
	.then(({ bytesRead, buffer }) => {
		// write `buffer` into file starting at `buffer.length` (in the file)
		return disk.write(buf, 0, buf.length, buf.length);
	})
	.then(({ bytesWritten, buffer }) => {
		// flush
		return disk.flush();
	});
});


```

### Open a file readOnly, use the recordWrites mode, then stream the contents somewhere.

```javascript

const Promise = require('bluebird');
const filedisk = require('file-disk');

const BUF = Buffer.alloc(1024);

Promise.using(filedisk.openFile('/path/to/some/file', 'r'), (fd) => {
	const disk = new filedisk.FileDisk(fd, true, true);

	// read `BUF.length` bytes starting at 0 from the file into `BUF`
	return disk.read(BUF, 0, BUF.length, 0);
	.then(({ bytesRead, buffer }) => {
		// write `buffer` into file starting at `buffer.length` (in the file)
		return disk.write(buffer, 0, buffer.length, buffer.length);
	})
	.then(({ bytesWritten, buffer }) => {
		const buf2 = Buffer.alloc(1024);
		// read what we've just written
		return disk.read(buf2, 0, buffer.length, 0);
	})
	.then(({ bytesRead, buffer }) => {
		// writes are stored in memory
		assert(BUF.equals(buffer));
	})
	.then(() => {
		return disk.getStream();
	})
	.then((stream) => {
		// pipe the stream somewhere
		return new Promise((resolve, reject) => {
			stream.pipe(someWritableStream)
			.on('close', resolve)
			.on('error', reject);
		});
	});
});

```
