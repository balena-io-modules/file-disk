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

`FileDisk.getCapacity(callback(err, size))`

 - `callback(err, size)` will be called with the size of the disk image in
 bytes

`FileDisk.read(buffer, bufferOffset, length, fileOffset, callback(err, bytesRead, buffer))`

 - behaves like [fs.read](https://nodejs.org/api/fs.html#fs_fs_read_fd_buffer_offset_length_position_callback)

`FileDisk.write(buffer, bufferOffset, length, fileOffset, callback(err, bytesWritten))`

 - behaves like [fs.write](https://nodejs.org/api/fs.html#fs_fs_write_fd_buffer_offset_length_position_callback)

`FileDisk.flush(callback(err))`

 - behaves like [fs.fdatasync](https://nodejs.org/api/fs.html#fs_fs_fdatasync_fd_callback)

`FileDisk.discard(offset, length, callback(err))`

 - not implemented

`FileDisk.getStream(position, length, highWaterMark, callback(err, stream))`
 - `position` start reading from this offset (`null` means zero)
 - `length` read that amount of bytes (`null` means read until the end)
 - `highWaterMark` [optional, defaults to 16384, minimum 16] is the size of
 chunks that will be read
 - `callback(err, stream)` will be called with a readable stream of the disk
 content

`FileDisk.getDiscardedChunks()` returns the list of discarded chunks. Each chunk
has a `start` and `end` properties. `end` position is inclusive.

`FileDisk.getBlockMap(blockSize, callback(err, blockmap))` using the disk's
discarded chunks and the given blockSize, it calls back with
a [`BlockMap`](https://github.com/resin-io-modules/blockmap).
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
const filedisk = Promise.promisifyAll(require('file-disk'), { multiArgs: true });

Promise.using(filedisk.openFile('/path/to/some/file', 'r+'), function(fd) {
	const disk = new filedisk.FileDisk(fd)

	// get file size
	return disk.getCapacityAsync()
	.spread(function(size) {
		console.log("size:", size);
		const buf = Buffer.alloc(1024);
		// read `buf.length` bytes starting at 0 from the file into `buf`
		return disk.readAsync(buf, 0, buf.length, 0);
	})
	.spread(function(bytesRead, buf) {
		// write `buf` into file starting at `buf.length` (in the file)
		return disk.writeAsync(buf, 0, buf.length, buf.length);
	})
	.spread(function(bytesWritten) {
		// flush
		return disk.flushAsync();
	});
});


```

### Open a file readOnly, use the recordWrites mode, then stream the contents somewhere.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('file-disk'), { multiArgs: true });

const buf = Buffer.alloc(1024);

Promise.using(filedisk.openFile('/path/to/some/file', 'r'), function(fd) {
	const disk = new filedisk.FileDisk(fd, true, true);

	// read `buf.length` bytes starting at 0 from the file into `buf`
	return disk.readAsync(buf, 0, buf.length, 0);
	.spread(function(bytesRead, buf) {
		// write `buf` into file starting at `buf.length` (in the file)
		return disk.writeAsync(buf, 0, buf.length, buf.length);
	})
	.spread(function(bytesWritten) {
		const buf2 = Buffer.alloc(1024);
		// read what we've just written
		return disk.readAsync(buf2, 0, buf.length, 0);
	});
	.spread(function(bytesRead, buf2) {
		// writes are stored in memory
		assert(buf.equals(buf2));
	})
	.then(function() {
		return disk.getStreamAsync(null, null);
	})
	.spread(function(stream) {
		// pipe the stream somewhere
		return new Promise(function(resolve, reject) {
			stream.pipe(someWritableStream)
			.on('close', resolve)
			.on('error', reject);
		});
	});
});

```
