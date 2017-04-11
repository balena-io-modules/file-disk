# resin-file-disk
Handles reads / writes on disk image files.

[![Build Status](https://travis-ci.org/resin-io-modules/resin-file-disk.svg?branch=master)](https://travis-ci.org/resin-io-modules/resin-file-disk)

## API

**Warning: The API exposed by this library is still forming and can change at
any time!**

### FileDisk

`new FileDisk(fd, readOnly, recordWrites)`

 - `fd` is a file descriptor returned by `fs.open`
 - `readOnly` a boolean (default `false`)
 - `recordWrites`, a boolean (default `false`); if you use `readOnly` without
 `recordWrites`, all write requests will be lost.

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

### S3Disk

`S3Disk` acts like `FileDisk` except it reads the image file from S3 instead of
the filesystem. `S3Disk` has `readOnly` and `recordWrites` enabled. This can
not be changed.

`new S3Disk(s3, bucket, key)`

 - `s3` is an s3 connection.
 - `bucket` is the S3 bucket to use.
 - `key` is the key (file name) to use in the bucket.

For more information about S3Disk parameters see
[the aws documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

## Examples

### Read 1024 first bytes, write them starting at position 1024 then flush.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

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

### Open a file readOnly and use the recordWrites mode.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

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
	});
});

```
