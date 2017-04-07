# resin-file-disk
Handles reads / writes on disk image files.

## API

**Warning: The API exposed by this library is still forming and can change at
any time!**

### FileDisk

`new FileDisk(path, readOnly, recordWrites)`

 - `path` is the path to the disk image
 - `readOnly` a boolean (default `false`)
 - `recordWrites`, a boolean (default `false`); if you use `readOnly` without
 `recordWrites`, all write requests will be lost.

`FileDisk.open(callback(err, disk))`

 - `callback(err, disk)` when the disk is open.

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
`FileDisk.close(callback(err))`
 - behaves like [fs.close](https://nodejs.org/api/fs.html#fs_fs_close_fd_callback)

### S3Disk

`S3Disk` acts like `FileDisk` except it reads the image file from S3 instead of
the filesystem. `S3Disk` has `readOnly` and `recordWrites` enabled. This can
not be changed.

```javascript
new S3Disk(
	bucket,
	key,
	accessKey,
	secretKey,
	endpoint=null,
	sslEnabled=true,
	s3ForcePathStyle=true,
	signatureVersion='v4'
);
```

 - `bucket` is the S3 bucket to use.
 - `key` is the key (file name) to use in the bucket.
 - `accessKey` is the S3 access key.
 - `secretKey` is the S3 secret key.
 - `endpoint` [optional] allows to override the S3 URL.
 - `sslEnabled` (defaults to true).
 - `s3ForcePathStyle` (defaults to true).
 - `signatureVersion` (defaults to `'v4'`).

For more information about S3Disk parameters see
[the aws documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

## Examples

### Read 1024 first bytes, write them starting at position 1024 then flush.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

const disk = new filedisk.FileDisk('/path/to/some/file')

disk.openAsync()
.then(function() {
	// get file size
	return disk.getCapacityAsync();
})
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
})
.then(function() {
	return disk.closeAsync();
});


```

### Open a file readOnly and use the recordWrites mode.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

const disk = new filedisk.FileDisk('/path/to/some/file', true, true)

const buf = Buffer.alloc(1024);

disk.openAsync()
.then(function() {
	// read `buf.length` bytes starting at 0 from the file into `buf`
	return disk.readAsync(buf, 0, buf.length, 0);
})
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
	return disk.closeAsync();
});

```
