# resin-file-disk
Handles reads / writes on disk image files.

## API

**Warning: The API exposed by this library is still forming and can change at
any time!**

### FileDisk

`new FileDisk(path, mapping, readOnly, recordWrites)`

 - `path` is the path to the disk image
 - `mapping` is an object mapping `read`, `write` and `flush` to numerical
 values. When `FileDisk.request` is called, these values will be used to call
 the right method. Example: `{ read: 0, write: 1, flush: 2 }`
 - `readOnly` a boolean (default `false`)
 - `recordWrites`, a boolean (default `false`); if you use `readOnly` without
 `recordWrites`, all write requests will be lost.


`FileDisk.getCapacity(callback)`

 - `callback(err, size)` will be called with the size of the disk image in
 bytes


`FileDisk.request(type, offset, length, buffer, callback)`
 - `type` is a number indicating the type of the request (as defined in the
 `mapping` parameter of `new FileDisk`.
 - `offset` is the offset where the read / write will start in the file.
 - `length` is the number of bytes to read / write from / to the file.
 - `buffer` is a `Buffer`. In case of read requests it is where the read data
 will be stored. In case of write requests it is the buffer containing the data
 to write.
 - `callback` will be called with different parameters depending on the request
 `type`:
   - `callback(err, bytesRead, buffer)` for read requests;
   - `callback(err, bytesWritten)` for write requests;
   - `callback(err)` for flush requests.

### S3Disk

`S3Disk` acts like `FileDisk` except it reads the image file from S3 instead of
the filesystem. `S3Disk` has `readOnly` and `recordWrites` enabled. This can
not be changed.

```javascript
new S3Disk(
	mapping,
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

 - `mapping` same as for `FileDisk`
 - `bucket` is the S3 bucket to use.
 - `key` is the key (file name) to use in the bucket.
 - `accessKey` is the S3 access key.
 - `secretKey` is the S3 secret key.
 - `endpoint` [optional] allows to override the S3 URL.
 - `sslEnabled` (defaults to true).
 - `s3ForcePathStyle` (defaults to true).
 - `signatureVersion` (defaults to `'v4'`).

For all parameters except `mapping` see [the aws documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

## Examples

### Read 1024 first bytes, write them starting at position 1024 then flush.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

const READ = 0;
const WRITE = 1;
const FLUSH = 2;

const mapping = {
	read: READ,
	write: WRITE,
	flush: FLUSH
};

const disk = new filedisk.FileDisk('/path/to/some/file', mapping)


// get file size
disk.getCapacityAsync()
.spread(function(size) {
	console.log("size:", size);
	const buf = Buffer.alloc(1024);
	// read `buf.length` bytes starting at 0 from the file into `buf`
	return disk.requestAsync(READ, 0, buf.length, buf);
})
.spread(function(bytesRead, buf) {
	// write `buf` into file starting at `buf.length` (in the file)
	return disk.requestAsync(WRITE, buf.length, buf.length, buf);
})
.spread(function(bytesWritten) {
	// flush
	return disk.requestAsync(FLUSH, null, null, null);
});


```

### Open a file readOnly and use the recordWrites mode.

```javascript

const Promise = require('bluebird');
const filedisk = Promise.promisifyAll(require('resin-file-disk'), { multiArgs: true });

const READ = 0;
const WRITE = 1;
const FLUSH = 2;

const mapping = {
	read: READ,
	write: WRITE,
	flush: FLUSH
};

const disk = new filedisk.FileDisk('/path/to/some/file', mapping, true, true)

const buf = Buffer.alloc(1024);

// read `buf.length` bytes starting at 0 from the file into `buf`
disk.requestAsync(READ, 0, buf.length, buf)
.spread(function(bytesRead, buf) {
	// write `buf` into file starting at `buf.length` (in the file)
	return disk.requestAsync(WRITE, buf.length, buf.length, buf);
})
.spread(function(bytesWritten) {
	const buf2 = Buffer.alloc(1024);
	// read what we've just written
	return disk.requestAsync(READ, buf.length, buf.length, buf2);
});
.spread(function(bytesRead, buf2) {
	// writes are stored in memory
	assert(buf.equals(buf2));
});

```
