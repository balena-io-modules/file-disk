import * as AWS from 'aws-sdk';
import * as Promise from 'bluebird';
import { Readable } from 'stream';

import { getBlockMap } from './blockmap';
import { BufferDiskChunk, DiscardDiskChunk, DiskChunk } from './diskchunk';
import * as fs from './fs';
import { intervalIntersection, Interval } from './interval-intersection';

export { BufferDiskChunk, DiscardDiskChunk, DiskChunk, Interval };
export { ReadResult, WriteResult } from './fs';

const MIN_HIGH_WATER_MARK = 16;
const DEFAULT_HIGH_WATER_MARK = 16384;

export class DiskStream extends Readable {
	constructor(private readonly disk: Disk, private readonly capacity: number, highWaterMark: number, private position: number) {
		super({ highWaterMark: Math.max(highWaterMark, MIN_HIGH_WATER_MARK) });
	}

	_read(size: number): void {
		const length = Math.min(size, this.capacity - this.position);
		if (length <= 0) {
			this.push(null);
			return;
		}
		this.disk.read(
			Buffer.allocUnsafe(length),
			0,  // buffer offset
			length,
			this.position,  // disk offset
		)
		.then(({ bytesRead, buffer }) => {
			this.position += bytesRead;
			if (this.push(buffer)) {
				this._read(size);
			}
		})
		.catch((err) => {
			this.emit('error', err);
		});
	}
}

type ReadPlan = (Interval | DiskChunk)[];

export const openFile = (path: string, flags: string | number, mode?: number): Promise.Disposer<number> => {
	// Opens a file and closes it when you're done using it.
	// Arguments are the same that for `fs.open()`
	// Use it with Bluebird's `using`, example:
	// Promise.using(openFile('/some/path', 'r+'), (fd) => {
	//   doSomething(fd);
	// });
	return fs.open(path, flags, mode)
	.disposer((fd) => {
		return fs.close(fd);
	});
};

export abstract class Disk {
	// Subclasses need to implement:
	// * _getCapacity(): Promise<Number>
	// * _read(buffer, bufferOffset, length, fileOffset): Promise<{ bytesRead: Number, buffer: Buffer }>
	// * _write(buffer, bufferOffset, length, fileOffset): Promise<{ bytesWritten: Number, buffer: Buffer }> [only for writable disks]
	// * _flush(): Promise<void> [only for writable disks]
	//
	// Users of instances of subclasses can use:
	// * getCapacity(): Promise<Number>
	// * read(buffer, bufferOffset, length, fileOffset): Promise<{ bytesRead: Number, buffer: Buffer }>
	// * write(buffer, bufferOffset, length, fileOffset): Promise<{ bytesWritten: Number, buffer: Buffer }>
	// * flush(): Promise<void>
	// * discard(offset, length): Promise<void>
	// * getStream([position, [length, [highWaterMark]]]): Promise<stream.Readable>
	//   * position: start reading from this offset (defaults to zero)
	//   * length: read that amount of bytes (defaults to (disk capacity - position))
	//   * highWaterMark: size of chunks that will be read (default 16384, minimum 16)
	readonly knownChunks: DiskChunk[] = [];  // sorted list of non overlapping DiskChunks
	capacity: number | null = null;

	constructor(public readOnly: boolean = false, public recordWrites: boolean = false, public recordReads: boolean = false, public discardIsZero: boolean = true) {
	}

	protected abstract _getCapacity(): Promise<number>;

	protected abstract _read(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.ReadResult>;

	protected abstract _write(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.WriteResult>;

	protected abstract _flush(): Promise<void>;

	public read(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number) {
		const plan = this.createReadPlan(fileOffset, length);
		return this.readAccordingToPlan(buffer, plan);
	}

	public write(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number) {
		if (this.recordWrites) {
			const chunk = new BufferDiskChunk(
				buffer.slice(bufferOffset, bufferOffset + length),
				fileOffset,
			);
			this.insertDiskChunk(chunk);
		} else {
			// Special case: we do not record writes but we may have recorded
			// some discards. We want to remove any discard overlapping this
			// write.
			// In order to do this we do as if we were inserting a chunk: this
			// will slice existing discards in this area if there are any.
			const chunk = new DiscardDiskChunk(fileOffset, length);
			// The `false` below means "don't insert the chunk into knownChunks"
			this.insertDiskChunk(chunk, false);
		}
		if (this.readOnly) {
			return Promise.resolve({ bytesWritten: length, buffer });
		} else {
			return this._write(buffer, bufferOffset, length, fileOffset);
		}
	}

	public flush() {
		if (this.readOnly) {
			return Promise.resolve();
		} else {
			return this._flush();
		}
	}

	public discard(offset: number, length: number): Promise<void> {
		this.insertDiskChunk(new DiscardDiskChunk(offset, length));
		return Promise.resolve();
	}

	public getCapacity(): Promise<number> {
		if (this.capacity !== null) {
			return Promise.resolve(this.capacity);
		}
		return this._getCapacity()
		.then((capacity) => {
			this.capacity = capacity;
			return capacity;
		});
	}

	public getStream(position: number = 0, length: number | null = null, highWaterMark: number = DEFAULT_HIGH_WATER_MARK): Promise<DiskStream> {
		return this.getCapacity()
		.then((end) => {
			if (length !== null) {
				end = Math.min(position + length, end);
			}
			return new DiskStream(this, end, highWaterMark, position);
		});
	}

	public getDiscardedChunks(): DiskChunk[] {
		return this.knownChunks.filter((chunk) => {
			return (chunk instanceof DiscardDiskChunk);
		});
	}

	public getBlockMap(blockSize: number, calculateChecksums: boolean) {
		return this.getCapacity()
		.then((capacity) => {
			return getBlockMap(this, blockSize, capacity, calculateChecksums);
		});
	}

	private insertDiskChunk(chunk: DiskChunk, insert: boolean = true): void {
		let insertAt = 0;
		for (let i = 0; i < this.knownChunks.length; i++) {
			const other = this.knownChunks[i];
			if (other.start > chunk.end) {
				break;
			}
			if (other.start < chunk.start) {
				insertAt = i + 1;
			} else {
				insertAt = i;
			}
			if (!chunk.intersects(other)) {
				continue;
			} else if (other.includedIn(chunk)) {
				// Delete other
				this.knownChunks.splice(i, 1);
				i--;
			} else {
				// Cut other
				const newChunks = other.cut(chunk);
				this.knownChunks.splice(i, 1, ...newChunks);
				i += newChunks.length - 1;
			}
		}
		if (insert) {
			this.knownChunks.splice(insertAt, 0, chunk);
		}
	}

	private createReadPlan(offset: number, length: number): ReadPlan {
		const end = offset + length - 1;
		const interval: Interval = [ offset, end ];
		let chunks = this.knownChunks;
		if (!this.discardIsZero) {
			chunks = chunks.filter((chunk) => {
				return !(chunk instanceof DiscardDiskChunk);
			});
		}
		const intersections: DiskChunk[] = [];
		chunks.forEach((chunk) => {
			const inter = intervalIntersection(interval, chunk.interval());
			if (inter !== null) {
				intersections.push(chunk.slice(inter[0], inter[1]));
			}
		});
		if (intersections.length === 0) {
			return [ interval ];
		}
		const readPlan: ReadPlan = [];
		let chunk;
		for (chunk of intersections) {
			if (offset < chunk.start) {
				readPlan.push([offset, chunk.start - 1]);
			}
			readPlan.push(chunk);
			offset = chunk.end + 1;
		}
		if (chunk && (end > chunk.end)) {
			readPlan.push([chunk.end + 1, end]);
		}
		return readPlan;
	}

	private readAccordingToPlan(buffer: Buffer, plan: ReadPlan) {
		let offset = 0;
		return Promise.each(plan, (entry) => {
			if (entry instanceof DiskChunk) {
				const data = entry.data();
				const length = Math.min(data.length, buffer.length - offset);
				data.copy(buffer, offset, 0, length);
				offset += length;
			} else {
				const length = entry[1] - entry[0] + 1;
				return this._read(buffer, offset, length, entry[0])
				.then(() => {
					if (this.recordReads) {
						const chunk = new BufferDiskChunk(
							Buffer.from(buffer.slice(offset, offset + length)),
							entry[0],
						);
						this.insertDiskChunk(chunk);
					}
					offset += length;
				});
			}
		})
		.then(() => {  // Using .return() here wouldn't work because offset would be 0
			return { bytesRead: offset, buffer };
		});
	}
}

export class FileDisk extends Disk {
	constructor(protected readonly fd: number, readOnly: boolean = false, recordWrites: boolean = false, recordReads: boolean = false, discardIsZero: boolean = true) {
		super(readOnly, recordWrites, recordReads, discardIsZero);
	}

	protected _getCapacity() {
		return fs.fstat(this.fd).get('size');
	}

	protected _read(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.ReadResult> {
		return fs.read(this.fd, buffer, bufferOffset, length, fileOffset);
	}

	protected _write(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.WriteResult> {
		return fs.write(this.fd, buffer, bufferOffset, length, fileOffset);
	}

	protected _flush(): Promise<void> {
		return fs.fdatasync(this.fd).return();
	}
}

interface S3Params {
	Bucket: string;
	Key: string;
	Range?: string;
}

const getContentLength = (response: AWS.S3.GetObjectOutput | AWS.S3.HeadObjectOutput): number => {
	const contentLength = response.ContentLength;
	if (contentLength === undefined) {
		throw new Error('No ContentLength in S3 response');
	}
	return contentLength;
};

export class S3Disk extends Disk {
	constructor(protected readonly s3: AWS.S3, protected readonly bucket: string, protected readonly key: string, recordReads: boolean = false, discardIsZero: boolean = true) {
		super(true, true, recordReads, discardIsZero);
	}

	private getS3Params(): S3Params {
		return { Bucket: this.bucket, Key: this.key };
	}

	protected _getCapacity(): Promise<number> {
		// Make sure we return a Bluebird Promise
		return Promise.resolve(this.s3.headObject(this.getS3Params()).promise())
		.then(getContentLength);
	}

	protected _read(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.ReadResult> {
		const params = this.getS3Params();
		params.Range = `bytes=${fileOffset}-${fileOffset + length - 1}`;
		// Make sure we return a Bluebird Promise
		return Promise.resolve(this.s3.getObject(params).promise())
		.then((data) => {
			(data.Body as Buffer).copy(buffer, bufferOffset);
			return { bytesRead: getContentLength(data), buffer };
		});
	}

	protected _write(buffer: Buffer, bufferOffset: number, length: number, fileOffset: number): Promise<fs.WriteResult> {
		return Promise.reject(new Error("Can't write in a S3Disk"));
	}

	protected _flush(): Promise<void> {
		return Promise.reject(new Error("Can't flush a S3Disk"));
	}
}
