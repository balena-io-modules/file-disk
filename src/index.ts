import { promises as fs } from 'fs';
import { Readable, Transform } from 'stream';

import { BufferDiskChunk, DiscardDiskChunk, DiskChunk } from './diskchunk';
import { Interval, intervalIntersection } from './interval-intersection';
import { getRanges, Range } from './mapped-ranges';

export { BufferDiskChunk, DiscardDiskChunk, DiskChunk, Interval, Range };

const MIN_HIGH_WATER_MARK = 16;
const DEFAULT_HIGH_WATER_MARK = 16384;

export interface ReadResult {
	bytesRead: number;
	buffer: Buffer;
}

export interface WriteResult {
	bytesWritten: number;
	buffer: Buffer;
}

export class DiskStream extends Readable {
	private isReading: boolean = false;

	constructor(
		private readonly disk: Disk,
		private readonly capacity: number,
		highWaterMark: number,
		private position: number,
	) {
		super({ highWaterMark: Math.max(highWaterMark, MIN_HIGH_WATER_MARK) });
	}

	private async __read(size: number): Promise<void> {
		if (this.isReading) {
			// We're already reading, return.
			return;
		}
		this.isReading = true;
		while (this.isReading) {
			// Don't read out of bounds
			const length = Math.min(size, this.capacity - this.position);
			if (length <= 0) {
				// Nothing left to read: push null to signal end of stream.
				this.isReading = this.push(null);
				return;
			}
			let bytesRead: number;
			let buffer: Buffer;
			try {
				({ bytesRead, buffer } = await this.disk.read(
					Buffer.allocUnsafe(length),
					0,
					length,
					this.position,
				));
			} catch (err) {
				this.emit('error', err);
				return;
			}
			this.position += bytesRead;
			// this.push() returns true if we need to continue reading.
			this.isReading = this.push(buffer);
		}
	}

	public _read(size: number): void {
		this.__read(size);
	}
}

class DiskTransformStream extends Transform {
	// Adds the recorded writes to the original disk stream.
	private position: number = 0;
	private readonly chunks: Iterator<DiskChunk>;
	private currentChunk: DiskChunk | null;

	constructor(private readonly disk: Disk) {
		super();
		this.chunks = this.getKnownChunks();
		this.currentChunk = this.chunks.next().value;
	}

	private *getKnownChunks(): Iterator<DiskChunk> {
		for (const chunk of this.disk.knownChunks) {
			if (chunk instanceof BufferDiskChunk) {
				yield chunk;
			}
		}
	}

	public _transform(chunk: Buffer, _enc: string, cb: () => void): void {
		const start: number = this.position;
		const end: number = start + chunk.length - 1;
		const interval: Interval = [start, end];
		while (this.currentChunk) {
			if (intervalIntersection(this.currentChunk.interval(), interval)) {
				const buf = this.currentChunk.data();
				const startShift = this.currentChunk.start - start;
				const endShift = this.currentChunk.end - end;
				const sourceStart = startShift < 0 ? -startShift : 0;
				const sourceEnd = buf.length - Math.max(endShift, 0);
				const targetStart = Math.max(startShift, 0);
				buf.copy(chunk, targetStart, sourceStart, sourceEnd);
			}
			if (this.currentChunk.end > end) {
				break;
			} else {
				this.currentChunk = this.chunks.next().value;
			}
		}
		this.push(chunk);
		this.position = end + 1;
		cb();
	}
}

type ReadPlan = Array<Interval | DiskChunk>;

export async function withOpenFile<T>(
	path: string,
	flags: string | number,
	fn: (handle: fs.FileHandle) => Promise<T>,
): Promise<T> {
	// Opens a file and closes it when you're done using it.
	// Arguments are the same that for `fs.promises.open()`
	// Example:
	// await withOpenFile('/some/path', 'r+', async (handle) => {
	//   doSomething(handle);
	// });
	const handle = await fs.open(path, flags);
	try {
		return await fn(handle);
	} finally {
		await handle.close();
	}
}

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
	public readonly knownChunks: DiskChunk[] = []; // sorted list of non overlapping DiskChunks
	public capacity: number | null = null;

	constructor(
		public readOnly: boolean = false,
		public recordWrites: boolean = false,
		public recordReads: boolean = false,
		public discardIsZero: boolean = true,
	) {}

	protected abstract async _getCapacity(): Promise<number>;

	protected abstract async _read(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<ReadResult>;

	protected abstract async _write(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<WriteResult>;

	protected abstract async _flush(): Promise<void>;

	public getTransformStream(): Transform {
		// Returns a Transform that adds the recorded writes to the original image stream.
		return new DiskTransformStream(this);
	}

	public async read(
		buffer: Buffer,
		_bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<ReadResult> {
		const plan = this.createReadPlan(fileOffset, length);
		return await this.readAccordingToPlan(buffer, plan);
	}

	public async write(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<WriteResult> {
		if (this.recordWrites) {
			const chunk = new BufferDiskChunk(
				buffer.slice(bufferOffset, bufferOffset + length),
				fileOffset,
			);
			await this.insertDiskChunk(chunk);
		} else {
			// Special case: we do not record writes but we may have recorded
			// some discards. We want to remove any discard overlapping this
			// write.
			// In order to do this we do as if we were inserting a chunk: this
			// will slice existing discards in this area if there are any.
			const chunk = new DiscardDiskChunk(fileOffset, length);
			// The `false` below means "don't insert the chunk into knownChunks"
			await this.insertDiskChunk(chunk, false);
		}
		if (this.readOnly) {
			return { bytesWritten: length, buffer };
		} else {
			return await this._write(buffer, bufferOffset, length, fileOffset);
		}
	}

	public async flush(): Promise<void> {
		if (!this.readOnly) {
			return await this._flush();
		}
	}

	public async discard(offset: number, length: number): Promise<void> {
		await this.insertDiskChunk(new DiscardDiskChunk(offset, length));
	}

	public async getCapacity(): Promise<number> {
		if (this.capacity === null) {
			this.capacity = await this._getCapacity();
		}
		return this.capacity;
	}

	public async getStream(
		position: number = 0,
		length: number | null = null,
		highWaterMark: number = DEFAULT_HIGH_WATER_MARK,
	): Promise<DiskStream> {
		let end = await this.getCapacity();
		if (length !== null) {
			end = Math.min(position + length, end);
		}
		return new DiskStream(this, end, highWaterMark, position);
	}

	public getDiscardedChunks(): DiskChunk[] {
		return this.knownChunks.filter((chunk) => {
			return chunk instanceof DiscardDiskChunk;
		});
	}

	public async getRanges(blockSize: number): Promise<Range[]> {
		return Array.from(await getRanges(this, blockSize));
	}

	private async insertDiskChunk(
		chunk: DiskChunk,
		insert: boolean = true,
	): Promise<void> {
		const capacity = await this.getCapacity();
		if (chunk.start < 0 || chunk.end > capacity) {
			// Invalid chunk
			return;
		}
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
		const interval: Interval = [offset, end];
		let chunks = this.knownChunks;
		if (!this.discardIsZero) {
			chunks = chunks.filter((c) => !(c instanceof DiscardDiskChunk));
		}
		const intersections: DiskChunk[] = [];
		chunks.forEach((c) => {
			const inter = intervalIntersection(interval, c.interval());
			if (inter !== null) {
				intersections.push(c.slice(inter[0], inter[1]));
			}
		});
		if (intersections.length === 0) {
			return [interval];
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
		if (chunk && end > chunk.end) {
			readPlan.push([chunk.end + 1, end]);
		}
		return readPlan;
	}

	private async readAccordingToPlan(
		buffer: Buffer,
		plan: ReadPlan,
	): Promise<ReadResult> {
		let offset = 0;
		for (const entry of plan) {
			if (entry instanceof DiskChunk) {
				const data = entry.data();
				const length = Math.min(data.length, buffer.length - offset);
				data.copy(buffer, offset, 0, length);
				offset += length;
			} else {
				const length = entry[1] - entry[0] + 1;
				await this._read(buffer, offset, length, entry[0]);
				if (this.recordReads) {
					const chunk = new BufferDiskChunk(
						Buffer.from(buffer.slice(offset, offset + length)),
						entry[0],
					);
					await this.insertDiskChunk(chunk);
				}
				offset += length;
			}
		}
		return { bytesRead: offset, buffer };
	}
}

export class FileDisk extends Disk {
	constructor(
		protected readonly handle: fs.FileHandle,
		readOnly: boolean = false,
		recordWrites: boolean = false,
		recordReads: boolean = false,
		discardIsZero: boolean = true,
	) {
		super(readOnly, recordWrites, recordReads, discardIsZero);
	}

	protected async _getCapacity(): Promise<number> {
		const stats = await this.handle.stat();
		return stats.size;
	}

	protected async _read(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<ReadResult> {
		return await this.handle.read(buffer, bufferOffset, length, fileOffset);
	}

	protected async _write(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<WriteResult> {
		return await this.handle.write(buffer, bufferOffset, length, fileOffset);
	}

	protected async _flush(): Promise<void> {
		await this.handle.datasync();
	}
}

export class BufferDisk extends Disk {
	constructor(
		private readonly buffer: Buffer,
		readOnly: boolean = false,
		recordWrites: boolean = false,
		recordReads: boolean = false,
		discardIsZero: boolean = true,
	) {
		super(readOnly, recordWrites, recordReads, discardIsZero);
	}

	protected async _getCapacity(): Promise<number> {
		return this.buffer.length;
	}

	protected async _read(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<ReadResult> {
		const bytesRead = this.buffer.copy(
			buffer,
			bufferOffset,
			fileOffset,
			fileOffset + length,
		);
		return { buffer, bytesRead };
	}

	protected async _write(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<WriteResult> {
		const bytesWritten = buffer.copy(
			this.buffer,
			fileOffset,
			bufferOffset,
			bufferOffset + length,
		);
		return { buffer, bytesWritten };
	}

	protected async _flush(): Promise<void> {
		// Nothing to do to flush a BufferDisk
	}
}
