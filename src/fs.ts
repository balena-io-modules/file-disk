import * as Promise from 'bluebird';
import * as fs from 'fs';

const _read = Promise.promisify(fs.read, { context: fs, multiArgs: true });
const _write = Promise.promisify(fs.write, { context: fs, multiArgs: true }) as (fd: number, buffer: Buffer, offset?: number, length?: number, position?: number) => any;

export const open = Promise.promisify(fs.open, { context: fs }) as (path: string, flags: string | number, mode?: number) => Promise<number>;
export const close = Promise.promisify(fs.close, { context: fs }) as (fd: number) => Promise<void>;
export const fstat = Promise.promisify(fs.fstat, { context: fs }) as (fd: number) => Promise<fs.Stats>;
export const fdatasync = Promise.promisify(fs.fdatasync, { context: fs }) as (fd: number) => Promise<void>;

export interface ReadResult {
	bytesRead: number;
	buffer: Buffer;
}

export interface WriteResult {
	bytesWritten: number;
	buffer: Buffer;
}

export const read = (fd: number, buffer: Buffer, offset: number, length: number, position: number): Promise<ReadResult> => {
	return _read(fd, buffer, offset, length, position)
	.spread((bytesRead: number, buffer: Buffer) => {
		return { bytesRead, buffer };
	});
};

export const write = (fd: number, buffer: Buffer, offset: number, length: number, position: number): Promise<WriteResult> => {
	return _write(fd, buffer, offset, length, position)
	.spread((bytesWritten: number, buffer: Buffer) => {
		return { bytesWritten, buffer };
	});
};
