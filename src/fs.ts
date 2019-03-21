import * as Bluebird from 'bluebird';
import * as fs from 'fs';

const _read = Bluebird.promisify(fs.read, { context: fs, multiArgs: true });

const _write = Bluebird.promisify(fs.write, {
	context: fs,
	multiArgs: true,
}) as (
	fd: number,
	buffer: Buffer,
	offset?: number,
	length?: number,
	position?: number,
) => any;

export const open = Bluebird.promisify(fs.open, { context: fs }) as (
	path: string,
	flags: string | number,
	mode?: number,
) => Bluebird<number>;

export const close = Bluebird.promisify(fs.close, { context: fs }) as (
	fd: number,
) => Bluebird<void>;

export const fstat = Bluebird.promisify(fs.fstat, { context: fs });

export const fdatasync = Bluebird.promisify(fs.fdatasync, { context: fs }) as (
	fd: number,
) => Bluebird<void>;

export interface ReadResult {
	bytesRead: number;
	buffer: Buffer;
}

export interface WriteResult {
	bytesWritten: number;
	buffer: Buffer;
}

export const read = (
	fd: number,
	buf: Buffer,
	offset: number,
	length: number,
	position: number,
): Bluebird<ReadResult> => {
	return _read(fd, buf, offset, length, position).spread(
		(bytesRead: number, buffer: Buffer) => {
			return { bytesRead, buffer };
		},
	);
};

export const write = (
	fd: number,
	buf: Buffer,
	offset: number,
	length: number,
	position: number,
): Bluebird<WriteResult> => {
	return _write(fd, buf, offset, length, position).spread(
		(bytesWritten: number, buffer: Buffer) => {
			return { bytesWritten, buffer };
		},
	);
};
