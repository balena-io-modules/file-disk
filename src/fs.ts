import * as Bluebird from 'bluebird';
import * as fs from 'fs';

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
): Promise<ReadResult> => {
	return new Promise((resolve, reject) => {
		fs.read(
			fd,
			buf,
			offset,
			length,
			position,
			(error: Error | null, bytesRead: number, buffer: Buffer) => {
				if (error !== null) {
					reject(error);
				} else {
					resolve({ bytesRead, buffer });
				}
			},
		);
	});
};

export const write = (
	fd: number,
	buf: Buffer,
	offset: number,
	length: number,
	position: number,
): Promise<WriteResult> => {
	return new Promise((resolve, reject) => {
		fs.write(
			fd,
			buf,
			offset,
			length,
			position,
			(error: Error | null, bytesWritten: number, buffer: Buffer) => {
				if (error !== null) {
					reject(error);
				} else {
					resolve({ bytesWritten, buffer });
				}
			},
		);
	});
};
