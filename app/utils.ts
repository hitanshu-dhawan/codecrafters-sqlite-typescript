import type { DataValueType } from "./database-types";

/**
 * Represents a variable-length integer read from the database file.
 */
export interface Varint {
    value: number;
    bytesRead: number;
}

/**
 * Reads a variable-length integer (varint) from the given DataView.
 * Varints are used in SQLite to store 64-bit two's complement integers.
 * They use between 1 and 9 bytes. The high-order bit of each byte indicates
 * if there are more bytes to follow.
 * 
 * @param data The DataView to read from.
 * @returns The read value and the number of bytes read.
 */
export function readVarint(data: DataView): Varint {

    let byte = 0;
    const bytes: number[] = [];

    do {
        byte = data.getUint8(bytes.length);
        bytes.push(byte);
    } while (bytes.length < 9 && (byte & 0x80) === 0x80);

    let value = 0;

    bytes.forEach((byte, index) => {
        // TODO: Support 9-th byte
        value |= (byte & 0x7F) << ((bytes.length - index - 1) * 7);
    });

    return {
        value,
        bytesRead: bytes.length,
    };
}

/**
 * Creates a new DataView that is a slice of the original DataView, advanced by the given number of bytes.
 * 
 * @param view The original DataView.
 * @param advanceBy The number of bytes to advance.
 * @returns A new DataView starting from the advanced position.
 */
export function advanceDataView(view: DataView, advanceBy: number): DataView {
    return new DataView(view.buffer, view.byteOffset + advanceBy, view.byteLength - advanceBy);
}

/**
 * Compares two arrays of database values. Used for sorting or finding records in an index.
 * 
 * @param record1 The first record (array of values).
 * @param record2 The second record (array of values).
 * @returns -1 if record1 < record2, 1 if record1 > record2, 0 if equal.
 */
export function recordArraysSortingPredicate(record1: DataValueType[], record2: DataValueType[]): number {
    const commonLength = Math.min(record1.length, record2.length);

    for (let i = 0; i < commonLength; i++) {
        const comparisonResult = compareSqliteValues(record1[i], record2[i]);

        if (comparisonResult !== 0) {
            return comparisonResult;
        }
    }

    return 0;
}

/**
 * Compares two SQLite values.
 * 
 * @param value1 The first value.
 * @param value2 The second value.
 * @returns -1 if value1 < value2, 1 if value1 > value2, 0 if equal.
 */
export function compareSqliteValues(value1: DataValueType, value2: DataValueType): number {
    if (value1 === value2) {
        return 0;
    }

    if (value1 === null) {
        return -1;
    }

    if (value2 === null) {
        return 1;
    }

    if (typeof value1 === typeof value2) {
        return value1 < value2 ? -1 : 1;
    }

    if (typeof value1 === "number") {
        return -1;
    }

    return 1;
}