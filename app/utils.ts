import type { DataValueType } from "./database-types";

export interface Varint {
    value: number;
    bytesRead: number;
}

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

export function advanceDataView(view: DataView, advanceBy: number): DataView {
    return new DataView(view.buffer, view.byteOffset + advanceBy, view.byteLength - advanceBy);
}

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