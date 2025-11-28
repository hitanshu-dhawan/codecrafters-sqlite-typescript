export enum ValueType {
    Null = 'null',
    Integer = 'integer',
    Zero = 'zero',
    One = 'one',
    String = 'string',
    Other = 'other',
}

export type DatabaseType = {
    valueType: ValueType;
    size: number;
}

export type RecordValue = {
    size: number;
} & (
        { valueType: ValueType.Null, value: null }
        | { valueType: ValueType.Integer, value: number }
        | { valueType: ValueType.Zero, value: 0 }
        | { valueType: ValueType.One, value: 1 }
        | { valueType: ValueType.String, value: string }
        | { valueType: ValueType.Other, value: null }
    )

export type ColumnDefinition = {
    name: string;
    isPrimaryKey?: boolean;
}

export type TableSchema = ColumnDefinition[];

export type DataValueType = number | string | null;
export type Row = Record<string, DataValueType>;

export function serialCodeToDatabaseType(code: number): DatabaseType {
    if (code === 0) {
        return {
            valueType: ValueType.Null,
            size: 0,
        };
    } else if (code >= 1 && code <= 4) {
        return {
            valueType: ValueType.Integer,
            size: code,
        };
    } else if (code === 5) {
        return {
            valueType: ValueType.Integer,
            size: 6,
        };
    } else if (code === 6) {
        return {
            valueType: ValueType.Integer,
            size: 8,
        };
    } else if (code === 8) {
        return {
            valueType: ValueType.Zero,
            size: 0,
        };
    } else if (code === 9) {
        return {
            valueType: ValueType.One,
            size: 0,
        };
    } else if (code >= 13 && (code % 2) === 1) {
        return {
            valueType: ValueType.String,
            size: (code - 13) / 2,
        };
    } else {
        return {
            valueType: ValueType.Other,
            size: 0,
        };
    }
}