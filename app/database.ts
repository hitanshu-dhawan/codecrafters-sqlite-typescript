import { constants } from 'fs';
import { type FileHandle, open } from 'fs/promises';
import { advanceDataView, readVarint, recordArraysSortingPredicate } from './utils';
import { serialCodeToDatabaseType, ValueType, type DatabaseType, type DataValueType, type RecordValue, type Row, type TableSchema } from './database-types';
import { parseSql, type CreateTableNode } from './sql-parser';
import { scanSql } from './sql-scanner';

/**
 * Represents the database connection and metadata.
 */
export interface Database {
    pageSize: number;
    tablesCount: number;
    fileHandle: FileHandle;
}

const DATABASE_HEADER_SIZE = 100;
const PAGE_SIZE_OFFSET = 16;
const PAGE_HEADER_SIZE = 8;
const CELLS_COUNT_OFFSET = 3;

/**
 * Opens a database file and reads the header to initialize the Database object.
 * 
 * @param filePath The path to the database file.
 * @returns A Promise that resolves to the Database object.
 */
export async function openDatabase(filePath: string): Promise<Database> {
    const fileHandle = await open(filePath, constants.O_RDONLY);
    const databaseHeader = new Uint8Array(DATABASE_HEADER_SIZE);
    await fileHandle.read(databaseHeader, 0, databaseHeader.length, 0);

    const databaseHeaderView = new DataView(databaseHeader.buffer);
    const pageSize = databaseHeaderView.getUint16(PAGE_SIZE_OFFSET);

    const schemaTablePageHeader = new Uint8Array(PAGE_HEADER_SIZE);
    await fileHandle.read(schemaTablePageHeader, 0, PAGE_HEADER_SIZE, DATABASE_HEADER_SIZE);

    const schemaTableHeaderView = new DataView(schemaTablePageHeader.buffer);
    const tablesCount = schemaTableHeaderView.getUint16(CELLS_COUNT_OFFSET);

    return {
        fileHandle,
        pageSize,
        tablesCount,
    };
}

/**
 * Closes the database file handle.
 * 
 * @param database The Database object.
 */
export async function closeDatabase(database: Database): Promise<void> {
    await database.fileHandle.close();
}

/**
 * Enum representing the type of a B-tree page.
 */
export enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

export const SupportedPageTypes = new Set([
    PageType.InteriorTable,
    PageType.LeafTable,
    PageType.InteriorIndex,
    PageType.LeafIndex
]);

function isInteriorPage(pageType: PageType) {
    return pageType === PageType.InteriorTable || pageType === PageType.InteriorIndex;
}

/**
 * Represents a page in the database file.
 */
export interface DatabasePage {
    pageNumber: number;
    type: PageType;
    cellContentAreaOffset: number;
    firstFreeblockOffset: number;
    cellsNumber: number;
    fragmentedFreeBytes: number;
    rightMostPointer?: number;
    pageView: DataView;
    cellPointerArrayView: DataView;
}

/**
 * Reads a page from the database file.
 * 
 * @param database The Database object.
 * @param pageNumber The page number (1-based).
 * @returns A Promise that resolves to the DatabasePage object.
 */
export async function readPage(database: Database, pageNumber: number): Promise<DatabasePage> {
    const page = new Uint8Array(database.pageSize);
    const pageStart = (pageNumber - 1) * database.pageSize;
    await database.fileHandle.read(page, 0, database.pageSize, pageStart);

    const pageHeaderView = new DataView(page.buffer, pageNumber === 1 ? DATABASE_HEADER_SIZE : 0);

    const pageType = pageHeaderView.getUint8(0);
    if (!SupportedPageTypes.has(pageType)) {
        throw new Error(`Unsupported page type ${pageType} of page no. ${pageNumber}`);
    }

    const firstFreeblockOffset = pageHeaderView.getUint16(1);
    const cellsNumber = pageHeaderView.getUint16(3);
    const cellContentAreaOffset = pageHeaderView.getUint16(5) || 65536;
    const fragmentedFreeBytes = pageHeaderView.getUint8(7);

    const isInterior = isInteriorPage(pageType);

    const pageDetails: DatabasePage = {
        pageNumber,
        type: pageType,
        cellContentAreaOffset,
        firstFreeblockOffset,
        cellsNumber,
        fragmentedFreeBytes,
        pageView: new DataView(page.buffer),
        cellPointerArrayView: advanceDataView(pageHeaderView, isInterior ? 12 : 8),
    };

    if (isInterior) {
        pageDetails.rightMostPointer = pageHeaderView.getUint32(8);
    }

    return pageDetails;
}

const sqliteSchemaTableSchema: TableSchema = [
    {
        name: "type",
    },
    {
        name: "name",
    },
    {
        name: "tbl_name",
    },
    {
        name: "rootpage",
    },
    {
        name: "sql",
    },
];

const sqliteSchemaTable: Table = {
    name: 'sqlite_schema',
    rootPage: 1,
    sql: `CREATE TABLE sqlite_schema(
        type text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
    );`,
}

/**
 * Represents a table in the database.
 */
export interface Table {
    name: string;
    rootPage: number;
    sql: string;
}

/**
 * Represents an index in the database.
 */
export interface Index {
    tableName: string;
    rootPage: number;
    sql: string;
}

interface TablesAndIndexes {
    tables: Table[];
    indexes: Index[];
}

/**
 * Retrieves all tables and indexes from the `sqlite_schema` table.
 * 
 * @param database The Database object.
 * @returns An object containing arrays of tables and indexes.
 */
export async function getTablesAndIndexes(database: Database): Promise<TablesAndIndexes> {
    const columnsToRetrieve = new Set(sqliteSchemaTableSchema.map(column => column.name));

    const schemaTableRows = await getAllTableRows(database, sqliteSchemaTable, sqliteSchemaTableSchema, columnsToRetrieve);

    const tables = schemaTableRows
        .filter(row => row["type"] === "table")
        .map(row => {
            const tableNameValue = row["tbl_name"];
            const rootPageValue = row["rootpage"];
            const sqlValue = row["sql"];

            if (
                typeof tableNameValue !== "string"
                || typeof rootPageValue !== "number"
                || typeof sqlValue !== "string"
            ) {
                throw new Error('sqlite_schema table is invalid');
            }

            const table: Table = {
                name: tableNameValue,
                rootPage: rootPageValue,
                sql: sqlValue,
            };
            return table;
        });

    const indexes = schemaTableRows
        .filter(row => row["type"] === "index")
        .map(row => {
            const tableNameValue = row["tbl_name"];
            const rootPageValue = row["rootpage"];
            const sqlValue = row["sql"];

            if (
                typeof tableNameValue !== "string"
                || typeof rootPageValue !== "number"
                || typeof sqlValue !== "string"
            ) {
                throw new Error('sqlite_schema table is invalid');
            }

            const index: Index = {
                tableName: tableNameValue,
                rootPage: rootPageValue,
                sql: sqlValue,
            };
            return index;
        });

    return { tables, indexes };
}

/**
 * Reads a record from a table leaf cell.
 * 
 * @param cell The table leaf cell containing the record.
 * @param tableSchema The schema of the table.
 * @param columnsToRetrieve The set of column names to retrieve.
 * @returns A Row object containing the retrieved values.
 */
function readRecord(cell: TableLeafCell, tableSchema: TableSchema, columnsToRetrieve: Set<string>): Row {
    let currentRecordView = cell.recordBodyView;
    const row: Row = {};

    cell.serialTypeCodes.forEach((serialCode, columnIndex) => {
        const databaseType = serialCodeToDatabaseType(serialCode);
        if (databaseType.valueType === ValueType.Other) {
            throw new Error(`Unsupported serial code ${serialCode}`);
        }

        const columnDefinition = tableSchema[columnIndex];
        const columnName = columnDefinition.name;

        if (columnsToRetrieve.has(columnName)) {
            row[columnName] = readRecordColumnValue(databaseType, currentRecordView);
        }

        currentRecordView = advanceDataView(currentRecordView, databaseType.size);
    });

    return row;
}

interface TableLeafCell {
    recordSize: number;
    rowId: number;
    serialTypeCodes: number[];
    recordBodyView: DataView;
}

/**
 * Reads a single column value from a record.
 * 
 * @param databaseType The type and size of the value.
 * @param currentRecordView The DataView pointing to the value.
 * @returns The read value.
 */
function readRecordColumnValue(
    databaseType: DatabaseType,
    currentRecordView: DataView<ArrayBufferLike>,
) {
    if (databaseType.valueType === ValueType.String) {
        const bufferValueSlice = currentRecordView.buffer.slice(
            currentRecordView.byteOffset,
            currentRecordView.byteOffset + databaseType.size
        );
        return Buffer.from(bufferValueSlice).toString('utf-8');
    } else if (databaseType.valueType === ValueType.Null) {
        return null;
    } else if (databaseType.valueType === ValueType.Zero) {
        return 0;
    } else if (databaseType.valueType === ValueType.One) {
        return 1;
    } else if (databaseType.valueType === ValueType.Integer) {
        let value = 0;
        switch (databaseType.size) {
            case 1:
                value = currentRecordView.getInt8(0);
                break;
            case 2:
                value = currentRecordView.getInt16(0);
                break;
            case 3:
                value = (currentRecordView.getUint8(0) << 16) | (currentRecordView.getUint8(1) << 8) | currentRecordView.getUint8(2);
                break;
            case 4:
                value = currentRecordView.getInt32(0);
                break;
            default:
                throw new Error(`Unsupported integer size ${databaseType.size}`);
        }
        return value;
    } else {
        throw new Error(`Unsupported size ${databaseType.size}`);
    }
}

/**
 * Reads a record as an array of values. Used for index records.
 * 
 * @param cell The index cell containing the record.
 * @returns An array of values.
 */
function readRecordAsArray(cell: IndexInteriorCell | IndexLeafCell): DataValueType[] {
    const record: DataValueType[] = [];

    let recordView = cell.keyRecordBodyView;
    cell.serialTypeCodes.forEach(serialCode => {
        const databaseType = serialCodeToDatabaseType(serialCode);
        record.push(readRecordColumnValue(databaseType, recordView));
        recordView = advanceDataView(recordView, databaseType.size);
    });

    return record;
}

/**
 * Reads a cell from a table leaf page.
 * 
 * @param cellView The DataView pointing to the cell.
 * @returns The parsed TableLeafCell.
 */
function readTableLeafCell(cellView: DataView): TableLeafCell {
    const recordSizeVarint = readVarint(cellView);

    const rowIdView = advanceDataView(cellView, recordSizeVarint.bytesRead);
    const rowIdVarint = readVarint(rowIdView);

    const recordHeaderView = advanceDataView(rowIdView, rowIdVarint.bytesRead);
    const headerSizeVarint = readVarint(recordHeaderView);

    const serialTypeCodes: number[] = [];
    let headerBytesRead = headerSizeVarint.bytesRead;
    let currentHeaderView = advanceDataView(recordHeaderView, headerSizeVarint.bytesRead);

    while (headerBytesRead < headerSizeVarint.value) {
        const serialTypeVarint = readVarint(currentHeaderView);
        currentHeaderView = advanceDataView(currentHeaderView, serialTypeVarint.bytesRead);
        headerBytesRead += serialTypeVarint.bytesRead;
        serialTypeCodes.push(serialTypeVarint.value);
    }
    const recordBodyView = advanceDataView(recordHeaderView, headerSizeVarint.value);

    return {
        recordSize: recordSizeVarint.value,
        rowId: rowIdVarint.value,
        recordBodyView,
        serialTypeCodes,
    };
}

interface IndexLeafCell {
    recordSize: number;
    serialTypeCodes: number[];
    keyRecordBodyView: DataView;
}

/**
 * Reads a cell from an index leaf page.
 * 
 * @param cellView The DataView pointing to the cell.
 * @returns The parsed IndexLeafCell.
 */
function readIndexLeafCell(cellView: DataView): IndexLeafCell {
    const recordSizeVarint = readVarint(cellView);

    const recordHeaderView = advanceDataView(cellView, recordSizeVarint.bytesRead);
    const headerSizeVarint = readVarint(recordHeaderView);

    const serialTypeCodes: number[] = [];
    let headerBytesRead = headerSizeVarint.bytesRead;
    let currentHeaderView = advanceDataView(recordHeaderView, headerSizeVarint.bytesRead);

    while (headerBytesRead < headerSizeVarint.value) {
        const serialTypeVarint = readVarint(currentHeaderView);
        currentHeaderView = advanceDataView(currentHeaderView, serialTypeVarint.bytesRead);
        headerBytesRead += serialTypeVarint.bytesRead;
        serialTypeCodes.push(serialTypeVarint.value);
    }
    const recordBodyView = advanceDataView(recordHeaderView, headerSizeVarint.value);

    return {
        recordSize: recordSizeVarint.value,
        keyRecordBodyView: recordBodyView,
        serialTypeCodes,
    };
}

interface TableInteriorCell {
    leftChildPointer: number;
    key: number;
}

/**
 * Reads a cell from a table interior page.
 * 
 * @param cellView The DataView pointing to the cell.
 * @returns The parsed TableInteriorCell.
 */
function readTableInteriorCell(cellView: DataView): TableInteriorCell {
    const cell: TableInteriorCell = {
        leftChildPointer: cellView.getUint32(0),
        key: readVarint(advanceDataView(cellView, 4)).value,
    };
    return cell;
}

interface IndexInteriorCell {
    leftChildPointer: number;
    serialTypeCodes: number[];
    keyRecordBodyView: DataView;
}

/**
 * Reads a cell from an index interior page.
 * 
 * @param cellView The DataView pointing to the cell.
 * @returns The parsed IndexInteriorCell.
 */
function readIndexInteriorCell(cellView: DataView): IndexInteriorCell {
    const leftChildPointer = cellView.getUint32(0);

    const payloadSizeView = advanceDataView(cellView, 4);
    const payloadSizeVarint = readVarint(payloadSizeView);

    const recordHeaderView = advanceDataView(payloadSizeView, payloadSizeVarint.bytesRead);
    const headerSizeVarint = readVarint(recordHeaderView);

    const serialTypeCodes: number[] = [];
    let headerBytesRead = headerSizeVarint.bytesRead;
    let currentHeaderView = advanceDataView(recordHeaderView, headerSizeVarint.bytesRead);

    while (headerBytesRead < headerSizeVarint.value) {
        const serialTypeVarint = readVarint(currentHeaderView);
        currentHeaderView = advanceDataView(currentHeaderView, serialTypeVarint.bytesRead);
        headerBytesRead += serialTypeVarint.bytesRead;
        serialTypeCodes.push(serialTypeVarint.value);
    }
    const keyRecordBodyView = advanceDataView(recordHeaderView, headerSizeVarint.value);

    return {
        leftChildPointer,
        keyRecordBodyView,
        serialTypeCodes,
    };
}

/**
 * Parses the SQL CREATE TABLE statement to get the table schema.
 * 
 * @param table The table object.
 * @returns The table schema.
 */
export function getTableSchema(table: Table): TableSchema {
    const tokens = scanSql(table.sql);
    const createTableNode = parseSql(tokens) as CreateTableNode;
    return createTableNode.tableSchema;
}

/**
 * Retrieves all rows from a table, optionally filtering them.
 * This function performs a full table scan.
 * 
 * @param database The Database object.
 * @param table The table to retrieve rows from.
 * @param tableSchema The schema of the table.
 * @param columnsToRetrieve The set of column names to retrieve.
 * @param filter An optional filter function.
 * @returns A Promise that resolves to an array of rows.
 */
export async function getAllTableRows(
    database: Database,
    table: Table,
    tableSchema: TableSchema,
    columnsToRetrieve: Set<string>,
    filter?: (rows: Row[]) => Row[],
): Promise<Row[]> {
    const rootPage = await readPage(database, table.rootPage);

    let rows: Row[] = [];
    const pages = [rootPage];

    while (pages.length) {
        const page = pages.shift()!;

        if (page.type === PageType.LeafTable) {
            let pageRows = getAllTableLeafPageRows(page, tableSchema, columnsToRetrieve);
            if (filter) {
                pageRows = filter(pageRows);
            }
            rows = rows.concat(pageRows);
        } else if (page.type === PageType.InteriorTable) {
            const cells = getAllTableInteriorPageCells(page);
            const readChildPagesPromises = cells.map(cell => readPage(database, cell.leftChildPointer))
                .concat([readPage(database, page.rightMostPointer!)]);
            const childPages = await Promise.all(readChildPagesPromises);
            pages.push(...childPages);
        }
    }

    return rows;
}

/**
 * Retrieves row IDs from an index that match a given value.
 * This function traverses the B-tree index.
 * 
 * @param database The Database object.
 * @param index The index to search.
 * @param indexValue The value to search for.
 * @returns A Promise that resolves to an array of row IDs.
 */
export async function getRowIdsWithIndexValue(
    database: Database,
    index: Index,
    indexValue: DataValueType[],
): Promise<number[]> {
    const rootPage = await readPage(database, index.rootPage);

    let rowIds: number[] = [];
    const pages = [rootPage];

    while (pages.length) {
        const page = pages.shift()!;

        if (page.type === PageType.LeafIndex) {
            const cells = getAllIndexLeafPageCells(page);

            for (const cell of cells) {
                const keyRecord = readRecordAsArray(cell);
                const comparisonResult = recordArraysSortingPredicate(keyRecord, indexValue);

                if (comparisonResult === 0) {
                    const rowId = keyRecord.pop()! as number;
                    rowIds.push(rowId);
                }
                if (comparisonResult > 0) {
                    break;
                }
            }
        } else if (page.type === PageType.InteriorIndex) {
            const cells = getAllIndexInteriorPageCells(page);

            const cellsToSearchFurther = [];
            let foundGreaterKey = false;

            for (const cell of cells) {
                const keyRecord = readRecordAsArray(cell);
                const comparisonResult = recordArraysSortingPredicate(keyRecord, indexValue);

                if (comparisonResult >= 0) {
                    cellsToSearchFurther.push(cell.leftChildPointer);

                    if (comparisonResult === 0) {
                        const rowId = keyRecord.pop()! as number;
                        rowIds.push(rowId);
                    }
                }

                if (comparisonResult > 0) {
                    foundGreaterKey = true;
                    break;
                }
            }

            if (!foundGreaterKey) {
                cellsToSearchFurther.push(page.rightMostPointer!);
            }

            const readChildPagesPromises = cellsToSearchFurther.map(pointer => readPage(database, pointer));
            const childPages = await Promise.all(readChildPagesPromises);
            pages.push(...childPages);
        }
    }

    return rowIds;
}

function createPagesCache(database: Database) {
    const pagePromiseByPageNumer: Record<number, Promise<DatabasePage>> = {};

    return {
        getPage(pageNumber: number) {
            if (!pagePromiseByPageNumer[pageNumber]) {
                pagePromiseByPageNumer[pageNumber] = readPage(database, pageNumber);
            }
            return pagePromiseByPageNumer[pageNumber];
        }
    }
}

/**
 * Retrieves rows from a table by their row IDs.
 * This function uses the B-tree structure to efficiently find rows.
 * 
 * @param database The Database object.
 * @param table The table to retrieve rows from.
 * @param tableSchema The schema of the table.
 * @param columnsToRetrieve The set of column names to retrieve.
 * @param rowIds The list of row IDs to retrieve.
 * @returns A Promise that resolves to an array of rows.
 */
export async function getRowsByRowIds(
    database: Database,
    table: Table,
    tableSchema: TableSchema,
    columnsToRetrieve: Set<string>,
    rowIds: number[],
): Promise<Row[]> {
    const pagesCache = createPagesCache(database);
    const primaryKeyColumn = tableSchema.find(column => column.isPrimaryKey);

    async function getRowWithId(rowId: number): Promise<Row> {
        const rootPage = await pagesCache.getPage(table.rootPage);

        const pages = [rootPage];

        while (pages.length) {
            const page = pages.shift()!;

            if (page.type === PageType.LeafTable) {
                const rowCell = getCellWithRowIdFromTableLeafPage(page, rowId);
                if (!rowCell) {
                    throw new Error(`Row with id ${rowId} not found in page ${page.pageNumber}`);
                }
                const record = readRecord(rowCell, tableSchema, columnsToRetrieve);
                if (primaryKeyColumn) {
                    record[primaryKeyColumn.name] = rowCell.rowId;
                }
                return record;
            } else if (page.type === PageType.InteriorTable) {
                const pointerToPageWithTheRow = getChildTablePageContainingRowId(page, rowId);
                const childPage = await pagesCache.getPage(pointerToPageWithTheRow);
                pages.push(childPage);
            }
        }

        throw new Error(`Row with id ${rowId} not found`);
    }

    const searchPromises = rowIds.map(rowId => getRowWithId(rowId));
    return Promise.all(searchPromises);
}

/**
 * Retrieves all rows from a table leaf page.
 * 
 * @param page The table leaf page.
 * @param tableSchema The schema of the table.
 * @param columnsToRetrieve The set of column names to retrieve.
 * @returns An array of rows.
 */
export function getAllTableLeafPageRows(
    page: DatabasePage,
    tableSchema: TableSchema,
    columnsToRetrieve: Set<string>
): Row[] {
    const cellPointerArrayView = page.cellPointerArrayView;
    const primaryKeyColumn = tableSchema.find(column => column.isPrimaryKey);

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const schemaTableRows = offsets.map(offset => {
        const cellView = advanceDataView(page.pageView, offset);
        const cell = readTableLeafCell(cellView);
        const row = readRecord(cell, tableSchema, columnsToRetrieve);

        if (primaryKeyColumn) {
            row[primaryKeyColumn.name] = cell.rowId;
        }

        return row;
    });

    return schemaTableRows;
}

/**
 * Finds the child page pointer in an interior table page that contains the given row ID.
 * This uses binary search on the cell keys.
 * 
 * @param page The interior table page.
 * @param rowId The row ID to search for.
 * @returns The page number of the child page.
 */
export function getChildTablePageContainingRowId(
    page: DatabasePage,
    rowId: number,
): number {
    const cellPointerArrayView = page.cellPointerArrayView;

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const cellsCacheByOffset: Record<number, TableInteriorCell> = {};

    let left = 0;
    let right = offsets.length;

    while (left < right) {
        const mid = Math.floor((left + right) / 2);

        const cellOffset = offsets[mid];
        let cell: TableInteriorCell;
        const cachedCell = cellsCacheByOffset[cellOffset];
        if (cachedCell) {
            cell = cachedCell;
        } else {
            const cellView = advanceDataView(page.pageView, cellOffset);
            cell = readTableInteriorCell(cellView);
            cellsCacheByOffset[cellOffset] = cell;
        }

        if (cell.key < rowId) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (left === offsets.length) {
        return page.rightMostPointer!;
    }

    const cellOffset = offsets[left];
    let foundCell: TableInteriorCell;
    const cachedCell = cellsCacheByOffset[cellOffset];
    if (cachedCell) {
        foundCell = cachedCell;
    } else {
        const cellView = advanceDataView(page.pageView, cellOffset);
        foundCell = readTableInteriorCell(cellView);
        cellsCacheByOffset[cellOffset] = foundCell;
    }

    return foundCell.leftChildPointer;
}

/**
 * Finds the cell in a table leaf page that corresponds to the given row ID.
 * This uses binary search on the cell row IDs.
 * 
 * @param page The table leaf page.
 * @param rowId The row ID to search for.
 * @returns The TableLeafCell if found, or null.
 */
export function getCellWithRowIdFromTableLeafPage(
    page: DatabasePage,
    rowId: number,
): TableLeafCell | null {
    const cellPointerArrayView = page.cellPointerArrayView;

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const cellsCacheByOffset: Record<number, TableLeafCell> = {};

    let left = 0;
    let right = offsets.length;

    while (left <= right) {
        const mid = Math.floor((left + right) / 2);

        const cellOffset = offsets[mid];
        let cell: TableLeafCell;
        const cachedCell = cellsCacheByOffset[cellOffset];
        if (cachedCell) {
            cell = cachedCell;
        } else {
            const cellView = advanceDataView(page.pageView, cellOffset);
            cell = readTableLeafCell(cellView);
            cellsCacheByOffset[cellOffset] = cell;
        }
        if (cell.rowId === rowId) {
            return cell;
        }
        if (cell.rowId < rowId) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return null;
}

/**
 * Retrieves all cells from a table interior page.
 * 
 * @param page The table interior page.
 * @returns An array of TableInteriorCell.
 */
export function getAllTableInteriorPageCells(
    page: DatabasePage,
): TableInteriorCell[] {
    const cellPointerArrayView = page.cellPointerArrayView;

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const cells = offsets.map(offset => {
        const cellView = advanceDataView(page.pageView, offset);
        return readTableInteriorCell(cellView);
    });
    return cells;
}

/**
 * Retrieves all cells from an index interior page.
 * 
 * @param page The index interior page.
 * @returns An array of IndexInteriorCell.
 */
export function getAllIndexInteriorPageCells(
    page: DatabasePage,
): IndexInteriorCell[] {
    const cellPointerArrayView = page.cellPointerArrayView;

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const cells = offsets.map(offset => {
        const cellView = advanceDataView(page.pageView, offset);
        return readIndexInteriorCell(cellView);
    });
    return cells;
}

/**
 * Retrieves all cells from an index leaf page.
 * 
 * @param page The index leaf page.
 * @returns An array of IndexLeafCell.
 */
export function getAllIndexLeafPageCells(
    page: DatabasePage,
): IndexLeafCell[] {
    const cellPointerArrayView = page.cellPointerArrayView;

    const offsets = Array(page.cellsNumber).keys()
        .map(itemIndex => cellPointerArrayView.getUint16(itemIndex * 2))
        .toArray();

    const cells = offsets.map(offset => {
        const cellView = advanceDataView(page.pageView, offset);
        return readIndexLeafCell(cellView);
    });
    return cells;
}