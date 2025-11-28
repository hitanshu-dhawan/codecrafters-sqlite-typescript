import { getAllTableRows, getRowIdsWithIndexValue, getRowsByRowIds, getTablesAndIndexes, getTableSchema, type Database, type Index, type Table } from "./database";
import type { Row } from "./database-types";
import { CreateIndexNode, NodeType, parseSql, SelectNode } from "./sql-parser";
import { scanSql } from "./sql-scanner";

/**
 * Executes a SQL command on the given database.
 * 
 * @param database The Database object.
 * @param command The SQL command string.
 * @returns A Promise that resolves when execution is complete.
 */
export async function executeSql(database: Database, command: string): Promise<void> {
    const tokens = scanSql(command);
    const sqlCommand = parseSql(tokens);

    if (sqlCommand.type === NodeType.Select) {
        await executeSelect(sqlCommand);
    } else {
        throw new Error(`Unknown command ${command}`);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param selectNode The parsed SELECT statement node.
     */
    async function executeSelect(selectNode: SelectNode) {
        const { tables, indexes } = await getTablesAndIndexes(database);
        const table = tables.find(table => table.name === selectNode.table);
        if (!table) {
            throw new Error(`Table '${selectNode.table}' not found`);
        }

        const { columns, where } = selectNode;
        if (columns.includes('count(*)')) {
            if (columns.length !== 1) {
                throw new Error(`Cannot use count(*) with other columns`);
            }
        }

        const tableSchema = getTableSchema(table);
        const allColumnNames = new Set(tableSchema.map(column => column.name));

        const columnsToRetrieve = new Set(columns.includes('count(*)') ? [] : columns);
        if (selectNode.where) {
            columnsToRetrieve.add(selectNode.where.columnName);
        }

        columnsToRetrieve.values().forEach(column => {
            if (!allColumnNames.has(column)) {
                throw new Error(`Column ${column} not found`);
            }
        });

        const whereFilter = where ? (rows: Row[]): Row[] => {
            return rows.filter(row => row[where.columnName] === where.value);
        } : undefined;

        const index = where ? findIndexForTableAndColumn(indexes, table, where.columnName) : undefined;

        let rows: Row[];
        if (where && index) {
            const rowsIds = await getRowIdsWithIndexValue(database, index, [where.value]);
            rows = await getRowsByRowIds(database, table, tableSchema, columnsToRetrieve, rowsIds);
        } else {
            rows = await getAllTableRows(database, table, tableSchema, columnsToRetrieve, whereFilter);
        }

        if (columns.includes('count(*)')) {
            console.log(rows.length);
            return;
        }

        if (!rows.length) {
            return;
        }

        const mappedRows = rows.map(row => columns.map(column => row[column]));
        console.log(
            mappedRows
                .map(row => row.join('|'))
                .join('\n')
        );
    }

    /**
     * Finds an index for a specific table and column.
     * 
     * @param indexes The list of all indexes.
     * @param table The table to find the index for.
     * @param column The column name.
     * @returns The matching Index object, or undefined if not found.
     */
    function findIndexForTableAndColumn(indexes: Index[], table: Table, column: string): Index | undefined {
        const indexesForTable = indexes.filter(index => index.tableName === table.name);

        const parsedIndexes = indexesForTable.map(index => {
            const tokens = scanSql(index.sql);
            const createIndexCommand = parseSql(tokens) as CreateIndexNode;
            return { ...index, columns: createIndexCommand.columns };
        });

        return parsedIndexes.find(index => index.columns[0] === column);
    }
}