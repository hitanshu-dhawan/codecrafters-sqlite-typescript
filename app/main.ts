import { closeDatabase, getTablesAndIndexes, openDatabase } from './database';
import { executeSql } from './sql-executor';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

if (command === ".dbinfo") {
    const database = await openDatabase(databaseFilePath);

    console.log(`database page size: ${database.pageSize}`);
    console.log(`number of tables: ${database.tablesCount}`);

    await closeDatabase(database);
} else if (command === ".tables") {
    const database = await openDatabase(databaseFilePath);

    const { tables } = await getTablesAndIndexes(database);
    const tablesNames = tables.map(table => table.name);

    console.log(tablesNames.join(' '));

    await closeDatabase(database);
} else {
    const database = await openDatabase(databaseFilePath);

    try {
        await executeSql(database, command);
    } catch (error: any) {
        console.error(error?.message || 'Unknown error');
    }

    await closeDatabase(database);
}