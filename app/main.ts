import { open } from 'fs/promises';
import { constants } from 'fs';

const args = process.argv;
const databaseFilePath: string = args[2];
const command: string = args[3];

if (command === ".dbinfo") {
    // Open the database file in read-only mode
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);

    // Read the first 100 bytes of the file (the database header)
    // See: https://www.sqlite.org/fileformat.html#the_database_header
    const databaseHeaderBuffer: Uint8Array = new Uint8Array(100);
    await databaseFileHandler.read(databaseHeaderBuffer, 0, databaseHeaderBuffer.length, 0);

    // The page size is a 2-byte integer at offset 16 in the header
    const pageSize = new DataView(databaseHeaderBuffer.buffer, 0, databaseHeaderBuffer.byteLength).getUint16(16);
    console.log(`database page size: ${pageSize}`);

    // Read b-tree page header (after the 100-byte file header)
    // See: https://www.sqlite.org/fileformat.html#b_tree_pages
    const bTreePageHeaderBuffer: Uint8Array = new Uint8Array(8);
    await databaseFileHandler.read(bTreePageHeaderBuffer, 0, bTreePageHeaderBuffer.length, 100);

    // The number of cells is a 2-byte big-endian integer at offset 3 in the b-tree page header
    const numberOfTables = new DataView(bTreePageHeaderBuffer.buffer, 0, bTreePageHeaderBuffer.byteLength).getUint16(3);
    console.log(`number of tables: ${numberOfTables}`);

    await databaseFileHandler.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
