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
    const buffer: Uint8Array = new Uint8Array(100);
    await databaseFileHandler.read(buffer, 0, buffer.length, 0);

    // The page size is a 2-byte integer at offset 16 in the header
    const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(16);
    console.log(`database page size: ${pageSize}`);

    await databaseFileHandler.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
