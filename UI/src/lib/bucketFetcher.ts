import { Storage, Bucket } from '@google-cloud/storage';
import { writable, Writable } from 'svelte/store';

//Set up the storage client and read input files
const storage: Storage = new Storage();
const bucket: Bucket = storage.bucket('mrcw');
export const books: Writable<string[]> = writable([]);

export const fetchFiles = async () => {
    const [files] = await bucket.getFiles({prefix: 'input/'});
    books.set(files.map(file => file.name));
}